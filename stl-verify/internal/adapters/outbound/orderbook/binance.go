package orderbook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/httpclient"
	"github.com/archon-research/stl/stl-verify/internal/pkg/wsclient"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

const (
	exchangeBinance = "binance"

	// binanceWSBase is the spot combined-stream WebSocket endpoint. Even a single
	// symbol uses the combined endpoint so every frame is wrapped in {stream,data}.
	binanceWSBase = "wss://stream.binance.com:9443"
	// binanceRESTBase is the spot REST endpoint serving depth snapshots.
	binanceRESTBase = "https://api.binance.com"

	// binanceDepthStream is the diff-depth stream suffix. The 100ms variant gives
	// lower latency than the default 1000ms stream for high-frequency books.
	binanceDepthStream = "@depth@100ms"
	// binanceSnapshotLimit is the REST depth requested (max supported is 5000).
	binanceSnapshotLimit = 5000

	// binanceMaxStreamsPerConn caps streams per connection well under Binance's
	// 1024 limit, so typical symbol sets ride a single connection.
	binanceMaxStreamsPerConn = 200

	// binanceMaxBuffer bounds buffered diffs awaiting a snapshot; exceeding it
	// means the snapshot is stuck and the connection should be torn down.
	binanceMaxBuffer = 100_000
)

// Compile-time check that BinanceProvider implements outbound.OrderbookProvider.
var _ outbound.OrderbookProvider = (*BinanceProvider)(nil)

// BinanceProvider streams L2 books from Binance spot. It seeds each book from a
// REST depth snapshot, buffers the WebSocket diff stream during the fetch, and
// reconciles the two via the snapshot's lastUpdateId before emitting.
type BinanceProvider struct {
	cfg        Config
	logger     *slog.Logger
	http       *httpclient.Client
	restBase   string // overridable in tests
	wsBase     string // overridable in tests
	maxStreams int
}

// NewBinanceProvider creates a Binance orderbook provider.
func NewBinanceProvider(cfg Config) *BinanceProvider {
	cfg = cfg.withDefaults()
	logger := cfg.Logger.With("component", "binance-orderbook")

	httpCfg := httpclient.DefaultConfig()
	httpCfg.Timeout = 30 * time.Second
	return &BinanceProvider{
		cfg:        cfg,
		logger:     logger,
		http:       httpclient.NewClient(httpCfg, logger, binanceParseError),
		restBase:   binanceRESTBase,
		wsBase:     binanceWSBase,
		maxStreams: binanceMaxStreamsPerConn,
	}
}

// Name returns the exchange identifier.
func (p *BinanceProvider) Name() string { return exchangeBinance }

// Watch subscribes to the given Binance symbols (e.g. "BTCUSDT") and streams
// aggregated books. Symbols are split across the fewest connections allowed.
func (p *BinanceProvider) Watch(ctx context.Context, symbols []string) (<-chan entity.OrderbookUpdate, error) {
	if err := validateSymbols(symbols); err != nil {
		return nil, err
	}
	groups := chunkSymbols(symbols, p.maxStreams)
	out := runConnections(ctx, groups, p.cfg.OutputBuffer, func(ctx context.Context, group []string, out chan<- entity.OrderbookUpdate) {
		reconnectLoop(ctx, p.cfg, p.logger, func(ctx context.Context, ready func()) error {
			return p.connectAndSync(ctx, group, out, ready)
		})
	})
	return out, nil
}

// binanceState is the per-symbol synchronisation state for one connection.
type binanceState struct {
	book        *entity.Orderbook
	synced      bool  // snapshot applied and stream caught up
	expectFirst bool  // next diff is the first after a snapshot
	lastID      int64 // last applied final update id (`u`)
	fetching    bool  // a snapshot fetch is in flight
	buffer      []binanceDepthDiff
}

type binanceDepthDiff struct {
	EventType string     `json:"e"`
	EventTime int64      `json:"E"`
	Symbol    string     `json:"s"`
	FirstID   int64      `json:"U"`
	FinalID   int64      `json:"u"`
	Bids      [][]string `json:"b"`
	Asks      [][]string `json:"a"`
}

type binanceCombined struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

type binanceSnapshot struct {
	LastUpdateID int64      `json:"lastUpdateId"`
	Bids         [][]string `json:"bids"`
	Asks         [][]string `json:"asks"`
}

type binanceSnapshotResult struct {
	symbol string
	snap   binanceSnapshot
	err    error
}

// connectAndSync owns one WebSocket connection for the symbol group: it dials,
// buffers diffs, fetches snapshots, reconciles, and then streams live deltas
// until the connection drops or ctx is cancelled.
func (p *BinanceProvider) connectAndSync(ctx context.Context, group []string, out chan<- entity.OrderbookUpdate, ready func()) error {
	ws, err := wsclient.Dial(ctx, p.streamURL(group), p.cfg.ws())
	if err != nil {
		return err
	}
	defer ws.Close()
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	ready() // connection established: reset reconnect backoff

	states := make(map[string]*binanceState, len(group))
	snapCh := make(chan binanceSnapshotResult, len(group)+1)
	for _, sym := range group {
		up := strings.ToUpper(sym)
		st := &binanceState{book: entity.NewOrderbook(exchangeBinance, up)}
		states[up] = st
		p.requestSnapshot(connCtx, st, sym, snapCh)
	}

	type wsFrame struct {
		raw []byte
		err error
	}
	frameCh := make(chan wsFrame, 1)
	go func() {
		for {
			raw, err := ws.Next(connCtx)
			select {
			case frameCh <- wsFrame{raw: raw, err: err}:
			case <-connCtx.Done():
			}
			if err != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case res := <-snapCh:
			if err := p.applySnapshot(connCtx, states, res, out, snapCh); err != nil {
				return err
			}
		case frame := <-frameCh:
			if frame.err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("websocket: %w", frame.err)
			}
			if err := p.handleDiff(connCtx, states, frame.raw, out, snapCh); err != nil {
				return err
			}
		}
	}
}

// requestSnapshot marks the symbol as fetching and launches an async REST fetch.
// All state mutation stays on the main loop; the goroutine only performs IO and
// delivers its result over snapCh.
func (p *BinanceProvider) requestSnapshot(ctx context.Context, st *binanceState, symbol string, snapCh chan<- binanceSnapshotResult) {
	st.fetching = true
	go func() {
		snap, err := p.fetchSnapshot(ctx, symbol)
		select {
		case snapCh <- binanceSnapshotResult{symbol: symbol, snap: snap, err: err}:
		case <-ctx.Done():
		}
	}()
}

// applySnapshot seeds a book from a snapshot and replays the buffered diffs that
// follow it. A snapshot fetch error is fatal to the connection (reconnect with
// backoff handles persistent failures and avoids a tight refetch loop).
func (p *BinanceProvider) applySnapshot(ctx context.Context, states map[string]*binanceState, res binanceSnapshotResult, out chan<- entity.OrderbookUpdate, snapCh chan<- binanceSnapshotResult) error {
	st := states[strings.ToUpper(res.symbol)]
	if st == nil {
		return nil
	}
	st.fetching = false
	if res.err != nil {
		return fmt.Errorf("fetch snapshot for %s: %w", res.symbol, res.err)
	}

	bids, err := parseLevels(res.snap.Bids)
	if err != nil {
		return fmt.Errorf("snapshot bids for %s: %w", res.symbol, err)
	}
	asks, err := parseLevels(res.snap.Asks)
	if err != nil {
		return fmt.Errorf("snapshot asks for %s: %w", res.symbol, err)
	}

	st.book.Reset()
	st.book.ReplaceSide(entity.Bid, bids)
	st.book.ReplaceSide(entity.Ask, asks)
	st.lastID = res.snap.LastUpdateID
	st.book.LastUpdateID = res.snap.LastUpdateID
	st.synced = true
	st.expectFirst = true

	buffered := st.buffer
	st.buffer = nil
	for _, diff := range buffered {
		if err := p.applyDiff(st, diff); err != nil {
			if errors.Is(err, errSequenceGap) {
				p.beginResync(ctx, st, res.symbol, snapCh, diff)
				return nil
			}
			return fmt.Errorf("replay diff for %s: %w", res.symbol, err)
		}
	}

	if st.book.Crossed() {
		// A freshly seeded book that is already crossed is corrupt at the source;
		// drop the connection so it reconnects and re-snapshots from scratch.
		return fmt.Errorf("snapshot for %s produced a crossed book", res.symbol)
	}

	emit(ctx, out, st.book, true, time.Now())
	return nil
}

// handleDiff routes one WebSocket frame: it buffers diffs for un-synced symbols
// and applies them once synced, emitting the updated book.
func (p *BinanceProvider) handleDiff(ctx context.Context, states map[string]*binanceState, raw []byte, out chan<- entity.OrderbookUpdate, snapCh chan<- binanceSnapshotResult) error {
	diff, ok, err := decodeBinanceDiff(raw)
	if err != nil {
		return err
	}
	if !ok {
		return nil // non-data frame (e.g. subscription ack)
	}

	st := states[strings.ToUpper(diff.Symbol)]
	if st == nil {
		return nil // symbol we did not subscribe to
	}

	if !st.synced {
		st.buffer = append(st.buffer, diff)
		if len(st.buffer) > binanceMaxBuffer {
			return fmt.Errorf("diff buffer overflow for %s awaiting snapshot", diff.Symbol)
		}
		return nil
	}

	if err := p.applyDiff(st, diff); err != nil {
		if errors.Is(err, errSequenceGap) {
			p.logger.Warn("sequence gap, resyncing", "symbol", diff.Symbol, "lastID", st.lastID, "firstID", diff.FirstID)
			p.beginResync(ctx, st, diff.Symbol, snapCh, diff)
			return nil
		}
		return fmt.Errorf("apply diff for %s: %w", diff.Symbol, err)
	}

	if st.book.Crossed() {
		p.logger.Warn("crossed book after diff, resyncing", "symbol", diff.Symbol, "lastID", st.lastID)
		p.beginResync(ctx, st, diff.Symbol, snapCh, diff)
		return nil
	}

	emit(ctx, out, st.book, false, time.UnixMilli(diff.EventTime))
	return nil
}

// applyDiff applies one diff per the Binance reconciliation rules. It must only
// be called once the symbol is synced (st.synced); un-synced diffs are buffered
// by handleDiff instead. The rules:
//   - drop diffs fully covered by the snapshot (u <= lastID)
//   - the first diff after a snapshot must satisfy U <= lastID+1 <= u
//   - every later diff must be contiguous: U == lastID+1
//
// A broken sequence returns errSequenceGap to trigger a re-sync.
func (p *BinanceProvider) applyDiff(st *binanceState, diff binanceDepthDiff) error {
	if diff.FinalID <= st.lastID {
		return nil // stale: already covered by the snapshot or a prior diff
	}
	if st.expectFirst {
		if diff.FirstID > st.lastID+1 {
			return errSequenceGap // missed events between snapshot and stream
		}
		st.expectFirst = false
	} else if diff.FirstID != st.lastID+1 {
		return errSequenceGap
	}

	if err := applyDeltaLevels(st.book, entity.Bid, diff.Bids); err != nil {
		return err
	}
	if err := applyDeltaLevels(st.book, entity.Ask, diff.Asks); err != nil {
		return err
	}
	st.lastID = diff.FinalID
	st.book.LastUpdateID = diff.FinalID
	return nil
}

// beginResync resets a symbol to the buffering state and re-requests a snapshot.
// The diff that triggered the gap is retained so reconciliation stays continuous.
func (p *BinanceProvider) beginResync(ctx context.Context, st *binanceState, symbol string, snapCh chan<- binanceSnapshotResult, trigger binanceDepthDiff) {
	st.synced = false
	st.expectFirst = false
	st.lastID = 0
	st.buffer = st.buffer[:0]
	st.buffer = append(st.buffer, trigger)
	if !st.fetching {
		p.requestSnapshot(ctx, st, symbol, snapCh)
	}
}

// decodeBinanceDiff unwraps a combined-stream frame and decodes the depth diff.
// ok is false for frames that are not depth updates (acks, other event types).
func decodeBinanceDiff(raw []byte) (binanceDepthDiff, bool, error) {
	var env binanceCombined
	if err := json.Unmarshal(raw, &env); err != nil {
		return binanceDepthDiff{}, false, fmt.Errorf("decode combined frame: %w", err)
	}
	if len(env.Data) == 0 {
		return binanceDepthDiff{}, false, nil
	}
	var diff binanceDepthDiff
	if err := json.Unmarshal(env.Data, &diff); err != nil {
		return binanceDepthDiff{}, false, fmt.Errorf("decode depth diff: %w", err)
	}
	if diff.EventType != "depthUpdate" {
		return binanceDepthDiff{}, false, nil
	}
	return diff, true, nil
}

func (p *BinanceProvider) streamURL(group []string) string {
	streams := make([]string, len(group))
	for i, sym := range group {
		streams[i] = strings.ToLower(sym) + binanceDepthStream
	}
	return p.wsBase + "/stream?streams=" + strings.Join(streams, "/")
}

func (p *BinanceProvider) fetchSnapshot(ctx context.Context, symbol string) (binanceSnapshot, error) {
	reqURL := fmt.Sprintf("%s/api/v3/depth?symbol=%s&limit=%d",
		p.restBase, url.QueryEscape(strings.ToUpper(symbol)), binanceSnapshotLimit)
	var snap binanceSnapshot
	if err := p.http.DoRequest(ctx, httpclient.RequestConfig{URL: reqURL}, &snap); err != nil {
		return binanceSnapshot{}, err
	}
	if snap.LastUpdateID == 0 {
		return binanceSnapshot{}, fmt.Errorf("snapshot for %s missing lastUpdateId", symbol)
	}
	return snap, nil
}

// binanceParseError extracts Binance's {"code","msg"} API error body.
func binanceParseError(statusCode int, body []byte) error {
	var apiErr struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	}
	if err := json.Unmarshal(body, &apiErr); err == nil && apiErr.Code != 0 {
		return fmt.Errorf("binance API error %d: %s", apiErr.Code, apiErr.Msg)
	}
	if statusCode >= 400 {
		return fmt.Errorf("binance HTTP %d: %s", statusCode, strconv.Quote(string(body)))
	}
	return nil
}

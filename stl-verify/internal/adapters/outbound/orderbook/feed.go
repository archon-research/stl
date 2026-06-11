// Package orderbook implements outbound.OrderbookProvider for centralised
// exchanges.
//
// Each adapter maintains a full local book per symbol: it seeds an L2 book from a
// snapshot, then keeps it current by applying the exchange's WebSocket delta
// stream, emitting a fully aggregated book on every change. Symbols are
// multiplexed over the minimum number of WebSocket connections the exchange
// supports, and connections reconnect automatically with exponential backoff.
//
// The package deliberately contains no persistence: it produces a stream of
// entity.OrderbookUpdate values and leaves storage to the service layer.
package orderbook

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/wsclient"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Shared feed for exchanges that deliver the initial snapshot in-band over the
// WebSocket: connection lifecycle, subscribe, keepalive, emission and
// reconnection are identical, and only the wire format differs, supplied per
// exchange via exchangeFeed. Venues whose snapshot arrives out-of-band (e.g. a
// REST depth endpoint) do not fit exchangeFeed; they implement Watch directly
// on the shared pieces (runConnections, reconnectLoop, emitter).

// bookChange marks a book that changed while handling one inbound frame.
type bookChange struct {
	book       *entity.Orderbook
	isSnapshot bool
	t          time.Time
}

// bookSet holds the per-symbol books for a single connection.
type bookSet struct {
	exchange string
	books    map[string]*entity.Orderbook
}

func newBookSet(exchange string) *bookSet {
	return &bookSet{exchange: exchange, books: make(map[string]*entity.Orderbook)}
}

// getOrCreate returns the book for symbol, creating an empty one on first use.
func (bs *bookSet) getOrCreate(symbol string) *entity.Orderbook {
	b, ok := bs.books[symbol]
	if !ok {
		b = entity.NewOrderbook(bs.exchange, symbol)
		bs.books[symbol] = b
	}
	return b
}

// exchangeFeed describes a snapshot-over-WebSocket exchange: static
// metadata plus a factory for per-connection frame handlers.
type exchangeFeed interface {
	// name is the exchange identifier (e.g. "okx").
	name() string
	// normalizeSymbol canonicalises and validates one symbol, erroring on a bad format.
	normalizeSymbol(symbol string) (string, error)
	// endpoint returns the WebSocket URL.
	endpoint() string
	// subscribeMessages returns the frames to send after connecting.
	subscribeMessages(group []string) []any
	// newHandler creates a fresh frame handler for one connection, scoped to the
	// symbols in group (so it can reject book data for anything unsubscribed).
	// Because each connection (and reconnection) gets its own handler, handlers
	// may hold mutable per-connection sync state without locking.
	newHandler(group []string, logger *slog.Logger) frameHandler
}

// frameHandler maintains one connection's books and synchronisation state. Its
// handle method is only ever called from a single goroutine.
type frameHandler interface {
	// handle parses one inbound frame, applies it to the handler's books, and
	// returns the books that should be emitted. A single frame may legitimately
	// fan out to multiple bookChanges (e.g. one frame carrying events for several
	// symbols), so the result is a slice. Each returned book's Symbol must
	// upper-case to a member of the subscription group: the feed matches it
	// against the group to decide when the connection is fully synced. Returning
	// errSequenceGap (or any error) causes the feed to drop the connection and
	// reconnect.
	handle(raw []byte) ([]bookChange, error)
}

// errSequenceGap signals a break in a venue's update stream that requires
// re-synchronising from a fresh snapshot.
var errSequenceGap = errors.New("orderbook update sequence gap")

// errUnexpectedSymbol signals book data for a symbol we never subscribed to.
// The reconnect it triggers re-sends only our subscriptions, so it self-heals
// rather than emitting a book we cannot account for.
var errUnexpectedSymbol = errors.New("orderbook update for unsubscribed symbol")

// appPinger is an optional interface for exchanges that require an
// application-level keepalive (e.g. OKX's "ping" text frame) in addition to
// protocol-level pings.
type appPinger interface {
	// appPing returns the raw text frame to send and the interval between sends.
	appPing() (frame []byte, interval time.Duration)
}

// feedProvider implements outbound.OrderbookProvider for any
// exchangeFeed.
type feedProvider struct {
	cfg        Config
	logger     *slog.Logger
	exchange   exchangeFeed
	maxSymbols int
	metrics    *metrics
}

var _ outbound.OrderbookProvider = (*feedProvider)(nil)

func newFeedProvider(cfg Config, exchange exchangeFeed, maxSymbols int) (*feedProvider, error) {
	cfg = cfg.withDefaults()
	m, err := newMetrics(cfg.MeterProvider, exchange.name())
	if err != nil {
		return nil, fmt.Errorf("creating %s orderbook metrics: %w", exchange.name(), err)
	}
	return &feedProvider{
		cfg:        cfg,
		logger:     cfg.Logger.With("component", exchange.name()+"-orderbook"),
		exchange:   exchange,
		maxSymbols: maxSymbols,
		metrics:    m,
	}, nil
}

// Name returns the exchange identifier.
func (p *feedProvider) Name() string { return p.exchange.name() }

// Watch subscribes to symbols and streams aggregated books, splitting symbols
// across the fewest connections the exchange allows.
func (p *feedProvider) Watch(ctx context.Context, symbols []string) (<-chan entity.OrderbookUpdate, error) {
	if len(symbols) == 0 {
		return nil, errors.New("at least one symbol is required")
	}
	symbols, err := normalizeSymbols(symbols, p.exchange.normalizeSymbol)
	if err != nil {
		return nil, err
	}
	groups := chunkSymbols(dedupSymbols(symbols), p.maxSymbols)
	out := runConnections(ctx, groups, p.cfg.OutputBuffer, func(ctx context.Context, group []string, out chan<- entity.OrderbookUpdate) {
		reconnectLoop(ctx, p.cfg, p.logger, p.metrics, func(ctx context.Context, ready func()) error {
			return p.runConnection(ctx, group, out, ready)
		})
	})
	return out, nil
}

// runConnection owns one WebSocket connection: it dials, subscribes, optionally
// starts an application-level keepalive, then applies frames until the
// connection drops or ctx is cancelled.
func (p *feedProvider) runConnection(ctx context.Context, group []string, out chan<- entity.OrderbookUpdate, ready func()) error {
	ws, err := wsclient.Dial(ctx, p.exchange.endpoint(), p.cfg.Config)
	if err != nil {
		return err
	}
	defer ws.Close()
	p.metrics.connUp()
	defer p.metrics.connDown()

	// Scope helper goroutines (the app-level keepalive) to this connection rather
	// than to the long-lived Watch ctx, so they stop the moment this connection
	// ends instead of leaking across every reconnect.
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for _, msg := range p.exchange.subscribeMessages(group) {
		if err := ws.WriteJSON(msg); err != nil {
			return fmt.Errorf("send subscribe: %w", err)
		}
	}

	if pinger, ok := p.exchange.(appPinger); ok {
		p.startAppPing(connCtx, ws, pinger)
	}

	handler := p.exchange.newHandler(group, p.logger)
	em := newEmitter(out, p.logger, p.metrics)
	// Reset the reconnect backoff only once every symbol in the group has produced
	// its initial snapshot. Resetting on the first symbol would let one healthy
	// symbol mask another that never syncs (e.g. a bad symbol that errors after a
	// good one snapshots), turning a partial-sync failure into a reconnect storm.
	pending := symbolSet(group)
	readyCalled := false
	for {
		frame, err := ws.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("websocket: %w", err)
		}
		changes, err := handler.handle(frame.Data)
		if err != nil {
			return err
		}
		for _, s := range changes {
			em.emit(s.book, s.isSnapshot, s.t)
			if s.isSnapshot && !readyCalled {
				delete(pending, strings.ToUpper(s.book.Symbol))
				if len(pending) == 0 {
					readyCalled = true
					ready()
				}
			}
		}
	}
}

// startAppPing launches a goroutine that sends the exchange's keepalive frame on
// its interval until the connection closes or ctx is cancelled.
func (p *feedProvider) startAppPing(ctx context.Context, ws *wsclient.Conn, pinger appPinger) {
	frame, interval := pinger.appPing()
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := ws.WriteText(frame); err != nil {
					// The app ping is the primary keepalive for the venues that require
					// it; if it can't be sent, close so we reconnect promptly instead of
					// idling to the read timeout. Warn, since it explains the reconnect
					// an operator sees.
					p.logger.Warn("app ping failed, closing connection", "error", err)
					ws.Close()
					return
				}
			}
		}
	}()
}

// runConnections starts one goroutine per symbol group running connect, and
// returns the shared output channel. The channel is closed once ctx is
// cancelled and every connection goroutine has returned.
func runConnections(
	ctx context.Context,
	groups [][]string,
	buffer int,
	connect func(ctx context.Context, group []string, out chan<- entity.OrderbookUpdate),
) <-chan entity.OrderbookUpdate {
	out := make(chan entity.OrderbookUpdate, buffer)

	var wg sync.WaitGroup
	for _, g := range groups {
		wg.Add(1)
		go func(group []string) {
			defer wg.Done()
			connect(ctx, group, out)
		}(g)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

// reconnectLoop runs connect repeatedly until ctx is cancelled, backing off
// exponentially between attempts. connect is passed a ready callback it should
// invoke once the connection is fully established and synchronised; calling it
// resets the backoff so a long-lived connection that later drops reconnects
// promptly.
func reconnectLoop(
	ctx context.Context,
	cfg Config,
	logger *slog.Logger,
	m *metrics,
	connect func(ctx context.Context, ready func()) error,
) {
	backoff := cfg.InitialBackoff
	// lostAt marks when a previously synced group stopped delivering data; the
	// next full sync closes the window as a resync sample. Failures before the
	// group's first-ever sync are cold start, not resync, and stay out of the
	// histogram.
	var lostAt time.Time
	synced := false
	for {
		if ctx.Err() != nil {
			return
		}

		err := connect(ctx, func() {
			backoff = cfg.InitialBackoff
			synced = true
			if !lostAt.IsZero() {
				m.resynced(time.Since(lostAt))
				lostAt = time.Time{}
			}
		})

		if ctx.Err() != nil {
			return
		}
		if synced && lostAt.IsZero() {
			lostAt = time.Now()
		}
		m.reconnected(err)
		logger.Warn("orderbook connection lost, reconnecting", "error", err, "backoff", backoff)

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(time.Duration(float64(backoff)*cfg.BackoffFactor), cfg.MaxBackoff)
	}
}

// emitter delivers full-book updates to one connection's output channel. It never
// blocks: a blocking send would park the read loop, stop the socket draining, and
// wedge the connection with no reconnect. Updates are dropped when the consumer is
// behind, which is safe because every update carries the whole book. A dropped
// snapshot's discontinuity is not lost, though: its IsSnapshot flag is carried
// onto the next delivered update for that symbol.
type emitter struct {
	out      chan<- entity.OrderbookUpdate
	logger   *slog.Logger
	metrics  *metrics
	deferred map[string]bool // symbol -> a snapshot was dropped; re-flag next delivery

	dropped     int // drops since the last drop warning
	lastDropLog time.Time
}

// dropLogInterval rate-limits the dropped-updates warning so a stalled consumer
// is visible without the log itself becoming the next bottleneck.
const dropLogInterval = time.Minute

func newEmitter(out chan<- entity.OrderbookUpdate, logger *slog.Logger, m *metrics) *emitter {
	return &emitter{out: out, logger: logger, metrics: m, deferred: make(map[string]bool)}
}

// emit sends book without blocking and reports whether it was delivered. The
// update is flagged a snapshot when isSnapshot is set or a snapshot was dropped
// earlier for this symbol.
func (e *emitter) emit(book *entity.Orderbook, isSnapshot bool, t time.Time) bool {
	snapshot := isSnapshot || e.deferred[book.Symbol]
	// Skip building the clone when the buffer is already full.
	if c := cap(e.out); c > 0 && len(e.out) >= c {
		return e.drop(book.Symbol, snapshot)
	}
	select {
	case e.out <- entity.NewOrderbookUpdate(book, snapshot, t, time.Now()):
		delete(e.deferred, book.Symbol)
		e.metrics.emitted(book.Symbol, snapshot)
		return true
	default:
		return e.drop(book.Symbol, snapshot)
	}
}

// drop records an undelivered update. The first drop warns immediately; later
// drops are counted and surfaced at most once per dropLogInterval.
func (e *emitter) drop(symbol string, snapshot bool) bool {
	if snapshot {
		e.deferred[symbol] = true
	}
	e.metrics.dropped()
	e.dropped++
	if now := time.Now(); now.Sub(e.lastDropLog) >= dropLogInterval {
		e.logger.Warn("orderbook updates dropped, consumer not keeping up",
			"dropped", e.dropped, "symbol", symbol)
		e.dropped = 0
		e.lastDropLog = now
	}
	return false
}

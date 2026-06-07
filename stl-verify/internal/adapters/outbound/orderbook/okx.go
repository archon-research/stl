package orderbook

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

const (
	exchangeOKX = "okx"
	// okxWSBase is the OKX v5 public WebSocket.
	okxWSBase = "wss://ws.okx.com:8443/ws/v5/public"
	// okxChannel is the public 400-depth order book channel (snapshot then
	// updates, with seqId/prevSeqId continuity). The plan references
	// "books-l2-tbt", which carries the same message shape but requires a
	// logged-in VIP session; "books" is used here for public access.
	okxChannel      = "books"
	okxMaxSymbols   = 50
	okxPingInterval = 20 * time.Second
)

// Compile-time check that OKXProvider implements outbound.OrderbookProvider.
var _ outbound.OrderbookProvider = (*OKXProvider)(nil)

// OKXProvider streams L2 books from OKX. The books channel pushes a snapshot
// then incremental updates; each update's prevSeqId must match the previously
// applied seqId, otherwise the book is re-synchronised via reconnect.
type OKXProvider struct {
	*wsSnapshotProvider
}

// NewOKXProvider creates an OKX orderbook provider. Symbols are dash-separated
// instrument ids (e.g. "BTC-USDT"); Watch upper-cases and validates them.
func NewOKXProvider(cfg Config) *OKXProvider {
	ex := &okxExchange{wsBase: okxWSBase}
	return &OKXProvider{
		wsSnapshotProvider: newWSSnapshotProvider(cfg, ex, okxMaxSymbols),
	}
}

type okxExchange struct {
	wsBase string
}

func (e *okxExchange) name() string             { return exchangeOKX }
func (e *okxExchange) endpoint([]string) string { return e.wsBase }

func (e *okxExchange) normalizeSymbol(s string) (string, error) {
	return normalizeSeparatedPair(s, "-")
}
func (e *okxExchange) newHandler() frameHandler {
	return &okxHandler{books: newBookSet(exchangeOKX), lastSeq: make(map[string]int64)}
}

// appPing satisfies appPinger: OKX disconnects idle connections after 30s and
// expects a raw "ping" text frame (answered with "pong").
func (e *okxExchange) appPing() ([]byte, time.Duration) {
	return []byte("ping"), okxPingInterval
}

func (e *okxExchange) subscribeMessages(group []string) ([]any, error) {
	args := make([]map[string]string, len(group))
	for i, sym := range group {
		args[i] = map[string]string{"channel": okxChannel, "instId": sym}
	}
	return []any{map[string]any{"op": "subscribe", "args": args}}, nil
}

type okxFrame struct {
	Event string `json:"event"` // set on control frames: "subscribe", "error"
	Code  string `json:"code"`
	Msg   string `json:"msg"`
	Arg   struct {
		Channel string `json:"channel"`
		InstID  string `json:"instId"`
	} `json:"arg"`
	Action string `json:"action"` // "snapshot" | "update"
	Data   []struct {
		Asks      [][]string `json:"asks"`
		Bids      [][]string `json:"bids"`
		TS        string     `json:"ts"`
		SeqID     int64      `json:"seqId"`
		PrevSeqID int64      `json:"prevSeqId"`
	} `json:"data"`
}

type okxHandler struct {
	books   *bookSet
	lastSeq map[string]int64 // instId -> last applied seqId
}

func (h *okxHandler) handle(raw []byte) ([]emitSignal, error) {
	if string(raw) == "pong" {
		return nil, nil // reply to our keepalive ping
	}

	var f okxFrame
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("decode okx frame: %w", err)
	}
	if f.Event == "error" {
		return nil, fmt.Errorf("okx error %s: %s", f.Code, f.Msg)
	}
	if f.Event != "" {
		return nil, nil // subscribe ack / other control frame
	}
	if f.Action == "" || len(f.Data) == 0 {
		return nil, nil
	}

	instID := f.Arg.InstID
	book := h.books.get(instID)
	signals := make([]emitSignal, 0, len(f.Data))

	for _, d := range f.Data {
		isSnapshot := f.Action == "snapshot"
		if isSnapshot {
			book.Reset()
		} else {
			last, ok := h.lastSeq[instID]
			if !ok {
				// An update before any snapshot would be applied to an empty book,
				// silently producing a partial (wrong) book. Force a re-sync instead.
				return nil, fmt.Errorf("%w: okx %s update before snapshot", errSequenceGap, instID)
			}
			if d.PrevSeqID != last {
				if d.SeqID == d.PrevSeqID {
					continue // documented no-change push (seqId == prevSeqId); nothing to apply
				}
				return nil, fmt.Errorf("%w: okx %s prevSeqId %d != last %d", errSequenceGap, instID, d.PrevSeqID, last)
			}
		}

		if err := applyDeltaLevels(book, entity.Bid, d.Bids); err != nil {
			return nil, fmt.Errorf("okx %s bids: %w", instID, err)
		}
		if err := applyDeltaLevels(book, entity.Ask, d.Asks); err != nil {
			return nil, fmt.Errorf("okx %s asks: %w", instID, err)
		}
		h.lastSeq[instID] = d.SeqID
		book.LastUpdateID = d.SeqID
		signals = append(signals, emitSignal{book: book, isSnapshot: isSnapshot, t: parseUnixMillisOrZero(d.TS)})
	}
	return signals, nil
}

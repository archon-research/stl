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
	// okxChannel is the public 400-depth book channel (snapshot then seqId/prevSeqId
	// updates). "books-l2-tbt" has the same shape but needs a VIP login, so the
	// public "books" is used here.
	okxChannel = "books"
	// okxMaxSymbols caps order book channels per connection. OKX's order book
	// channel docs recommend spreading 50/400-depth subscriptions across
	// connections of fewer than 30 channels each; books is 400-depth, so we stay
	// below 30.
	okxMaxSymbols   = 29
	okxPingInterval = 20 * time.Second
)

// NewOKXProvider creates a provider that streams L2 books from OKX. The books
// channel pushes a snapshot then incremental updates; each update's prevSeqId
// must match the previously applied seqId, otherwise the book is re-synchronised
// via reconnect.
//
// Symbols are dash-separated instrument ids (e.g. "BTC-USDT"); Watch upper-cases
// and validates them.
//
// Docs: https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-order-book-channel
func NewOKXProvider(cfg Config) outbound.OrderbookProvider {
	return newFeedProvider(cfg, &okxExchange{wsBase: okxWSBase}, okxMaxSymbols)
}

type okxExchange struct {
	wsBase string
}

// Compile-time check that okxExchange supplies an application-level keepalive.
var _ appPinger = (*okxExchange)(nil)

func (e *okxExchange) name() string             { return exchangeOKX }
func (e *okxExchange) endpoint([]string) string { return e.wsBase }

func (e *okxExchange) normalizeSymbol(s string) (string, error) {
	return normalizeSeparatedPair(s, "-")
}
func (e *okxExchange) newHandler(group []string) frameHandler {
	return &okxHandler{books: newBookSet(exchangeOKX), lastSeq: make(map[string]int64), allowed: symbolSet(group)}
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
	allowed map[string]bool  // subscribed instIds; nil allows all (tests)
}

func (h *okxHandler) handle(raw []byte) ([]bookChange, error) {
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
	if !symbolAllowed(h.allowed, instID) {
		return nil, fmt.Errorf("%w: okx %s", errUnexpectedSymbol, instID)
	}
	book := h.books.getOrCreate(instID)
	changes := make([]bookChange, 0, len(f.Data))

	// A snapshot frame rebuilds the whole book and emits exactly one aggregated
	// snapshot: reset once, apply every data object, then emit at the end so a
	// multi-object frame never surfaces a partial book.
	isSnapshot := f.Action == "snapshot"
	if isSnapshot {
		book.Reset()
	}
	var snapshotTS time.Time

	for _, d := range f.Data {
		if !isSnapshot {
			last, ok := h.lastSeq[instID]
			if !ok {
				// An update before any snapshot would be applied to an empty book,
				// silently producing a partial (wrong) book. Force a re-sync instead.
				return nil, fmt.Errorf("%w: okx %s update before snapshot", errSequenceGap, instID)
			}
			// OKX no-update message: empty book with seqId == prevSeqId == the last
			// applied seqId (it repeats the current sequence, it does not advance).
			// It changes nothing, so emit nothing and leave the sequence untouched.
			if len(d.Bids) == 0 && len(d.Asks) == 0 && d.SeqID == d.PrevSeqID && d.SeqID == last {
				continue
			}
			// Any other break in the chain (prevSeqId != last, including an OKX seqId
			// reset, where seqId < prevSeqId) is a gap: reconnect for a fresh snapshot.
			// The initial prevSeqId == -1 is not seen here; it rides the first message,
			// which is a snapshot handled above.
			if d.PrevSeqID != last {
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
		ts := parseUnixMillisOrZero(d.TS)
		if isSnapshot {
			snapshotTS = ts
			continue
		}
		changes = append(changes, bookChange{book: book, isSnapshot: false, t: ts})
	}
	if isSnapshot {
		return []bookChange{{book: book, isSnapshot: true, t: snapshotTS}}, nil
	}
	return changes, nil
}

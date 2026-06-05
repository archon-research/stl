package orderbook

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

const (
	exchangeCoinbase = "coinbase"
	// coinbaseWSBase is the Coinbase Advanced Trade market-data WebSocket.
	coinbaseWSBase = "wss://advanced-trade-ws.coinbase.com"
	// coinbaseMaxSymbols caps symbols per connection.
	coinbaseMaxSymbols = 50
)

// Compile-time check that CoinbaseProvider implements outbound.OrderbookProvider.
var _ outbound.OrderbookProvider = (*CoinbaseProvider)(nil)

// CoinbaseProvider streams L2 books from Coinbase Advanced Trade. The level2
// channel delivers a "snapshot" event followed by "update" events; the
// connection-wide monotonic sequence_num detects dropped frames (a single
// counter per connection, not per product). The subscribe frame is
// unauthenticated.
type CoinbaseProvider struct {
	*wsSnapshotProvider
}

// NewCoinbaseProvider creates a Coinbase orderbook provider.
func NewCoinbaseProvider(cfg Config) *CoinbaseProvider {
	ex := &coinbaseExchange{wsBase: coinbaseWSBase}
	return &CoinbaseProvider{
		wsSnapshotProvider: newWSSnapshotProvider(cfg, ex, coinbaseMaxSymbols),
	}
}

type coinbaseExchange struct {
	wsBase string
}

func (e *coinbaseExchange) name() string             { return exchangeCoinbase }
func (e *coinbaseExchange) endpoint([]string) string { return e.wsBase }
func (e *coinbaseExchange) newHandler() frameHandler {
	return &coinbaseHandler{books: newBookSet(exchangeCoinbase)}
}

func (e *coinbaseExchange) subscribeMessages(group []string) ([]any, error) {
	msg := map[string]any{
		"type":        "subscribe",
		"channel":     "level2",
		"product_ids": group,
	}
	return []any{msg}, nil
}

type coinbaseFrame struct {
	Channel     string `json:"channel"`
	SequenceNum int64  `json:"sequence_num"`
	Timestamp   string `json:"timestamp"`
	Events      []struct {
		Type      string `json:"type"` // "snapshot" | "update"
		ProductID string `json:"product_id"`
		Updates   []struct {
			Side        string `json:"side"` // "bid" | "offer"
			PriceLevel  string `json:"price_level"`
			NewQuantity string `json:"new_quantity"`
		} `json:"updates"`
	} `json:"events"`
}

type coinbaseHandler struct {
	books   *bookSet
	haveSeq bool
	nextSeq int64
}

func (h *coinbaseHandler) handle(raw []byte) ([]emitSignal, error) {
	var f coinbaseFrame
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("decode coinbase frame: %w", err)
	}

	switch f.Channel {
	case "l2_data":
		// handled below
	case "error":
		return nil, fmt.Errorf("coinbase error frame: %s", string(raw))
	default:
		// subscriptions, heartbeats, and unknown channels carry no book data.
		return nil, nil
	}

	if h.haveSeq && f.SequenceNum != h.nextSeq {
		return nil, fmt.Errorf("%w: coinbase sequence want %d got %d", errSequenceGap, h.nextSeq, f.SequenceNum)
	}
	h.nextSeq = f.SequenceNum + 1
	h.haveSeq = true

	t := parseRFC3339OrNow(f.Timestamp)

	// A frame may carry events for several products; emit each affected book once,
	// preserving first-seen order, and flag any that received a snapshot.
	type affectedBook struct {
		book       *entity.Orderbook
		isSnapshot bool
	}
	affected := make(map[string]*affectedBook)
	order := make([]string, 0, len(f.Events))

	for _, ev := range f.Events {
		book := h.books.get(ev.ProductID)
		isSnapshot := ev.Type == "snapshot"
		if isSnapshot {
			book.Reset()
		}
		for _, u := range ev.Updates {
			side, err := coinbaseSide(u.Side)
			if err != nil {
				return nil, err
			}
			lvl, err := parseLevel(u.PriceLevel, u.NewQuantity)
			if err != nil {
				return nil, fmt.Errorf("coinbase %s: %w", ev.ProductID, err)
			}
			book.ApplyLevel(side, lvl.Price, lvl.Size)
		}
		a, ok := affected[ev.ProductID]
		if !ok {
			a = &affectedBook{book: book}
			affected[ev.ProductID] = a
			order = append(order, ev.ProductID)
		}
		a.isSnapshot = a.isSnapshot || isSnapshot
	}

	signals := make([]emitSignal, 0, len(order))
	for _, pid := range order {
		a := affected[pid]
		signals = append(signals, emitSignal{book: a.book, isSnapshot: a.isSnapshot, t: t})
	}
	return signals, nil
}

func coinbaseSide(s string) (entity.Side, error) {
	switch strings.ToLower(s) {
	case "bid", "buy":
		return entity.Bid, nil
	case "offer", "ask", "sell":
		return entity.Ask, nil
	default:
		return entity.Bid, fmt.Errorf("coinbase unknown side %q", s)
	}
}

package orderbook

import (
	"encoding/json"
	"fmt"
	"log/slog"
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

	// coinbaseSubscribeChannel is the channel requested in the subscribe frame;
	// Coinbase echoes the book data back on coinbaseDataChannel. The request and
	// response names intentionally differ, so they are pinned here together.
	coinbaseSubscribeChannel = "level2"
	coinbaseDataChannel      = "l2_data"
)

// Compile-time check that CoinbaseProvider implements outbound.OrderbookProvider.
var _ outbound.OrderbookProvider = (*CoinbaseProvider)(nil)

// CoinbaseProvider streams L2 books from Coinbase Advanced Trade. The level2
// channel delivers a "snapshot" event followed by "update" events; the
// connection-wide monotonic sequence_num detects dropped frames (a single
// counter per connection, not per product), and a per-product snapshot guard
// rejects an update that arrives before that product's snapshot. The subscribe
// frame is unauthenticated.
type CoinbaseProvider struct {
	*wsSnapshotProvider
}

// NewCoinbaseProvider creates a Coinbase orderbook provider.
func NewCoinbaseProvider(cfg Config) *CoinbaseProvider {
	cfg = cfg.withDefaults()
	ex := &coinbaseExchange{
		wsBase: coinbaseWSBase,
		logger: cfg.Logger.With("component", exchangeCoinbase+"-orderbook"),
	}
	return &CoinbaseProvider{
		wsSnapshotProvider: newWSSnapshotProvider(cfg, ex, coinbaseMaxSymbols),
	}
}

type coinbaseExchange struct {
	wsBase string
	logger *slog.Logger
}

func (e *coinbaseExchange) name() string             { return exchangeCoinbase }
func (e *coinbaseExchange) endpoint([]string) string { return e.wsBase }
func (e *coinbaseExchange) newHandler() frameHandler { return newCoinbaseHandler(e.logger) }

func (e *coinbaseExchange) subscribeMessages(group []string) ([]any, error) {
	msg := map[string]any{
		"type":        "subscribe",
		"channel":     coinbaseSubscribeChannel,
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
	books        *bookSet
	logger       *slog.Logger
	haveSeq      bool
	nextSeq      int64
	seenSnapshot map[string]bool // product_id -> a snapshot has been applied
	logged       map[string]bool // channel -> an "unrecognized channel" warning was emitted
}

func newCoinbaseHandler(logger *slog.Logger) *coinbaseHandler {
	if logger == nil {
		logger = slog.Default()
	}
	return &coinbaseHandler{
		books:        newBookSet(exchangeCoinbase),
		logger:       logger,
		seenSnapshot: make(map[string]bool),
		logged:       make(map[string]bool),
	}
}

func (h *coinbaseHandler) handle(raw []byte) ([]emitSignal, error) {
	var f coinbaseFrame
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("decode coinbase frame: %w", err)
	}

	// sequence_num increases by 1 for every message on the connection, across all
	// channels (subscriptions and heartbeats included). The continuity check must
	// therefore advance on every frame; gating it on l2_data only false-positives
	// whenever a skipped non-l2_data frame consumes a sequence number.
	if h.haveSeq && f.SequenceNum != h.nextSeq {
		return nil, fmt.Errorf("%w: coinbase sequence want %d got %d", errSequenceGap, h.nextSeq, f.SequenceNum)
	}
	h.nextSeq = f.SequenceNum + 1
	h.haveSeq = true

	switch f.Channel {
	case coinbaseDataChannel:
		// handled below
	case "error":
		return nil, fmt.Errorf("coinbase error frame: %s", string(raw))
	case "subscriptions", "heartbeats", "":
		return nil, nil // control frames carry no book data
	default:
		// An unrecognized channel means a protocol change (e.g. a renamed data
		// channel), which would otherwise degrade silently to "no book ever
		// emitted". Surface it once per connection so the change is observable.
		if !h.logged[f.Channel] {
			h.logged[f.Channel] = true
			h.logger.Warn("coinbase: unrecognized channel, no book data extracted", "channel", f.Channel)
		}
		return nil, nil
	}

	t := parseRFC3339OrZero(f.Timestamp)

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
			h.seenSnapshot[ev.ProductID] = true
		} else if !h.seenSnapshot[ev.ProductID] {
			// An update before any snapshot would be applied to an empty book,
			// emitting a partial (wrong) book; force a re-sync instead.
			return nil, fmt.Errorf("%w: coinbase %s update before snapshot", errSequenceGap, ev.ProductID)
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

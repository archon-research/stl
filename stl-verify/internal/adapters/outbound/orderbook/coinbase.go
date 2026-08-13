package orderbook

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

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

// NewCoinbaseProvider creates a provider that streams L2 books from Coinbase
// Advanced Trade. The level2 channel delivers a "snapshot" event followed by
// "update" events; the connection-wide monotonic sequence_num detects dropped
// frames (a single counter per connection, not per product), and a per-product
// snapshot guard rejects an update that arrives before that product's snapshot.
// The subscribe frame is unauthenticated.
//
// Symbols are dash-separated pairs (e.g. "BTC-USD"); Watch upper-cases and
// validates them.
//
// Docs: https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-channels
func NewCoinbaseProvider(cfg Config) (outbound.OrderbookProvider, error) {
	return newFeedProvider(cfg, &coinbaseExchange{}, coinbaseMaxSymbols)
}

type coinbaseExchange struct{}

func (e *coinbaseExchange) name() string     { return exchangeCoinbase }
func (e *coinbaseExchange) endpoint() string { return coinbaseWSBase }

func (e *coinbaseExchange) normalizeSymbol(s string) (string, error) {
	return normalizeSeparatedPair(s, "-")
}
func (e *coinbaseExchange) newHandler(group []string, logger *slog.Logger) frameHandler {
	return &coinbaseHandler{
		books:        newBookSet(exchangeCoinbase),
		logger:       logger,
		allowed:      symbolSet(group),
		seenSnapshot: make(map[string]bool),
		logged:       make(map[string]bool),
	}
}

func (e *coinbaseExchange) subscribeMessages(group []string) []any {
	msg := map[string]any{
		"type":        "subscribe",
		"channel":     coinbaseSubscribeChannel,
		"product_ids": group,
	}
	return []any{msg}
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
	allowed      map[string]bool // subscribed product_ids
	haveSeq      bool
	nextSeq      int64
	seenSnapshot map[string]bool // product_id -> a snapshot has been applied
	logged       map[string]bool // channel -> an "unrecognized channel" warning was emitted
}

func (h *coinbaseHandler) handle(raw []byte) ([]bookChange, error) {
	var f coinbaseFrame
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("decode coinbase frame: %w", err)
	}

	// sequence_num increases by 1 for every message on the connection, across all
	// channels (subscriptions and heartbeats included). The continuity check must
	// therefore advance on every frame; gating it on l2_data only false-positives
	// whenever a skipped non-l2_data frame consumes a sequence number. The first
	// frame seen after subscribe anchors the counter at whatever value it carries
	// (the counter is connection-wide and starts wherever our subscribe lands),
	// which is trusted by design.
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
	// preserving first-seen order, and flag any that received a snapshot. index maps
	// a product_id to its slot in changes so repeated events coalesce.
	changes := make([]bookChange, 0, len(f.Events))
	index := make(map[string]int)

	for _, ev := range f.Events {
		if !symbolAllowed(h.allowed, ev.ProductID) {
			return nil, fmt.Errorf("%w: coinbase %s", errUnexpectedSymbol, ev.ProductID)
		}
		book := h.books.getOrCreate(ev.ProductID)
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
				return nil, fmt.Errorf("coinbase %s: %w", ev.ProductID, err)
			}
			lvl, err := parseLevel(u.PriceLevel, u.NewQuantity)
			if err != nil {
				return nil, fmt.Errorf("coinbase %s: %w", ev.ProductID, err)
			}
			book.ApplyLevel(side, lvl.Price, lvl.Size)
		}
		if i, ok := index[ev.ProductID]; ok {
			changes[i].isSnapshot = changes[i].isSnapshot || isSnapshot
		} else {
			index[ev.ProductID] = len(changes)
			changes = append(changes, bookChange{book: book, isSnapshot: isSnapshot, t: t})
		}
	}
	return changes, nil
}

// parseRFC3339OrZero parses Coinbase's RFC3339 timestamp, returning the zero
// Time when it is empty or malformed. The zero Time signals "no usable venue
// event time"; it is never substituted with the local clock (the local clock is
// recorded separately as OrderbookUpdate.IngestedAt).
func parseRFC3339OrZero(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}
	}
	return t.UTC()
}

// coinbaseSide maps a level2 side token to a book side. The feed uses "bid" and
// "offer" only; anything else is a protocol change we fail loudly on (callers
// check the error) rather than silently mis-classifying.
func coinbaseSide(s string) (entity.Side, error) {
	switch s {
	case "bid":
		return entity.Bid, nil
	case "offer":
		return entity.Ask, nil
	default:
		return entity.Bid, fmt.Errorf("coinbase unknown side %q", s)
	}
}

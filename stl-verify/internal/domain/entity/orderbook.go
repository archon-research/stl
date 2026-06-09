package entity

import (
	"maps"
	"time"
)

// Side identifies which half of the book a price level belongs to.
type Side int

const (
	// Bid is the buy side. Best (most aggressive) bid is the highest price.
	Bid Side = iota
	// Ask is the sell side. Best (most aggressive) ask is the lowest price.
	Ask
)

// PriceLevel is a single aggregated L2 level: the total resting size at a price.
//
// Price and Size are the exact decimal strings the exchange published. They are
// never parsed into a float for storage, so the value written downstream (e.g.
// the 1s JSONB/NUMERIC dump to TimescaleDB) byte-matches what the venue sent and
// loses no precision.
type PriceLevel struct {
	Price string
	Size  string
}

// Orderbook is the aggregated L2 state for one symbol on one exchange, built from
// a snapshot and kept current by deltas.
//
// Bids and asks are maps keyed by the exact price string: delta application is
// O(1) and a delete matches its insert as long as the venue formats a price
// consistently within a stream (adapters reject exponent/sign forms via
// IsCanonicalDecimal). Sizes are kept verbatim.
//
// Not safe for concurrent use: each book is owned by one goroutine, and adapters
// hand consumers a Clone to read while they keep mutating their own copy.
type Orderbook struct {
	Exchange string
	Symbol   string

	bids map[string]string // price -> size, exact exchange text
	asks map[string]string
}

// NewOrderbook returns an empty book for the given exchange and symbol.
func NewOrderbook(exchange, symbol string) *Orderbook {
	return &Orderbook{
		Exchange: exchange,
		Symbol:   symbol,
		bids:     make(map[string]string),
		asks:     make(map[string]string),
	}
}

// Reset clears every level. Adapters call it before applying a fresh snapshot
// when re-synchronising after a gap or reconnect.
func (ob *Orderbook) Reset() {
	clear(ob.bids)
	clear(ob.asks)
}

func (ob *Orderbook) side(s Side) map[string]string {
	if s == Ask {
		return ob.asks
	}
	return ob.bids
}

// ApplyLevel upserts a price level in O(1), keyed and valued by the exact
// exchange strings. A canonical-zero size (the exchange convention for "level
// removed") deletes the level, as does any non-canonical size, so malformed text
// can never be stored. Validation that should fail the frame and force a resync
// lives in the adapter (parseLevel); this method only decides set vs delete.
func (ob *Orderbook) ApplyLevel(s Side, price, size string) {
	m := ob.side(s)
	if !IsCanonicalDecimal(size) || IsZeroDecimal(size) {
		delete(m, price)
		return
	}
	m[price] = size
}

// Bids returns the buy levels. Asks returns the sell levels. Both are returned
// in unspecified order: the package does no ordering, leaving any sort to the
// consumer (e.g. ORDER BY price at query time) so no decimal parsing is needed
// here.
func (ob *Orderbook) Bids() []PriceLevel { return levels(ob.bids) }

// Asks returns the sell levels (see Bids for ordering semantics).
func (ob *Orderbook) Asks() []PriceLevel { return levels(ob.asks) }

func levels(m map[string]string) []PriceLevel {
	out := make([]PriceLevel, 0, len(m))
	for p, s := range m {
		out = append(out, PriceLevel{Price: p, Size: s})
	}
	return out
}

// Depth returns the number of distinct price levels on a side.
func (ob *Orderbook) Depth(s Side) int { return len(ob.side(s)) }

// Clone returns an independent deep copy of the book.
func (ob *Orderbook) Clone() *Orderbook {
	cp := &Orderbook{
		Exchange: ob.Exchange,
		Symbol:   ob.Symbol,
		bids:     make(map[string]string, len(ob.bids)),
		asks:     make(map[string]string, len(ob.asks)),
	}
	maps.Copy(cp.bids, ob.bids)
	maps.Copy(cp.asks, ob.asks)
	return cp
}

// OrderbookUpdate is one point-in-time view of an aggregated book, emitted by
// an OrderbookProvider after it applies a change. Book is an independent copy
// the receiver may read freely. Because every update carries the complete book
// (not an incremental delta), a consumer that falls behind may safely skip
// intermediate updates and still hold a correct book.
type OrderbookUpdate struct {
	Book *Orderbook
	// Time is the venue event time (when the exchange produced the update), in
	// UTC, or nil when the feed carries no event time or the timestamp was
	// absent/unparseable. A nil Time is never fabricated from the local clock;
	// persist it as NULL and use IngestedAt for a timestamp that is always present.
	Time *time.Time
	// IngestedAt is the wall-clock time this process produced the update, in UTC.
	// It is always set, and lets a consumer distinguish event time from ingestion
	// time and spot a stale or mis-stamped event.
	IngestedAt time.Time
	IsSnapshot bool // true for the first update emitted after a (re)sync
}

// NewOrderbookUpdate builds an update carrying an independent clone of book. A
// zero eventTime is treated as "no venue event time" and left as a nil Time
// rather than fabricated from the local clock; ingestedAt is always recorded.
// Both timestamps are normalised to UTC.
func NewOrderbookUpdate(book *Orderbook, isSnapshot bool, eventTime, ingestedAt time.Time) OrderbookUpdate {
	upd := OrderbookUpdate{
		Book:       book.Clone(),
		IngestedAt: ingestedAt.UTC(),
		IsSnapshot: isSnapshot,
	}
	if !eventTime.IsZero() {
		t := eventTime.UTC()
		upd.Time = &t
	}
	return upd
}

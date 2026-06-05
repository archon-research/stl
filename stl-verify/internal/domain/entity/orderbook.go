package entity

import (
	"math"
	"strconv"
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

// String returns the lower-case side name ("bid"/"ask").
func (s Side) String() string {
	switch s {
	case Bid:
		return "bid"
	case Ask:
		return "ask"
	default:
		return "unknown"
	}
}

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

// Orderbook is the aggregated L2 state for one symbol on one exchange. It is
// built from a snapshot and kept current by applying a stream of deltas.
//
// Storage and performance:
//
// Bids and asks are stored as maps keyed by the exact price string, so delta
// application — the high-frequency hot path — is O(1) per level, and a delete
// matches its insert exactly (an exchange formats a given price consistently
// within one stream). Values are the exchange's size strings, kept verbatim.
//
// Concurrency: an Orderbook is NOT safe for concurrent use. Each book is owned
// by exactly one goroutine (the connection that maintains it). Adapters hand a
// Clone to consumers so the consumer can read it while the adapter keeps
// mutating its own copy.
type Orderbook struct {
	Exchange string
	Symbol   string

	// LastUpdateID is the exchange sequence number of the most recently applied
	// update. Its exact meaning is exchange-specific (e.g. Binance's `u`);
	// adapters use it to detect gaps between consecutive deltas. Zero means no
	// update has been applied yet.
	LastUpdateID int64

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

// Reset clears every level and the sequence number. Adapters call it before
// applying a fresh snapshot when re-synchronising after a gap or reconnect.
func (ob *Orderbook) Reset() {
	clear(ob.bids)
	clear(ob.asks)
	ob.LastUpdateID = 0
}

func (ob *Orderbook) side(s Side) map[string]string {
	if s == Ask {
		return ob.asks
	}
	return ob.bids
}

// ApplyLevel upserts a price level in O(1), keyed and valued by the exact
// exchange strings. A size of zero (the exchange convention for "level removed")
// — or any non-positive/non-finite size — deletes the level. Price validation is
// the adapter's responsibility (see parseLevel); this only decides set vs delete.
func (ob *Orderbook) ApplyLevel(s Side, price, size string) {
	m := ob.side(s)
	v, err := strconv.ParseFloat(size, 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) || v <= 0 {
		delete(m, price)
		return
	}
	m[price] = size
}

// ReplaceSide overwrites an entire side from snapshot levels.
func (ob *Orderbook) ReplaceSide(s Side, levels []PriceLevel) {
	clear(ob.side(s))
	for _, lvl := range levels {
		ob.ApplyLevel(s, lvl.Price, lvl.Size)
	}
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
		Exchange:     ob.Exchange,
		Symbol:       ob.Symbol,
		LastUpdateID: ob.LastUpdateID,
		bids:         make(map[string]string, len(ob.bids)),
		asks:         make(map[string]string, len(ob.asks)),
	}
	for p, s := range ob.bids {
		cp.bids[p] = s
	}
	for p, s := range ob.asks {
		cp.asks[p] = s
	}
	return cp
}

// OrderbookUpdate is one point-in-time view of an aggregated book, emitted by
// an OrderbookProvider after it applies a change. Book is an independent copy
// the receiver may read freely. Because every update carries the complete book
// (not an incremental delta), a consumer that falls behind may safely skip
// intermediate updates and still hold a correct book.
type OrderbookUpdate struct {
	Exchange   string
	Symbol     string
	Book       *Orderbook
	Time       time.Time
	IsSnapshot bool // true for the first update emitted after a (re)sync
}

// NewOrderbookUpdate builds an update carrying an independent clone of book.
// Time is normalised to UTC so every emitted update is zone-consistent
// regardless of the source timestamp's location (event time, ingestion time, or
// a parsed exchange string).
func NewOrderbookUpdate(book *Orderbook, isSnapshot bool, t time.Time) OrderbookUpdate {
	return OrderbookUpdate{
		Exchange:   book.Exchange,
		Symbol:     book.Symbol,
		Book:       book.Clone(),
		Time:       t.UTC(),
		IsSnapshot: isSnapshot,
	}
}

package entity

import (
	"math"
	"sort"
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
type PriceLevel struct {
	Price float64
	Size  float64
}

// Orderbook is the aggregated L2 state for one symbol on one exchange. It is
// built from a snapshot and kept current by applying a stream of deltas.
//
// Storage and performance:
//
// Bids and asks are stored as maps keyed by price so that delta application —
// the high-frequency hot path — is O(1) per level. Decimal price strings from
// an exchange are parsed once by the adapter; identical prices parse to the
// same float64, so the map naturally de-duplicates levels without relying on
// string formatting. Sorted views (Bids/Asks) are produced on demand, so only
// callers that actually need ordering (e.g. a periodic persistence dump) pay
// the O(n log n) sort.
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

	bids map[float64]float64 // price -> size
	asks map[float64]float64 // price -> size
}

// NewOrderbook returns an empty book for the given exchange and symbol.
func NewOrderbook(exchange, symbol string) *Orderbook {
	return &Orderbook{
		Exchange: exchange,
		Symbol:   symbol,
		bids:     make(map[float64]float64),
		asks:     make(map[float64]float64),
	}
}

// Reset clears every level and the sequence number. Adapters call it before
// applying a fresh snapshot when re-synchronising after a gap or reconnect.
func (ob *Orderbook) Reset() {
	clear(ob.bids)
	clear(ob.asks)
	ob.LastUpdateID = 0
}

func (ob *Orderbook) side(s Side) map[float64]float64 {
	if s == Ask {
		return ob.asks
	}
	return ob.bids
}

// ApplyLevel upserts a price level in O(1). A size of zero (the exchange
// convention for "level removed") deletes the level. Non-finite or
// non-positive prices and non-finite sizes are ignored so a malformed delta can
// never poison the map (a NaN key is impossible to delete).
func (ob *Orderbook) ApplyLevel(s Side, price, size float64) {
	if price <= 0 || math.IsNaN(price) || math.IsInf(price, 0) {
		return
	}
	m := ob.side(s)
	if size <= 0 || math.IsNaN(size) || math.IsInf(size, 0) {
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

// Bids returns the buy levels sorted best-first (highest price first).
func (ob *Orderbook) Bids() []PriceLevel { return sortedLevels(ob.bids, true) }

// Asks returns the sell levels sorted best-first (lowest price first).
func (ob *Orderbook) Asks() []PriceLevel { return sortedLevels(ob.asks, false) }

func sortedLevels(m map[float64]float64, descending bool) []PriceLevel {
	out := make([]PriceLevel, 0, len(m))
	for p, sz := range m {
		out = append(out, PriceLevel{Price: p, Size: sz})
	}
	sort.Slice(out, func(i, j int) bool {
		if descending {
			return out[i].Price > out[j].Price
		}
		return out[i].Price < out[j].Price
	})
	return out
}

// BestBid returns the highest bid, or ok=false when the bid side is empty.
func (ob *Orderbook) BestBid() (PriceLevel, bool) { return bestLevel(ob.bids, true) }

// BestAsk returns the lowest ask, or ok=false when the ask side is empty.
func (ob *Orderbook) BestAsk() (PriceLevel, bool) { return bestLevel(ob.asks, false) }

func bestLevel(m map[float64]float64, max bool) (PriceLevel, bool) {
	found := false
	var bestPrice float64
	for p := range m {
		if !found || (max && p > bestPrice) || (!max && p < bestPrice) {
			bestPrice, found = p, true
		}
	}
	if !found {
		return PriceLevel{}, false
	}
	return PriceLevel{Price: bestPrice, Size: m[bestPrice]}, true
}

// Depth returns the number of distinct price levels on a side.
func (ob *Orderbook) Depth(s Side) int { return len(ob.side(s)) }

// Crossed reports whether the best bid is strictly above the best ask, which
// signals a corrupt or desynchronised book. It returns false when either side
// is empty.
func (ob *Orderbook) Crossed() bool {
	bb, okBid := ob.BestBid()
	ba, okAsk := ob.BestAsk()
	if !okBid || !okAsk {
		return false
	}
	return bb.Price > ba.Price
}

// Clone returns an independent deep copy of the book.
func (ob *Orderbook) Clone() *Orderbook {
	cp := &Orderbook{
		Exchange:     ob.Exchange,
		Symbol:       ob.Symbol,
		LastUpdateID: ob.LastUpdateID,
		bids:         make(map[float64]float64, len(ob.bids)),
		asks:         make(map[float64]float64, len(ob.asks)),
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
func NewOrderbookUpdate(book *Orderbook, isSnapshot bool, t time.Time) OrderbookUpdate {
	return OrderbookUpdate{
		Exchange:   book.Exchange,
		Symbol:     book.Symbol,
		Book:       book.Clone(),
		Time:       t.UTC(),
		IsSnapshot: isSnapshot,
	}
}

package entity

import "time"

// OrderbookSnapshot is one persisted point-in-time view of an aggregated L2 book
// for a single symbol on a single exchange: the top-N bids and asks captured at a
// tick. It is the domain-pure record the persistence layer writes, decoupled from
// the live OrderbookUpdate the provider streams.
//
// Bids and Asks are already trimmed to the configured depth and ordered best
// first (bids: highest price first; asks: lowest price first), and carry the exact
// exchange decimal strings (never float64) so the stored value byte-matches what
// the venue sent.
type OrderbookSnapshot struct {
	Exchange string
	Symbol   string
	// EventTime is the venue event time copied from the source update, or nil when
	// the feed carried no usable event time. It is persisted as NULL and never
	// fabricated from a local clock.
	EventTime *time.Time
	// IngestedAt is when the provider produced the source update (always set).
	IngestedAt time.Time
	// PersistedAt is when this snapshot was captured for writing (the tick time).
	// It is the hypertable partition column, so it is always set.
	PersistedAt time.Time
	Bids        []PriceLevel
	Asks        []PriceLevel
}

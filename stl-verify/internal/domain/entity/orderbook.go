package entity

import "time"

// OrderbookLevel represents a single price level in an orderbook.
type OrderbookLevel struct {
	Price     float64 `json:"price"`
	Size      float64 `json:"sz"`
	Liquidity float64 `json:"liquidity"` // Price * Size (USD notional)
}

// OrderbookSnapshot represents a point-in-time orderbook from a single exchange.
type OrderbookSnapshot struct {
	Exchange   string           `json:"exchange"`
	Symbol     string           `json:"symbol"`     // Normalized symbol (e.g., "BTC", "ETH")
	Bids       []OrderbookLevel `json:"bids"`       // Sorted best (highest) first
	Asks       []OrderbookLevel `json:"asks"`       // Sorted best (lowest) first
	CapturedAt time.Time        `json:"captured_at"`
	LatencyMs  int              `json:"latency_ms"` // Exchange → our service
}

// NewOrderbookLevel creates a level with pre-computed liquidity.
func NewOrderbookLevel(price, size float64) OrderbookLevel {
	return OrderbookLevel{
		Price:     price,
		Size:      size,
		Liquidity: price * size,
	}
}

package outbound

import (
	"context"
	"errors"
	"time"
)

// ErrRequestRejected marks a request the provider itself refused: a bad API key,
// a plan that does not cover the resource, an unknown asset path. Retrying
// reproduces it exactly, so a caller holding a retry budget matches on it to stop
// on the first attempt instead of spending the whole budget on a fixed answer.
//
// Rate limiting is deliberately NOT this. A 429 is transient and worth retrying,
// so adapters must leave it unwrapped.
var ErrRequestRejected = errors.New("provider rejected the request")

// PriceData is a source-agnostic price point.
type PriceData struct {
	SourceAssetID string
	PriceUSD      float64
	MarketCapUSD  *float64
	Timestamp     time.Time
}

// VolumeData is a source-agnostic volume point.
type VolumeData struct {
	SourceAssetID string
	VolumeUSD     float64
	Timestamp     time.Time // Hourly
}

// HistoricalData contains both price and volume time series.
type HistoricalData struct {
	SourceAssetID string
	Prices        []PricePoint
	Volumes       []VolumePoint
	MarketCaps    []MarketCapPoint
}

// PricePoint represents a single price data point in a time series.
type PricePoint struct {
	Timestamp time.Time
	PriceUSD  float64
}

// VolumePoint represents a single volume data point in a time series.
type VolumePoint struct {
	Timestamp time.Time
	VolumeUSD float64
}

// MarketCapPoint represents a single market cap data point in a time series.
type MarketCapPoint struct {
	Timestamp    time.Time
	MarketCapUSD float64
}

// PriceProvider is the interface for any price data source.
type PriceProvider interface {
	// Name returns the provider name (e.g., "coingecko").
	Name() string

	// SupportsHistorical returns true if the provider supports historical data fetching.
	SupportsHistorical() bool

	// GetCurrentPrices fetches current prices for given asset IDs.
	GetCurrentPrices(ctx context.Context, assetIDs []string) ([]PriceData, error)

	// GetHistoricalData fetches historical prices AND volumes for a single asset.
	// Important: For accurate data, fetch in 30-day chunks (CoinGecko reduces resolution for larger ranges).
	GetHistoricalData(ctx context.Context, assetID string, from, to time.Time) (*HistoricalData, error)
}

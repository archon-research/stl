package entity

import (
	"fmt"
	"time"
)

// PriceSource represents a price data provider (CoinGecko, Chainlink, etc.)
type PriceSource struct {
	ID                 int64
	Name               string
	DisplayName        string
	BaseURL            string
	RateLimitPerMin    int
	SupportsHistorical bool
	Enabled            bool
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// NewPriceSource creates a new PriceSource entity with validation.
func NewPriceSource(id int64, name, displayName, baseURL string, rateLimitPerMin int, supportsHistorical, enabled bool) (*PriceSource, error) {
	ps := &PriceSource{
		ID:                 id,
		Name:               name,
		DisplayName:        displayName,
		BaseURL:            baseURL,
		RateLimitPerMin:    rateLimitPerMin,
		SupportsHistorical: supportsHistorical,
		Enabled:            enabled,
		CreatedAt:          time.Now(),
		UpdatedAt:          time.Now(),
	}
	if err := ps.Validate(); err != nil {
		return nil, fmt.Errorf("NewPriceSource: %w", err)
	}
	return ps, nil
}

func (ps *PriceSource) Validate() error {
	if ps.ID <= 0 {
		return fmt.Errorf("id must be positive, got %d", ps.ID)
	}
	if ps.Name == "" {
		return fmt.Errorf("name must not be empty")
	}
	if ps.DisplayName == "" {
		return fmt.Errorf("displayName must not be empty")
	}
	return nil
}

// PriceAsset represents a tracked asset for a specific source.
//
// Exactly one of two identities is valid: TokenID set (prices go to
// offchain_token_price), or OffchainOnly true (no token exists by design —
// XRP, HYPE, native BTC/SOL — prices go to offchain_asset_price). TokenID nil
// with OffchainOnly false is a configuration defect: the original catalog seed
// resolved token ids by symbol match, so a mismatch leaves TokenID nil by
// accident, and treating that as "offchain asset" would bury its prices in a
// table nothing reads.
type PriceAsset struct {
	ID            int64
	SourceID      int64
	SourceAssetID string
	TokenID       *int64
	OffchainOnly  bool
	Name          string
	Symbol        string
	Enabled       bool
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// NewPriceAsset creates a new PriceAsset entity with validation.
func NewPriceAsset(id, sourceID int64, sourceAssetID string, tokenID *int64, name, symbol string, enabled bool) (*PriceAsset, error) {
	pa := &PriceAsset{
		ID:            id,
		SourceID:      sourceID,
		SourceAssetID: sourceAssetID,
		TokenID:       tokenID,
		Name:          name,
		Symbol:        symbol,
		Enabled:       enabled,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}
	if err := pa.Validate(); err != nil {
		return nil, fmt.Errorf("NewPriceAsset: %w", err)
	}
	return pa, nil
}

func (pa *PriceAsset) Validate() error {
	if pa.ID <= 0 {
		return fmt.Errorf("id must be positive, got %d", pa.ID)
	}
	if pa.SourceID <= 0 {
		return fmt.Errorf("sourceID must be positive, got %d", pa.SourceID)
	}
	if pa.SourceAssetID == "" {
		return fmt.Errorf("sourceAssetID must not be empty")
	}
	if pa.Name == "" {
		return fmt.Errorf("name must not be empty")
	}
	if pa.Symbol == "" {
		return fmt.Errorf("symbol must not be empty")
	}
	return nil
}

// AssetPrice stores price data for assets with no token row (XRP, HYPE, native
// BTC/SOL): keyed by the offchain_price_asset catalog row instead of a token_id,
// because these assets have no on-chain address to make a token from.
type AssetPrice struct {
	AssetID      int64
	SourceID     int16
	PriceUSD     float64
	MarketCapUSD *float64
	VolumeUSD    *float64
	Timestamp    time.Time
}

// NewAssetPrice creates a new AssetPrice entity with validation.
func NewAssetPrice(assetID int64, sourceID int16, priceUSD float64, marketCapUSD *float64, volumeUSD *float64, timestamp time.Time) (*AssetPrice, error) {
	ap := &AssetPrice{
		AssetID:      assetID,
		SourceID:     sourceID,
		PriceUSD:     priceUSD,
		MarketCapUSD: marketCapUSD,
		VolumeUSD:    volumeUSD,
		Timestamp:    timestamp,
	}
	if err := ap.Validate(); err != nil {
		return nil, fmt.Errorf("NewAssetPrice: %w", err)
	}
	return ap, nil
}

func (ap *AssetPrice) Validate() error {
	if ap.AssetID <= 0 {
		return fmt.Errorf("assetID must be positive, got %d", ap.AssetID)
	}
	if ap.SourceID <= 0 {
		return fmt.Errorf("sourceID must be positive, got %d", ap.SourceID)
	}
	if ap.PriceUSD < 0 {
		return fmt.Errorf("priceUSD must be non-negative, got %f", ap.PriceUSD)
	}
	if ap.Timestamp.IsZero() {
		return fmt.Errorf("timestamp must not be zero")
	}
	return nil
}

// TokenPrice stores price data for on-chain tokens.
type TokenPrice struct {
	TokenID      int64
	SourceID     int16
	PriceUSD     float64
	MarketCapUSD *float64
	VolumeUSD    *float64
	Timestamp    time.Time
}

// NewTokenPrice creates a new TokenPrice entity with validation.
func NewTokenPrice(tokenID int64, sourceID int16, priceUSD float64, marketCapUSD *float64, volumeUSD *float64, timestamp time.Time) (*TokenPrice, error) {
	tp := &TokenPrice{
		TokenID:      tokenID,
		SourceID:     sourceID,
		PriceUSD:     priceUSD,
		MarketCapUSD: marketCapUSD,
		VolumeUSD:    volumeUSD,
		Timestamp:    timestamp,
	}
	if err := tp.Validate(); err != nil {
		return nil, fmt.Errorf("NewTokenPrice: %w", err)
	}
	return tp, nil
}

func (tp *TokenPrice) Validate() error {
	if tp.TokenID <= 0 {
		return fmt.Errorf("tokenID must be positive, got %d", tp.TokenID)
	}
	if tp.SourceID <= 0 {
		return fmt.Errorf("sourceID must be positive, got %d", tp.SourceID)
	}
	if tp.PriceUSD < 0 {
		return fmt.Errorf("priceUSD must be non-negative, got %f", tp.PriceUSD)
	}
	if tp.Timestamp.IsZero() {
		return fmt.Errorf("timestamp must not be zero")
	}
	return nil
}

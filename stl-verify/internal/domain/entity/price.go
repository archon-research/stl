package entity

import (
	"fmt"
	"time"
)

// PriceSource represents a price data provider (CoinGecko, Chainlink, etc.)
type PriceSource struct {
	ID                 int64
	Name               string // "coingecko", "chainlink"
	DisplayName        string // "CoinGecko", "Chainlink"
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
	if err := ps.validate(); err != nil {
		return nil, err
	}
	return ps, nil
}

func (ps *PriceSource) validate() error {
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
type PriceAsset struct {
	ID            int64
	SourceID      int64
	SourceAssetID string // Source-specific ID (CoinGecko ID, Chainlink feed, etc.)
	TokenID       *int64 // Optional FK to Token table
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
	if err := pa.validate(); err != nil {
		return nil, err
	}
	return pa, nil
}

func (pa *PriceAsset) validate() error {
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

// TokenPrice stores price data for on-chain tokens.
type TokenPrice struct {
	ID            int64
	TokenID       int64   // FK to token table (required - on-chain only)
	ChainID       int     // Required
	Source        string  // Denormalized for query efficiency
	SourceAssetID string  // Source-specific identifier
	PriceUSD      float64 // Price in USD
	MarketCapUSD  *float64
	Timestamp     time.Time
	CreatedAt     time.Time
}

// NewTokenPrice creates a new TokenPrice entity with validation.
func NewTokenPrice(tokenID int64, chainID int, source, sourceAssetID string, priceUSD float64, marketCapUSD *float64, timestamp time.Time) (*TokenPrice, error) {
	tp := &TokenPrice{
		TokenID:       tokenID,
		ChainID:       chainID,
		Source:        source,
		SourceAssetID: sourceAssetID,
		PriceUSD:      priceUSD,
		MarketCapUSD:  marketCapUSD,
		Timestamp:     timestamp,
		CreatedAt:     time.Now(),
	}
	if err := tp.validate(); err != nil {
		return nil, err
	}
	return tp, nil
}

func (tp *TokenPrice) validate() error {
	if tp.TokenID <= 0 {
		return fmt.Errorf("tokenID must be positive, got %d", tp.TokenID)
	}
	if tp.ChainID <= 0 {
		return fmt.Errorf("chainID must be positive, got %d", tp.ChainID)
	}
	if tp.Source == "" {
		return fmt.Errorf("source must not be empty")
	}
	if tp.SourceAssetID == "" {
		return fmt.Errorf("sourceAssetID must not be empty")
	}
	if tp.PriceUSD < 0 {
		return fmt.Errorf("priceUSD must be non-negative, got %f", tp.PriceUSD)
	}
	if tp.Timestamp.IsZero() {
		return fmt.Errorf("timestamp must not be zero")
	}
	return nil
}

// TokenVolume stores hourly trading volume data for on-chain tokens.
type TokenVolume struct {
	ID            int64
	TokenID       int64   // Required
	ChainID       int     // Required
	Source        string  // Denormalized
	SourceAssetID string  // Source-specific identifier
	VolumeUSD     float64 // Trading volume in USD
	Timestamp     time.Time
	CreatedAt     time.Time
}

// NewTokenVolume creates a new TokenVolume entity with validation.
func NewTokenVolume(tokenID int64, chainID int, source, sourceAssetID string, volumeUSD float64, timestamp time.Time) (*TokenVolume, error) {
	tv := &TokenVolume{
		TokenID:       tokenID,
		ChainID:       chainID,
		Source:        source,
		SourceAssetID: sourceAssetID,
		VolumeUSD:     volumeUSD,
		Timestamp:     timestamp,
		CreatedAt:     time.Now(),
	}
	if err := tv.validate(); err != nil {
		return nil, err
	}
	return tv, nil
}

func (tv *TokenVolume) validate() error {
	if tv.TokenID <= 0 {
		return fmt.Errorf("tokenID must be positive, got %d", tv.TokenID)
	}
	if tv.ChainID <= 0 {
		return fmt.Errorf("chainID must be positive, got %d", tv.ChainID)
	}
	if tv.Source == "" {
		return fmt.Errorf("source must not be empty")
	}
	if tv.SourceAssetID == "" {
		return fmt.Errorf("sourceAssetID must not be empty")
	}
	if tv.VolumeUSD < 0 {
		return fmt.Errorf("volumeUSD must be non-negative, got %f", tv.VolumeUSD)
	}
	if tv.Timestamp.IsZero() {
		return fmt.Errorf("timestamp must not be zero")
	}
	return nil
}

package entity

import (
	"fmt"
	"time"
)

// OracleSource represents an onchain oracle price provider (e.g., SparkLend).
type OracleSource struct {
	ID                  int64
	Name                string
	DisplayName         string
	ChainID             int
	PoolAddressProvider []byte // 20 bytes
	DeploymentBlock     int64
	Enabled             bool
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// OracleAsset represents a token tracked by an oracle source.
type OracleAsset struct {
	ID             int64
	OracleSourceID int64
	TokenID        int64
	Enabled        bool
	CreatedAt      time.Time
}

// OnchainTokenPrice stores an oracle price for a token at a specific block.
type OnchainTokenPrice struct {
	TokenID        int64
	OracleSourceID int16
	BlockNumber    int64
	BlockVersion   int16
	Timestamp      time.Time
	OracleAddress  []byte // 20 bytes
	PriceUSD       float64
}

// NewOnchainTokenPrice creates a new OnchainTokenPrice entity with validation.
func NewOnchainTokenPrice(tokenID int64, oracleSourceID int16, blockNumber int64, blockVersion int16, timestamp time.Time, oracleAddress []byte, priceUSD float64) (*OnchainTokenPrice, error) {
	p := &OnchainTokenPrice{
		TokenID:        tokenID,
		OracleSourceID: oracleSourceID,
		BlockNumber:    blockNumber,
		BlockVersion:   blockVersion,
		Timestamp:      timestamp,
		OracleAddress:  oracleAddress,
		PriceUSD:       priceUSD,
	}
	if err := p.validate(); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *OnchainTokenPrice) validate() error {
	if p.TokenID <= 0 {
		return fmt.Errorf("tokenID must be positive, got %d", p.TokenID)
	}
	if p.OracleSourceID <= 0 {
		return fmt.Errorf("oracleSourceID must be positive, got %d", p.OracleSourceID)
	}
	if p.BlockNumber <= 0 {
		return fmt.Errorf("blockNumber must be positive, got %d", p.BlockNumber)
	}
	if p.Timestamp.IsZero() {
		return fmt.Errorf("timestamp must not be zero")
	}
	if len(p.OracleAddress) != 20 {
		return fmt.Errorf("invalid oracle address length: expected 20, got %d", len(p.OracleAddress))
	}
	if p.PriceUSD < 0 {
		return fmt.Errorf("priceUSD must be non-negative, got %f", p.PriceUSD)
	}
	return nil
}

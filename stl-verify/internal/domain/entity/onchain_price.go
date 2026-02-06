package entity

import (
	"fmt"
	"time"
)

// Oracle represents an onchain oracle price provider (e.g., SparkLend).
type Oracle struct {
	ID              int64
	Name            string
	DisplayName     string
	ChainID         int
	Address         [20]byte
	DeploymentBlock int64
	Enabled         bool
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// OracleAsset represents a token tracked by an oracle.
type OracleAsset struct {
	ID        int64
	OracleID  int64
	TokenID   int64
	Enabled   bool
	CreatedAt time.Time
}

// OnchainTokenPrice stores an oracle price for a token at a specific block.
type OnchainTokenPrice struct {
	TokenID      int64
	OracleID     int16
	BlockNumber  int64
	BlockVersion int16
	Timestamp    time.Time
	PriceUSD     float64
}

// NewOnchainTokenPrice creates a new OnchainTokenPrice entity with validation.
func NewOnchainTokenPrice(tokenID int64, oracleID int16, blockNumber int64, blockVersion int16, timestamp time.Time, priceUSD float64) (*OnchainTokenPrice, error) {
	p := &OnchainTokenPrice{
		TokenID:      tokenID,
		OracleID:     oracleID,
		BlockNumber:  blockNumber,
		BlockVersion: blockVersion,
		Timestamp:    timestamp,
		PriceUSD:     priceUSD,
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
	if p.OracleID <= 0 {
		return fmt.Errorf("oracleID must be positive, got %d", p.OracleID)
	}
	if p.BlockNumber <= 0 {
		return fmt.Errorf("blockNumber must be positive, got %d", p.BlockNumber)
	}
	if p.Timestamp.IsZero() {
		return fmt.Errorf("timestamp must not be zero")
	}
	if p.PriceUSD < 0 {
		return fmt.Errorf("priceUSD must be non-negative, got %f", p.PriceUSD)
	}
	return nil
}

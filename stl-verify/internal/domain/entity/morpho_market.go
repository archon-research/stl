package entity

import (
	"fmt"
	"math/big"
)

// MorphoMarket represents a Morpho Blue isolated market.
type MorphoMarket struct {
	ID                int64
	ProtocolID        int64
	MarketID          []byte // 32-byte keccak256 hash
	LoanTokenID       int64
	CollateralTokenID int64
	OracleAddress     []byte   // 20 bytes
	IrmAddress        []byte   // 20 bytes (interest rate model)
	LLTV              *big.Int // liquidation loan-to-value (scaled by 1e18)
	CreatedAtBlock    int64
}

// NewMorphoMarket creates a new MorphoMarket entity with validation.
func NewMorphoMarket(protocolID int64, marketID []byte, loanTokenID, collateralTokenID int64, oracleAddress, irmAddress []byte, lltv *big.Int, createdAtBlock int64) (*MorphoMarket, error) {
	m := &MorphoMarket{
		ProtocolID:        protocolID,
		MarketID:          marketID,
		LoanTokenID:       loanTokenID,
		CollateralTokenID: collateralTokenID,
		OracleAddress:     oracleAddress,
		IrmAddress:        irmAddress,
		LLTV:              lltv,
		CreatedAtBlock:    createdAtBlock,
	}
	if err := m.validate(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *MorphoMarket) validate() error {
	if m.ProtocolID <= 0 {
		return fmt.Errorf("protocolID must be positive, got %d", m.ProtocolID)
	}
	if len(m.MarketID) != 32 {
		return fmt.Errorf("marketID must be 32 bytes, got %d", len(m.MarketID))
	}
	if m.LoanTokenID <= 0 {
		return fmt.Errorf("loanTokenID must be positive, got %d", m.LoanTokenID)
	}
	if m.CollateralTokenID <= 0 {
		return fmt.Errorf("collateralTokenID must be positive, got %d", m.CollateralTokenID)
	}
	if len(m.OracleAddress) != 20 {
		return fmt.Errorf("oracleAddress must be 20 bytes, got %d", len(m.OracleAddress))
	}
	if len(m.IrmAddress) != 20 {
		return fmt.Errorf("irmAddress must be 20 bytes, got %d", len(m.IrmAddress))
	}
	if m.LLTV == nil {
		return fmt.Errorf("lltv must not be nil")
	}
	if m.LLTV.Sign() < 0 {
		return fmt.Errorf("lltv must be non-negative")
	}
	if m.CreatedAtBlock <= 0 {
		return fmt.Errorf("createdAtBlock must be positive, got %d", m.CreatedAtBlock)
	}
	return nil
}

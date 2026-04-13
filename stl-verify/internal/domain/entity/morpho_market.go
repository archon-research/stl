package entity

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// MorphoMarket represents a Morpho Blue isolated market.
type MorphoMarket struct {
	ID                int64
	ChainID           int64
	ProtocolID        int64
	MarketID          common.Hash
	LoanTokenID       int64
	CollateralTokenID int64
	OracleAddress     common.Address
	IrmAddress        common.Address
	LLTV              *big.Int
	CreatedAtBlock    int64
}

// NewMorphoMarket creates a new MorphoMarket entity with validation.
func NewMorphoMarket(chainID, protocolID int64, marketID common.Hash, loanTokenID, collateralTokenID int64, oracleAddress, irmAddress common.Address, lltv *big.Int, createdAtBlock int64) (*MorphoMarket, error) {
	m := &MorphoMarket{
		ChainID:           chainID,
		ProtocolID:        protocolID,
		MarketID:          marketID,
		LoanTokenID:       loanTokenID,
		CollateralTokenID: collateralTokenID,
		OracleAddress:     oracleAddress,
		IrmAddress:        irmAddress,
		LLTV:              lltv,
		CreatedAtBlock:    createdAtBlock,
	}
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *MorphoMarket) Validate() error {
	if m.ChainID <= 0 {
		return fmt.Errorf("chainID must be positive, got %d", m.ChainID)
	}
	if m.ProtocolID <= 0 {
		return fmt.Errorf("protocolID must be positive, got %d", m.ProtocolID)
	}
	if m.MarketID == (common.Hash{}) {
		return fmt.Errorf("marketID must not be empty")
	}
	if m.LoanTokenID <= 0 {
		return fmt.Errorf("loanTokenID must be positive, got %d", m.LoanTokenID)
	}
	if m.CollateralTokenID <= 0 {
		return fmt.Errorf("collateralTokenID must be positive, got %d", m.CollateralTokenID)
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

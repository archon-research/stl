package entity

import (
	"fmt"
	"math/big"
)

// SparkLendReserveData represents the state of a SparkLend reserve at a specific block.
type SparkLendReserveData struct {
	ID          int64
	ProtocolID  int64
	TokenID     int64
	BlockNumber int64
	// Reserve state
	Unbacked                *big.Int
	AccruedToTreasuryScaled *big.Int
	TotalAToken             *big.Int
	TotalStableDebt         *big.Int
	TotalVariableDebt       *big.Int
	// Interest rates (ray - 27 decimals)
	LiquidityRate           *big.Int
	VariableBorrowRate      *big.Int
	StableBorrowRate        *big.Int
	AverageStableBorrowRate *big.Int
	// Indexes (ray - 27 decimals)
	LiquidityIndex      *big.Int
	VariableBorrowIndex *big.Int
	// Timestamps
	LastUpdateTimestamp int64
}

// NewSparkLendReserveData creates a new SparkLendReserveData entity.
func NewSparkLendReserveData(id, protocolID, tokenID, blockNumber int64) (*SparkLendReserveData, error) {
	srd := &SparkLendReserveData{
		ID:          id,
		ProtocolID:  protocolID,
		TokenID:     tokenID,
		BlockNumber: blockNumber,
	}
	if err := srd.validate(); err != nil {
		return nil, err
	}
	return srd, nil
}

// validate checks that all fields have valid values.
func (srd *SparkLendReserveData) validate() error {
	if srd.ID <= 0 {
		return fmt.Errorf("id must be positive, got %d", srd.ID)
	}
	if srd.ProtocolID <= 0 {
		return fmt.Errorf("protocolID must be positive, got %d", srd.ProtocolID)
	}
	if srd.TokenID <= 0 {
		return fmt.Errorf("tokenID must be positive, got %d", srd.TokenID)
	}
	if srd.BlockNumber <= 0 {
		return fmt.Errorf("blockNumber must be positive, got %d", srd.BlockNumber)
	}
	return nil
}

// WithRates sets the interest rates on the reserve data.
// Note: This mutates the receiver and returns it for chaining.
func (s *SparkLendReserveData) WithRates(liquidityRate, variableBorrowRate, stableBorrowRate, avgStableBorrowRate *big.Int) *SparkLendReserveData {
	s.LiquidityRate = liquidityRate
	s.VariableBorrowRate = variableBorrowRate
	s.StableBorrowRate = stableBorrowRate
	s.AverageStableBorrowRate = avgStableBorrowRate
	return s
}

// WithIndexes sets the indexes on the reserve data.
// Note: This mutates the receiver and returns it for chaining.
func (s *SparkLendReserveData) WithIndexes(liquidityIndex, variableBorrowIndex *big.Int) *SparkLendReserveData {
	s.LiquidityIndex = liquidityIndex
	s.VariableBorrowIndex = variableBorrowIndex
	return s
}

// WithTotals sets the total amounts on the reserve data.
// Note: This mutates the receiver and returns it for chaining.
func (s *SparkLendReserveData) WithTotals(unbacked, accruedToTreasury, totalAToken, totalStableDebt, totalVariableDebt *big.Int) *SparkLendReserveData {
	s.Unbacked = unbacked
	s.AccruedToTreasuryScaled = accruedToTreasury
	s.TotalAToken = totalAToken
	s.TotalStableDebt = totalStableDebt
	s.TotalVariableDebt = totalVariableDebt
	return s
}

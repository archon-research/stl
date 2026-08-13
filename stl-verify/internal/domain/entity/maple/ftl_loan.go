package maple

import "fmt"

// FTLLoan represents a Maple fixed-term loan contract (registry row, one per
// loan). Unlike the open-term Loan, an FTL's collateral and funds are mainnet
// ERC-20s, so both are referenced by token-registry id. Snapshot data
// (balances, terms, state) lives in FTLLoanState.
type FTLLoan struct {
	ChainID           int64
	ProtocolID        int64
	LoanAddress       []byte // 20 bytes, loan.id
	PoolID            int64  // fundingPool (PoolV2) -> maple_pool.id
	BorrowerUserID    int64
	CollateralTokenID int64 // collateralAsset -> token.id
	FundsTokenID      int64 // liquidityAsset -> token.id
}

// NewFTLLoan creates a new FTLLoan entity with validation.
func NewFTLLoan(chainID, protocolID int64, loanAddress []byte, poolID, borrowerUserID, collateralTokenID, fundsTokenID int64) (*FTLLoan, error) {
	l := &FTLLoan{
		ChainID:           chainID,
		ProtocolID:        protocolID,
		LoanAddress:       loanAddress,
		PoolID:            poolID,
		BorrowerUserID:    borrowerUserID,
		CollateralTokenID: collateralTokenID,
		FundsTokenID:      fundsTokenID,
	}
	if err := l.Validate(); err != nil {
		return nil, fmt.Errorf("NewFTLLoan: %w", err)
	}
	return l, nil
}

// Validate checks that all fields have valid values. fundsTokenID may equal
// collateralTokenID: Maple allows a loan funded and collateralized in the same
// asset, so that is not treated as an error.
func (l *FTLLoan) Validate() error {
	if l.ChainID <= 0 {
		return fmt.Errorf("chainID must be positive, got %d", l.ChainID)
	}
	if l.ProtocolID <= 0 {
		return fmt.Errorf("protocolID must be positive, got %d", l.ProtocolID)
	}
	if len(l.LoanAddress) != 20 {
		return fmt.Errorf("loanAddress must be 20 bytes, got %d", len(l.LoanAddress))
	}
	if l.PoolID <= 0 {
		return fmt.Errorf("maplePoolID must be positive, got %d", l.PoolID)
	}
	if l.BorrowerUserID <= 0 {
		return fmt.Errorf("borrowerUserID must be positive, got %d", l.BorrowerUserID)
	}
	if l.CollateralTokenID <= 0 {
		return fmt.Errorf("collateralTokenID must be positive, got %d", l.CollateralTokenID)
	}
	if l.FundsTokenID <= 0 {
		return fmt.Errorf("fundsTokenID must be positive, got %d", l.FundsTokenID)
	}
	return nil
}

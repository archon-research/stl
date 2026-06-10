package entity

import "fmt"

// MapleLoanMetaTypes that mark a loan as an internal Maple position.
const (
	MapleLoanMetaTypeAMM      = "amm"
	MapleLoanMetaTypeStrategy = "strategy"
)

// MapleLoanMeta describes an internal Maple position (DeFi strategy or AMM
// position). It is nil for external borrower loans. WalletAddress may be a
// non-EVM address (Base/Solana custody wallets), so it is kept as a string.
type MapleLoanMeta struct {
	Type          string // 'amm' | 'strategy'
	AssetSymbol   string
	Dex           string
	WalletAddress string
	WalletType    string
	Location      string
}

// MapleLoan represents a Maple Open Term Loan contract (registry row, one per
// loan). Snapshot data lives in MapleLoanState / MapleLoanCollateral.
type MapleLoan struct {
	ID             int64
	ChainID        int64
	ProtocolID     int64
	LoanAddress    []byte // 20 bytes, openTermLoan.id
	LoanType       string // 'OTL'
	MaplePoolID    int64
	BorrowerUserID int64
	LoanMeta       *MapleLoanMeta // nil for external loans
}

// NewMapleLoan creates a new MapleLoan entity with validation.
func NewMapleLoan(chainID, protocolID int64, loanAddress []byte, maplePoolID, borrowerUserID int64, loanMeta *MapleLoanMeta) (*MapleLoan, error) {
	l := &MapleLoan{
		ChainID:        chainID,
		ProtocolID:     protocolID,
		LoanAddress:    loanAddress,
		LoanType:       "OTL",
		MaplePoolID:    maplePoolID,
		BorrowerUserID: borrowerUserID,
		LoanMeta:       loanMeta,
	}
	if err := l.Validate(); err != nil {
		return nil, fmt.Errorf("NewMapleLoan: %w", err)
	}
	return l, nil
}

// Validate checks that all fields have valid values.
func (l *MapleLoan) Validate() error {
	if l.ChainID <= 0 {
		return fmt.Errorf("chainID must be positive, got %d", l.ChainID)
	}
	if l.ProtocolID <= 0 {
		return fmt.Errorf("protocolID must be positive, got %d", l.ProtocolID)
	}
	if len(l.LoanAddress) != 20 {
		return fmt.Errorf("loanAddress must be 20 bytes, got %d", len(l.LoanAddress))
	}
	if l.LoanType == "" {
		return fmt.Errorf("loanType must not be empty")
	}
	if l.MaplePoolID <= 0 {
		return fmt.Errorf("maplePoolID must be positive, got %d", l.MaplePoolID)
	}
	if l.BorrowerUserID <= 0 {
		return fmt.Errorf("borrowerUserID must be positive, got %d", l.BorrowerUserID)
	}
	if l.LoanMeta != nil && l.LoanMeta.Type == "" {
		return fmt.Errorf("loanMeta.type must not be empty when loanMeta is present")
	}
	return nil
}

// IsInternal reports whether the loan is an internal Maple position
// (loanMeta.type in {'amm', 'strategy'}), matching the generated
// is_internal column on maple_loan.
func (l *MapleLoan) IsInternal() bool {
	if l.LoanMeta == nil {
		return false
	}
	return l.LoanMeta.Type == MapleLoanMetaTypeAMM || l.LoanMeta.Type == MapleLoanMetaTypeStrategy
}

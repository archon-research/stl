package maple

import "fmt"

// LoanTypeOTL is the loan_type discriminator for Open Term Loans, the
// only Maple loan type indexed today.
const LoanTypeOTL = "OTL"

// LoanMeta carries Maple's loan metadata. The loan is an internal Maple
// position when Type is 'amm' or 'strategy'; live data also shows metadata
// with a null Type and other types ('tBills', 'intercompany'), all persisted
// raw. WalletAddress may be a non-EVM address (Base/Solana custody wallets),
// so it is kept as a string. Every field may be empty.
type LoanMeta struct {
	Type          string // '' | 'amm' | 'strategy' | 'tBills' | 'intercompany' | ...
	AssetSymbol   string
	DexName       string
	WalletAddress string
	WalletType    string
	Location      string
}

// Loan represents a Maple Open Term Loan contract (registry row, one per
// loan). Snapshot data lives in LoanState / LoanCollateral.
type Loan struct {
	ChainID        int64
	ProtocolID     int64
	LoanAddress    []byte // 20 bytes, openTermLoan.id
	LoanType       string // LoanTypeOTL
	PoolID         int64
	BorrowerUserID int64
	LoanMeta       *LoanMeta // nil for external loans
}

// NewLoan creates a new Loan entity with validation.
func NewLoan(chainID, protocolID int64, loanAddress []byte, maplePoolID, borrowerUserID int64, loanMeta *LoanMeta) (*Loan, error) {
	l := &Loan{
		ChainID:        chainID,
		ProtocolID:     protocolID,
		LoanAddress:    loanAddress,
		LoanType:       LoanTypeOTL,
		PoolID:         maplePoolID,
		BorrowerUserID: borrowerUserID,
		LoanMeta:       loanMeta,
	}
	if err := l.Validate(); err != nil {
		return nil, fmt.Errorf("NewLoan: %w", err)
	}
	return l, nil
}

// Validate checks that all fields have valid values.
func (l *Loan) Validate() error {
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
	if l.PoolID <= 0 {
		return fmt.Errorf("maplePoolID must be positive, got %d", l.PoolID)
	}
	if l.BorrowerUserID <= 0 {
		return fmt.Errorf("borrowerUserID must be positive, got %d", l.BorrowerUserID)
	}
	return nil
}

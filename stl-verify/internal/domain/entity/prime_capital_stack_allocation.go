package entity

import "time"

// PrimeCapitalStackAllocation is one row of a prime's per-allocation
// risk-capital breakdown as reported by Sky's Star monitor at a single sync
// cycle — the breakdown behind the totals in PrimeCapitalStackSnapshot.
//
// Amounts are decimal strings, not float64: they are USD figures carried to 18
// decimal places upstream, and a float would silently round them. They are
// already-normalized USD (not raw token-decimal integers), so nothing here is
// scaled by token.decimals. CRR is the upstream 0-1 fraction, not a percentage.
//
// Identity fields are upstream's claims verbatim, not registry references:
// the feed reports tokens STL does not index, and a reference row must stay
// traceable to what the feed said.
type PrimeCapitalStackAllocation struct {
	PrimeID  int64
	SyncedAt time.Time

	ProtocolName string
	Network      string
	// Nil for a network STL has no chain id for, which is a fact about the
	// mapping rather than missing data.
	ChainID *int64

	Symbol string
	// Nil when upstream omits it.
	Name         *string
	TokenAddress string
	// Nil when upstream omits them.
	LoanTokenAddress *string
	LoanTokenSymbol  *string

	ExposureUSD            string
	RequiredRiskCapitalUSD string
	CRR                    string // 0-1 fraction, not percent

	Source  string
	BuildID int
}

package entity

import "time"

// PrimeReferencePosition is one of a prime's balance-sheet positions as
// reported by Sky's internal feed at a single sync cycle.
//
// A different question from PrimeCapitalStackAllocation: that is the
// risk-capital breakdown, this is the balance sheet. The two are not
// interchangeable.
//
// Amounts are decimal strings, not float64: they are USD figures carried to 18
// decimal places upstream, and a float would silently round them. They are
// already-normalized USD (not raw token-decimal integers).
//
// Identity fields are upstream's claims verbatim, not registry references:
// the feed reports tokens STL does not index, and a reference row must stay
// traceable to what the feed said.
type PrimeReferencePosition struct {
	PrimeID  int64
	SyncedAt time.Time

	ProtocolName string
	Network      string
	// Nil for a network STL has no chain id for, which is a fact about the
	// mapping rather than missing data.
	ChainID *int64

	TokenSymbol string
	// Nil when upstream omits it.
	TokenName    *string
	TokenAddress string

	AssetsUSD string
	// Nil when upstream omits them, which is distinct from zero.
	AllocatedAssetsUSD *string
	IdleAssetsUSD      *string

	Source  string
	BuildID int
}

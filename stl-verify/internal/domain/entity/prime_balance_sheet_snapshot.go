package entity

import "time"

// PrimeBalanceSheetSnapshot is one prime's balance sheet on one day, as
// published by Sky.
//
// Amounts are decimal strings for the same reason as
// PrimeCapitalStackSnapshot: they are already-normalized USD carried to 18
// decimal places, and a float would round them.
type PrimeBalanceSheetSnapshot struct {
	PrimeID    int64
	ObservedAt time.Time

	TreasuryBalanceUSD string
	AssetsUSD          string
	AllocatedAssetsUSD string
	IdleAssetsUSD      string
	DebtUSD            string
	BackstopCapitalUSD string

	Source  string
	BuildID int
}

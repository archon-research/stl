package entity

import "time"

// PrimeCapitalStackSnapshot is one prime's risk capital as reported by Sky's
// Star monitor at a single sync cycle.
//
// Amounts are decimal strings, not float64: they are USD figures carried to 18
// decimal places upstream, and a float would silently round them. They are
// already-normalized USD (not raw token-decimal integers), so nothing here is
// scaled by token.decimals.
type PrimeCapitalStackSnapshot struct {
	PrimeID  int64
	SyncedAt time.Time

	ExposureUSD                   string
	RequiredRiskCapitalUSD        string
	TotalRiskCapitalUSD           string
	JuniorRiskCapitalUSD          string
	SeniorRiskCapitalUSD          string
	InternalJuniorRiskCapitalUSD  string
	ExternalJuniorRiskCapitalUSD  string
	TokenizedJuniorRiskCapitalUSD string
	InternalSeniorRiskCapitalUSD  string
	ExternalSeniorRiskCapitalUSD  string

	// Nil when the monitor omits it, which is distinct from a ratio of zero.
	EncumbranceRatio *string
	ExposureShare    string
	EPIUtilization   string
	SPJUtilization   string

	Source  string
	BuildID int
}

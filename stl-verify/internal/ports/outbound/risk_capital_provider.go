package outbound

import "context"

// RiskCapitalPrimeRow is one prime-level row from an upstream risk-capital source.
type RiskCapitalPrimeRow struct {
	PrimeName          string
	TotalRC            string
	FinancialRRC       string
	Exposure           string
	RiskToleranceRatio string
}

// RiskCapitalProvider fetches upstream prime-level risk-capital rows.
type RiskCapitalProvider interface {
	FetchPrimeRows(ctx context.Context) ([]RiskCapitalPrimeRow, error)
}

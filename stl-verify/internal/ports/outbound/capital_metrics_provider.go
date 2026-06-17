package outbound

import "context"

// RiskCapitalRow is one upstream risk-capital row from the Star monitor, keyed
// by star name. All amounts are decimal strings exactly as received upstream.
type RiskCapitalRow struct {
	Star               string
	Exposure           string
	TotalRC            string
	FinancialRRC       string
	ExposureShare      string
	RiskToleranceRatio string
}

// CapitalMetricsProvider fetches current per-prime capital metrics from an
// external upstream (the Star risk-capital monitor).
type CapitalMetricsProvider interface {
	Name() string
	FetchRiskCapital(ctx context.Context) ([]RiskCapitalRow, error)
}

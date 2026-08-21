package outbound

import "context"

// RiskCapitalPrimeSnapshot is one prime's risk-capital figures from an upstream
// monitor, as decimal strings exactly as reported.
//
// Parsing and rescaling belong to the consumer, not the transport: every field
// is stored raw so a change in upstream's encoding surfaces as a parse failure
// rather than as a silently wrong number.
type RiskCapitalPrimeSnapshot struct {
	Star string

	Exposure                   string
	RequiredRiskCapital        string
	TotalRiskCapital           string
	JuniorRiskCapital          string
	SeniorRiskCapital          string
	InternalJuniorRiskCapital  string
	ExternalJuniorRiskCapital  string
	TokenizedJuniorRiskCapital string
	InternalSeniorRiskCapital  string
	ExternalSeniorRiskCapital  string

	// Nil when the monitor omits it, which is distinct from a ratio of zero.
	EncumbranceRatio *string
	ExposureShare    string
	EPIUtilization   string
	SPJUtilization   string
}

// RiskCapitalProvider fetches prime-level risk-capital snapshots from an
// upstream monitor.
type RiskCapitalProvider interface {
	// FetchPrimeSnapshots returns a snapshot for each of `stars` the monitor
	// covers. The monitor reports primes STL does not track, so the caller names
	// the ones it wants rather than taking everything on offer. A requested star
	// the monitor does not cover is simply absent from the result; absence is a
	// statement about coverage and never means zero exposure.
	FetchPrimeSnapshots(ctx context.Context, stars []string) ([]RiskCapitalPrimeSnapshot, error)
}

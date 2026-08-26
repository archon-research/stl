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

// RiskCapitalAllocationRow is one row of a prime's per-allocation risk-capital
// breakdown from an upstream monitor, as decimal strings exactly as reported.
//
// Parsing and rescaling belong to the consumer, not the transport: every figure
// is carried raw so a change in upstream's encoding surfaces as a parse failure
// rather than as a silently wrong number. CRR is the upstream 0-1 fraction.
type RiskCapitalAllocationRow struct {
	Star string

	Protocol string
	Network  string
	// Nil for a network the vendor vocabulary has no chain id for.
	ChainID *int64

	Symbol string
	// Nil when the monitor omits them.
	Name             *string
	TokenAddress     string
	LoanTokenAddress *string
	LoanTokenSymbol  *string

	Exposure            string
	RequiredRiskCapital string
	CRR                 string
}

// RiskCapitalAllocationProvider fetches the per-allocation breakdown behind the
// prime-level snapshots.
type RiskCapitalAllocationProvider interface {
	// FetchPrimeAllocations returns the breakdown rows for each of `stars`.
	// Unlike FetchPrimeSnapshots, the caller must pass only stars the monitor
	// covers (taken from the same cycle's snapshots): the breakdown route
	// answers an unknown star with a 500 indistinguishable from a fault.
	// A covered star may legitimately return zero rows only when its exposure
	// is zero; the caller owns that cross-check because exposure arrives on the
	// snapshot, not here.
	FetchPrimeAllocations(ctx context.Context, stars []string) ([]RiskCapitalAllocationRow, error)
}

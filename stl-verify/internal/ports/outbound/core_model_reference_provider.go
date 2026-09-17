package outbound

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// CoreModelReferenceMarketRow is one market's row of the upstream CORE overview, as
// decimal strings exactly as reported.
//
// Parsing and rescaling belong to the consumer, not the transport: every figure
// is carried raw so a change in upstream's encoding surfaces as a parse failure
// rather than as a silently wrong number. CRRs and ProbNoBadDebt are upstream's
// 0-1 fractions. Date is upstream's calendar day as an ISO string.
type CoreModelReferenceMarketRow struct {
	Network string
	// Nil for a network the vendor vocabulary has no chain id for.
	ChainID      *int64
	Protocol     string
	MarketUID    string
	MarketSymbol string

	LoanTokenSymbol  string
	LoanTokenAddress string

	Date string

	NScenarios           int
	HorizonDays          int
	EffectiveHorizonDays int

	TotalSupply   string
	ProbNoBadDebt string
	CRREL         string
	CRRVaR        string
	CRRES         string
	CRRELSE       string
	CRRVaRSE      string
	CRRESSE       string
	CRRFloor      string

	ExternalFlowEnabled bool
}

// CoreModelReferenceVaultRow is one vault's row of the upstream CORE overview, as
// decimal strings exactly as reported. Same encoding rules as the market row.
// CRRELSE and CRRES are nil for a Method of "override", which has no
// simulation behind it; the provider rejects their absence on any other method.
type CoreModelReferenceVaultRow struct {
	Network      string
	ChainID      *int64
	Protocol     string
	VaultAddress string
	VaultSymbol  string
	VaultName    string
	Version      string

	LoanTokenSymbol  string
	LoanTokenAddress string
	Method           string

	Date string

	NMarkets    int
	TotalAssets string
	IdleAssets  string
	CRREL       string
	CRRELSE     *string
	CRRES       *string
}

// CoreModelReferenceOverview is everything one read of the upstream overview
// reported, gathered so a cycle persists markets and vaults from the same
// observation.
type CoreModelReferenceOverview struct {
	Markets []CoreModelReferenceMarketRow
	Vaults  []CoreModelReferenceVaultRow
}

// CoreModelReferenceProvider fetches the current CORE model results the upstream
// dashboard publishes.
type CoreModelReferenceProvider interface {
	// FetchOverview returns every market and vault the dashboard reports today.
	// The feed publishes one result per calendar day and answers a date
	// parameter by ignoring it, so this is always "now"; history is the
	// caller's to accumulate.
	FetchOverview(ctx context.Context) (CoreModelReferenceOverview, error)
}

// CoreModelReferenceMarketResultRepository persists market rows of a cycle.
type CoreModelReferenceMarketResultRepository interface {
	// SaveMarketResults writes within the caller's transaction, so the caller
	// controls what else commits or rolls back with it.
	SaveMarketResults(ctx context.Context, tx pgx.Tx, results []entity.CoreModelReferenceMarketResult) error
}

// CoreModelReferenceVaultResultRepository persists vault rows of a cycle.
type CoreModelReferenceVaultResultRepository interface {
	// SaveVaultResults writes within the caller's transaction, like
	// SaveMarketResults, so a cycle's markets and vaults land together.
	SaveVaultResults(ctx context.Context, tx pgx.Tx, results []entity.CoreModelReferenceVaultResult) error
}

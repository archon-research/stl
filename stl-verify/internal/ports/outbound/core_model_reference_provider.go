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
// CRRELSE and CRRES are nil exactly when Method is "override", which has no
// simulation behind it; the provider rejects a row that breaks that in either
// direction.
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

// RowRejection is one upstream row the provider refused to carry: a field
// missing, a figure out of range, or a figure the row's method forbids. The row
// is dropped from the cycle and reported here so the rest of the cycle still
// lands and the loss is counted rather than silent.
type RowRejection struct {
	// Kind is "market" or "vault".
	Kind string
	// Identity is the row as upstream spelled it (network/protocol/uid), or
	// its index when the identity fields are what is missing.
	Identity string
	Reason   string
}

// CoreModelReferenceOverview is everything one read of the upstream overview
// reported, gathered so a cycle persists markets and vaults from the same
// observation. Rejected lists the rows that were dropped on the way.
type CoreModelReferenceOverview struct {
	Markets  []CoreModelReferenceMarketRow
	Vaults   []CoreModelReferenceVaultRow
	Rejected []RowRejection
}

// CoreModelReferenceProvider fetches the current CORE model results the upstream
// dashboard publishes.
type CoreModelReferenceProvider interface {
	// FetchOverview returns every market and vault the dashboard reports today.
	// The feed publishes one result per calendar day and answers a date
	// parameter by ignoring it, so this is always "now"; history is the
	// caller's to accumulate. A row that fails validation is returned under
	// Rejected instead of failing the fetch; only a transport fault, a failed
	// envelope or a duplicate identity fails it.
	FetchOverview(ctx context.Context) (CoreModelReferenceOverview, error)
}

// CoreModelReferenceMarketResultRepository persists market rows of a cycle.
type CoreModelReferenceMarketResultRepository interface {
	// SaveMarketResults writes within the caller's transaction, so the caller
	// controls what else commits or rolls back with it. It returns how many rows
	// the database actually inserted; a row whose identity and synced_at were
	// already written under the same build conflicts away and is not counted.
	SaveMarketResults(ctx context.Context, tx pgx.Tx, results []entity.CoreModelReferenceMarketResult) (inserted int, err error)
}

// CoreModelReferenceVaultResultRepository persists vault rows of a cycle.
type CoreModelReferenceVaultResultRepository interface {
	// SaveVaultResults writes within the caller's transaction, like
	// SaveMarketResults, so a cycle's markets and vaults land together, and
	// returns the inserted count under the same rule.
	SaveVaultResults(ctx context.Context, tx pgx.Tx, results []entity.CoreModelReferenceVaultResult) (inserted int, err error)
}

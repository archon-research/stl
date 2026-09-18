package entity

import "time"

// CoreModelReferenceDataSource is the provenance slug on every row STL records from
// the upstream CORE model dashboard feed.
//
// Distinct from ReferenceDataSource on purpose: Sky's Star monitor and this
// feed are two different upstream models answering two different questions,
// and the API serves them under two provenances.
const CoreModelReferenceDataSource = "coremodel:dashboard"

// CoreModelReferenceMarketResult is one market's CORE model result as the upstream
// dashboard reported it at a single sync cycle.
//
// Figures are decimal strings, not float64: upstream carries them to 18
// decimal places and a float would silently round them. USD fields are
// already-normalized USD, not raw token-decimal integers. CRRs and
// ProbNoBadDebt are upstream's 0-1 fractions, not percentages; CRRFloor is
// kept apart from CRREL and never pre-added.
//
// Identity fields are upstream's claims verbatim, not registry references: the
// feed covers networks and markets STL does not index, and a reference row
// must stay traceable to what the feed said.
type CoreModelReferenceMarketResult struct {
	Network string
	// Nil for a network STL has no chain id for, which is a fact about the
	// mapping rather than missing data.
	ChainID      *int64
	ProtocolName string
	MarketUID    string
	MarketSymbol string

	LoanTokenSymbol  string
	LoanTokenAddress string

	// ModelDate is the calendar day of the upstream run; SyncedAt is when STL
	// observed it. They differ whenever the feed is read more than once a day.
	ModelDate time.Time
	SyncedAt  time.Time

	NScenarios           int
	HorizonDays          int
	EffectiveHorizonDays int

	TotalSupplyUSD string
	ProbNoBadDebt  string
	CRREL          string
	CRRVaR         string
	CRRES          string
	CRRELSE        string
	CRRVaRSE       string
	CRRESSE        string
	CRRFloor       string

	ExternalFlowEnabled bool

	Source  string
	BuildID int
}

// CoreModelReferenceVaultResult is one vault's CORE model result as the upstream
// dashboard reported it at a single sync cycle: the vault-level aggregate of
// the market rows written by the same cycle.
//
// Same encoding rules as CoreModelReferenceMarketResult. Method says how upstream
// produced the figure ("model" or "override"); an override row's CRR is a
// governance-set constant, not a model output, so it carries no standard error
// and no expected shortfall — CRRELSE and CRRES are nil exactly then, and the
// provider and the table CHECK both enforce the two directions.
type CoreModelReferenceVaultResult struct {
	Network      string
	ChainID      *int64
	ProtocolName string
	VaultAddress string
	VaultSymbol  string
	VaultName    string
	VersionLabel string

	LoanTokenSymbol  string
	LoanTokenAddress string
	Method           string

	ModelDate time.Time
	SyncedAt  time.Time

	NMarkets       int
	TotalAssetsUSD string
	IdleAssetsUSD  string
	CRREL          string
	CRRELSE        *string
	CRRES          *string

	Source  string
	BuildID int
}

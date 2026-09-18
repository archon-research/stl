// Package core_model_reference_indexer accumulates the upstream CORE model's
// results as a reference time series.
//
// The upstream dashboard publishes one result per market per calendar day and
// no way to ask for a past one, so a reference figure can never be
// reconstructed for a day that was not observed. This service observes the
// overview each cycle and appends what it saw.
package core_model_reference_indexer

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// A row is stale when its model_date is further behind the cycle's UTC day
// than this. One day of slack: upstream runs shortly after midnight UTC, so a
// cycle early in the day legitimately still sees yesterday's date.
const freshnessAllowanceDays = 1

// Clock returns the current time; injected so a cycle's synced_at is testable.
type Clock func() time.Time

// Service fetches CORE reference results and persists them.
type Service struct {
	deps      Deps
	buildID   int
	now       Clock
	telemetry *Telemetry
	logger    *slog.Logger
}

// Deps holds the service's ports. Named fields keep the two repositories,
// which take the same argument shapes, from being swapped at the call site.
type Deps struct {
	Provider   outbound.CoreModelReferenceProvider
	MarketRepo outbound.CoreModelReferenceMarketResultRepository
	VaultRepo  outbound.CoreModelReferenceVaultResultRepository
	// TxManager coordinates markets and vaults in one transaction: the two
	// join exactly on synced_at, so they must land together or not at all.
	TxManager outbound.TxManager
}

// NewService creates a CORE reference indexer.
func NewService(
	deps Deps,
	buildID int,
	now Clock,
	telemetry *Telemetry,
	logger *slog.Logger,
) (*Service, error) {
	// A forgotten Deps field would otherwise nil-deref mid-cycle; misconstruction
	// must die at wiring time instead.
	if err := deps.validate(); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}
	if now == nil {
		now = time.Now
	}
	return &Service{
		deps:      deps,
		buildID:   buildID,
		now:       now,
		telemetry: telemetry,
		logger:    logger.With("component", "core-model-reference-indexer"),
	}, nil
}

func (d Deps) validate() error {
	missing := []string{}
	for _, port := range []struct {
		name string
		set  bool
	}{
		{"Provider", d.Provider != nil},
		{"MarketRepo", d.MarketRepo != nil},
		{"VaultRepo", d.VaultRepo != nil},
		{"TxManager", d.TxManager != nil},
	} {
		if !port.set {
			missing = append(missing, port.name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("core model reference indexer wired without: %s", strings.Join(missing, ", "))
	}
	return nil
}

// Run observes the upstream overview once and appends what it reported.
func (s *Service) Run(ctx context.Context) error {
	overview, err := s.observeUpstream(ctx)
	if err != nil {
		return err
	}

	// One timestamp per cycle, shared by every table it writes, so the market
	// rows and their vault aggregates join exactly on synced_at.
	syncedAt := s.now().UTC()

	markets, err := s.toMarketResults(overview.Markets, syncedAt)
	if err != nil {
		return err
	}
	vaults, err := s.toVaultResults(overview.Vaults, syncedAt)
	if err != nil {
		return err
	}

	inserted, err := s.persistCycle(ctx, markets, vaults)
	if err != nil {
		return err
	}
	s.reportRejections(ctx, overview.Rejected)
	s.reportUnmappedNetworks(ctx, markets, vaults)
	s.reportStaleness(ctx, markets, vaults, syncedAt)

	s.logger.Info("core reference sync complete",
		"markets", len(markets), "vaults", len(vaults),
		"markets_inserted", inserted.markets, "vaults_inserted", inserted.vaults,
		"rejected", len(overview.Rejected))
	return nil
}

// observeUpstream reads the overview and rejects an empty half. The model
// covers dozens of markets and several vaults today, so covering none — with
// or without rejected rows — means the feed broke or its shape drifted. That
// must not read as "nothing to do", which would leave a silent hole in the
// series.
func (s *Service) observeUpstream(ctx context.Context) (outbound.CoreModelReferenceOverview, error) {
	overview, err := s.deps.Provider.FetchOverview(ctx)
	if err != nil {
		return outbound.CoreModelReferenceOverview{}, fmt.Errorf("fetching core overview: %w", err)
	}
	if len(overview.Markets) == 0 {
		return outbound.CoreModelReferenceOverview{}, fmt.Errorf("core overview reported no markets (%d rows rejected)", len(overview.Rejected))
	}
	if len(overview.Vaults) == 0 {
		return outbound.CoreModelReferenceOverview{}, fmt.Errorf("core overview reported no vaults (%d rows rejected)", len(overview.Rejected))
	}
	return overview, nil
}

// insertedCounts is what the database actually inserted in one cycle.
type insertedCounts struct{ markets, vaults int }

// persistCycle saves markets and vaults in one transaction: they promise to
// join exactly on synced_at, every table is append-only and a retry stamps a
// fresh synced_at, so a partial commit would strand a permanent half-cycle no
// retry repairs. The written counters take the inserted counts, not the batch
// sizes: a cycle whose rows all conflicted away (a clock stepping back onto an
// already-written synced_at) must read as zero, or WritesZero cannot see it.
func (s *Service) persistCycle(
	ctx context.Context,
	markets []entity.CoreModelReferenceMarketResult,
	vaults []entity.CoreModelReferenceVaultResult,
) (insertedCounts, error) {
	var inserted insertedCounts
	err := s.deps.TxManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		n, err := s.deps.MarketRepo.SaveMarketResults(ctx, tx, markets)
		if err != nil {
			return fmt.Errorf("saving core market results: %w", err)
		}
		inserted.markets = n
		n, err = s.deps.VaultRepo.SaveVaultResults(ctx, tx, vaults)
		if err != nil {
			return fmt.Errorf("saving core vault results: %w", err)
		}
		inserted.vaults = n
		return nil
	})
	if err != nil {
		return insertedCounts{}, err
	}
	s.telemetry.RecordMarketsWritten(ctx, inserted.markets)
	s.telemetry.RecordVaultsWritten(ctx, inserted.vaults)
	if inserted.markets < len(markets) || inserted.vaults < len(vaults) {
		s.logger.Warn("some rows of this cycle were already written under this build and conflicted away",
			"markets_submitted", len(markets), "markets_inserted", inserted.markets,
			"vaults_submitted", len(vaults), "vaults_inserted", inserted.vaults)
	}
	return inserted, nil
}

// reportRejections surfaces the rows the provider dropped. Each is a day of
// that market or vault the series will never get back, so every one is logged
// with its reason and counted for the alert; the cycle itself is not failed,
// because failing it would lose the other rows too.
func (s *Service) reportRejections(ctx context.Context, rejected []outbound.RowRejection) {
	for _, r := range rejected {
		s.logger.Warn("upstream row rejected; its result for this cycle is lost",
			"kind", r.Kind, "identity", r.Identity, "reason", r.Reason)
	}
	s.telemetry.RecordRowsRejected(ctx, len(rejected))
}

// reportUnmappedNetworks surfaces rows stored with a NULL chain id. The row is
// correct as recorded — the network is upstream's claim — but a read-time
// registry join drops it, so the map must be extended and nothing else
// notices: the write succeeds and every counter advances.
func (s *Service) reportUnmappedNetworks(
	ctx context.Context,
	markets []entity.CoreModelReferenceMarketResult,
	vaults []entity.CoreModelReferenceVaultResult,
) {
	// Keyed on the folded name the client's lookup missed on, so one gap in the
	// map is one series and one alert whatever casing upstream used, and the
	// label is the key an operator will type into the map.
	unmapped := map[string]int{}
	for _, m := range markets {
		if m.ChainID == nil {
			unmapped[strings.ToLower(m.Network)]++
		}
	}
	for _, v := range vaults {
		if v.ChainID == nil {
			unmapped[strings.ToLower(v.Network)]++
		}
	}
	for network, count := range unmapped {
		s.logger.Warn("network has no chain id in the feed client's map; rows stored with chain_id NULL",
			"network", network, "rows", count)
		s.telemetry.RecordUnmappedNetworkRows(ctx, network, count)
	}
}

// reportStaleness measures how far upstream's run day has fallen behind the
// cycle. Staleness is not an error — the rows are what upstream reported and
// are recorded as such — and a few rows always lag, because upstream does not
// re-run every small market daily. What signals "upstream stopped" is the
// cycle whose freshest row is stale, and nothing else notices that: the cycle
// succeeds and the written counters advance.
func (s *Service) reportStaleness(
	ctx context.Context,
	markets []entity.CoreModelReferenceMarketResult,
	vaults []entity.CoreModelReferenceVaultResult,
	syncedAt time.Time,
) {
	cutoff := syncedAt.Truncate(24*time.Hour).AddDate(0, 0, -freshnessAllowanceDays)
	dates := make([]time.Time, 0, len(markets)+len(vaults))
	for _, m := range markets {
		dates = append(dates, m.ModelDate)
	}
	for _, v := range vaults {
		dates = append(dates, v.ModelDate)
	}
	if len(dates) == 0 {
		return
	}

	stale, newest, oldest := 0, dates[0], dates[0]
	for _, d := range dates {
		if d.Before(cutoff) {
			stale++
		}
		if d.After(newest) {
			newest = d
		}
		if d.Before(oldest) {
			oldest = d
		}
	}
	s.telemetry.RecordStaleRows(ctx, stale)

	if newest.Before(cutoff) {
		s.telemetry.RecordStaleCycle(ctx)
		s.logger.Warn("every core result is stale upstream; the model has not published a new day",
			"newest_model_date", newest.Format(time.DateOnly), "rows", len(dates))
		return
	}
	if stale > 0 {
		s.logger.Info("some core results lag the current model day",
			"stale_rows", stale, "rows", len(dates),
			"oldest_model_date", oldest.Format(time.DateOnly), "newest_model_date", newest.Format(time.DateOnly))
	}
}

func (s *Service) toMarketResults(
	rows []outbound.CoreModelReferenceMarketRow,
	syncedAt time.Time,
) ([]entity.CoreModelReferenceMarketResult, error) {
	results := make([]entity.CoreModelReferenceMarketResult, 0, len(rows))
	for _, row := range rows {
		modelDate, err := parseModelDate(row.Date, fmt.Sprintf("market %s/%s/%s", row.Network, row.Protocol, row.MarketUID))
		if err != nil {
			return nil, err
		}
		results = append(results, entity.CoreModelReferenceMarketResult{
			Network:              row.Network,
			ChainID:              row.ChainID,
			ProtocolName:         row.Protocol,
			MarketUID:            row.MarketUID,
			MarketSymbol:         row.MarketSymbol,
			LoanTokenSymbol:      row.LoanTokenSymbol,
			LoanTokenAddress:     row.LoanTokenAddress,
			ModelDate:            modelDate,
			SyncedAt:             syncedAt,
			NScenarios:           row.NScenarios,
			HorizonDays:          row.HorizonDays,
			EffectiveHorizonDays: row.EffectiveHorizonDays,
			TotalSupplyUSD:       row.TotalSupply,
			ProbNoBadDebt:        row.ProbNoBadDebt,
			CRREL:                row.CRREL,
			CRRVaR:               row.CRRVaR,
			CRRES:                row.CRRES,
			CRRELSE:              row.CRRELSE,
			CRRVaRSE:             row.CRRVaRSE,
			CRRESSE:              row.CRRESSE,
			CRRFloor:             row.CRRFloor,
			ExternalFlowEnabled:  row.ExternalFlowEnabled,
			Source:               entity.CoreModelReferenceDataSource,
			BuildID:              s.buildID,
		})
	}
	return results, nil
}

func (s *Service) toVaultResults(
	rows []outbound.CoreModelReferenceVaultRow,
	syncedAt time.Time,
) ([]entity.CoreModelReferenceVaultResult, error) {
	results := make([]entity.CoreModelReferenceVaultResult, 0, len(rows))
	for _, row := range rows {
		modelDate, err := parseModelDate(row.Date, fmt.Sprintf("vault %s/%s/%s", row.Network, row.Protocol, row.VaultAddress))
		if err != nil {
			return nil, err
		}
		results = append(results, entity.CoreModelReferenceVaultResult{
			Network:          row.Network,
			ChainID:          row.ChainID,
			ProtocolName:     row.Protocol,
			VaultAddress:     row.VaultAddress,
			VaultSymbol:      row.VaultSymbol,
			VaultName:        row.VaultName,
			VersionLabel:     row.Version,
			LoanTokenSymbol:  row.LoanTokenSymbol,
			LoanTokenAddress: row.LoanTokenAddress,
			Method:           row.Method,
			ModelDate:        modelDate,
			SyncedAt:         syncedAt,
			NMarkets:         row.NMarkets,
			TotalAssetsUSD:   row.TotalAssets,
			IdleAssetsUSD:    row.IdleAssets,
			CRREL:            row.CRREL,
			CRRELSE:          row.CRRELSE,
			CRRES:            row.CRRES,
			Source:           entity.CoreModelReferenceDataSource,
			BuildID:          s.buildID,
		})
	}
	return results, nil
}

// parseModelDate reads upstream's calendar day. An unparseable date fails the
// cycle: a placeholder date would misfile the row under a day that never was.
func parseModelDate(raw, subject string) (time.Time, error) {
	modelDate, err := time.Parse(time.DateOnly, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("parsing date %q for %s: %w", raw, subject, err)
	}
	return modelDate.UTC(), nil
}

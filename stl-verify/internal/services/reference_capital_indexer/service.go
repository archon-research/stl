// Package reference_capital_indexer accumulates reference risk-capital snapshots.
//
// Sky's Star monitor publishes only a current snapshot and no history at any
// granularity, so a reference figure can never be reconstructed for a past
// instant. This service is the only way a reference time series comes to exist:
// it observes the monitor each cycle and appends what it saw.
package reference_capital_indexer

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Enough to close the gap since the last cycle and absorb one that did not run.
// The backfill seeded the year, so this is not a history window.
const balanceSheetLookbackDays = 3

// Clock returns the current time; injected so a cycle's synced_at is testable.
type Clock func() time.Time

// Service fetches reference risk-capital snapshots and persists them.
type Service struct {
	deps         Deps
	trackedStars []string
	buildID      int
	now          Clock
	telemetry    *Telemetry
	logger       *slog.Logger
}

// Deps holds the service's ports. A struct rather than positional arguments:
// ten ports is past the arity where call sites stay legible, and the same
// client legitimately fills two slots — named fields keep that intent visible.
type Deps struct {
	PrimeRepo          outbound.PrimeRepository
	CapitalRepo        outbound.PrimeCapitalStackRepository
	RiskProvider       outbound.RiskCapitalProvider
	AllocationProvider outbound.RiskCapitalAllocationProvider
	AllocationRepo     outbound.PrimeCapitalStackAllocationRepository
	SheetRepo          outbound.PrimeBalanceSheetRepository
	SheetProvider      outbound.BalanceSheetProvider
	PositionProvider   outbound.ReferencePositionProvider
	PositionRepo       outbound.PrimeReferencePositionRepository
	// TxManager coordinates snapshots, allocations and positions in one
	// transaction (persistCycle): the three join exactly on synced_at, so they
	// must land together or not at all. Balance sheets key on observed_at
	// instead and save outside it.
	TxManager outbound.TxManager
}

// NewService creates a capital stack sync service.
//
// trackedStars names the primes to record, and comes from the axis-synome
// contract rather than the prime table: the table still carries rows for primes
// STL has stopped tracking, so using it would accumulate snapshots nobody reads.
func NewService(
	deps Deps,
	trackedStars []string,
	buildID int,
	now Clock,
	telemetry *Telemetry,
	logger *slog.Logger,
) (*Service, error) {
	// A forgotten Deps field would otherwise nil-deref mid-cycle, after rows
	// were already persisted; misconstruction must die at wiring time instead.
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
		deps:         deps,
		trackedStars: trackedStars,
		buildID:      buildID,
		now:          now,
		telemetry:    telemetry,
		logger:       logger.With("component", "reference-capital-indexer"),
	}, nil
}

func (d Deps) validate() error {
	missing := []string{}
	for _, port := range []struct {
		name string
		set  bool
	}{
		{"PrimeRepo", d.PrimeRepo != nil},
		{"CapitalRepo", d.CapitalRepo != nil},
		{"RiskProvider", d.RiskProvider != nil},
		{"AllocationProvider", d.AllocationProvider != nil},
		{"AllocationRepo", d.AllocationRepo != nil},
		{"SheetRepo", d.SheetRepo != nil},
		{"SheetProvider", d.SheetProvider != nil},
		{"PositionProvider", d.PositionProvider != nil},
		{"PositionRepo", d.PositionRepo != nil},
		{"TxManager", d.TxManager != nil},
	} {
		if !port.set {
			missing = append(missing, port.name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("reference capital indexer wired without: %s", strings.Join(missing, ", "))
	}
	return nil
}

// Run observes the upstream monitor once and appends what it reported.
func (s *Service) Run(ctx context.Context) error {
	if len(s.trackedStars) == 0 {
		return fmt.Errorf("no tracked primes configured; nothing to observe")
	}

	primeIDs, err := s.primeIDsByName(ctx)
	if err != nil {
		return err
	}

	obs, err := s.observeUpstream(ctx)
	if err != nil {
		return err
	}

	// One timestamp per cycle, shared by every table it writes, so the
	// prime-level totals and their breakdowns join exactly on synced_at.
	syncedAt := s.now().UTC()

	snapshots, err := s.toSnapshots(obs.snapshots, primeIDs, syncedAt)
	if err != nil {
		return err
	}
	allocations, err := s.toAllocations(obs.allocations, primeIDs, syncedAt)
	if err != nil {
		return err
	}
	positions, err := s.toPositions(obs.positions, primeIDs, syncedAt)
	if err != nil {
		return err
	}
	sheets, err := s.toBalanceSheets(obs.balanceSheetDays, primeIDs)
	if err != nil {
		return err
	}

	if err := s.persistCycle(ctx, snapshots, allocations, positions, sheets); err != nil {
		return err
	}

	s.logger.Info("capital stack sync complete",
		"snapshots", len(snapshots), "allocations", len(allocations),
		"positions", len(positions), "balance_sheet_days", len(sheets))
	return nil
}

// cycleObservation is everything one cycle read from upstream, gathered before
// anything is persisted.
type cycleObservation struct {
	snapshots        []outbound.RiskCapitalPrimeSnapshot
	allocations      []outbound.RiskCapitalAllocationRow
	positions        []outbound.ReferencePositionRow
	balanceSheetDays []outbound.BalanceSheetDay
}

// observeUpstream completes every upstream read before the first save: the
// tables are append-only and each attempt stamps a fresh synced_at, so a cycle
// failing after a partial persist would strand healthy-looking rows that no
// retry can repair or join.
//
// The breakdown and positions are fetched only for the stars this cycle's
// snapshots cover: the breakdown route answers an unknown star with a 500
// indistinguishable from a fault, and the positions feed answers one with an
// empty list indistinguishable from a prime holding nothing.
//
// The balance-sheet window is short because the backfill seeded the year: it
// only closes the gap since the last cycle, plus a day's slack for one that
// did not run. The provider drops the current UTC day, so a run before
// upstream has published yesterday finds nothing new, which is not an error.
func (s *Service) observeUpstream(ctx context.Context) (cycleObservation, error) {
	snapshots, err := s.deps.RiskProvider.FetchPrimeSnapshots(ctx, s.trackedStars)
	if err != nil {
		return cycleObservation{}, fmt.Errorf("fetching prime snapshots: %w", err)
	}
	if len(snapshots) == 0 {
		// The monitor covers every prime STL tracks today, so covering none means
		// the feed broke or its vocabulary drifted. That must not read as
		// "nothing to do", which would leave a silent hole in the series.
		return cycleObservation{}, fmt.Errorf("upstream monitor covered none of the %d tracked primes", len(s.trackedStars))
	}

	s.reportUncovered(ctx, snapshots)
	stars := coveredStars(snapshots)

	allocations, err := s.deps.AllocationProvider.FetchPrimeAllocations(ctx, stars)
	if err != nil {
		return cycleObservation{}, fmt.Errorf("fetching prime allocations: %w", err)
	}
	if err := requireBreakdownCoverage(snapshots, allocations); err != nil {
		return cycleObservation{}, err
	}

	positions, err := s.deps.PositionProvider.FetchPositions(ctx, stars)
	if err != nil {
		return cycleObservation{}, fmt.Errorf("fetching reference positions: %w", err)
	}

	days, err := s.deps.SheetProvider.FetchHistory(ctx, s.trackedStars, balanceSheetLookbackDays)
	if err != nil {
		return cycleObservation{}, fmt.Errorf("fetching balance-sheet history: %w", err)
	}

	return cycleObservation{
		snapshots:        snapshots,
		allocations:      allocations,
		positions:        positions,
		balanceSheetDays: days,
	}, nil
}

// persistCycle saves what the cycle observed. Snapshots, allocations and
// positions share one transaction because they promise to join exactly on
// synced_at; every table is append-only and a retry stamps a fresh synced_at,
// so a partial commit would strand a permanent half-cycle no retry repairs.
// Balance sheets key on observed_at, a different axis with no such join, so
// they save after, once the shared transaction has committed.
func (s *Service) persistCycle(
	ctx context.Context,
	snapshots []entity.PrimeCapitalStackSnapshot,
	allocations []entity.PrimeCapitalStackAllocation,
	positions []entity.PrimeReferencePosition,
	sheets []entity.PrimeBalanceSheetSnapshot,
) error {
	err := s.deps.TxManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		if err := s.deps.CapitalRepo.SavePrimeCapitalSnapshots(ctx, tx, snapshots); err != nil {
			return fmt.Errorf("saving capital stack snapshots: %w", err)
		}
		if err := s.deps.AllocationRepo.SaveCapitalStackAllocations(ctx, tx, allocations); err != nil {
			return fmt.Errorf("saving capital stack allocations: %w", err)
		}
		if err := s.deps.PositionRepo.SaveReferencePositions(ctx, tx, positions); err != nil {
			return fmt.Errorf("saving reference positions: %w", err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	s.telemetry.RecordSnapshotsWritten(ctx, len(snapshots))
	s.telemetry.RecordAllocationsWritten(ctx, len(allocations))
	s.telemetry.RecordPositionsWritten(ctx, len(positions))

	if len(sheets) == 0 {
		return nil
	}
	if err := s.deps.SheetRepo.SaveBalanceSheetSnapshots(ctx, sheets); err != nil {
		return fmt.Errorf("saving balance sheet snapshots: %w", err)
	}
	s.logger.Info("balance sheet advanced", "days", len(sheets))
	return nil
}

// coveredStars names the stars a cycle's snapshots cover, in snapshot order.
func coveredStars(snapshots []outbound.RiskCapitalPrimeSnapshot) []string {
	stars := make([]string, 0, len(snapshots))
	for _, snap := range snapshots {
		stars = append(stars, normalizedStar(snap.Star))
	}
	return stars
}

// requireBreakdownCoverage rejects a cycle where the monitor reports exposure
// for a star but an empty breakdown. The totals and breakdown routes are two
// separately-computed snapshots, so that combination is upstream disagreeing
// with itself — and persisting it would publish "this prime holds nothing"
// against real exposure.
func requireBreakdownCoverage(
	snapshots []outbound.RiskCapitalPrimeSnapshot,
	allocations []outbound.RiskCapitalAllocationRow,
) error {
	counts := make(map[string]int, len(snapshots))
	for _, row := range allocations {
		counts[normalizedStar(row.Star)]++
	}

	for _, snap := range snapshots {
		if counts[normalizedStar(snap.Star)] > 0 {
			continue
		}
		exposure, ok := new(big.Rat).SetString(snap.Exposure)
		if !ok {
			return fmt.Errorf("unparseable exposure %q for prime %q", snap.Exposure, snap.Star)
		}
		if exposure.Sign() != 0 {
			return fmt.Errorf(
				"upstream monitor reported exposure %s for prime %q but an empty breakdown", snap.Exposure, snap.Star)
		}
	}
	return nil
}

func (s *Service) toAllocations(
	rows []outbound.RiskCapitalAllocationRow,
	primeIDs map[string]int64,
	syncedAt time.Time,
) ([]entity.PrimeCapitalStackAllocation, error) {
	allocations := make([]entity.PrimeCapitalStackAllocation, 0, len(rows))
	for _, row := range rows {
		primeID, ok := primeIDs[normalizedStar(row.Star)]
		if !ok {
			return nil, fmt.Errorf("upstream monitor reported unknown prime %q", row.Star)
		}
		allocations = append(allocations, entity.PrimeCapitalStackAllocation{
			PrimeID:                primeID,
			SyncedAt:               syncedAt,
			ProtocolName:           row.Protocol,
			Network:                row.Network,
			ChainID:                row.ChainID,
			Symbol:                 row.Symbol,
			Name:                   row.Name,
			TokenAddress:           row.TokenAddress,
			LoanTokenAddress:       row.LoanTokenAddress,
			LoanTokenSymbol:        row.LoanTokenSymbol,
			ExposureUSD:            row.Exposure,
			RequiredRiskCapitalUSD: row.RequiredRiskCapital,
			CRR:                    row.CRR,
			Source:                 entity.ReferenceDataSource,
			BuildID:                s.buildID,
		})
	}
	return allocations, nil
}

func (s *Service) toPositions(
	rows []outbound.ReferencePositionRow,
	primeIDs map[string]int64,
	syncedAt time.Time,
) ([]entity.PrimeReferencePosition, error) {
	positions := make([]entity.PrimeReferencePosition, 0, len(rows))
	for _, row := range rows {
		primeID, ok := primeIDs[normalizedStar(row.Star)]
		if !ok {
			return nil, fmt.Errorf("positions feed reported unknown prime %q", row.Star)
		}
		positions = append(positions, entity.PrimeReferencePosition{
			PrimeID:            primeID,
			SyncedAt:           syncedAt,
			ProtocolName:       row.Protocol,
			Network:            row.Network,
			ChainID:            row.ChainID,
			TokenSymbol:        row.TokenSymbol,
			TokenName:          row.TokenName,
			TokenAddress:       row.TokenAddress,
			AssetsUSD:          row.Assets,
			AllocatedAssetsUSD: row.AllocatedAssets,
			IdleAssetsUSD:      row.IdleAssets,
			Source:             entity.ReferenceDataSource,
			BuildID:            s.buildID,
		})
	}
	return positions, nil
}

// normalizedStar folds a star name for comparison, matching the clients'
// normalization so a case or padding difference cannot split one prime in two.
func normalizedStar(star string) string {
	return strings.ToLower(strings.TrimSpace(star))
}

func (s *Service) toBalanceSheets(
	days []outbound.BalanceSheetDay,
	primeIDs map[string]int64,
) ([]entity.PrimeBalanceSheetSnapshot, error) {
	sheets := make([]entity.PrimeBalanceSheetSnapshot, 0, len(days))
	for _, day := range days {
		primeID, ok := primeIDs[normalizedStar(day.Star)]
		if !ok {
			return nil, fmt.Errorf("balance-sheet feed reported unknown prime %q", day.Star)
		}

		observedAt, err := time.Parse(time.DateOnly, day.Date)
		if err != nil {
			return nil, fmt.Errorf("parsing date %q for prime %q: %w", day.Date, day.Star, err)
		}

		sheets = append(sheets, entity.PrimeBalanceSheetSnapshot{
			PrimeID:            primeID,
			ObservedAt:         observedAt.UTC(),
			TreasuryBalanceUSD: day.TreasuryBalance,
			AssetsUSD:          day.Assets,
			AllocatedAssetsUSD: day.AllocatedAssets,
			IdleAssetsUSD:      day.IdleAssets,
			DebtUSD:            day.Debt,
			BackstopCapitalUSD: day.BackstopCapital,
			Source:             entity.ReferenceDataSource,
			BuildID:            s.buildID,
		})
	}
	return sheets, nil
}

// reportUncovered surfaces the tracked primes this cycle did not observe.
//
// Partial coverage is not an error — the monitor's coverage is its own — but it
// stalls a prime's series silently: the read path gap-fills with locf, so the
// last value keeps serving as current. The cycle continues and the gap is
// counted, which is what an alert can key on.
func (s *Service) reportUncovered(ctx context.Context, rows []outbound.RiskCapitalPrimeSnapshot) {
	covered := make(map[string]bool, len(rows))
	for _, row := range rows {
		covered[normalizedStar(row.Star)] = true
	}

	for _, star := range s.trackedStars {
		normalized := normalizedStar(star)
		if covered[normalized] {
			continue
		}
		s.telemetry.RecordPrimeUncovered(ctx, normalized)
		s.logger.Warn("tracked prime not covered by the upstream monitor; its series will not advance",
			"star", normalized)
	}
}

func (s *Service) primeIDsByName(ctx context.Context) (map[string]int64, error) {
	primes, err := s.deps.PrimeRepo.ListPrimes(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing primes: %w", err)
	}
	if len(primes) == 0 {
		return nil, fmt.Errorf("no primes in the database to attribute snapshots to")
	}

	byName := make(map[string]int64, len(primes))
	for _, p := range primes {
		byName[normalizedStar(p.Name)] = p.ID
	}
	return byName, nil
}

func (s *Service) toSnapshots(
	rows []outbound.RiskCapitalPrimeSnapshot,
	primeIDs map[string]int64,
	syncedAt time.Time,
) ([]entity.PrimeCapitalStackSnapshot, error) {
	snapshots := make([]entity.PrimeCapitalStackSnapshot, 0, len(rows))
	for _, row := range rows {
		// A star the prime registry does not know is a naming drift or a prime
		// added upstream. Skipping it would leave a hole that looks identical to
		// a prime the monitor stopped covering, so the cycle fails and retries.
		primeID, ok := primeIDs[normalizedStar(row.Star)]
		if !ok {
			return nil, fmt.Errorf("upstream monitor reported unknown prime %q", row.Star)
		}
		snapshots = append(snapshots, s.toSnapshot(row, primeID, syncedAt))
	}
	return snapshots, nil
}

func (s *Service) toSnapshot(
	row outbound.RiskCapitalPrimeSnapshot,
	primeID int64,
	syncedAt time.Time,
) entity.PrimeCapitalStackSnapshot {
	return entity.PrimeCapitalStackSnapshot{
		PrimeID:                       primeID,
		SyncedAt:                      syncedAt,
		ExposureUSD:                   row.Exposure,
		RequiredRiskCapitalUSD:        row.RequiredRiskCapital,
		TotalRiskCapitalUSD:           row.TotalRiskCapital,
		JuniorRiskCapitalUSD:          row.JuniorRiskCapital,
		SeniorRiskCapitalUSD:          row.SeniorRiskCapital,
		InternalJuniorRiskCapitalUSD:  row.InternalJuniorRiskCapital,
		ExternalJuniorRiskCapitalUSD:  row.ExternalJuniorRiskCapital,
		TokenizedJuniorRiskCapitalUSD: row.TokenizedJuniorRiskCapital,
		InternalSeniorRiskCapitalUSD:  row.InternalSeniorRiskCapital,
		ExternalSeniorRiskCapitalUSD:  row.ExternalSeniorRiskCapital,
		EncumbranceRatio:              row.EncumbranceRatio,
		ExposureShare:                 row.ExposureShare,
		EPIUtilization:                row.EPIUtilization,
		SPJUtilization:                row.SPJUtilization,
		Source:                        entity.ReferenceDataSource,
		BuildID:                       s.buildID,
	}
}

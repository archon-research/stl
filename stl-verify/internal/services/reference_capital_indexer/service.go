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
// several ports share a shape, and named fields block a silent swap.
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
) *Service {
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
	}
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

	rows, err := s.deps.RiskProvider.FetchPrimeSnapshots(ctx, s.trackedStars)
	if err != nil {
		return fmt.Errorf("fetching prime snapshots: %w", err)
	}
	if len(rows) == 0 {
		// The monitor covers every prime STL tracks today, so covering none means
		// the feed broke or its vocabulary drifted. That must not read as
		// "nothing to do", which would leave a silent hole in the series.
		return fmt.Errorf("upstream monitor covered none of the %d tracked primes", len(s.trackedStars))
	}

	s.reportUncovered(ctx, rows)

	// One timestamp per cycle, shared by every table it writes, so the
	// prime-level totals and their breakdowns join exactly on synced_at.
	syncedAt := s.now().UTC()

	snapshots, err := s.toSnapshots(rows, primeIDs, syncedAt)
	if err != nil {
		return err
	}

	if err := s.deps.CapitalRepo.SavePrimeCapitalSnapshots(ctx, snapshots); err != nil {
		return fmt.Errorf("saving capital stack snapshots: %w", err)
	}

	s.telemetry.RecordSnapshotsWritten(ctx, len(snapshots))

	if err := s.recordAllocations(ctx, rows, primeIDs, syncedAt); err != nil {
		return err
	}

	if err := s.recordPositions(ctx, rows, primeIDs, syncedAt); err != nil {
		return err
	}

	if err := s.recordBalanceSheet(ctx, primeIDs); err != nil {
		return err
	}

	s.logger.Info("capital stack sync complete", "snapshots", len(snapshots))
	return nil
}

// recordAllocations persists the per-allocation breakdown behind the cycle's
// snapshots. Fetched only for the stars this cycle's snapshots cover: the
// breakdown route answers an unknown star with a 500 indistinguishable from a
// genuine fault.
func (s *Service) recordAllocations(
	ctx context.Context,
	snapshots []outbound.RiskCapitalPrimeSnapshot,
	primeIDs map[string]int64,
	syncedAt time.Time,
) error {
	rows, err := s.deps.AllocationProvider.FetchPrimeAllocations(ctx, coveredStars(snapshots))
	if err != nil {
		return fmt.Errorf("fetching prime allocations: %w", err)
	}

	if err := requireBreakdownCoverage(snapshots, rows); err != nil {
		return err
	}

	allocations, err := s.toAllocations(rows, primeIDs, syncedAt)
	if err != nil {
		return err
	}

	if err := s.deps.AllocationRepo.SaveCapitalStackAllocations(ctx, allocations); err != nil {
		return fmt.Errorf("saving capital stack allocations: %w", err)
	}

	s.telemetry.RecordAllocationsWritten(ctx, len(allocations))
	return nil
}

// recordPositions persists the cycle's balance-sheet positions. Fetched only
// for the stars this cycle's snapshots cover: the feed answers an unknown star
// with an empty list, so coverage must be established before asking.
func (s *Service) recordPositions(
	ctx context.Context,
	snapshots []outbound.RiskCapitalPrimeSnapshot,
	primeIDs map[string]int64,
	syncedAt time.Time,
) error {
	rows, err := s.deps.PositionProvider.FetchPositions(ctx, coveredStars(snapshots))
	if err != nil {
		return fmt.Errorf("fetching reference positions: %w", err)
	}

	positions, err := s.toPositions(rows, primeIDs, syncedAt)
	if err != nil {
		return err
	}

	if err := s.deps.PositionRepo.SaveReferencePositions(ctx, positions); err != nil {
		return fmt.Errorf("saving reference positions: %w", err)
	}

	s.telemetry.RecordPositionsWritten(ctx, len(positions))
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

// recordBalanceSheet advances the daily balance sheet, whose figures the monitor
// does not carry — assets_usd among them.
//
// The window is short because the backfill seeded the year: this only has to
// close the gap since the last cycle, plus a day's slack for a cycle that did
// not run. The provider drops the current UTC day, so a run before upstream has
// published yesterday finds nothing new, which is not an error.
func (s *Service) recordBalanceSheet(ctx context.Context, primeIDs map[string]int64) error {
	days, err := s.deps.SheetProvider.FetchHistory(ctx, s.trackedStars, balanceSheetLookbackDays)
	if err != nil {
		return fmt.Errorf("fetching balance-sheet history: %w", err)
	}

	sheets, err := s.toBalanceSheets(days, primeIDs)
	if err != nil {
		return err
	}
	if len(sheets) == 0 {
		return nil
	}

	if err := s.deps.SheetRepo.SaveBalanceSheetSnapshots(ctx, sheets); err != nil {
		return fmt.Errorf("saving balance sheet snapshots: %w", err)
	}

	s.logger.Info("balance sheet advanced", "days", len(sheets))
	return nil
}

func (s *Service) toBalanceSheets(
	days []outbound.BalanceSheetDay,
	primeIDs map[string]int64,
) ([]entity.PrimeBalanceSheetSnapshot, error) {
	sheets := make([]entity.PrimeBalanceSheetSnapshot, 0, len(days))
	for _, day := range days {
		primeID, ok := primeIDs[strings.ToLower(strings.TrimSpace(day.Star))]
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
		covered[strings.ToLower(strings.TrimSpace(row.Star))] = true
	}

	for _, star := range s.trackedStars {
		normalized := strings.ToLower(strings.TrimSpace(star))
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
		byName[strings.ToLower(strings.TrimSpace(p.Name))] = p.ID
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
		primeID, ok := primeIDs[strings.ToLower(strings.TrimSpace(row.Star))]
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

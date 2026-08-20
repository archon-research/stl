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
	primeRepo     outbound.PrimeRepository
	capitalRepo   outbound.PrimeCapitalStackRepository
	riskProvider  outbound.RiskCapitalProvider
	sheetRepo     outbound.PrimeBalanceSheetRepository
	sheetProvider outbound.BalanceSheetProvider
	trackedStars  []string
	buildID       int
	now           Clock
	telemetry     *Telemetry
	logger        *slog.Logger
}

// NewService creates a capital stack sync service.
//
// trackedStars names the primes to record, and comes from the axis-synome
// contract rather than the prime table: the table still carries rows for primes
// STL has stopped tracking, so using it would accumulate snapshots nobody reads.
func NewService(
	primeRepo outbound.PrimeRepository,
	capitalRepo outbound.PrimeCapitalStackRepository,
	riskProvider outbound.RiskCapitalProvider,
	sheetRepo outbound.PrimeBalanceSheetRepository,
	sheetProvider outbound.BalanceSheetProvider,
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
		primeRepo:     primeRepo,
		capitalRepo:   capitalRepo,
		riskProvider:  riskProvider,
		sheetRepo:     sheetRepo,
		sheetProvider: sheetProvider,
		trackedStars:  trackedStars,
		buildID:       buildID,
		now:           now,
		telemetry:     telemetry,
		logger:        logger.With("component", "reference-capital-indexer"),
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

	rows, err := s.riskProvider.FetchPrimeSnapshots(ctx, s.trackedStars)
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

	snapshots, err := s.toSnapshots(rows, primeIDs, s.now().UTC())
	if err != nil {
		return err
	}

	if err := s.capitalRepo.SavePrimeCapitalSnapshots(ctx, snapshots); err != nil {
		return fmt.Errorf("saving capital stack snapshots: %w", err)
	}

	s.telemetry.RecordSnapshotsWritten(ctx, len(snapshots))

	if err := s.recordBalanceSheet(ctx, primeIDs); err != nil {
		return err
	}

	s.logger.Info("capital stack sync complete", "snapshots", len(snapshots))
	return nil
}

// recordBalanceSheet advances the daily balance sheet, whose figures the monitor
// does not carry — assets_usd among them.
//
// The window is short because the backfill seeded the year: this only has to
// close the gap since the last cycle, plus a day's slack for a cycle that did
// not run. The provider drops the current UTC day, so a run before upstream has
// published yesterday finds nothing new, which is not an error.
func (s *Service) recordBalanceSheet(ctx context.Context, primeIDs map[string]int64) error {
	days, err := s.sheetProvider.FetchHistory(ctx, s.trackedStars, balanceSheetLookbackDays)
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

	if err := s.sheetRepo.SaveBalanceSheetSnapshots(ctx, sheets); err != nil {
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
	primes, err := s.primeRepo.ListPrimes(ctx)
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

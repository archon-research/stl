// Package reference_capital_backfill seeds the reference history that predates
// STL's own observation of Sky.
//
// The Star monitor publishes no history, so the capital-stack syncer can only
// accumulate forward from the day it first ran. This service fills the year
// before that from Sky's balance-sheet feed, which is the only source that
// holds it. It is one-shot by design: the range it covers stops growing once
// the syncer takes over.
package reference_capital_backfill

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Service backfills per-prime daily balance sheets.
type Service struct {
	primeRepo    outbound.PrimeRepository
	sheetRepo    outbound.PrimeBalanceSheetRepository
	provider     outbound.BalanceSheetProvider
	trackedStars []string
	daysAgo      int
	buildID      int
	logger       *slog.Logger
}

// NewService creates a balance-sheet backfill service.
func NewService(
	primeRepo outbound.PrimeRepository,
	sheetRepo outbound.PrimeBalanceSheetRepository,
	provider outbound.BalanceSheetProvider,
	trackedStars []string,
	daysAgo int,
	buildID int,
	logger *slog.Logger,
) *Service {
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{
		primeRepo:    primeRepo,
		sheetRepo:    sheetRepo,
		provider:     provider,
		trackedStars: trackedStars,
		daysAgo:      daysAgo,
		buildID:      buildID,
		logger:       logger.With("component", "reference-capital-backfill"),
	}
}

// Run fetches the history window once and persists it.
func (s *Service) Run(ctx context.Context) error {
	if len(s.trackedStars) == 0 {
		return fmt.Errorf("no tracked primes configured; nothing to backfill")
	}

	primeIDs, err := s.primeIDsByName(ctx)
	if err != nil {
		return err
	}

	days, err := s.provider.FetchHistory(ctx, s.trackedStars, s.daysAgo)
	if err != nil {
		return fmt.Errorf("fetching balance-sheet history: %w", err)
	}

	if err := requireEveryStarCovered(s.trackedStars, days); err != nil {
		return err
	}

	snapshots, err := s.toSnapshots(days, primeIDs)
	if err != nil {
		return err
	}

	if err := s.sheetRepo.SaveBalanceSheetSnapshots(ctx, snapshots); err != nil {
		return fmt.Errorf("saving balance sheet snapshots: %w", err)
	}

	s.logger.Info("balance sheet backfill complete", "snapshots", len(snapshots), "daysAgo", s.daysAgo)
	return nil
}

// requireEveryStarCovered refuses a backfill that would seed only some of the
// tracked primes. This runs once, and the write is ON CONFLICT DO NOTHING, so a
// prime missing here keeps a permanent hole that a later re-run cannot repair —
// unlike the syncer, whose partial coverage recurs every cycle and is alerted on.
func requireEveryStarCovered(tracked []string, days []outbound.BalanceSheetDay) error {
	covered := make(map[string]bool, len(days))
	for _, day := range days {
		covered[strings.ToLower(strings.TrimSpace(day.Star))] = true
	}

	missing := make([]string, 0, len(tracked))
	for _, star := range tracked {
		if !covered[strings.ToLower(strings.TrimSpace(star))] {
			missing = append(missing, star)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("balance-sheet history has no rows for %v; refusing to seed a partial backfill", missing)
	}
	return nil
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
	days []outbound.BalanceSheetDay,
	primeIDs map[string]int64,
) ([]entity.PrimeBalanceSheetSnapshot, error) {
	snapshots := make([]entity.PrimeBalanceSheetSnapshot, 0, len(days))
	for _, day := range days {
		primeID, ok := primeIDs[day.Star]
		if !ok {
			return nil, fmt.Errorf("upstream feed reported unknown prime %q", day.Star)
		}

		observedAt, err := time.Parse(time.DateOnly, day.Date)
		if err != nil {
			return nil, fmt.Errorf("parsing date %q for prime %q: %w", day.Date, day.Star, err)
		}

		snapshots = append(snapshots, entity.PrimeBalanceSheetSnapshot{
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
	return snapshots, nil
}

// Package capital_stack_syncer accumulates reference risk-capital snapshots.
//
// Sky's Star monitor publishes only a current snapshot and no history at any
// granularity, so a reference figure can never be reconstructed for a past
// instant. This service is the only way a reference time series comes to exist:
// it observes the monitor each cycle and appends what it saw.
package capital_stack_syncer

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

const sourceSlug = "skyeco:star-monitoring:risk-capital"

// Clock returns the current time; injected so a cycle's synced_at is testable.
type Clock func() time.Time

// Service fetches reference risk-capital snapshots and persists them.
type Service struct {
	primeRepo    outbound.PrimeRepository
	capitalRepo  outbound.PrimeCapitalStackRepository
	riskProvider outbound.RiskCapitalProvider
	buildID      int
	now          Clock
	logger       *slog.Logger
}

// NewService creates a capital stack sync service.
func NewService(
	primeRepo outbound.PrimeRepository,
	capitalRepo outbound.PrimeCapitalStackRepository,
	riskProvider outbound.RiskCapitalProvider,
	buildID int,
	now Clock,
	logger *slog.Logger,
) *Service {
	if logger == nil {
		logger = slog.Default()
	}
	if now == nil {
		now = time.Now
	}
	return &Service{
		primeRepo:    primeRepo,
		capitalRepo:  capitalRepo,
		riskProvider: riskProvider,
		buildID:      buildID,
		now:          now,
		logger:       logger.With("component", "capital-stack-syncer"),
	}
}

// Run observes the upstream monitor once and appends what it reported.
func (s *Service) Run(ctx context.Context) error {
	primeIDs, err := s.primeIDsByName(ctx)
	if err != nil {
		return err
	}

	rows, err := s.riskProvider.FetchPrimeSnapshots(ctx)
	if err != nil {
		return fmt.Errorf("fetching prime snapshots: %w", err)
	}
	if len(rows) == 0 {
		// The monitor always tracks at least one prime; an empty list means the
		// shape changed or the feed broke, which must not read as "nothing to do".
		return fmt.Errorf("upstream monitor reported no primes")
	}

	snapshots, err := s.toSnapshots(rows, primeIDs, s.now().UTC())
	if err != nil {
		return err
	}

	if err := s.capitalRepo.SavePrimeCapitalSnapshots(ctx, snapshots); err != nil {
		return fmt.Errorf("saving capital stack snapshots: %w", err)
	}

	s.logger.Info("capital stack sync complete", "snapshots", len(snapshots))
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
		Source:                        sourceSlug,
		BuildID:                       s.buildID,
	}
}

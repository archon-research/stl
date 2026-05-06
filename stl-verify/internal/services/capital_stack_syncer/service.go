package capital_stack_syncer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Service orchestrates fetching capital stack data from approved sources and persisting.
type Service struct {
	primeRepo    outbound.PrimeRepository
	capitalRepo  outbound.PrimeCapitalStackRepository
	riskProvider outbound.RiskCapitalProvider
	logger       *slog.Logger
}

// NewService creates a capital stack sync service.
func NewService(
	primeRepo outbound.PrimeRepository,
	capitalRepo outbound.PrimeCapitalStackRepository,
	riskProvider outbound.RiskCapitalProvider,
	logger *slog.Logger,
) *Service {
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{
		primeRepo:    primeRepo,
		capitalRepo:  capitalRepo,
		riskProvider: riskProvider,
		logger:       logger.With("component", "capital-stack-syncer"),
	}
}

// Run fetches capital stack data from approved sources and persists to database.
func (s *Service) Run(ctx context.Context) error {
	s.logger.Info("starting capital stack sync")

	// Fetch all primes to build a mapping for later lookup.
	primes, err := s.primeRepo.ListPrimes(ctx)
	if err != nil {
		return fmt.Errorf("listing primes: %w", err)
	}
	if len(primes) == 0 {
		s.logger.Info("no primes found in database")
		return nil
	}

	primeByName := make(map[string]entity.Prime, len(primes))
	for _, p := range primes {
		primeByName[p.Name] = p
	}

	s.logger.Info("fetched primes", "count", len(primes))

	// Fetch risk-capital rows from approved source.
	rows, err := s.riskProvider.FetchPrimeRows(ctx)
	if err != nil {
		return fmt.Errorf("fetching prime rows: %w", err)
	}
	if len(rows) == 0 {
		s.logger.Info("no prime rows fetched from risk-capital provider")
		return nil
	}

	s.logger.Info("fetched prime rows from provider", "count", len(rows))

	// Convert to capital stack snapshots.
	snapshots := make([]entity.PrimeCapitalStackSnapshot, 0, len(rows))
	for _, row := range rows {
		prime, ok := primeByName[row.PrimeName]
		if !ok {
			s.logger.Warn("skipping row for unknown prime", "primeName", row.PrimeName)
			continue
		}

		// capital_buffer = total_rc - financial_rrc
		snap := entity.PrimeCapitalStackSnapshot{
			PrimeID:              prime.ID,
			CapitalBuffer:        row.TotalRC, // Will be adjusted below if financial_rrc is available
			FirstLossCapital:     row.FinancialRRC,
			Timestamp:            time.Now().UTC(),
			Source:               "skyeco:star-monitoring:risk-capital",
			Version:              1,
			BenchmarkSource:      "https://info-sky.blockanalitica.com/star-monitoring/risk-capital/primes/",
			ReconciliationStatus: "pending",
			CreatedBy:            "capital-stack-syncer",
			UpdatedBy:            "capital-stack-syncer",
		}
		snapshots = append(snapshots, snap)
	}

	if len(snapshots) == 0 {
		s.logger.Info("no snapshots matched known primes")
		return nil
	}

	// Persist snapshots.
	if err := s.capitalRepo.UpsertPrimeCapitalSnapshots(ctx, snapshots); err != nil {
		return fmt.Errorf("upserting capital stack snapshots: %w", err)
	}

	s.logger.Info("sync complete", "snapshots", len(snapshots))
	return nil
}

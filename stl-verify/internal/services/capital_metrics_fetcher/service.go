// Package capital_metrics_fetcher snapshots per-prime capital metrics from the
// upstream Star risk-capital monitor into the capital_metrics_snapshot table,
// giving the API a time series to aggregate.
package capital_metrics_fetcher

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

type ServiceConfig struct {
	// BenchmarkSource is recorded on each snapshot to attribute the upstream.
	BenchmarkSource string
	Logger          *slog.Logger
}

type Service struct {
	provider outbound.CapitalMetricsProvider
	repo     outbound.CapitalMetricsRepository
	source   string
	logger   *slog.Logger
}

func NewService(cfg ServiceConfig, provider outbound.CapitalMetricsProvider, repo outbound.CapitalMetricsRepository) (*Service, error) {
	if provider == nil {
		return nil, fmt.Errorf("provider cannot be nil")
	}
	if repo == nil {
		return nil, fmt.Errorf("repo cannot be nil")
	}
	if cfg.BenchmarkSource == "" {
		return nil, fmt.Errorf("benchmark source is required")
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &Service{
		provider: provider,
		repo:     repo,
		source:   cfg.BenchmarkSource,
		logger:   logger.With("component", "capital-metrics-fetcher", "provider", provider.Name()),
	}, nil
}

// FetchAndStore fetches the latest risk-capital rows, matches each to a known
// prime by name (case-insensitive, matching the live endpoint), and persists
// one snapshot per matched prime under a single synced_at timestamp.
//
// Primes with no upstream match, and rows that fail validation, are skipped
// with a warning rather than failing the whole run: a single malformed row
// must not block every other prime's snapshot, and Temporal retries would not
// fix bad upstream data.
func (s *Service) FetchAndStore(ctx context.Context) error {
	primes, err := s.repo.GetPrimes(ctx)
	if err != nil {
		return fmt.Errorf("listing primes: %w", err)
	}
	if len(primes) == 0 {
		s.logger.Info("no primes to snapshot")
		return nil
	}

	rows, err := s.provider.FetchRiskCapital(ctx)
	if err != nil {
		return fmt.Errorf("fetching risk capital: %w", err)
	}

	byStar := make(map[string]outbound.RiskCapitalRow, len(rows))
	for _, row := range rows {
		byStar[normalizeName(row.Star)] = row
	}

	syncedAt := time.Now().UTC()
	snapshots := make([]*entity.CapitalMetricsSnapshot, 0, len(primes))
	for _, prime := range primes {
		row, ok := byStar[normalizeName(prime.Name)]
		if !ok {
			s.logger.Warn("no upstream risk-capital row matched prime",
				"primeID", prime.ID, "primeName", prime.Name)
			continue
		}

		var ratio *string
		if trimmed := strings.TrimSpace(row.RiskToleranceRatio); trimmed != "" {
			ratio = &trimmed
		}

		snapshot := &entity.CapitalMetricsSnapshot{
			PrimeID:            prime.ID,
			RiskCapital:        strings.TrimSpace(row.Exposure),
			TotalCapital:       strings.TrimSpace(row.TotalRC),
			FirstLossCapital:   strings.TrimSpace(row.FinancialRRC),
			RiskToCapitalRatio: ratio,
			BenchmarkSource:    s.source,
			SyncedAt:           syncedAt,
		}
		if err := snapshot.Validate(); err != nil {
			s.logger.Warn("skipping invalid capital metrics snapshot",
				"primeID", prime.ID, "primeName", prime.Name, "error", err)
			continue
		}
		snapshots = append(snapshots, snapshot)
	}

	if len(snapshots) == 0 {
		s.logger.Warn("no capital metrics snapshots to store", "primeCount", len(primes), "rowCount", len(rows))
		return nil
	}

	if err := s.repo.SaveSnapshots(ctx, snapshots); err != nil {
		return fmt.Errorf("saving snapshots: %w", err)
	}

	s.logger.Info("stored capital metrics snapshots", "count", len(snapshots))
	return nil
}

func normalizeName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

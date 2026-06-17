package postgres

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"slices"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that CapitalMetricsRepository implements the port interface.
var _ outbound.CapitalMetricsRepository = (*CapitalMetricsRepository)(nil)

// CapitalMetricsRepository persists capital-metrics snapshots to Postgres.
type CapitalMetricsRepository struct {
	pool   *pgxpool.Pool
	txm    *TxManager
	logger *slog.Logger
}

func NewCapitalMetricsRepository(pool *pgxpool.Pool, txm *TxManager, logger *slog.Logger) *CapitalMetricsRepository {
	if logger == nil {
		logger = slog.Default()
	}
	return &CapitalMetricsRepository{
		pool:   pool,
		txm:    txm,
		logger: logger.With("component", "capital-metrics-repo"),
	}
}

// GetPrimes returns all rows from the prime table.
// vault_address is stored as BYTEA and scanned into common.Address.
func (r *CapitalMetricsRepository) GetPrimes(ctx context.Context) ([]entity.Prime, error) {
	const q = `
		SELECT id, name, vault_address, created_at
		FROM prime
		ORDER BY id ASC
	`

	rows, err := r.pool.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("query primes: %w", err)
	}
	defer rows.Close()

	var primes []entity.Prime
	for rows.Next() {
		var p entity.Prime
		var addrBytes []byte
		if err := rows.Scan(&p.ID, &p.Name, &addrBytes, &p.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan prime row: %w", err)
		}
		p.VaultAddress = common.BytesToAddress(addrBytes)
		primes = append(primes, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate prime rows: %w", err)
	}

	return primes, nil
}

// SaveSnapshots writes all snapshots in a single batched transaction. Decimal
// strings are sent verbatim into NUMERIC columns. Duplicate snapshots (same
// prime_id + synced_at) are skipped via ON CONFLICT DO NOTHING, so a retried
// cronjob run is idempotent.
func (r *CapitalMetricsRepository) SaveSnapshots(ctx context.Context, snapshots []*entity.CapitalMetricsSnapshot) error {
	if len(snapshots) == 0 {
		return nil
	}

	// Stable natural-key ordering keeps lock acquisition deterministic across
	// concurrent callers. See ADR-0002 §3.
	slices.SortFunc(snapshots, func(a, b *entity.CapitalMetricsSnapshot) int {
		return cmp.Or(
			cmp.Compare(a.PrimeID, b.PrimeID),
			a.SyncedAt.Compare(b.SyncedAt),
		)
	})

	return r.txm.WithTransaction(ctx, func(tx pgx.Tx) error {
		const q = `
			INSERT INTO capital_metrics_snapshot
				(prime_id, risk_capital, total_capital, first_loss_capital, risk_to_capital_ratio, benchmark_source, synced_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (prime_id, synced_at) DO NOTHING
		`

		batch := &pgx.Batch{}
		for _, s := range snapshots {
			batch.Queue(q,
				s.PrimeID,
				s.RiskCapital,
				s.TotalCapital,
				s.FirstLossCapital,
				s.RiskToCapitalRatio,
				s.BenchmarkSource,
				s.SyncedAt,
			)
		}

		results := tx.SendBatch(ctx, batch)
		for i, s := range snapshots {
			if _, err := results.Exec(); err != nil {
				_ = results.Close()
				return fmt.Errorf("insert capital metrics snapshot %d (prime_id=%d): %w", i, s.PrimeID, err)
			}
		}
		if err := results.Close(); err != nil {
			return fmt.Errorf("close batch: %w", err)
		}

		r.logger.Debug("capital metrics snapshots saved", "count", len(snapshots))
		return nil
	})
}

package postgres

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that PrimeCapitalStackRepository implements the port.
var _ outbound.PrimeCapitalStackRepository = (*PrimeCapitalStackRepository)(nil)

// PrimeCapitalStackRepository persists prime capital stack snapshots.
type PrimeCapitalStackRepository struct {
	pool   *pgxpool.Pool
	txm    *TxManager
	logger *slog.Logger
}

// NewPrimeCapitalStackRepository creates a new PrimeCapitalStackRepository.
func NewPrimeCapitalStackRepository(pool *pgxpool.Pool, txm *TxManager, logger *slog.Logger) *PrimeCapitalStackRepository {
	if logger == nil {
		logger = slog.Default()
	}
	return &PrimeCapitalStackRepository{
		pool:   pool,
		txm:    txm,
		logger: logger.With("component", "prime-capital-stack-repo"),
	}
}

// UpsertPrimeCapitalSnapshots inserts or updates prime capital stack snapshots in one transaction.
func (r *PrimeCapitalStackRepository) UpsertPrimeCapitalSnapshots(
	ctx context.Context,
	snapshots []entity.PrimeCapitalStackSnapshot,
) error {
	if len(snapshots) == 0 {
		return nil
	}

	return r.txm.WithTransaction(ctx, func(tx pgx.Tx) error {
		const q = `
			INSERT INTO prime_capital_stack (
				prime_id,
				capital_buffer,
				first_loss_capital,
				timestamp,
				source,
				version,
				benchmark_source,
				reconciliation_status,
				created_by,
				updated_by
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			ON CONFLICT (prime_id, timestamp) DO UPDATE SET
				capital_buffer = EXCLUDED.capital_buffer,
				first_loss_capital = EXCLUDED.first_loss_capital,
				source = EXCLUDED.source,
				version = EXCLUDED.version,
				benchmark_source = EXCLUDED.benchmark_source,
				reconciliation_status = EXCLUDED.reconciliation_status,
				updated_by = EXCLUDED.updated_by,
				updated_at = now()
		`

		batch := &pgx.Batch{}
		for _, s := range snapshots {
			batch.Queue(
				q,
				s.PrimeID,
				s.CapitalBuffer,
				s.FirstLossCapital,
				s.Timestamp,
				s.Source,
				s.Version,
				s.BenchmarkSource,
				s.ReconciliationStatus,
				s.CreatedBy,
				s.UpdatedBy,
			)
		}

		results := tx.SendBatch(ctx, batch)
		for i, s := range snapshots {
			if _, err := results.Exec(); err != nil {
				_ = results.Close()
				return fmt.Errorf("upsert capital stack snapshot %d (prime_id=%d): %w", i, s.PrimeID, err)
			}
		}
		if err := results.Close(); err != nil {
			return fmt.Errorf("close batch: %w", err)
		}

		r.logger.Info("upserted prime capital stack snapshots", "count", len(snapshots))
		return nil
	})
}

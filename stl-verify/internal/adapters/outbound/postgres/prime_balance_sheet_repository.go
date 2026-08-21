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

var _ outbound.PrimeBalanceSheetRepository = (*PrimeBalanceSheetRepository)(nil)

// PrimeBalanceSheetRepository persists per-prime daily balance sheets.
type PrimeBalanceSheetRepository struct {
	pool   *pgxpool.Pool
	txm    *TxManager
	logger *slog.Logger
}

// NewPrimeBalanceSheetRepository creates a new PrimeBalanceSheetRepository.
func NewPrimeBalanceSheetRepository(pool *pgxpool.Pool, txm *TxManager, logger *slog.Logger) *PrimeBalanceSheetRepository {
	if logger == nil {
		logger = slog.Default()
	}
	return &PrimeBalanceSheetRepository{
		pool:   pool,
		txm:    txm,
		logger: logger.With("component", "prime-balance-sheet-repo"),
	}
}

// SaveBalanceSheetSnapshots inserts a backfill run's rows in one transaction.
//
// Insert-only, like every reference table: the trigger assigns
// processing_version, so re-running under the same build conflicts away rather
// than rewriting history.
func (r *PrimeBalanceSheetRepository) SaveBalanceSheetSnapshots(
	ctx context.Context,
	snapshots []entity.PrimeBalanceSheetSnapshot,
) error {
	if len(snapshots) == 0 {
		return nil
	}

	return r.txm.WithTransaction(ctx, func(tx pgx.Tx) error {
		const q = `
			INSERT INTO prime_reference_balance_sheet (
				prime_id,
				observed_at,
				treasury_balance_usd,
				assets_usd,
				allocated_assets_usd,
				idle_assets_usd,
				debt_usd,
				backstop_capital_usd,
				source,
				build_id
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			ON CONFLICT (prime_id, observed_at, processing_version) DO NOTHING
		`

		batch := &pgx.Batch{}
		for _, s := range snapshots {
			batch.Queue(
				q,
				s.PrimeID,
				s.ObservedAt,
				s.TreasuryBalanceUSD,
				s.AssetsUSD,
				s.AllocatedAssetsUSD,
				s.IdleAssetsUSD,
				s.DebtUSD,
				s.BackstopCapitalUSD,
				s.Source,
				s.BuildID,
			)
		}

		results := tx.SendBatch(ctx, batch)
		for i, s := range snapshots {
			if _, err := results.Exec(); err != nil {
				_ = results.Close()
				return fmt.Errorf("insert balance sheet snapshot %d (prime_id=%d): %w", i, s.PrimeID, err)
			}
		}
		if err := results.Close(); err != nil {
			return fmt.Errorf("close batch: %w", err)
		}

		r.logger.Info("saved prime balance sheet snapshots", "count", len(snapshots))
		return nil
	})
}

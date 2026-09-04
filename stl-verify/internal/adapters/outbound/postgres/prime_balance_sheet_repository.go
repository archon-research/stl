package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

var _ outbound.PrimeBalanceSheetRepository = (*PrimeBalanceSheetRepository)(nil)

// PrimeBalanceSheetRepository persists per-prime daily balance sheets.
type PrimeBalanceSheetRepository struct {
	pool   *pgxpool.Pool
	txm    *TxManager
	logger *slog.Logger
	runID  buildregistry.RunID
}

// NewPrimeBalanceSheetRepository creates a new PrimeBalanceSheetRepository.
func NewPrimeBalanceSheetRepository(pool *pgxpool.Pool, txm *TxManager, logger *slog.Logger, runID buildregistry.RunID) *PrimeBalanceSheetRepository {
	if logger == nil {
		logger = slog.Default()
	}
	return &PrimeBalanceSheetRepository{
		pool:   pool,
		txm:    txm,
		logger: logger.With("component", "prime-balance-sheet-repo"),
		runID:  runID,
	}
}

// SaveBalanceSheetSnapshots inserts a backfill run's rows in one transaction.
//
// Insert-only, like every reference table: the trigger assigns
// processing_version, so re-running under the same build conflicts away rather
// than rewriting history. Returns the rows actually inserted (not
// len(snapshots): a conflicted-away row counts as attempted but not inserted)
// alongside how many of those started a prime's day fresh (processing_version
// 0) rather than correcting an already-stored one — a deploy's build_id change
// makes every row in the lookback insert as a correction, so only the fresh
// count is safe for an alert to key on.
func (r *PrimeBalanceSheetRepository) SaveBalanceSheetSnapshots(
	ctx context.Context,
	snapshots []entity.PrimeBalanceSheetSnapshot,
) (inserted int, newDays int, err error) {
	if len(snapshots) == 0 {
		return 0, 0, nil
	}

	err = r.txm.WithTransaction(ctx, func(tx pgx.Tx) error {
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
				build_id,
				run_id
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
			ON CONFLICT (prime_id, observed_at, processing_version) DO NOTHING
			RETURNING processing_version
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
				r.runID,
			)
		}

		results := tx.SendBatch(ctx, batch)
		for i, s := range snapshots {
			var processingVersion int
			scanErr := results.QueryRow().Scan(&processingVersion)
			if scanErr != nil {
				if errors.Is(scanErr, pgx.ErrNoRows) {
					continue // conflicted away: not inserted
				}
				_ = results.Close()
				return fmt.Errorf("insert balance sheet snapshot %d (prime_id=%d): %w", i, s.PrimeID, scanErr)
			}
			inserted++
			if processingVersion == 0 {
				newDays++
			}
		}
		if closeErr := results.Close(); closeErr != nil {
			return fmt.Errorf("close batch: %w", closeErr)
		}

		r.logger.Info("saved prime balance sheet snapshots",
			"inserted", inserted, "new_days", newDays, "attempted", len(snapshots))
		return nil
	})
	if err != nil {
		return 0, 0, err
	}
	return inserted, newDays, nil
}

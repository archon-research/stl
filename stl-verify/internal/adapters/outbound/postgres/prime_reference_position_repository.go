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

// Compile-time check that PrimeReferencePositionRepository implements the port.
var _ outbound.PrimeReferencePositionRepository = (*PrimeReferencePositionRepository)(nil)

// PrimeReferencePositionRepository persists per-prime balance-sheet positions.
type PrimeReferencePositionRepository struct {
	pool   *pgxpool.Pool
	txm    *TxManager
	logger *slog.Logger
}

// NewPrimeReferencePositionRepository creates a new PrimeReferencePositionRepository.
func NewPrimeReferencePositionRepository(
	pool *pgxpool.Pool,
	txm *TxManager,
	logger *slog.Logger,
) *PrimeReferencePositionRepository {
	if logger == nil {
		logger = slog.Default()
	}
	return &PrimeReferencePositionRepository{
		pool:   pool,
		txm:    txm,
		logger: logger.With("component", "prime-reference-position-repo"),
	}
}

// SaveReferencePositions inserts a cycle's positions in one transaction.
//
// Insert-only: a row is immutable once written, and the BEFORE INSERT trigger
// assigns processing_version, so a re-run under the same build_id reuses its
// version and conflicts away rather than overwriting history.
func (r *PrimeReferencePositionRepository) SaveReferencePositions(
	ctx context.Context,
	positions []entity.PrimeReferencePosition,
) error {
	if len(positions) == 0 {
		return nil
	}

	return r.txm.WithTransaction(ctx, func(tx pgx.Tx) error {
		const q = `
			INSERT INTO prime_reference_position (
				prime_id,
				synced_at,
				network,
				chain_id,
				protocol_name,
				token_symbol,
				token_name,
				token_address,
				assets_usd,
				allocated_assets_usd,
				idle_assets_usd,
				source,
				build_id
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
			ON CONFLICT (prime_id, synced_at, network, token_address, processing_version) DO NOTHING
		`

		batch := &pgx.Batch{}
		for _, p := range positions {
			batch.Queue(
				q,
				p.PrimeID,
				p.SyncedAt,
				p.Network,
				p.ChainID,
				p.ProtocolName,
				p.TokenSymbol,
				p.TokenName,
				p.TokenAddress,
				p.AssetsUSD,
				p.AllocatedAssetsUSD,
				p.IdleAssetsUSD,
				p.Source,
				p.BuildID,
			)
		}

		results := tx.SendBatch(ctx, batch)
		for i, p := range positions {
			if _, err := results.Exec(); err != nil {
				_ = results.Close()
				return fmt.Errorf("insert reference position %d (prime_id=%d, token=%s): %w",
					i, p.PrimeID, p.TokenAddress, err)
			}
		}
		if err := results.Close(); err != nil {
			return fmt.Errorf("close batch: %w", err)
		}

		r.logger.Info("saved prime reference positions", "count", len(positions))
		return nil
	})
}

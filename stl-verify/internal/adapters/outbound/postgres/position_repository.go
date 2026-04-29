package postgres

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that PositionRepository implements outbound.PositionRepository
var _ outbound.PositionRepository = (*PositionRepository)(nil)

// PositionRepository is a PostgreSQL implementation of the outbound.PositionRepository port.
type PositionRepository struct {
	pool      *pgxpool.Pool
	logger    *slog.Logger
	buildID   buildregistry.BuildID
	batchSize int
}

// NewPositionRepository creates a new PostgreSQL Position repository.
// If batchSize is <= 0, the default batch size from DefaultRepositoryConfig() is used.
// Returns an error if the database pool is nil.
//
// Note: This function does not verify that the database connection is alive.
// Use a separate health check or call pool.Ping() if connection validation is needed.
func NewPositionRepository(pool *pgxpool.Pool, logger *slog.Logger, buildID buildregistry.BuildID, batchSize int) (*PositionRepository, error) {
	if pool == nil {
		return nil, fmt.Errorf("database pool cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if batchSize <= 0 {
		batchSize = DefaultRepositoryConfig().PositionBatchSize
	}
	return &PositionRepository{
		pool:      pool,
		logger:    logger,
		buildID:   buildID,
		batchSize: batchSize,
	}, nil
}

// SaveBorrower saves a single borrower (debt) position record within an external transaction.
// Uses append-only semantics: ON CONFLICT DO NOTHING preserves the first write.
func (r *PositionRepository) SaveBorrower(ctx context.Context, tx pgx.Tx, b *entity.Borrower) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO borrower (user_id, protocol_id, token_id, block_number, block_version, amount, change, event_type, tx_hash, created_at, build_id)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		 ON CONFLICT (user_id, protocol_id, token_id, block_number, block_version, processing_version, created_at) DO NOTHING`,
		b.UserID, b.ProtocolID, b.TokenID, b.BlockNumber, b.BlockVersion,
		b.Amount, b.Change, b.EventType, b.TxHash, b.CreatedAt, int(r.buildID))

	if err != nil {
		return fmt.Errorf("failed to save borrower: %w", err)
	}
	return nil
}

// SaveBorrowers saves multiple borrower (debt) position records using pgx.Batch.
// Uses ON CONFLICT DO NOTHING to ensure immutability - existing records are never modified.
//
// Borrowers are sorted by natural key before queueing so the per-row advisory
// lock in assign_processing_version_borrower is acquired in a transaction-stable
// order across concurrent callers — required to avoid deadlocks when multiple
// build_ids reprocess overlapping rows. See ADR-0002 §3.
func (r *PositionRepository) SaveBorrowers(ctx context.Context, tx pgx.Tx, borrowers []*entity.Borrower) error {
	if len(borrowers) == 0 {
		return nil
	}

	slices.SortFunc(borrowers, func(a, b *entity.Borrower) int {
		return cmp.Or(
			cmp.Compare(a.UserID, b.UserID),
			cmp.Compare(a.ProtocolID, b.ProtocolID),
			cmp.Compare(a.TokenID, b.TokenID),
			cmp.Compare(a.BlockNumber, b.BlockNumber),
			cmp.Compare(a.BlockVersion, b.BlockVersion),
			a.CreatedAt.Compare(b.CreatedAt),
		)
	})

	batch := &pgx.Batch{}
	for _, b := range borrowers {
		batch.Queue(
			`INSERT INTO borrower (user_id, protocol_id, token_id, block_number, block_version, amount, change, event_type, tx_hash, created_at, build_id)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
			 ON CONFLICT (user_id, protocol_id, token_id, block_number, block_version, processing_version, created_at) DO NOTHING`,
			b.UserID, b.ProtocolID, b.TokenID, b.BlockNumber, b.BlockVersion,
			b.Amount, b.Change, b.EventType, b.TxHash, b.CreatedAt, int(r.buildID),
		)
	}

	br := tx.SendBatch(ctx, batch)

	for i := range borrowers {
		if _, err := br.Exec(); err != nil {
			innerErr := br.Close()
			if innerErr != nil {
				return fmt.Errorf("failed to save borrower record %d: %w; additionally, failed to close batch: %v", i, err, innerErr)
			}

			return fmt.Errorf("failed to save borrower record %d: %w", i, err)
		}
	}

	if err := br.Close(); err != nil {
		return fmt.Errorf("closing borrower batch: %w", err)
	}

	return nil
}

// SaveBorrowerCollateral saves a single collateral position record within an external transaction.
// Uses append-only semantics: ON CONFLICT DO NOTHING preserves the first write.
func (r *PositionRepository) SaveBorrowerCollateral(ctx context.Context, tx pgx.Tx, bc *entity.BorrowerCollateral) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO borrower_collateral (user_id, protocol_id, token_id, block_number, block_version, amount, change, event_type, tx_hash, collateral_enabled, created_at, build_id)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		 ON CONFLICT (user_id, protocol_id, token_id, block_number, block_version, processing_version, created_at) DO NOTHING`,
		bc.UserID, bc.ProtocolID, bc.TokenID, bc.BlockNumber, bc.BlockVersion,
		bc.Amount, bc.Change, bc.EventType, bc.TxHash, bc.CollateralEnabled, bc.CreatedAt, int(r.buildID))

	if err != nil {
		return fmt.Errorf("failed to save collateral: %w", err)
	}
	return nil
}

// SaveBorrowerCollaterals saves multiple borrower collateral position records using pgx.Batch.
// Uses ON CONFLICT DO NOTHING to ensure immutability - existing records are never modified.
// Returns nil if collaterals slice is empty.
//
// Collaterals are sorted by natural key before queueing for the same reason as
// SaveBorrowers above. See ADR-0002 §3.
func (r *PositionRepository) SaveBorrowerCollaterals(ctx context.Context, tx pgx.Tx, collaterals []*entity.BorrowerCollateral) error {
	if len(collaterals) == 0 {
		return nil
	}

	slices.SortFunc(collaterals, func(a, b *entity.BorrowerCollateral) int {
		return cmp.Or(
			cmp.Compare(a.UserID, b.UserID),
			cmp.Compare(a.ProtocolID, b.ProtocolID),
			cmp.Compare(a.TokenID, b.TokenID),
			cmp.Compare(a.BlockNumber, b.BlockNumber),
			cmp.Compare(a.BlockVersion, b.BlockVersion),
			a.CreatedAt.Compare(b.CreatedAt),
		)
	})

	batch := &pgx.Batch{}
	for _, bc := range collaterals {
		batch.Queue(
			`INSERT INTO borrower_collateral (user_id, protocol_id, token_id, block_number, block_version, amount, change, event_type, tx_hash, collateral_enabled, created_at, build_id)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
			 ON CONFLICT (user_id, protocol_id, token_id, block_number, block_version, processing_version, created_at) DO NOTHING`,
			bc.UserID, bc.ProtocolID, bc.TokenID, bc.BlockNumber, bc.BlockVersion,
			bc.Amount, bc.Change, bc.EventType, bc.TxHash, bc.CollateralEnabled, bc.CreatedAt, int(r.buildID),
		)
	}

	br := tx.SendBatch(ctx, batch)

	for i := range collaterals {
		if _, err := br.Exec(); err != nil {
			br.Close()
			return fmt.Errorf("failed to save collateral record %d: %w", i, err)
		}
	}

	if err := br.Close(); err != nil {
		return fmt.Errorf("closing collateral batch: %w", err)
	}

	return nil
}

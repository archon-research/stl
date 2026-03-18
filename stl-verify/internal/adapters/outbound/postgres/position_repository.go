package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that PositionRepository implements outbound.PositionRepository
var _ outbound.PositionRepository = (*PositionRepository)(nil)

// PositionRepository is a PostgreSQL implementation of the outbound.PositionRepository port.
type PositionRepository struct {
	pool      *pgxpool.Pool
	logger    *slog.Logger
	batchSize int
}

// NewPositionRepository creates a new PostgreSQL Position repository.
// If batchSize is <= 0, the default batch size from DefaultRepositoryConfig() is used.
// Returns an error if the database pool is nil.
//
// Note: This function does not verify that the database connection is alive.
// Use a separate health check or call pool.Ping() if connection validation is needed.
func NewPositionRepository(pool *pgxpool.Pool, logger *slog.Logger, batchSize int) (*PositionRepository, error) {
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
		batchSize: batchSize,
	}, nil
}

// SaveBorrower saves a single borrower (debt) position record within an external transaction.
// amount is the full current outstanding debt; change is the event delta.
// Uses append-only semantics: ON CONFLICT DO NOTHING preserves the first write.
func (r *PositionRepository) SaveBorrower(ctx context.Context, tx pgx.Tx, userID, protocolID, tokenID, blockNumber int64, blockVersion int, amount, change *big.Int, eventType string, txHash []byte) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO borrower (user_id, protocol_id, token_id, block_number, block_version, amount, change, event_type, tx_hash)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		 ON CONFLICT (user_id, protocol_id, token_id, block_number, block_version) DO NOTHING`,
		userID, protocolID, tokenID, blockNumber, blockVersion, amount, change, eventType, txHash)

	if err != nil {
		return fmt.Errorf("failed to save borrower: %w", err)
	}
	return nil
}

// SaveBorrowers saves multiple borrower (debt) position records using pgx.Batch.
// Uses ON CONFLICT DO NOTHING to ensure immutability - existing records are never modified.
func (r *PositionRepository) SaveBorrowers(ctx context.Context, tx pgx.Tx, records []outbound.BorrowerRecord) error {
	if len(records) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for _, rec := range records {
		batch.Queue(
			`INSERT INTO borrower (user_id, protocol_id, token_id, block_number, block_version, amount, change, event_type, tx_hash)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			 ON CONFLICT (user_id, protocol_id, token_id, block_number, block_version) DO NOTHING`,
			rec.UserID, rec.ProtocolID, rec.TokenID, rec.BlockNumber, rec.BlockVersion, rec.Amount, rec.Change, rec.EventType, rec.TxHash,
		)
	}

	br := tx.SendBatch(ctx, batch)

	for i := range records {
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
// amount is the full current collateral balance; change is the event delta.
// Uses append-only semantics: ON CONFLICT DO NOTHING preserves the first write.
func (r *PositionRepository) SaveBorrowerCollateral(ctx context.Context, tx pgx.Tx, userID, protocolID, tokenID, blockNumber int64, blockVersion int, amount, change *big.Int, eventType string, txHash []byte, collateralEnabled bool) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO borrower_collateral (user_id, protocol_id, token_id, block_number, block_version, amount, change, event_type, tx_hash, collateral_enabled)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		 ON CONFLICT (user_id, protocol_id, token_id, block_number, block_version) DO NOTHING`,
		userID, protocolID, tokenID, blockNumber, blockVersion, amount, change, eventType, txHash, collateralEnabled)

	if err != nil {
		return fmt.Errorf("failed to save collateral: %w", err)
	}
	return nil
}

// SaveBorrowerCollaterals saves multiple borrower collateral position records using pgx.Batch.
// amount is the full current collateral balance; change is the event delta.
// Uses ON CONFLICT DO NOTHING to ensure immutability - existing records are never modified.
// This is critical for reproducible calculations: data used in a calculation must not change.
// Returns nil if records slice is empty.
func (r *PositionRepository) SaveBorrowerCollaterals(ctx context.Context, tx pgx.Tx, records []outbound.CollateralRecord) error {
	if len(records) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for _, rec := range records {
		batch.Queue(
			`INSERT INTO borrower_collateral (user_id, protocol_id, token_id, block_number, block_version, amount, change, event_type, tx_hash, collateral_enabled)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			 ON CONFLICT (user_id, protocol_id, token_id, block_number, block_version) DO NOTHING`,
			rec.UserID, rec.ProtocolID, rec.TokenID, rec.BlockNumber, rec.BlockVersion, rec.Amount, rec.Change, rec.EventType, rec.TxHash, rec.CollateralEnabled,
		)
	}

	br := tx.SendBatch(ctx, batch)

	for i := range records {
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

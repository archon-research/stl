package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that PositionRepository implements outbound.PositionRepository
var _ outbound.PositionRepository = (*PositionRepository)(nil)

// PositionRepository is a PostgreSQL implementation of the outbound.PositionRepository port.
type PositionRepository struct {
	db        *sql.DB
	logger    *slog.Logger
	batchSize int
}

// NewPositionRepository creates a new PostgreSQL Position repository.
// If batchSize is <= 0, the default batch size from DefaultRepositoryConfig() is used.
// Returns an error if the database connection is nil.
//
// Note: This function does not verify that the database connection is alive.
// Use a separate health check or call db.Ping() if connection validation is needed.
func NewPositionRepository(db *sql.DB, logger *slog.Logger, batchSize int) (*PositionRepository, error) {
	if db == nil {
		return nil, fmt.Errorf("database connection cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if batchSize <= 0 {
		batchSize = DefaultRepositoryConfig().PositionBatchSize
	}
	return &PositionRepository{
		db:        db,
		logger:    logger,
		batchSize: batchSize,
	}, nil
}

// SaveBorrowerWithTX saves a single borrower (debt) position record within an external transaction.
// Uses upsert semantics: ON CONFLICT updates the existing record.
func (r *PositionRepository) SaveBorrowerWithTX(ctx context.Context, tx *sql.Tx, userID, protocolID, tokenID, blockNumber int64, blockVersion int, amount string) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO borrower (user_id, protocol_id, token_id, block_number, block_version, amount, change)
		 VALUES ($1, $2, $3, $4, $5, $6, $6)
		 ON CONFLICT (user_id, protocol_id, token_id, block_number, block_version)
		 DO UPDATE SET amount = EXCLUDED.amount, change = EXCLUDED.change`,
		userID, protocolID, tokenID, blockNumber, blockVersion, amount)

	if err != nil {
		return fmt.Errorf("failed to save borrower: %w", err)
	}
	return nil
}

// SaveBorrowerCollateralWithTX saves a single collateral position record within an external transaction.
// Uses upsert semantics: ON CONFLICT updates the existing record.
func (r *PositionRepository) SaveBorrowerCollateralWithTX(ctx context.Context, tx *sql.Tx, userID, protocolID, tokenID, blockNumber int64, blockVersion int, amount string) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO borrower_collateral (user_id, protocol_id, token_id, block_number, block_version, amount, change)
		 VALUES ($1, $2, $3, $4, $5, $6, $6)
		 ON CONFLICT (user_id, protocol_id, token_id, block_number, block_version)
		 DO UPDATE SET amount = EXCLUDED.amount, change = EXCLUDED.change`,
		userID, protocolID, tokenID, blockNumber, blockVersion, amount)

	if err != nil {
		return fmt.Errorf("failed to save collateral: %w", err)
	}
	return nil
}

// UpsertBorrowers upserts borrower (debt) position records atomically.
// All records are inserted in a single transaction - if any batch fails, all changes are rolled back.
func (r *PositionRepository) UpsertBorrowers(ctx context.Context, borrowers []*entity.Borrower) error {
	if len(borrowers) == 0 {
		return nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer rollback(tx, r.logger)

	for i := 0; i < len(borrowers); i += r.batchSize {
		end := i + r.batchSize
		if end > len(borrowers) {
			end = len(borrowers)
		}
		batch := borrowers[i:end]

		if err := r.upsertBorrowerBatch(ctx, tx, batch); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

func (r *PositionRepository) upsertBorrowerBatch(ctx context.Context, tx *sql.Tx, borrowers []*entity.Borrower) error {
	if len(borrowers) == 0 {
		return nil
	}

	var sb strings.Builder
	sb.WriteString(`
		INSERT INTO borrower (user_id, protocol_id, token_id, block_number, block_version, amount, change)
		VALUES `)

	args := make([]any, 0, len(borrowers)*7)
	for i, b := range borrowers {
		if i > 0 {
			sb.WriteString(", ")
		}
		baseIdx := i * 7
		sb.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			baseIdx+1, baseIdx+2, baseIdx+3, baseIdx+4, baseIdx+5, baseIdx+6, baseIdx+7))

		amount, err := bigIntToNumeric(b.Amount)
		if err != nil {
			return fmt.Errorf("borrower[%d] (UserID=%d): failed to convert Amount to numeric: %w", i, b.UserID, err)
		}
		change, err := bigIntToNumeric(b.Change)
		if err != nil {
			return fmt.Errorf("borrower[%d] (UserID=%d): failed to convert Change to numeric: %w", i, b.UserID, err)
		}

		args = append(args, b.UserID, b.ProtocolID, b.TokenID, b.BlockNumber, b.BlockVersion, amount, change)
	}

	sb.WriteString(`
		ON CONFLICT (user_id, protocol_id, token_id, block_number, block_version) DO UPDATE SET
			amount = EXCLUDED.amount,
			change = EXCLUDED.change
	`)

	_, err := tx.ExecContext(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("failed to upsert borrower batch: %w", err)
	}
	return nil
}

// UpsertBorrowerCollateral upserts collateral position records atomically.
// All records are inserted in a single transaction - if any batch fails, all changes are rolled back.
func (r *PositionRepository) UpsertBorrowerCollateral(ctx context.Context, collateral []*entity.BorrowerCollateral) error {
	if len(collateral) == 0 {
		return nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer rollback(tx, r.logger)

	for i := 0; i < len(collateral); i += r.batchSize {
		end := i + r.batchSize
		if end > len(collateral) {
			end = len(collateral)
		}
		batch := collateral[i:end]

		if err := r.upsertBorrowerCollateralBatch(ctx, tx, batch); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

func (r *PositionRepository) upsertBorrowerCollateralBatch(ctx context.Context, tx *sql.Tx, collateral []*entity.BorrowerCollateral) error {
	if len(collateral) == 0 {
		return nil
	}

	var sb strings.Builder
	sb.WriteString(`
		INSERT INTO borrower_collateral (user_id, protocol_id, token_id, block_number, block_version, amount, change)
		VALUES `)

	args := make([]any, 0, len(collateral)*7)
	for i, c := range collateral {
		if i > 0 {
			sb.WriteString(", ")
		}
		baseIdx := i * 7
		sb.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			baseIdx+1, baseIdx+2, baseIdx+3, baseIdx+4, baseIdx+5, baseIdx+6, baseIdx+7))

		amount, err := bigIntToNumeric(c.Amount)
		if err != nil {
			return fmt.Errorf("borrower_collateral[%d] (UserID=%d): failed to convert Amount to numeric: %w", i, c.UserID, err)
		}
		change, err := bigIntToNumeric(c.Change)
		if err != nil {
			return fmt.Errorf("borrower_collateral[%d] (UserID=%d): failed to convert Change to numeric: %w", i, c.UserID, err)
		}

		args = append(args, c.UserID, c.ProtocolID, c.TokenID, c.BlockNumber, c.BlockVersion, amount, change)
	}

	sb.WriteString(`
		ON CONFLICT (user_id, protocol_id, token_id, block_number, block_version) DO UPDATE SET
			amount = EXCLUDED.amount,
			change = EXCLUDED.change
	`)

	_, err := tx.ExecContext(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("failed to upsert borrower collateral batch: %w", err)
	}
	return nil
}

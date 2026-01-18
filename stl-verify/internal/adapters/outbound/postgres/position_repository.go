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

// UpsertBorrowers upserts borrower (debt) position records.
func (r *PositionRepository) UpsertBorrowers(ctx context.Context, borrowers []*entity.Borrower) error {
	if len(borrowers) == 0 {
		return nil
	}

	for i := 0; i < len(borrowers); i += r.batchSize {
		end := i + r.batchSize
		if end > len(borrowers) {
			end = len(borrowers)
		}
		batch := borrowers[i:end]

		if err := r.upsertBorrowerBatch(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

func (r *PositionRepository) upsertBorrowerBatch(ctx context.Context, borrowers []*entity.Borrower) error {
	if len(borrowers) == 0 {
		return nil
	}

	for i, b := range borrowers {
		if b.Amount == nil {
			return fmt.Errorf("borrower[%d] (ID=%d, UserID=%d): Amount must not be nil", i, b.ID, b.UserID)
		}
		if b.Change == nil {
			return fmt.Errorf("borrower[%d] (ID=%d, UserID=%d): Change must not be nil", i, b.ID, b.UserID)
		}
	}

	var sb strings.Builder
	sb.WriteString(`
		INSERT INTO borrowers (id, user_id, protocol_id, token_id, block_number, block_version, amount, change)
		VALUES `)

	args := make([]any, 0, len(borrowers)*8)
	for i, b := range borrowers {
		if i > 0 {
			sb.WriteString(", ")
		}
		baseIdx := i * 8
		sb.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			baseIdx+1, baseIdx+2, baseIdx+3, baseIdx+4, baseIdx+5, baseIdx+6, baseIdx+7, baseIdx+8))

		amount := bigIntToNumeric(b.Amount)
		change := bigIntToNumeric(b.Change)

		args = append(args, b.ID, b.UserID, b.ProtocolID, b.TokenID, b.BlockNumber, b.BlockVersion, amount, change)
	}

	sb.WriteString(`
		ON CONFLICT (user_id, protocol_id, token_id, block_number, block_version) DO UPDATE SET
			amount = EXCLUDED.amount,
			change = EXCLUDED.change
	`)

	_, err := r.db.ExecContext(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("failed to upsert borrower batch: %w", err)
	}
	return nil
}

// UpsertBorrowerCollateral upserts collateral position records.
func (r *PositionRepository) UpsertBorrowerCollateral(ctx context.Context, collateral []*entity.BorrowerCollateral) error {
	if len(collateral) == 0 {
		return nil
	}

	for i := 0; i < len(collateral); i += r.batchSize {
		end := i + r.batchSize
		if end > len(collateral) {
			end = len(collateral)
		}
		batch := collateral[i:end]

		if err := r.upsertBorrowerCollateralBatch(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

func (r *PositionRepository) upsertBorrowerCollateralBatch(ctx context.Context, collateral []*entity.BorrowerCollateral) error {
	if len(collateral) == 0 {
		return nil
	}

	// Validate all entities before constructing the query.
	for i, c := range collateral {
		if c.Amount == nil {
			return fmt.Errorf("borrower_collateral[%d] (ID=%d, UserID=%d): Amount must not be nil", i, c.ID, c.UserID)
		}
		if c.Change == nil {
			return fmt.Errorf("borrower_collateral[%d] (ID=%d, UserID=%d): Change must not be nil", i, c.ID, c.UserID)
		}
	}

	var sb strings.Builder
	sb.WriteString(`
		INSERT INTO borrower_collateral (id, user_id, protocol_id, token_id, block_number, block_version, amount, change)
		VALUES `)

	args := make([]any, 0, len(collateral)*8)
	for i, c := range collateral {
		if i > 0 {
			sb.WriteString(", ")
		}
		baseIdx := i * 8
		sb.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			baseIdx+1, baseIdx+2, baseIdx+3, baseIdx+4, baseIdx+5, baseIdx+6, baseIdx+7, baseIdx+8))

		amount := bigIntToNumeric(c.Amount)
		change := bigIntToNumeric(c.Change)

		args = append(args, c.ID, c.UserID, c.ProtocolID, c.TokenID, c.BlockNumber, c.BlockVersion, amount, change)
	}

	sb.WriteString(`
		ON CONFLICT (user_id, protocol_id, token_id, block_number, block_version) DO UPDATE SET
			amount = EXCLUDED.amount,
			change = EXCLUDED.change
	`)

	_, err := r.db.ExecContext(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("failed to upsert borrower collateral batch: %w", err)
	}
	return nil
}

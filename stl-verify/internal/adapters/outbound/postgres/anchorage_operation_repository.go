package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check.
var _ outbound.AnchorageOperationRepository = (*AnchorageOperationRepository)(nil)

// AnchorageOperationRepository persists Anchorage operations to Postgres.
type AnchorageOperationRepository struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// NewAnchorageOperationRepository creates a new AnchorageOperationRepository.
func NewAnchorageOperationRepository(pool *pgxpool.Pool, logger *slog.Logger) *AnchorageOperationRepository {
	if logger == nil {
		logger = slog.Default()
	}
	return &AnchorageOperationRepository{
		pool:   pool,
		logger: logger.With("component", "anchorage-operation-repo"),
	}
}

// SaveOperations inserts a batch of operations in a single transaction.
// Uses ON CONFLICT to skip duplicates (idempotent).
func (r *AnchorageOperationRepository) SaveOperations(ctx context.Context, operations []entity.AnchorageOperation) error {
	if len(operations) == 0 {
		return nil
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	const cols = 11
	valueStrings := make([]string, 0, len(operations))
	valueArgs := make([]interface{}, 0, len(operations)*cols)

	for i, op := range operations {
		base := i * cols
		valueStrings = append(valueStrings, fmt.Sprintf(
			"($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			base+1, base+2, base+3, base+4, base+5, base+6,
			base+7, base+8, base+9, base+10, base+11,
		))
		valueArgs = append(valueArgs,
			op.PrimeID,
			op.OperationID,
			op.Action,
			op.Type,
			op.TypeID,
			op.AssetType,
			op.CustodyType,
			op.Quantity,
			op.Notes,
			op.CreatedAt,
			op.UpdatedAt,
		)
	}

	query := fmt.Sprintf(`
		INSERT INTO anchorage_operation (
			prime_id, operation_id, action, type, type_id,
			asset_type, custody_type, quantity, notes,
			created_at, updated_at
		) VALUES %s
		ON CONFLICT (operation_id, created_at) DO NOTHING`, strings.Join(valueStrings, ","))

	if _, err := tx.Exec(ctx, query, valueArgs...); err != nil {
		return fmt.Errorf("insert operations: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}

	return nil
}

// GetLastCursor returns the pagination cursor for the most recent operation,
// in the format Anchorage expects: "timestamp|id". Returns empty string if
// no operations exist.
func (r *AnchorageOperationRepository) GetLastCursor(ctx context.Context, primeID int64) (string, error) {
	var operationID string
	var createdAt time.Time
	err := r.pool.QueryRow(ctx,
		"SELECT operation_id, created_at FROM anchorage_operation WHERE prime_id = $1 ORDER BY created_at DESC LIMIT 1",
		primeID,
	).Scan(&operationID, &createdAt)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("get last cursor: %w", err)
	}

	// Anchorage expects cursor as "unix_seconds|operation_id"
	ts := fmt.Sprintf("%d", createdAt.UTC().Unix())
	return ts + "|" + operationID, nil
}

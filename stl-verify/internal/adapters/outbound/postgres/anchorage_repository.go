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

// Batch size limits to stay well under PostgreSQL's 65,535 parameter ceiling.
const (
	snapshotBatchSize  = 500 // 500 × 19 cols = 9,500 params
	operationBatchSize = 500 // 500 × 10 cols = 5,000 params
)

// Compile-time checks.
var (
	_ outbound.AnchorageSnapshotRepository  = (*AnchorageRepository)(nil)
	_ outbound.AnchorageOperationRepository = (*AnchorageRepository)(nil)
)

// AnchorageRepository persists Anchorage package snapshots and operations to Postgres.
type AnchorageRepository struct {
	pool   *pgxpool.Pool
	txm    *TxManager
	logger *slog.Logger
}

// NewAnchorageRepository creates a new AnchorageRepository.
func NewAnchorageRepository(pool *pgxpool.Pool, txm *TxManager, logger *slog.Logger) *AnchorageRepository {
	if logger == nil {
		logger = slog.Default()
	}
	return &AnchorageRepository{
		pool:   pool,
		txm:    txm,
		logger: logger.With("component", "anchorage-repo"),
	}
}

// ---------------------------------------------------------------------------
// Snapshots
// ---------------------------------------------------------------------------

// SaveSnapshots inserts package snapshots in batches within a single transaction.
func (r *AnchorageRepository) SaveSnapshots(ctx context.Context, snapshots []entity.AnchoragePackageSnapshot) error {
	if len(snapshots) == 0 {
		return nil
	}

	return r.txm.WithTransaction(ctx, func(tx pgx.Tx) error {
		for i := 0; i < len(snapshots); i += snapshotBatchSize {
			end := min(i+snapshotBatchSize, len(snapshots))
			if err := insertSnapshotBatch(ctx, tx, snapshots[i:end]); err != nil {
				return err
			}
		}
		return nil
	})
}

func insertSnapshotBatch(ctx context.Context, tx pgx.Tx, batch []entity.AnchoragePackageSnapshot) error {
	const cols = 19
	valueStrings := make([]string, 0, len(batch))
	valueArgs := make([]any, 0, len(batch)*cols)

	for i, snap := range batch {
		base := i * cols
		valueStrings = append(valueStrings, fmt.Sprintf(
			"($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8,
			base+9, base+10, base+11, base+12, base+13, base+14, base+15, base+16, base+17, base+18, base+19,
		))
		valueArgs = append(valueArgs,
			snap.PrimeID,
			snap.PackageID,
			snap.PledgorID,
			snap.SecuredPartyID,
			snap.Active,
			snap.State,
			snap.CurrentLTV,
			snap.ExposureValue,
			snap.PackageValue,
			snap.MarginCallLTV,
			snap.CriticalLTV,
			snap.MarginReturnLTV,
			snap.AssetType,
			snap.CustodyType,
			snap.AssetPrice,
			snap.AssetQuantity,
			snap.AssetWeightedValue,
			snap.LTVTimestamp,
			snap.SnapshotTime,
		)
	}

	query := fmt.Sprintf(`
		INSERT INTO anchorage_package_snapshot (
			prime_id, package_id, pledgor_id, secured_party_id, active, state,
			current_ltv, exposure_value, package_value,
			margin_call_ltv, critical_ltv, margin_return_ltv,
			asset_type, custody_type, asset_price, asset_quantity, asset_weighted_value,
			ltv_timestamp, snapshot_time
		) VALUES %s
		ON CONFLICT (prime_id, package_id, asset_type, custody_type, snapshot_time) DO NOTHING`, strings.Join(valueStrings, ","))

	if _, err := tx.Exec(ctx, query, valueArgs...); err != nil {
		return fmt.Errorf("insert snapshot batch: %w", err)
	}

	return nil
}

// ---------------------------------------------------------------------------
// Operations
// ---------------------------------------------------------------------------

// SaveOperations inserts operations in batches within a single transaction.
// Uses ON CONFLICT to skip duplicates (idempotent).
func (r *AnchorageRepository) SaveOperations(ctx context.Context, operations []entity.AnchorageOperation) error {
	if len(operations) == 0 {
		return nil
	}

	return r.txm.WithTransaction(ctx, func(tx pgx.Tx) error {
		for i := 0; i < len(operations); i += operationBatchSize {
			end := min(i+operationBatchSize, len(operations))
			if err := insertOperationBatch(ctx, tx, operations[i:end]); err != nil {
				return err
			}
		}
		return nil
	})
}

func insertOperationBatch(ctx context.Context, tx pgx.Tx, batch []entity.AnchorageOperation) error {
	const cols = 10
	valueStrings := make([]string, 0, len(batch))
	valueArgs := make([]any, 0, len(batch)*cols)

	for i, op := range batch {
		base := i * cols
		valueStrings = append(valueStrings, fmt.Sprintf(
			"($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			base+1, base+2, base+3, base+4, base+5, base+6,
			base+7, base+8, base+9, base+10,
		))
		valueArgs = append(valueArgs,
			op.PrimeID,
			op.OperationID,
			op.Action,
			op.OperationType,
			op.TypeID,
			op.AssetType,
			op.CustodyType,
			op.Quantity,
			op.Notes,
			op.CreatedAt,
		)
	}

	query := fmt.Sprintf(`
		INSERT INTO anchorage_operation (
			prime_id, operation_id, action, operation_type, type_id,
			asset_type, custody_type, quantity, notes,
			created_at
		) VALUES %s
		ON CONFLICT (operation_id, created_at) DO NOTHING`, strings.Join(valueStrings, ","))

	if _, err := tx.Exec(ctx, query, valueArgs...); err != nil {
		return fmt.Errorf("insert operation batch: %w", err)
	}

	return nil
}

// GetLastCursor returns the pagination cursor for the most recent operation,
// in the format Anchorage expects: "unix_seconds|operation_id".
// Returns empty string if no operations exist.
func (r *AnchorageRepository) GetLastCursor(ctx context.Context, primeID int64) (string, error) {
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

	ts := fmt.Sprintf("%d", createdAt.UTC().Unix())
	return ts + "|" + operationID, nil
}

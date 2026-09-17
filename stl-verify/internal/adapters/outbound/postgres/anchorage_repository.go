package postgres

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Batch size limits to stay well under PostgreSQL's 65,535 parameter ceiling.
const (
	snapshotBatchSize  = 500 // 500 × 20 cols = 10,000 params
	operationBatchSize = 500 // 500 × 11 cols = 5,500 params
)

// Compile-time checks.
var (
	_ outbound.AnchorageSnapshotRepository  = (*AnchorageRepository)(nil)
	_ outbound.AnchorageOperationRepository = (*AnchorageRepository)(nil)
)

// AnchorageRepository persists Anchorage package snapshots and operations to Postgres.
type AnchorageRepository struct {
	pool    *pgxpool.Pool
	txm     *TxManager
	logger  *slog.Logger
	buildID buildregistry.BuildID
	runID   buildregistry.RunID
}

// NewAnchorageRepository creates a new AnchorageRepository.
func NewAnchorageRepository(pool *pgxpool.Pool, txm *TxManager, logger *slog.Logger, buildID buildregistry.BuildID, runID buildregistry.RunID) *AnchorageRepository {
	if logger == nil {
		logger = slog.Default()
	}
	return &AnchorageRepository{
		pool:    pool,
		txm:     txm,
		logger:  logger.With("component", "anchorage-repo"),
		buildID: buildID,
		runID:   runID,
	}
}

// ---------------------------------------------------------------------------
// Snapshots
// ---------------------------------------------------------------------------

// SaveSnapshots inserts package snapshots in batches within a single transaction.
//
// Snapshots are sorted by natural key once (before chunking) so the per-row
// advisory lock in assign_processing_version_anchorage_package_snapshot is
// acquired in a transaction-stable order across concurrent callers. Sorting
// must precede chunking — otherwise the cross-chunk total order breaks. See
// ADR-0002 §3.
func (r *AnchorageRepository) SaveSnapshots(ctx context.Context, snapshots []entity.AnchoragePackageSnapshot) error {
	if len(snapshots) == 0 {
		return nil
	}

	slices.SortFunc(snapshots, func(a, b entity.AnchoragePackageSnapshot) int {
		return cmp.Or(
			cmp.Compare(a.PrimeID, b.PrimeID),
			cmp.Compare(a.PackageID, b.PackageID),
			cmp.Compare(a.AssetType, b.AssetType),
			cmp.Compare(a.CustodyType, b.CustodyType),
			a.SnapshotTime.Compare(b.SnapshotTime),
		)
	})

	return r.txm.WithTransaction(ctx, func(tx pgx.Tx) error {
		for i := 0; i < len(snapshots); i += snapshotBatchSize {
			end := min(i+snapshotBatchSize, len(snapshots))
			if err := insertSnapshotBatch(ctx, tx, snapshots[i:end], int(r.buildID), r.runID); err != nil {
				return err
			}
		}
		return nil
	})
}

func insertSnapshotBatch(ctx context.Context, tx pgx.Tx, batch []entity.AnchoragePackageSnapshot, buildID int, runID buildregistry.RunID) error {
	const cols = 21
	valueStrings := make([]string, 0, len(batch))
	valueArgs := make([]any, 0, len(batch)*cols)

	for i, snap := range batch {
		base := i * cols
		valueStrings = append(valueStrings, fmt.Sprintf(
			"($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8,
			base+9, base+10, base+11, base+12, base+13, base+14, base+15, base+16, base+17, base+18, base+19, base+20, base+21,
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
			buildID,
			runID,
		)
	}

	query := fmt.Sprintf(`
		INSERT INTO anchorage_package_snapshot (
			prime_id, package_id, pledgor_id, secured_party_id, active, state,
			current_ltv, exposure_value, package_value,
			margin_call_ltv, critical_ltv, margin_return_ltv,
			asset_type, custody_type, asset_price, asset_quantity, asset_weighted_value,
			ltv_timestamp, snapshot_time, build_id, run_id
		) VALUES %s
		ON CONFLICT (prime_id, package_id, asset_type, custody_type, processing_version, snapshot_time) DO NOTHING`, strings.Join(valueStrings, ","))

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
//
// Operations are sorted by natural key once (before chunking); same rationale
// as SaveSnapshots above. See ADR-0002 §3.
func (r *AnchorageRepository) SaveOperations(ctx context.Context, operations []entity.AnchorageOperation) error {
	if len(operations) == 0 {
		return nil
	}

	slices.SortFunc(operations, func(a, b entity.AnchorageOperation) int {
		return cmp.Or(
			cmp.Compare(a.OperationID, b.OperationID),
			a.CreatedAt.Compare(b.CreatedAt),
		)
	})

	return r.txm.WithTransaction(ctx, func(tx pgx.Tx) error {
		for i := 0; i < len(operations); i += operationBatchSize {
			end := min(i+operationBatchSize, len(operations))
			if err := insertOperationBatch(ctx, tx, operations[i:end], int(r.buildID), r.runID); err != nil {
				return err
			}
		}
		return nil
	})
}

func insertOperationBatch(ctx context.Context, tx pgx.Tx, batch []entity.AnchorageOperation, buildID int, runID buildregistry.RunID) error {
	const cols = 12
	valueStrings := make([]string, 0, len(batch))
	valueArgs := make([]any, 0, len(batch)*cols)

	for i, op := range batch {
		base := i * cols
		valueStrings = append(valueStrings, fmt.Sprintf(
			"($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			base+1, base+2, base+3, base+4, base+5, base+6,
			base+7, base+8, base+9, base+10, base+11, base+12,
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
			buildID,
			runID,
		)
	}

	query := fmt.Sprintf(`
		INSERT INTO anchorage_operation (
			prime_id, operation_id, action, operation_type, type_id,
			asset_type, custody_type, quantity, notes,
			created_at, build_id, run_id
		) VALUES %s
		ON CONFLICT (operation_id, processing_version, created_at) DO NOTHING`, strings.Join(valueStrings, ","))

	if _, err := tx.Exec(ctx, query, valueArgs...); err != nil {
		return fmt.Errorf("insert operation batch: %w", err)
	}

	return nil
}

// KnownOperationIDs returns the set of operation_ids already stored for the
// prime. The operations feed is tiny (tens of rows per quarter), so the sync
// fetches the full list from Anchorage each run and skips these; the API's
// `afterId` cursor for this endpoint is undocumented (`timestamp|id` with an
// unknown unit and sort direction) and a synthesised one returned nothing for
// five months (VEC-826).
func (r *AnchorageRepository) KnownOperationIDs(ctx context.Context, primeID int64) (map[string]struct{}, error) {
	rows, err := r.pool.Query(ctx,
		"SELECT DISTINCT operation_id FROM anchorage_operation WHERE prime_id = $1",
		primeID,
	)
	if err != nil {
		return nil, fmt.Errorf("list known operation ids: %w", err)
	}
	defer rows.Close()

	known := make(map[string]struct{})
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan operation id: %w", err)
		}
		known[id] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate operation ids: %w", err)
	}
	return known, nil
}

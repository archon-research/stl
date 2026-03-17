package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check.
var _ outbound.AnchorageSnapshotRepository = (*AnchorageSnapshotRepository)(nil)

// AnchorageSnapshotRepository persists Anchorage package snapshots to Postgres.
type AnchorageSnapshotRepository struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// NewAnchorageSnapshotRepository creates a new AnchorageSnapshotRepository.
func NewAnchorageSnapshotRepository(pool *pgxpool.Pool, logger *slog.Logger) *AnchorageSnapshotRepository {
	if logger == nil {
		logger = slog.Default()
	}
	return &AnchorageSnapshotRepository{
		pool:   pool,
		logger: logger.With("component", "anchorage-snapshot-repo"),
	}
}

// SaveSnapshots inserts a batch of package snapshots in a single transaction.
func (r *AnchorageSnapshotRepository) SaveSnapshots(ctx context.Context, snapshots []entity.AnchoragePackageSnapshot) error {
	if len(snapshots) == 0 {
		return nil
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	const cols = 19
	valueStrings := make([]string, 0, len(snapshots))
	valueArgs := make([]interface{}, 0, len(snapshots)*cols)

	for i, snap := range snapshots {
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
		) VALUES %s`, strings.Join(valueStrings, ","))

	if _, err := tx.Exec(ctx, query, valueArgs...); err != nil {
		return fmt.Errorf("insert snapshots: %w", err)
	}

	return tx.Commit(ctx)
}

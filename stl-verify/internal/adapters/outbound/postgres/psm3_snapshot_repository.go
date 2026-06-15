package postgres

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that PSM3SnapshotRepository implements the port interface.
var _ outbound.PSM3SnapshotRepository = (*PSM3SnapshotRepository)(nil)

// PSM3SnapshotRepository persists PSM3 reserve snapshots to Postgres.
// processing_version is assigned by a DB trigger; build_id is supplied at
// construction and passed into every INSERT for idempotent replay.
type PSM3SnapshotRepository struct {
	txm     *TxManager
	logger  *slog.Logger
	buildID buildregistry.BuildID
}

// NewPSM3SnapshotRepository creates a new PSM3SnapshotRepository.
func NewPSM3SnapshotRepository(txm *TxManager, logger *slog.Logger, buildID buildregistry.BuildID) *PSM3SnapshotRepository {
	if logger == nil {
		logger = slog.Default()
	}
	return &PSM3SnapshotRepository{
		txm:     txm,
		logger:  logger.With("component", "psm3-snapshot-repo"),
		buildID: buildID,
	}
}

// SaveSnapshot appends one snapshot row. Raw balances are stored as NUMERIC
// via big.Int's decimal string representation. Duplicate snapshots (same
// natural key) are silently skipped via ON CONFLICT DO NOTHING.
func (r *PSM3SnapshotRepository) SaveSnapshot(ctx context.Context, snap *entity.PSM3Snapshot) error {
	return r.txm.WithTransaction(ctx, func(tx pgx.Tx) error {
		const q = `
			INSERT INTO psm3_snapshot (
				chain_id, address,
				usds_balance, susds_balance, usdc_balance,
				total_assets, conversion_rate,
				block_number, block_version, block_timestamp,
				source, build_id
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
			ON CONFLICT (chain_id, block_number, block_version, processing_version, block_timestamp) DO NOTHING
		`

		_, err := tx.Exec(ctx, q,
			snap.ChainID,
			snap.Address.Bytes(),
			snap.State.USDSBalance.String(),
			snap.State.SUSDSBalance.String(),
			snap.State.USDCBalance.String(),
			snap.State.TotalAssets.String(),
			snap.State.ConversionRate.String(),
			snap.BlockNumber,
			snap.BlockVersion,
			snap.BlockTimestamp,
			snap.Source,
			int(r.buildID),
		)
		if err != nil {
			return fmt.Errorf("insert psm3 snapshot (chain=%d block=%d): %w", snap.ChainID, snap.BlockNumber, err)
		}

		r.logger.Debug("psm3 snapshot saved", "chain", snap.ChainID, "block", snap.BlockNumber)
		return nil
	})
}

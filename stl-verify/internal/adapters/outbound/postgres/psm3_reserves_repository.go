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

// Compile-time check that PSM3ReservesRepository implements the port interface.
var _ outbound.PSM3ReservesRepository = (*PSM3ReservesRepository)(nil)

// PSM3ReservesRepository persists PSM3 reserve snapshots to Postgres.
// processing_version is assigned by a DB trigger; build_id is supplied at
// construction and passed into every INSERT for idempotent replay.
type PSM3ReservesRepository struct {
	txm     *TxManager
	logger  *slog.Logger
	buildID buildregistry.BuildID
}

// NewPSM3ReservesRepository creates a new PSM3ReservesRepository.
func NewPSM3ReservesRepository(txm *TxManager, logger *slog.Logger, buildID buildregistry.BuildID) *PSM3ReservesRepository {
	if logger == nil {
		logger = slog.Default()
	}
	return &PSM3ReservesRepository{
		txm:     txm,
		logger:  logger.With("component", "psm3-reserves-repo"),
		buildID: buildID,
	}
}

// SaveReserves appends one snapshot row. Raw balances are stored as NUMERIC
// via big.Int's decimal string representation. Duplicate snapshots (same
// natural key) are silently skipped via ON CONFLICT DO NOTHING.
func (r *PSM3ReservesRepository) SaveReserves(ctx context.Context, snap *entity.PSM3Reserves) error {
	return r.txm.WithTransaction(ctx, func(tx pgx.Tx) error {
		const q = `
			INSERT INTO psm3_reserves (
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
			return fmt.Errorf("insert psm3 reserves (chain=%d block=%d): %w", snap.ChainID, snap.BlockNumber, err)
		}

		r.logger.Debug("psm3 reserves saved", "chain", snap.ChainID, "block", snap.BlockNumber)
		return nil
	})
}

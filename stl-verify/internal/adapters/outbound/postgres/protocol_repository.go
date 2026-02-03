package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that ProtocolRepository implements outbound.ProtocolRepository
var _ outbound.ProtocolRepository = (*ProtocolRepository)(nil)

// ProtocolRepository is a PostgreSQL implementation of the outbound.ProtocolRepository port.
type ProtocolRepository struct {
	pool      *pgxpool.Pool
	logger    *slog.Logger
	batchSize int
}

// NewProtocolRepository creates a new PostgreSQL Protocol repository.
// If batchSize is <= 0, the default batch size from DefaultRepositoryConfig() is used.
// Returns an error if the database pool is nil.
//
// Note: This function does not verify that the database connection is alive.
// Use a separate health check or call pool.Ping() if connection validation is needed.
func NewProtocolRepository(pool *pgxpool.Pool, logger *slog.Logger, batchSize int) (*ProtocolRepository, error) {
	if pool == nil {
		return nil, fmt.Errorf("database pool cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if batchSize <= 0 {
		batchSize = DefaultRepositoryConfig().ProtocolBatchSize
	}
	return &ProtocolRepository{
		pool:      pool,
		logger:    logger,
		batchSize: batchSize,
	}, nil
}

// GetProtocolByAddress retrieves a protocol by its chain ID and address.
func (r *ProtocolRepository) GetProtocolByAddress(ctx context.Context, chainID int64, address string) (*entity.Protocol, error) {
	var protocol entity.Protocol
	err := r.pool.QueryRow(ctx,
		`SELECT id, chain_id, address, name, protocol_type, created_at_block
		 FROM protocol
		 WHERE chain_id = $1 AND address = $2`,
		chainID, address).Scan(
		&protocol.ID,
		&protocol.ChainID,
		&protocol.Address,
		&protocol.Name,
		&protocol.ProtocolType,
		&protocol.CreatedAtBlock,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil // Protocol not found
		}
		return nil, fmt.Errorf("failed to get protocol by address: %w", err)
	}

	return &protocol, nil
}

// UpsertSparkLendReserveData upserts SparkLend reserve data records atomically.
// All records are inserted in a single transaction - if any batch fails, all changes are rolled back.
func (r *ProtocolRepository) UpsertSparkLendReserveData(ctx context.Context, data []*entity.SparkLendReserveData) error {
	if len(data) == 0 {
		return nil
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer rollback(ctx, tx, r.logger)

	for i := 0; i < len(data); i += r.batchSize {
		end := i + r.batchSize
		if end > len(data) {
			end = len(data)
		}
		batch := data[i:end]

		if err := r.upsertSparkLendReserveDataBatch(ctx, tx, batch); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

func (r *ProtocolRepository) upsertSparkLendReserveDataBatch(ctx context.Context, tx pgx.Tx, data []*entity.SparkLendReserveData) error {
	if len(data) == 0 {
		return nil
	}

	var sb strings.Builder
	sb.WriteString(`
		INSERT INTO sparklend_reserve_data (
			protocol_id, token_id, block_number, block_version,
			unbacked, accrued_to_treasury_scaled, total_a_token, total_stable_debt, total_variable_debt,
			liquidity_rate, variable_borrow_rate, stable_borrow_rate, average_stable_borrow_rate,
			liquidity_index, variable_borrow_index, last_update_timestamp,
			decimals, ltv, liquidation_threshold, liquidation_bonus, reserve_factor,
			usage_as_collateral_enabled, borrowing_enabled, stable_borrow_rate_enabled, is_active, is_frozen
		) VALUES `)

	args := make([]any, 0, len(data)*26)
	for i, d := range data {
		if i > 0 {
			sb.WriteString(", ")
		}
		baseIdx := i * 26
		sb.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			baseIdx+1, baseIdx+2, baseIdx+3, baseIdx+4, baseIdx+5, baseIdx+6, baseIdx+7, baseIdx+8,
			baseIdx+9, baseIdx+10, baseIdx+11, baseIdx+12, baseIdx+13, baseIdx+14, baseIdx+15, baseIdx+16,
			baseIdx+17, baseIdx+18, baseIdx+19, baseIdx+20, baseIdx+21, baseIdx+22, baseIdx+23, baseIdx+24,
			baseIdx+25, baseIdx+26))
		args = append(args, d.ProtocolID, d.TokenID, d.BlockNumber, d.BlockVersion)

		for _, valToConvert := range []*big.Int{
			d.Unbacked,
			d.AccruedToTreasuryScaled,
			d.TotalAToken,
			d.TotalStableDebt,
			d.TotalVariableDebt,
			d.LiquidityRate,
			d.VariableBorrowRate,
			d.StableBorrowRate,
			d.AverageStableBorrowRate,
			d.LiquidityIndex,
			d.VariableBorrowIndex,
		} {
			convertedVal, err := bigIntToNumeric(valToConvert)
			if err != nil {
				return fmt.Errorf("sparklend_reserve_data[%d] (ProtocolID=%d, TokenID=%d): numeric fields must not be nil", i, d.ProtocolID, d.TokenID)
			}
			args = append(args, convertedVal)
		}

		args = append(args, d.LastUpdateTimestamp)

		// Configuration fields
		for _, valToConvert := range []*big.Int{
			d.Decimals,
			d.LTV,
			d.LiquidationThreshold,
			d.LiquidationBonus,
			d.ReserveFactor,
		} {
			convertedVal, err := bigIntToNumeric(valToConvert)
			if err != nil {
				return fmt.Errorf("sparklend_reserve_data[%d] (ProtocolID=%d, TokenID=%d): configuration numeric fields must not be nil", i, d.ProtocolID, d.TokenID)
			}
			args = append(args, convertedVal)
		}

		args = append(args, d.UsageAsCollateralEnabled, d.BorrowingEnabled, d.StableBorrowRateEnabled, d.IsActive, d.IsFrozen)
	}

	sb.WriteString(`
		ON CONFLICT (protocol_id, token_id, block_number, block_version) DO UPDATE SET
			unbacked = EXCLUDED.unbacked,
			accrued_to_treasury_scaled = EXCLUDED.accrued_to_treasury_scaled,
			total_a_token = EXCLUDED.total_a_token,
			total_stable_debt = EXCLUDED.total_stable_debt,
			total_variable_debt = EXCLUDED.total_variable_debt,
			liquidity_rate = EXCLUDED.liquidity_rate,
			variable_borrow_rate = EXCLUDED.variable_borrow_rate,
			stable_borrow_rate = EXCLUDED.stable_borrow_rate,
			average_stable_borrow_rate = EXCLUDED.average_stable_borrow_rate,
			liquidity_index = EXCLUDED.liquidity_index,
			variable_borrow_index = EXCLUDED.variable_borrow_index,
			last_update_timestamp = EXCLUDED.last_update_timestamp,
			decimals = EXCLUDED.decimals,
			ltv = EXCLUDED.ltv,
			liquidation_threshold = EXCLUDED.liquidation_threshold,
			liquidation_bonus = EXCLUDED.liquidation_bonus,
			reserve_factor = EXCLUDED.reserve_factor,
			usage_as_collateral_enabled = EXCLUDED.usage_as_collateral_enabled,
			borrowing_enabled = EXCLUDED.borrowing_enabled,
			stable_borrow_rate_enabled = EXCLUDED.stable_borrow_rate_enabled,
			is_active = EXCLUDED.is_active,
			is_frozen = EXCLUDED.is_frozen
	`)

	_, err := tx.Exec(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("failed to upsert sparklend reserve data batch: %w", err)
	}
	return nil
}

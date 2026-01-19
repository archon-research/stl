package postgres

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"log/slog"
	"math/big"
	"strings"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that ProtocolRepository implements outbound.ProtocolRepository
var _ outbound.ProtocolRepository = (*ProtocolRepository)(nil)

// ProtocolRepository is a PostgreSQL implementation of the outbound.ProtocolRepository port.
type ProtocolRepository struct {
	db        *sql.DB
	logger    *slog.Logger
	batchSize int
}

// NewProtocolRepository creates a new PostgreSQL Protocol repository.
// If batchSize is <= 0, the default batch size from DefaultRepositoryConfig() is used.
// Returns an error if the database connection is nil.
//
// Note: This function does not verify that the database connection is alive.
// Use a separate health check or call db.Ping() if connection validation is needed.
func NewProtocolRepository(db *sql.DB, logger *slog.Logger, batchSize int) (*ProtocolRepository, error) {
	if db == nil {
		return nil, fmt.Errorf("database connection cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if batchSize <= 0 {
		batchSize = DefaultRepositoryConfig().ProtocolBatchSize
	}
	return &ProtocolRepository{
		db:        db,
		logger:    logger,
		batchSize: batchSize,
	}, nil
}

// UpsertChains upserts chain records.
func (r *ProtocolRepository) UpsertChains(ctx context.Context, chains []*entity.Chain) error {
	if len(chains) == 0 {
		return nil
	}

	query := `
		INSERT INTO chains (chain_id, name, updated_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (chain_id) DO UPDATE SET
			name = EXCLUDED.name,
			updated_at = NOW()
	`

	for _, chain := range chains {
		if _, err := r.db.ExecContext(ctx, query, chain.ChainID, chain.Name); err != nil {
			return fmt.Errorf("failed to upsert chain %d: %w", chain.ChainID, err)
		}
	}
	return nil
}

// UpsertProtocols upserts protocol records.
func (r *ProtocolRepository) UpsertProtocols(ctx context.Context, protocols []*entity.Protocol) error {
	if len(protocols) == 0 {
		return nil
	}

	for i := 0; i < len(protocols); i += r.batchSize {
		end := i + r.batchSize
		if end > len(protocols) {
			end = len(protocols)
		}
		batch := protocols[i:end]

		if err := r.upsertProtocolBatch(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

func (r *ProtocolRepository) upsertProtocolBatch(ctx context.Context, protocols []*entity.Protocol) error {
	if len(protocols) == 0 {
		return nil
	}

	var sb strings.Builder
	sb.WriteString(`
		INSERT INTO protocols (id, chain_id, address, name, protocol_type, created_at_block, metadata, updated_at)
		VALUES `)

	args := make([]any, 0, len(protocols)*8)
	for i, protocol := range protocols {
		if i > 0 {
			sb.WriteString(", ")
		}
		baseIdx := i * 8
		sb.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, NOW())",
			baseIdx+1, baseIdx+2, baseIdx+3, baseIdx+4, baseIdx+5, baseIdx+6, baseIdx+7))

		metadata, err := marshalMetadata(protocol.Metadata)
		if err != nil {
			return fmt.Errorf("failed to marshal protocol metadata for protocol ID %d: %w", protocol.ID, err)
		}

		args = append(args, protocol.ID, protocol.ChainID, protocol.Address, protocol.Name, protocol.ProtocolType, protocol.CreatedAtBlock, metadata)
	}

	sb.WriteString(`
		ON CONFLICT (chain_id, address) DO UPDATE SET
			name = EXCLUDED.name,
			protocol_type = EXCLUDED.protocol_type,
			metadata = EXCLUDED.metadata,
			updated_at = NOW()
	`)

	_, err := r.db.ExecContext(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("failed to upsert protocol batch: %w", err)
	}
	return nil
}

// UpsertSparkLendReserveData upserts SparkLend reserve data records.
func (r *ProtocolRepository) UpsertSparkLendReserveData(ctx context.Context, data []*entity.SparkLendReserveData) error {
	if len(data) == 0 {
		return nil
	}

	for i := 0; i < len(data); i += r.batchSize {
		end := i + r.batchSize
		if end > len(data) {
			end = len(data)
		}
		batch := data[i:end]

		if err := r.upsertSparkLendReserveDataBatch(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

func (r *ProtocolRepository) upsertSparkLendReserveDataBatch(ctx context.Context, data []*entity.SparkLendReserveData) error {
	if len(data) == 0 {
		return nil
	}

	var sb strings.Builder
	sb.WriteString(`
		INSERT INTO sparklend_reserve_data (
			id, protocol_id, token_id, block_number,
			unbacked, accrued_to_treasury_scaled, total_a_token, total_stable_debt, total_variable_debt,
			liquidity_rate, variable_borrow_rate, stable_borrow_rate, average_stable_borrow_rate,
			liquidity_index, variable_borrow_index, last_update_timestamp
		) VALUES `)

	args := make([]any, 0, len(data)*16)
	for i, d := range data {
		if i > 0 {
			sb.WriteString(", ")
		}
		baseIdx := i * 16
		sb.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			baseIdx+1, baseIdx+2, baseIdx+3, baseIdx+4, baseIdx+5, baseIdx+6, baseIdx+7, baseIdx+8,
			baseIdx+9, baseIdx+10, baseIdx+11, baseIdx+12, baseIdx+13, baseIdx+14, baseIdx+15, baseIdx+16))
		args = append(args, d.ID, d.ProtocolID, d.TokenID, d.BlockNumber)

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
				return fmt.Errorf("sparklend_reserve_data[%d] (ID=%d, ProtocolID=%d, TokenID=%d): numeric fields must not be nil", i, d.ID, d.ProtocolID, d.TokenID)
			}
			args = append(args, convertedVal)
		}

		args = append(args, d.LastUpdateTimestamp)
	}

	sb.WriteString(`
		ON CONFLICT (protocol_id, token_id, block_number) DO UPDATE SET
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
			last_update_timestamp = EXCLUDED.last_update_timestamp
	`)

	_, err := r.db.ExecContext(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("failed to upsert sparklend reserve data batch: %w", err)
	}
	return nil
}

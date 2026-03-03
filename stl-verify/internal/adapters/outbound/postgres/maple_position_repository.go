package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that MaplePositionRepository implements outbound.MaplePositionRepository.
var _ outbound.MaplePositionRepository = (*MaplePositionRepository)(nil)

// MaplePositionRepository is a PostgreSQL implementation of the outbound.MaplePositionRepository port.
type MaplePositionRepository struct {
	pool      *pgxpool.Pool
	logger    *slog.Logger
	batchSize int
}

// NewMaplePositionRepository creates a new PostgreSQL Maple position repository.
// If batchSize is <= 0, the default batch size from DefaultRepositoryConfig() is used.
// Returns an error if the database pool is nil.
func NewMaplePositionRepository(pool *pgxpool.Pool, logger *slog.Logger, batchSize int) (*MaplePositionRepository, error) {
	if pool == nil {
		return nil, fmt.Errorf("database pool cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if batchSize <= 0 {
		batchSize = DefaultRepositoryConfig().PositionBatchSize
	}
	return &MaplePositionRepository{
		pool:      pool,
		logger:    logger,
		batchSize: batchSize,
	}, nil
}

// SaveBorrowerSnapshots persists Maple borrower (debt) position snapshots atomically.
func (r *MaplePositionRepository) SaveBorrowerSnapshots(ctx context.Context, snapshots []*entity.MapleBorrower) error {
	if len(snapshots) == 0 {
		return nil
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer rollback(ctx, tx, r.logger)

	for i := 0; i < len(snapshots); i += r.batchSize {
		end := min(i+r.batchSize, len(snapshots))
		if err := r.saveBorrowerBatch(ctx, tx, snapshots[i:end]); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}
	return nil
}

// saveBorrowerBatch inserts a batch of maple borrower records with ON CONFLICT upsert.
func (r *MaplePositionRepository) saveBorrowerBatch(ctx context.Context, tx pgx.Tx, snapshots []*entity.MapleBorrower) error {
	if len(snapshots) == 0 {
		return nil
	}

	var sb strings.Builder
	sb.WriteString(`
		INSERT INTO maple_borrower (user_id, protocol_id, pool_asset, pool_decimals, amount, block_number, block_version, loan_type, loan_asset_symbol, loan_dex_name, loan_location, loan_wallet_address, loan_wallet_type)
		VALUES `)

	args := make([]any, 0, len(snapshots)*13)
	for i, s := range snapshots {
		if i > 0 {
			sb.WriteString(", ")
		}
		baseIdx := i * 13
		sb.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			baseIdx+1, baseIdx+2, baseIdx+3, baseIdx+4, baseIdx+5, baseIdx+6, baseIdx+7, baseIdx+8, baseIdx+9, baseIdx+10, baseIdx+11, baseIdx+12, baseIdx+13))

		amount, err := bigIntToNumeric(s.Amount)
		if err != nil {
			return fmt.Errorf("maple_borrower[%d] (UserID=%d): converting amount to numeric: %w", i, s.UserID, err)
		}

		args = append(args, s.UserID, s.ProtocolID, s.PoolAsset, s.PoolDecimals, amount, s.BlockNumber, s.BlockVersion,
			nilIfEmpty(s.LoanType), nilIfEmpty(s.LoanAssetSymbol), nilIfEmpty(s.LoanDexName),
			nilIfEmpty(s.LoanLocation), nilIfEmpty(s.LoanWalletAddress), nilIfEmpty(s.LoanWalletType))
	}

	sb.WriteString(`
		ON CONFLICT (user_id, protocol_id, pool_asset, block_number, block_version) DO UPDATE SET
			pool_decimals = EXCLUDED.pool_decimals,
			amount = EXCLUDED.amount,
			loan_type = EXCLUDED.loan_type,
			loan_asset_symbol = EXCLUDED.loan_asset_symbol,
			loan_dex_name = EXCLUDED.loan_dex_name,
			loan_location = EXCLUDED.loan_location,
			loan_wallet_address = EXCLUDED.loan_wallet_address,
			loan_wallet_type = EXCLUDED.loan_wallet_type
	`)

	_, err := tx.Exec(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("upserting maple borrower batch: %w", err)
	}
	return nil
}

// SaveCollateralSnapshots persists Maple collateral position snapshots atomically.
func (r *MaplePositionRepository) SaveCollateralSnapshots(ctx context.Context, snapshots []*entity.MapleCollateral) error {
	if len(snapshots) == 0 {
		return nil
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer rollback(ctx, tx, r.logger)

	for i := 0; i < len(snapshots); i += r.batchSize {
		end := min(i+r.batchSize, len(snapshots))
		if err := r.saveCollateralBatch(ctx, tx, snapshots[i:end]); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}
	return nil
}

// saveCollateralBatch inserts a batch of maple collateral records with ON CONFLICT upsert.
func (r *MaplePositionRepository) saveCollateralBatch(ctx context.Context, tx pgx.Tx, snapshots []*entity.MapleCollateral) error {
	if len(snapshots) == 0 {
		return nil
	}

	var sb strings.Builder
	sb.WriteString(`
		INSERT INTO maple_collateral (user_id, protocol_id, collateral_asset, collateral_decimals, amount, custodian, state, liquidation_level, block_number, block_version, loan_type, loan_asset_symbol, loan_dex_name, loan_location, loan_wallet_address, loan_wallet_type)
		VALUES `)

	args := make([]any, 0, len(snapshots)*16)
	for i, s := range snapshots {
		if i > 0 {
			sb.WriteString(", ")
		}
		baseIdx := i * 16
		sb.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			baseIdx+1, baseIdx+2, baseIdx+3, baseIdx+4, baseIdx+5, baseIdx+6, baseIdx+7, baseIdx+8, baseIdx+9, baseIdx+10, baseIdx+11, baseIdx+12, baseIdx+13, baseIdx+14, baseIdx+15, baseIdx+16))

		amount, err := bigIntToNumeric(s.Amount)
		if err != nil {
			return fmt.Errorf("maple_collateral[%d] (UserID=%d): converting amount to numeric: %w", i, s.UserID, err)
		}

		var liqLevel *string
		if s.LiquidationLevel != nil {
			v := s.LiquidationLevel.String()
			liqLevel = &v
		}

		args = append(args, s.UserID, s.ProtocolID, s.CollateralAsset, s.CollateralDecimals, amount,
			nilIfEmpty(s.Custodian), nilIfEmpty(s.State), liqLevel, s.BlockNumber, s.BlockVersion,
			nilIfEmpty(s.LoanType), nilIfEmpty(s.LoanAssetSymbol), nilIfEmpty(s.LoanDexName),
			nilIfEmpty(s.LoanLocation), nilIfEmpty(s.LoanWalletAddress), nilIfEmpty(s.LoanWalletType))
	}

	sb.WriteString(`
		ON CONFLICT (user_id, protocol_id, collateral_asset, block_number, block_version) DO UPDATE SET
			collateral_decimals = EXCLUDED.collateral_decimals,
			amount = EXCLUDED.amount,
			custodian = EXCLUDED.custodian,
			state = EXCLUDED.state,
			liquidation_level = EXCLUDED.liquidation_level,
			loan_type = EXCLUDED.loan_type,
			loan_asset_symbol = EXCLUDED.loan_asset_symbol,
			loan_dex_name = EXCLUDED.loan_dex_name,
			loan_location = EXCLUDED.loan_location,
			loan_wallet_address = EXCLUDED.loan_wallet_address,
			loan_wallet_type = EXCLUDED.loan_wallet_type
	`)

	_, err := tx.Exec(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("upserting maple collateral batch: %w", err)
	}
	return nil
}

// nilIfEmpty returns nil if the string is empty, otherwise returns a pointer to the string.
func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

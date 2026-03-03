package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
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

// SaveLoanSnapshots persists Maple loan metadata snapshots atomically.
// Returns a map of loan_address (hex string) -> loan_id for use when persisting borrowers and collateral.
func (r *MaplePositionRepository) SaveLoanSnapshots(ctx context.Context, snapshots []*entity.MapleLoan) (map[string]int64, error) {
	if len(snapshots) == 0 {
		return make(map[string]int64), nil
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("beginning transaction: %w", err)
	}
	defer rollback(ctx, tx, r.logger)

	loanIDMap := make(map[string]int64, len(snapshots))

	for i := 0; i < len(snapshots); i += r.batchSize {
		end := min(i+r.batchSize, len(snapshots))
		batchMap, err := r.saveLoanBatch(ctx, tx, snapshots[i:end])
		if err != nil {
			return nil, err
		}
		maps.Copy(loanIDMap, batchMap)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing transaction: %w", err)
	}
	return loanIDMap, nil
}

// saveLoanBatch inserts a batch of maple loan records.
// Uses a no-op update on conflict to ensure RETURNING always yields the row ID.
// Returns a map of loan_address (hex string) -> loan_id.
func (r *MaplePositionRepository) saveLoanBatch(ctx context.Context, tx pgx.Tx, snapshots []*entity.MapleLoan) (map[string]int64, error) {
	if len(snapshots) == 0 {
		return make(map[string]int64), nil
	}

	loanColumns := []string{
		"loan_address", "protocol_id", "block_number", "block_version",
		"pool_address", "pool_name", "pool_asset_symbol", "pool_asset_decimals",
		"loan_type", "loan_asset_symbol", "loan_dex_name",
		"loan_location", "loan_wallet_address", "loan_wallet_type",
	}
	colCount := len(loanColumns)

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("INSERT INTO maple_loan (%s)\nVALUES ", strings.Join(loanColumns, ", ")))

	args := make([]any, 0, len(snapshots)*colCount)
	for i, s := range snapshots {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(buildPlaceholders(i, colCount))

		args = append(args, s.LoanAddress.Bytes(), s.ProtocolID, s.BlockNumber, s.BlockVersion,
			s.PoolAddress.Bytes(), s.PoolName, s.PoolAssetSymbol, s.PoolAssetDecimals,
			nilIfEmpty(s.LoanType), nilIfEmpty(s.LoanAssetSymbol), nilIfEmpty(s.LoanDexName),
			nilIfEmpty(s.LoanLocation), nilIfEmpty(s.LoanWalletAddress), nilIfEmpty(s.LoanWalletType))
	}

	sb.WriteString(`
		ON CONFLICT (loan_address, block_number, block_version) DO UPDATE SET
			id = maple_loan.id
		RETURNING id, loan_address
	`)

	rows, err := tx.Query(ctx, sb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("upserting maple loan batch: %w", err)
	}
	defer rows.Close()

	loanIDMap := make(map[string]int64, len(snapshots))
	for rows.Next() {
		var id int64
		var loanAddr []byte
		if err := rows.Scan(&id, &loanAddr); err != nil {
			return nil, fmt.Errorf("scanning loan ID: %w", err)
		}
		// Convert bytes to hex string for map key
		loanIDMap[fmt.Sprintf("0x%x", loanAddr)] = id
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating loan rows: %w", err)
	}

	return loanIDMap, nil
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

// saveBorrowerBatch inserts a batch of maple borrower records with ON CONFLICT DO NOTHING.
func (r *MaplePositionRepository) saveBorrowerBatch(ctx context.Context, tx pgx.Tx, snapshots []*entity.MapleBorrower) error {
	if len(snapshots) == 0 {
		return nil
	}

	borrowerColumns := []string{
		"loan_id", "user_id", "protocol_id", "pool_asset",
		"pool_decimals", "amount", "block_number", "block_version",
	}
	colCount := len(borrowerColumns)

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("\n\t\tINSERT INTO maple_borrower (%s)\n\t\tVALUES ", strings.Join(borrowerColumns, ", ")))

	args := make([]any, 0, len(snapshots)*colCount)
	for i, s := range snapshots {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(buildPlaceholders(i, colCount))

		amount, err := bigIntToNumeric(s.Amount)
		if err != nil {
			return fmt.Errorf("maple_borrower[%d] (LoanID=%d): converting amount to numeric: %w", i, s.LoanID, err)
		}

		args = append(args, s.LoanID, s.UserID, s.ProtocolID, s.PoolAsset, s.PoolDecimals, amount, s.BlockNumber, s.BlockVersion)
	}

	sb.WriteString(`
		ON CONFLICT (loan_id, block_number, block_version) DO NOTHING
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

// saveCollateralBatch inserts a batch of maple collateral records with ON CONFLICT DO NOTHING.
func (r *MaplePositionRepository) saveCollateralBatch(ctx context.Context, tx pgx.Tx, snapshots []*entity.MapleCollateral) error {
	if len(snapshots) == 0 {
		return nil
	}

	collateralColumns := []string{
		"loan_id", "user_id", "protocol_id", "collateral_asset",
		"collateral_decimals", "amount", "custodian", "state",
		"liquidation_level", "block_number", "block_version",
	}
	colCount := len(collateralColumns)

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("\n\t\tINSERT INTO maple_collateral (%s)\n\t\tVALUES ", strings.Join(collateralColumns, ", ")))

	args := make([]any, 0, len(snapshots)*colCount)
	for i, s := range snapshots {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(buildPlaceholders(i, colCount))

		amount, err := bigIntToNumeric(s.Amount)
		if err != nil {
			return fmt.Errorf("maple_collateral[%d] (LoanID=%d): converting amount to numeric: %w", i, s.LoanID, err)
		}

		var liqLevel *string
		if s.LiquidationLevel != nil {
			v := s.LiquidationLevel.String()
			liqLevel = &v
		}

		args = append(args, s.LoanID, s.UserID, s.ProtocolID, s.CollateralAsset, s.CollateralDecimals, amount,
			nilIfEmpty(s.Custodian), nilIfEmpty(s.State), liqLevel, s.BlockNumber, s.BlockVersion)
	}

	sb.WriteString(`
		ON CONFLICT (loan_id, block_number, block_version) DO NOTHING
	`)

	_, err := tx.Exec(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("upserting maple collateral batch: %w", err)
	}
	return nil
}

// buildPlaceholders generates a SQL placeholder tuple like "($1, $2, $3)" for the given
// row index and column count. Row 0 produces ($1, $2, ...), row 1 produces ($N+1, $N+2, ...), etc.
func buildPlaceholders(rowIdx, colCount int) string {
	parts := make([]string, colCount)
	base := rowIdx * colCount
	for i := range colCount {
		parts[i] = fmt.Sprintf("$%d", base+i+1)
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

// nilIfEmpty returns nil if the string is empty, otherwise returns a pointer to the string.
func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

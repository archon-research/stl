package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"sort"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that PositionRepository implements outbound.PositionRepository
var _ outbound.PositionRepository = (*PositionRepository)(nil)

// PositionRepository is a PostgreSQL implementation of the outbound.PositionRepository port.
type PositionRepository struct {
	pool      *pgxpool.Pool
	logger    *slog.Logger
	batchSize int
}

// NewPositionRepository creates a new PostgreSQL Position repository.
// If batchSize is <= 0, the default batch size from DefaultRepositoryConfig() is used.
// Returns an error if the database pool is nil.
//
// Note: This function does not verify that the database connection is alive.
// Use a separate health check or call pool.Ping() if connection validation is needed.
func NewPositionRepository(pool *pgxpool.Pool, logger *slog.Logger, batchSize int) (*PositionRepository, error) {
	if pool == nil {
		return nil, fmt.Errorf("database pool cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if batchSize <= 0 {
		batchSize = DefaultRepositoryConfig().PositionBatchSize
	}
	return &PositionRepository{
		pool:      pool,
		logger:    logger,
		batchSize: batchSize,
	}, nil
}

// SaveBorrower saves a single borrower (debt) position record within an external transaction.
// Uses upsert semantics: ON CONFLICT updates the existing record.
func (r *PositionRepository) SaveBorrower(ctx context.Context, tx pgx.Tx, userID, protocolID, tokenID, blockNumber int64, blockVersion int, amount, eventType string, txHash []byte) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO borrower (user_id, protocol_id, token_id, block_number, block_version, amount, change, event_type, tx_hash)
		 VALUES ($1, $2, $3, $4, $5, $6, $6, $7, $8)
		 ON CONFLICT (user_id, protocol_id, token_id, block_number, block_version)
		 DO UPDATE SET amount = EXCLUDED.amount, change = EXCLUDED.change, event_type = EXCLUDED.event_type, tx_hash = EXCLUDED.tx_hash`,
		userID, protocolID, tokenID, blockNumber, blockVersion, amount, eventType, txHash)

	if err != nil {
		return fmt.Errorf("failed to save borrower: %w", err)
	}
	return nil
}

// SaveBorrowerCollateral saves a single collateral position record within an external transaction.
// Uses upsert semantics: ON CONFLICT updates the existing record.
func (r *PositionRepository) SaveBorrowerCollateral(ctx context.Context, tx pgx.Tx, userID, protocolID, tokenID, blockNumber int64, blockVersion int, amount, eventType string, txHash []byte, collateralEnabled bool) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO borrower_collateral (user_id, protocol_id, token_id, block_number, block_version, amount, change, event_type, tx_hash, collateral_enabled)
		 VALUES ($1, $2, $3, $4, $5, $6, $6, $7, $8, $9)
		 ON CONFLICT (user_id, protocol_id, token_id, block_number, block_version)
		 DO UPDATE SET amount = EXCLUDED.amount, change = EXCLUDED.change, event_type = EXCLUDED.event_type, tx_hash = EXCLUDED.tx_hash, collateral_enabled = EXCLUDED.collateral_enabled`,
		userID, protocolID, tokenID, blockNumber, blockVersion, amount, eventType, txHash, collateralEnabled)

	if err != nil {
		return fmt.Errorf("failed to save collateral: %w", err)
	}
	return nil
}

// CollateralRecord represents a single collateral record for batch insertion.
// This is a simplified struct that matches the service layer's data format.
type CollateralRecord struct {
	UserID            int64
	ProtocolID        int64
	TokenID           int64
	BlockNumber       int64
	BlockVersion      int
	Amount            string // decimal-adjusted amount string
	EventType         string
	TxHash            []byte
	CollateralEnabled bool
}

// SaveBorrowerCollaterals saves multiple borrower collateral position records using pgx.Batch.
// Uses ON CONFLICT DO NOTHING to ensure immutability - existing records are never modified.
// This is critical for reproducible calculations: data used in a calculation must not change.
// Returns nil if records slice is empty.
func (r *PositionRepository) SaveBorrowerCollaterals(ctx context.Context, tx pgx.Tx, records []CollateralRecord) error {
	if len(records) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for _, rec := range records {
		batch.Queue(
			`INSERT INTO borrower_collateral (user_id, protocol_id, token_id, block_number, block_version, amount, change, event_type, tx_hash, collateral_enabled)
			 VALUES ($1, $2, $3, $4, $5, $6, $6, $7, $8, $9)
			 ON CONFLICT (user_id, protocol_id, token_id, block_number, block_version) DO NOTHING`,
			rec.UserID, rec.ProtocolID, rec.TokenID, rec.BlockNumber, rec.BlockVersion, rec.Amount, rec.EventType, rec.TxHash, rec.CollateralEnabled,
		)
	}

	br := tx.SendBatch(ctx, batch)
	defer br.Close()

	for i := range records {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("failed to save collateral record %d: %w", i, err)
		}
	}

	return nil
}

// UpsertBorrowers upserts borrower (debt) position records atomically.
// All records are inserted in a single transaction - if any batch fails, all changes are rolled back.
func (r *PositionRepository) UpsertBorrowers(ctx context.Context, borrowers []*entity.Borrower) error {
	if len(borrowers) == 0 {
		return nil
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer rollback(ctx, tx, r.logger)

	for i := 0; i < len(borrowers); i += r.batchSize {
		end := i + r.batchSize
		if end > len(borrowers) {
			end = len(borrowers)
		}
		batch := borrowers[i:end]

		if err := r.upsertBorrowerBatch(ctx, tx, batch); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

func (r *PositionRepository) upsertBorrowerBatch(ctx context.Context, tx pgx.Tx, borrowers []*entity.Borrower) error {
	if len(borrowers) == 0 {
		return nil
	}

	var sb strings.Builder
	sb.WriteString(`
		INSERT INTO borrower (user_id, protocol_id, token_id, block_number, block_version, amount, change, event_type, tx_hash)
		VALUES `)

	args := make([]any, 0, len(borrowers)*9)
	for i, b := range borrowers {
		if i > 0 {
			sb.WriteString(", ")
		}
		baseIdx := i * 9
		sb.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			baseIdx+1, baseIdx+2, baseIdx+3, baseIdx+4, baseIdx+5, baseIdx+6, baseIdx+7, baseIdx+8, baseIdx+9))

		amount, err := bigIntToNumeric(b.Amount)
		if err != nil {
			return fmt.Errorf("borrower[%d] (UserID=%d): failed to convert Amount to numeric: %w", i, b.UserID, err)
		}
		change, err := bigIntToNumeric(b.Change)
		if err != nil {
			return fmt.Errorf("borrower[%d] (UserID=%d): failed to convert Change to numeric: %w", i, b.UserID, err)
		}

		args = append(args, b.UserID, b.ProtocolID, b.TokenID, b.BlockNumber, b.BlockVersion, amount, change, b.EventType, b.TxHash)
	}

	sb.WriteString(`
		ON CONFLICT (user_id, protocol_id, token_id, block_number, block_version) DO UPDATE SET
			amount = EXCLUDED.amount,
			change = EXCLUDED.change,
			event_type = EXCLUDED.event_type,
			tx_hash = EXCLUDED.tx_hash
	`)

	_, err := tx.Exec(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("failed to upsert borrower batch: %w", err)
	}
	return nil
}

// UpsertBorrowerCollateral upserts collateral position records atomically.
// All records are inserted in a single transaction - if any batch fails, all changes are rolled back.
func (r *PositionRepository) UpsertBorrowerCollateral(ctx context.Context, collateral []*entity.BorrowerCollateral) error {
	if len(collateral) == 0 {
		return nil
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer rollback(ctx, tx, r.logger)

	for i := 0; i < len(collateral); i += r.batchSize {
		end := i + r.batchSize
		if end > len(collateral) {
			end = len(collateral)
		}
		batch := collateral[i:end]

		if err := r.upsertBorrowerCollateralBatch(ctx, tx, batch); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

func (r *PositionRepository) upsertBorrowerCollateralBatch(ctx context.Context, tx pgx.Tx, collateral []*entity.BorrowerCollateral) error {
	if len(collateral) == 0 {
		return nil
	}

	var sb strings.Builder
	sb.WriteString(`
		INSERT INTO borrower_collateral (user_id, protocol_id, token_id, block_number, block_version, amount, change, event_type, tx_hash, collateral_enabled)
		VALUES `)

	args := make([]any, 0, len(collateral)*10)
	for i, c := range collateral {
		if i > 0 {
			sb.WriteString(", ")
		}
		baseIdx := i * 10
		sb.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			baseIdx+1, baseIdx+2, baseIdx+3, baseIdx+4, baseIdx+5, baseIdx+6, baseIdx+7, baseIdx+8, baseIdx+9, baseIdx+10))

		amount, err := bigIntToNumeric(c.Amount)
		if err != nil {
			return fmt.Errorf("borrower_collateral[%d] (UserID=%d): failed to convert Amount to numeric: %w", i, c.UserID, err)
		}
		change, err := bigIntToNumeric(c.Change)
		if err != nil {
			return fmt.Errorf("borrower_collateral[%d] (UserID=%d): failed to convert Change to numeric: %w", i, c.UserID, err)
		}

		args = append(args, c.UserID, c.ProtocolID, c.TokenID, c.BlockNumber, c.BlockVersion, amount, change, c.EventType, c.TxHash, c.CollateralEnabled)
	}

	sb.WriteString(`
		ON CONFLICT (user_id, protocol_id, token_id, block_number, block_version) DO UPDATE SET
			amount = EXCLUDED.amount,
			change = EXCLUDED.change,
			event_type = EXCLUDED.event_type,
			tx_hash = EXCLUDED.tx_hash,
			collateral_enabled = EXCLUDED.collateral_enabled
	`)

	_, err := tx.Exec(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("failed to upsert borrower collateral batch: %w", err)
	}
	return nil
}

type latestPositionRow struct {
	userAddress  []byte
	tokenAddress []byte
	symbol       string
	amount       string
}

func (r *PositionRepository) ListLatestUserPositions(ctx context.Context, protocolID int64, limit int) ([]entity.UserLatestPositions, error) {
	debtRows, err := r.queryLatestDebt(ctx, protocolID)
	if err != nil {
		return nil, fmt.Errorf("list latest debt positions: %w", err)
	}

	collateralRows, err := r.queryLatestCollateral(ctx, protocolID)
	if err != nil {
		return nil, fmt.Errorf("list latest collateral positions: %w", err)
	}

	positions := make(map[string]*entity.UserLatestPositions)
	addAsset := func(userAddress string, asset entity.AssetAmount, isDebt bool) {
		entry, ok := positions[userAddress]
		if !ok {
			entry = &entity.UserLatestPositions{UserAddress: userAddress}
			positions[userAddress] = entry
		}
		if isDebt {
			entry.Debt = append(entry.Debt, asset)
			return
		}
		entry.Collateral = append(entry.Collateral, asset)
	}

	for _, row := range debtRows {
		amount, ok := new(big.Int).SetString(row.amount, 10)
		if !ok {
			return nil, fmt.Errorf("parse debt amount %q: invalid integer", row.amount)
		}
		userAddress := common.BytesToAddress(row.userAddress).Hex()
		addAsset(userAddress, entity.AssetAmount{
			TokenAddress: common.BytesToAddress(row.tokenAddress),
			Symbol:       row.symbol,
			Amount:       amount,
		}, true)
	}

	for _, row := range collateralRows {
		amount, ok := new(big.Int).SetString(row.amount, 10)
		if !ok {
			return nil, fmt.Errorf("parse collateral amount %q: invalid integer", row.amount)
		}
		userAddress := common.BytesToAddress(row.userAddress).Hex()
		addAsset(userAddress, entity.AssetAmount{
			TokenAddress: common.BytesToAddress(row.tokenAddress),
			Symbol:       row.symbol,
			Amount:       amount,
		}, false)
	}

	result := make([]entity.UserLatestPositions, 0, len(positions))
	for _, entry := range positions {
		if len(entry.Debt) == 0 && len(entry.Collateral) == 0 {
			continue
		}
		result = append(result, *entry)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].UserAddress < result[j].UserAddress
	})
	if limit > 0 && len(result) > limit {
		result = result[:limit]
	}

	return result, nil
}

func (r *PositionRepository) queryLatestDebt(ctx context.Context, protocolID int64) ([]latestPositionRow, error) {
	rows, err := r.pool.Query(ctx, `
		WITH latest AS (
			SELECT DISTINCT ON (b.user_id, b.token_id)
				b.user_id,
				b.token_id,
				b.amount,
				b.block_number,
				b.block_version
			FROM borrower b
			JOIN block_states bs ON bs.number = b.block_number AND bs.version = b.block_version
			WHERE b.protocol_id = $1
				AND bs.is_orphaned = false
			ORDER BY b.user_id, b.token_id, b.block_number DESC, b.block_version DESC
		)
		SELECT
			u.address,
			t.address,
			t.symbol,
			latest.amount
		FROM latest
		JOIN "user" u ON u.id = latest.user_id
		JOIN token t ON t.id = latest.token_id
		WHERE latest.amount <> 0
	`, protocolID)
	if err != nil {
		return nil, fmt.Errorf("query debt rows: %w", err)
	}
	defer rows.Close()

	var results []latestPositionRow
	for rows.Next() {
		var row latestPositionRow
		if err := rows.Scan(&row.userAddress, &row.tokenAddress, &row.symbol, &row.amount); err != nil {
			return nil, fmt.Errorf("scan debt row: %w", err)
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate debt rows: %w", err)
	}
	return results, nil
}

func (r *PositionRepository) queryLatestCollateral(ctx context.Context, protocolID int64) ([]latestPositionRow, error) {
	rows, err := r.pool.Query(ctx, `
		WITH latest AS (
			SELECT DISTINCT ON (c.user_id, c.token_id)
				c.user_id,
				c.token_id,
				c.amount,
				c.collateral_enabled,
				c.block_number,
				c.block_version
			FROM borrower_collateral c
			JOIN block_states bs ON bs.number = c.block_number AND bs.version = c.block_version
			WHERE c.protocol_id = $1
				AND bs.is_orphaned = false
			ORDER BY c.user_id, c.token_id, c.block_number DESC, c.block_version DESC
		)
		SELECT
			u.address,
			t.address,
			t.symbol,
			latest.amount
		FROM latest
		JOIN "user" u ON u.id = latest.user_id
		JOIN token t ON t.id = latest.token_id
		WHERE latest.amount <> 0
			AND latest.collateral_enabled = true
	`, protocolID)
	if err != nil {
		return nil, fmt.Errorf("query collateral rows: %w", err)
	}
	defer rows.Close()

	var results []latestPositionRow
	for rows.Next() {
		var row latestPositionRow
		if err := rows.Scan(&row.userAddress, &row.tokenAddress, &row.symbol, &row.amount); err != nil {
			return nil, fmt.Errorf("scan collateral row: %w", err)
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate collateral rows: %w", err)
	}
	return results, nil
}

// lending.go provides a PostgreSQL implementation for lending protocol data.
//
// This repository persists borrower positions and collateral data to PostgreSQL.
// It supports:
//   - Automatic schema migration via embedded SQL
//   - Auto-creation of users, tokens, and protocols
//   - Borrower position tracking with reorg support (block_version)
//   - Collateral position tracking
//
// The schema is defined in migrations/002_lending_protocol_tables.sql and is
// automatically applied via the Migrate() method.
package postgres

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"log/slog"
	"strings"

	"github.com/ethereum/go-ethereum/common"
)

//go:embed migrations/002_lending_protocol_tables.sql
var lendingSchema string

// LendingRepository handles all lending protocol database operations.
type LendingRepository struct {
	db      *sql.DB
	chainID int64
	logger  *slog.Logger
}

// NewLendingRepository creates a new lending repository.
func NewLendingRepository(db *sql.DB, chainID int64, logger *slog.Logger) *LendingRepository {
	if logger == nil {
		logger = slog.Default()
	}
	return &LendingRepository{
		db:      db,
		chainID: chainID,
		logger:  logger,
	}
}

// Migrate creates the lending protocol tables if they don't exist.
func (r *LendingRepository) Migrate(ctx context.Context) error {
	_, err := r.db.ExecContext(ctx, lendingSchema)
	if err != nil {
		return fmt.Errorf("failed to migrate lending schema: %w", err)
	}
	return nil
}

// BeginTx starts a new database transaction.
func (r *LendingRepository) BeginTx(ctx context.Context) (*sql.Tx, error) {
	return r.db.BeginTx(ctx, nil)
}

// EnsureUser creates a user if it doesn't exist and returns the user ID.
func (r *LendingRepository) EnsureUser(ctx context.Context, tx *sql.Tx, address common.Address) (int64, error) {
	var userID int64
	addressHex := strings.ToLower(address.Hex()) // Store as lowercase hex string

	// Try to get existing user
	err := tx.QueryRowContext(ctx,
		`SELECT id FROM users WHERE chain_id = $1 AND address = $2`,
		r.chainID, addressHex).Scan(&userID)

	if err == sql.ErrNoRows {
		// Create new user
		err = tx.QueryRowContext(ctx,
			`INSERT INTO users (chain_id, address, first_seen_block, created_at, updated_at)
			 VALUES ($1, $2, $3, NOW(), NOW())
			 RETURNING id`,
			r.chainID, addressHex, 0).Scan(&userID)
		if err != nil {
			return 0, fmt.Errorf("failed to create user: %w", err)
		}
		r.logger.Debug("user created", "address", address.Hex(), "id", userID)
	} else if err != nil {
		return 0, fmt.Errorf("failed to get user: %w", err)
	}

	return userID, nil
}

// GetOrCreateToken gets an existing token or creates it with the provided decimals.
func (r *LendingRepository) GetOrCreateToken(ctx context.Context, tx *sql.Tx, address common.Address, decimals int) (int64, error) {
	var tokenID int64
	addressHex := strings.ToLower(address.Hex())

	err := tx.QueryRowContext(ctx,
		`SELECT id FROM token WHERE chain_id = $1 AND address = $2`,
		r.chainID, addressHex).Scan(&tokenID)

	if err == sql.ErrNoRows {
		// Auto-create token with provided decimals
		r.logger.Info("auto-creating token", "address", address.Hex(), "decimals", decimals)
		err = tx.QueryRowContext(ctx,
			`INSERT INTO token (chain_id, address, symbol, decimals, created_at_block)
			 VALUES ($1, $2, $3, $4, $5)
			 RETURNING id`,
			r.chainID, addressHex, "UNKNOWN", decimals, 0).Scan(&tokenID)
		if err != nil {
			return 0, fmt.Errorf("failed to create token: %w", err)
		}
		r.logger.Info("token auto-created", "address", address.Hex(), "id", tokenID, "decimals", decimals)
		return tokenID, nil
	} else if err != nil {
		return 0, fmt.Errorf("failed to get token: %w", err)
	}

	return tokenID, nil
}

// GetOrCreateProtocol gets an existing protocol or creates it with default values.
func (r *LendingRepository) GetOrCreateProtocol(ctx context.Context, tx *sql.Tx, address common.Address) (int64, error) {
	var protocolID int64
	addressHex := strings.ToLower(address.Hex())

	err := tx.QueryRowContext(ctx,
		`SELECT id FROM protocol WHERE chain_id = $1 AND address = $2`,
		r.chainID, addressHex).Scan(&protocolID)

	if err == sql.ErrNoRows {
		// Auto-create protocol with default values
		r.logger.Info("auto-creating protocol", "address", address.Hex())
		err = tx.QueryRowContext(ctx,
			`INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block)
			 VALUES ($1, $2, $3, $4, $5)
			 RETURNING id`,
			r.chainID, addressHex, "UNKNOWN-"+address.Hex()[:10], "lending", 0).Scan(&protocolID)
		if err != nil {
			return 0, fmt.Errorf("failed to create protocol: %w", err)
		}
		r.logger.Info("protocol auto-created", "address", address.Hex(), "id", protocolID)
		return protocolID, nil
	} else if err != nil {
		return 0, fmt.Errorf("failed to get protocol: %w", err)
	}

	return protocolID, nil
}

// SaveBorrower inserts or updates a borrower position.
// amount should be the decimal-adjusted amount as a string (e.g., "1000.50" for USDC)
func (r *LendingRepository) SaveBorrower(ctx context.Context, tx *sql.Tx, userID, protocolID, tokenID, blockNumber int64, blockVersion int, amount string) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO borrowers (user_id, protocol_id, token_id, block_number, block_version, amount, change, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $6, NOW())
		 ON CONFLICT (user_id, protocol_id, token_id, block_number, block_version)
		 DO UPDATE SET amount = EXCLUDED.amount, change = EXCLUDED.change, created_at = NOW()`,
		userID, protocolID, tokenID, blockNumber, blockVersion, amount)

	if err != nil {
		return fmt.Errorf("failed to save borrower: %w", err)
	}
	return nil
}

// SaveBorrowerCollateral inserts or updates a collateral position.
// amount should be the decimal-adjusted amount as a string (e.g., "5000.123456" for USDC)
func (r *LendingRepository) SaveBorrowerCollateral(ctx context.Context, tx *sql.Tx, userID, protocolID, tokenID, blockNumber int64, blockVersion int, amount string) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO borrower_collateral (user_id, protocol_id, token_id, block_number, block_version, amount, change, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $6, NOW())
		 ON CONFLICT (user_id, protocol_id, token_id, block_number, block_version)
		 DO UPDATE SET amount = EXCLUDED.amount, change = EXCLUDED.change, created_at = NOW()`,
		userID, protocolID, tokenID, blockNumber, blockVersion, amount)

	if err != nil {
		return fmt.Errorf("failed to save collateral: %w", err)
	}
	return nil
}

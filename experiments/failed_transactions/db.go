package main

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// Database wraps the SQLite database connection
type Database struct {
	db *sql.DB
}

// NewDatabase creates a new database connection and initializes tables
func NewDatabase(dbPath string) (*Database, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Enable WAL mode for better concurrent access
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to set WAL mode: %w", err)
	}

	d := &Database{db: db}
	if err := d.initTables(); err != nil {
		db.Close()
		return nil, err
	}

	return d, nil
}

// initTables creates the required tables if they don't exist
func (d *Database) initTables() error {
	// Create failed_transactions table
	createFailedTxTable := `
	CREATE TABLE IF NOT EXISTS failed_transactions (
		block_number INTEGER,
		block_timestamp INTEGER,
		transaction_hash TEXT UNIQUE,
		from_address TEXT,
		to_address TEXT,
		function_name TEXT,
		function_args TEXT,
		gas_used TEXT,
		revert_reason TEXT
	);
	CREATE INDEX IF NOT EXISTS idx_failed_tx_block ON failed_transactions(block_number);
	CREATE INDEX IF NOT EXISTS idx_failed_tx_function ON failed_transactions(function_name);
	`

	if _, err := d.db.Exec(createFailedTxTable); err != nil {
		return fmt.Errorf("failed to create failed_transactions table: %w", err)
	}

	// Create sync state table for checkpointing
	createSyncStateTable := `
	CREATE TABLE IF NOT EXISTS failed_tx_sync_state (
		id INTEGER PRIMARY KEY CHECK (id = 1),
		last_processed_block INTEGER NOT NULL,
		updated_at INTEGER NOT NULL
	);
	`

	if _, err := d.db.Exec(createSyncStateTable); err != nil {
		return fmt.Errorf("failed to create sync_state table: %w", err)
	}

	return nil
}

// InsertFailedTransaction inserts a failed transaction record
func (d *Database) InsertFailedTransaction(tx *FailedTransaction) error {
	query := `
	INSERT OR IGNORE INTO failed_transactions (
		block_number, block_timestamp, transaction_hash,
		from_address, to_address, function_name,
		function_args, gas_used, revert_reason
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	_, err := d.db.Exec(query,
		tx.BlockNumber,
		tx.BlockTimestamp,
		tx.TransactionHash,
		tx.FromAddress,
		tx.ToAddress,
		tx.FunctionName,
		tx.FunctionArgs,
		tx.GasUsed,
		tx.RevertReason,
	)

	if err != nil {
		return fmt.Errorf("failed to insert transaction: %w", err)
	}

	return nil
}

// InsertFailedTransactionsBatch inserts multiple failed transactions in a batch
func (d *Database) InsertFailedTransactionsBatch(txs []*FailedTransaction) error {
	if len(txs) == 0 {
		return nil
	}

	tx, err := d.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT OR IGNORE INTO failed_transactions (
			block_number, block_timestamp, transaction_hash,
			from_address, to_address, function_name,
			function_args, gas_used, revert_reason
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, ftx := range txs {
		_, err := stmt.Exec(
			ftx.BlockNumber,
			ftx.BlockTimestamp,
			ftx.TransactionHash,
			ftx.FromAddress,
			ftx.ToAddress,
			ftx.FunctionName,
			ftx.FunctionArgs,
			ftx.GasUsed,
			ftx.RevertReason,
		)
		if err != nil {
			return fmt.Errorf("failed to insert transaction %s: %w", ftx.TransactionHash, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// GetSyncState retrieves the last sync state
func (d *Database) GetSyncState() (*SyncState, error) {
	var state SyncState
	err := d.db.QueryRow(`
		SELECT last_processed_block, updated_at 
		FROM failed_tx_sync_state 
		WHERE id = 1
	`).Scan(&state.LastProcessedBlock, &state.UpdatedAt)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get sync state: %w", err)
	}

	return &state, nil
}

// SaveSyncState saves the current sync state (upsert)
func (d *Database) SaveSyncState(lastBlock uint64) error {
	query := `
	INSERT INTO failed_tx_sync_state (id, last_processed_block, updated_at)
	VALUES (1, ?, ?)
	ON CONFLICT(id) DO UPDATE SET
		last_processed_block = excluded.last_processed_block,
		updated_at = excluded.updated_at
	`

	_, err := d.db.Exec(query, lastBlock, time.Now().Unix())
	if err != nil {
		return fmt.Errorf("failed to save sync state: %w", err)
	}

	return nil
}

// GetFailedTransactionCount returns the total count of failed transactions
func (d *Database) GetFailedTransactionCount() (int64, error) {
	var count int64
	err := d.db.QueryRow("SELECT COUNT(*) FROM failed_transactions").Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get count: %w", err)
	}
	return count, nil
}

// Close closes the database connection
func (d *Database) Close() error {
	return d.db.Close()
}

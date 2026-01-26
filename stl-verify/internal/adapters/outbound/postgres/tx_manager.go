// Package postgres provides PostgreSQL implementations of repository interfaces.
package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that TxManager implements outbound.TxManager
var _ outbound.TxManager = (*TxManager)(nil)

// TxManager provides transaction lifecycle management across multiple repositories.
// Services use this to coordinate writes that span multiple aggregates/repositories.
//
// Usage:
//
//	txm := postgres.NewTxManager(db, logger)
//	err := txm.WithTransaction(ctx, func(tx *sql.Tx) error {
//	    userID, err := userRepo.GetOrCreateUserWithTX(ctx, tx, user)
//	    if err != nil {
//	        return err // triggers rollback
//	    }
//	    return tokenRepo.UpsertTokenWithTX(ctx, tx, token)
//	})
type TxManager struct {
	db     *sql.DB
	logger *slog.Logger
}

// NewTxManager creates a new transaction manager.
// Returns an error if the database connection is nil.
func NewTxManager(db *sql.DB, logger *slog.Logger) (*TxManager, error) {
	if db == nil {
		return nil, fmt.Errorf("database connection cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &TxManager{
		db:     db,
		logger: logger,
	}, nil
}

// TxOptions configures transaction behavior.
type TxOptions struct {
	// Isolation sets the transaction isolation level.
	// Default: uses database default (typically ReadCommitted).
	Isolation sql.IsolationLevel
	// ReadOnly marks the transaction as read-only.
	// Some databases can optimize read-only transactions.
	ReadOnly bool
}

// WithTransaction executes fn within a database transaction.
// If fn returns an error, the transaction is rolled back.
// If fn succeeds, the transaction is committed.
//
// The transaction is automatically rolled back if:
//   - fn returns an error
//   - fn panics (panic is re-raised after rollback)
//   - commit fails
func (m *TxManager) WithTransaction(ctx context.Context, fn func(tx *sql.Tx) error) error {
	return m.WithTransactionOptions(ctx, nil, fn)
}

// WithTransactionOptions executes fn within a database transaction with custom options.
func (m *TxManager) WithTransactionOptions(ctx context.Context, opts *TxOptions, fn func(tx *sql.Tx) error) error {
	var txOpts *sql.TxOptions
	if opts != nil {
		txOpts = &sql.TxOptions{
			Isolation: opts.Isolation,
			ReadOnly:  opts.ReadOnly,
		}
	}

	tx, err := m.db.BeginTx(ctx, txOpts)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Handle panic by rolling back and re-raising
	defer func() {
		if p := recover(); p != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				m.logger.Error("failed to rollback transaction after panic", "error", rbErr)
			}
			panic(p)
		}
	}()

	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			m.logger.Error("failed to rollback transaction", "error", rbErr, "originalError", err)
		}
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

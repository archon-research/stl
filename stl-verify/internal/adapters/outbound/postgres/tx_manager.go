// Package postgres provides PostgreSQL implementations of repository interfaces.
package postgres

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that TxManager implements outbound.TxManager
var _ outbound.TxManager = (*TxManager)(nil)

// TxManager provides transaction lifecycle management across multiple repositories.
// Services use this to coordinate writes that span multiple aggregates/repositories.
//
// Usage:
//
//	txm := postgres.NewTxManager(pool, logger)
//	err := txm.WithTransaction(ctx, func(tx pgx.Tx) error {
//	    userID, err := userRepo.GetOrCreateUser(ctx, tx, user)
//	    if err != nil {
//	        return err // triggers rollback
//	    }
//	    return tokenRepo.UpsertToken(ctx, tx, token)
//	})
type TxManager struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// NewTxManager creates a new transaction manager.
// Returns an error if the pool is nil.
func NewTxManager(pool *pgxpool.Pool, logger *slog.Logger) (*TxManager, error) {
	if pool == nil {
		return nil, fmt.Errorf("database pool cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &TxManager{
		pool:   pool,
		logger: logger,
	}, nil
}

// TxOptions configures transaction behavior.
type TxOptions struct {
	// IsoLevel sets the transaction isolation level.
	// Default: uses database default (typically ReadCommitted).
	IsoLevel pgx.TxIsoLevel
	// AccessMode sets the transaction access mode (ReadWrite or ReadOnly).
	AccessMode pgx.TxAccessMode
	// DeferrableMode sets the transaction deferrable mode.
	DeferrableMode pgx.TxDeferrableMode
}

// WithTransaction executes fn within a database transaction.
// If fn returns an error, the transaction is rolled back.
// If fn succeeds, the transaction is committed.
//
// The transaction is automatically rolled back if:
//   - fn returns an error
//   - fn panics (panic is re-raised after rollback)
//   - commit fails
func (m *TxManager) WithTransaction(ctx context.Context, fn func(tx pgx.Tx) error) error {
	return m.WithTransactionOptions(ctx, nil, fn)
}

// WithTransactionOptions executes fn within a database transaction with custom options.
func (m *TxManager) WithTransactionOptions(ctx context.Context, opts *TxOptions, fn func(tx pgx.Tx) error) error {
	var txOpts pgx.TxOptions
	if opts != nil {
		txOpts = pgx.TxOptions{
			IsoLevel:       opts.IsoLevel,
			AccessMode:     opts.AccessMode,
			DeferrableMode: opts.DeferrableMode,
		}
	}

	tx, err := m.pool.BeginTx(ctx, txOpts)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Handle panic by rolling back and re-raising
	defer func() {
		if p := recover(); p != nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				m.logger.Error("failed to rollback transaction after panic", "error", rbErr)
			}
			panic(p)
		}
	}()

	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(ctx); rbErr != nil {
			m.logger.Error("failed to rollback transaction", "error", rbErr, "originalError", err)
		}
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

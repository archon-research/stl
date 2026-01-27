package outbound

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// TxManager defines the interface for database transaction management.
// Services inject this to coordinate writes across multiple repositories
// within a single atomic transaction.
//
// This separates transaction lifecycle concerns from individual repositories,
// allowing repositories to focus on their specific domain operations while
// services control transactional boundaries.
type TxManager interface {
	// WithTransaction executes fn within a database transaction.
	// If fn returns an error, the transaction is rolled back.
	// If fn succeeds, the transaction is committed.
	// This is the preferred method for most use cases.
	WithTransaction(ctx context.Context, fn func(tx pgx.Tx) error) error
}

package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// querier is what *pgxpool.Pool and pgx.Tx have in common, so a repository built on
// it can be re-scoped to one transaction (WithTx) without its port changing. Begin on
// a pgx.Tx opens a savepoint, so a repository's own transactions nest correctly.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Begin(ctx context.Context) (pgx.Tx, error)
}

var (
	_ querier = (*pgxpool.Pool)(nil)
	_ querier = (pgx.Tx)(nil)
)

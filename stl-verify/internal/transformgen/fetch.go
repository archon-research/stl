package transformgen

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// FetchRawSchemas reads the public column lists (ordinal order) and primary keys for
// the given tables from information_schema. It is the single raw-schema reader shared
// by the gen-transformed command and the regen-diff conformance test, so both exercise
// the same code path and cannot drift.
func FetchRawSchemas(ctx context.Context, pool *pgxpool.Pool, tables []string) (map[string]RawSchema, error) {
	columns, err := fetchColumns(ctx, pool, tables)
	if err != nil {
		return nil, fmt.Errorf("fetching columns: %w", err)
	}
	pks, err := fetchPrimaryKeys(ctx, pool, tables)
	if err != nil {
		return nil, fmt.Errorf("fetching primary keys: %w", err)
	}

	out := make(map[string]RawSchema, len(tables))
	for _, t := range tables {
		cols := columns[t]
		if len(cols) == 0 {
			return nil, fmt.Errorf("no columns found for table %q", t)
		}
		pk := pks[t]
		if len(pk) == 0 {
			return nil, fmt.Errorf("no primary key found for table %q", t)
		}
		out[t] = RawSchema{Table: t, Columns: cols, PrimaryKey: pk}
	}
	return out, nil
}

// fetchColumns reads the columns of the given public tables, keyed by table, in
// ascending ordinal_position order.
func fetchColumns(ctx context.Context, pool *pgxpool.Pool, tables []string) (map[string][]RawColumn, error) {
	rows, err := pool.Query(ctx, `
		SELECT table_name, column_name, ordinal_position
		FROM information_schema.columns
		WHERE table_schema = 'public'
		  AND table_name = ANY($1)
		ORDER BY table_name, ordinal_position`, tables)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string][]RawColumn)
	for rows.Next() {
		var table, name string
		var ordinal int
		if err := rows.Scan(&table, &name, &ordinal); err != nil {
			return nil, err
		}
		out[table] = append(out[table], RawColumn{Name: name, Ordinal: ordinal})
	}
	return out, rows.Err()
}

// fetchPrimaryKeys reads the primary-key columns of the given public tables, keyed by
// table, in the order the constraint declares them (ordinal_position within the key).
func fetchPrimaryKeys(ctx context.Context, pool *pgxpool.Pool, tables []string) (map[string][]string, error) {
	rows, err := pool.Query(ctx, `
		SELECT tc.table_name, kcu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
		  ON tc.constraint_name = kcu.constraint_name
		  AND tc.table_schema = kcu.table_schema
		  AND tc.table_name = kcu.table_name
		WHERE tc.table_schema = 'public'
		  AND tc.constraint_type = 'PRIMARY KEY'
		  AND tc.table_name = ANY($1)
		ORDER BY tc.table_name, kcu.ordinal_position`, tables)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string][]string)
	for rows.Next() {
		var table, column string
		if err := rows.Scan(&table, &column); err != nil {
			return nil, err
		}
		out[table] = append(out[table], column)
	}
	return out, rows.Err()
}

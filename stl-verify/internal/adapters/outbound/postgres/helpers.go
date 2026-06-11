// Package postgres provides PostgreSQL implementations of repository interfaces.
package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// rollback rolls back the transaction and logs the error if it fails.
func rollback(ctx context.Context, tx pgx.Tx, logger *slog.Logger) {
	if err := tx.Rollback(ctx); err != nil && err != pgx.ErrTxClosed {
		logger.Error("failed to rollback transaction", "error", err)
	}
}

// bigIntToNumeric converts a *big.Int to a string for NUMERIC column storage.
// This function expects non-nil input. All callers must validate nil values before calling.
// Postgres's NUMERIC type can handle arbitrary precision numbers as strings.
// This helper ensures that we store big.Int values correctly.
func bigIntToNumeric(b *big.Int) (string, error) {
	if b == nil {
		return "", fmt.Errorf("input big.Int is nil")
	}

	return b.String(), nil
}

// toNumeric converts a raw *big.Int token amount to a pgtype.Numeric with the
// given decimal shift applied. For example, 1500000 with 6 decimals becomes
// the numeric value 1.5. This avoids a *big.Int → string → numeric round-trip.
// raw must not be nil; callers must validate before calling.
func toNumeric(raw *big.Int, decimals int) pgtype.Numeric {
	return pgtype.Numeric{Int: new(big.Int).Set(raw), Exp: int32(-decimals), Valid: true}
}

// toNullableNumeric is like toNumeric but returns a NULL numeric when raw is nil.
func toNullableNumeric(raw *big.Int, decimals int) pgtype.Numeric {
	if raw == nil {
		return pgtype.Numeric{}
	}
	return pgtype.Numeric{Int: new(big.Int).Set(raw), Exp: int32(-decimals), Valid: true}
}

// collectBatchIDs sends a batch of single-row `RETURNING id` upserts and
// collects the ids keyed per row. The batch must be queued in the same order
// as rows. kind names the entity in error messages.
func collectBatchIDs[T any, K comparable](ctx context.Context, tx pgx.Tx, batch *pgx.Batch, rows []T, kind string, key func(T) K) (map[K]int64, error) {
	return collectBatchRows(ctx, tx, batch, rows, kind, func(row pgx.Row, item T) (K, int64, error) {
		var id int64
		if err := row.Scan(&id); err != nil {
			var zero K
			return zero, 0, fmt.Errorf("upserting %s %v: %w", kind, key(item), err)
		}
		return key(item), id, nil
	})
}

// collectBatchRows sends a batch of single-row upserts and collects
// (key, id) per row via scan, which may also reject a row with an error.
// The batch must be queued in the same order as rows; kind names the entity
// in error messages.
func collectBatchRows[T any, K comparable](ctx context.Context, tx pgx.Tx, batch *pgx.Batch, rows []T, kind string, scan func(row pgx.Row, item T) (K, int64, error)) (result map[K]int64, err error) {
	br := tx.SendBatch(ctx, batch)
	// A scan error takes precedence over the close error; the close error is
	// only surfaced when everything else succeeded.
	defer func() {
		if closeErr := br.Close(); closeErr != nil && err == nil {
			result = nil
			err = fmt.Errorf("closing %s batch: %w", kind, closeErr)
		}
	}()

	result = make(map[K]int64, len(rows))
	for _, item := range rows {
		key, id, scanErr := scan(br.QueryRow(), item)
		if scanErr != nil {
			return nil, scanErr
		}
		result[key] = id
	}
	return result, nil
}

// optionalNumeric converts an optional *big.Int to a nullable NUMERIC arg.
func optionalNumeric(b *big.Int) *string {
	if b == nil {
		return nil
	}
	s := b.String()
	return &s
}

// nullIfEmpty maps empty strings to SQL NULL.
func nullIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// sortedCopy returns a sorted copy of items, leaving the caller's slice
// untouched. Sorting before insert gives concurrent writers a stable
// row/advisory-lock acquisition order (ADR-0002).
func sortedCopy[T any](items []T, cmpFn func(a, b T) int) []T {
	sorted := make([]T, len(items))
	copy(sorted, items)
	slices.SortFunc(sorted, cmpFn)
	return sorted
}

// sortedByBytesKey returns a copy of items sorted by a bytes key.
func sortedByBytesKey[T any](items []T, key func(T) []byte) []T {
	return sortedCopy(items, func(a, b T) int { return bytes.Compare(key(a), key(b)) })
}

// writeValuesPlaceholders appends "($n, $n+1, ...)" for row i with the given
// column count, comma-separated from the previous row.
func writeValuesPlaceholders(sb *strings.Builder, row, cols int) {
	if row > 0 {
		sb.WriteString(", ")
	}
	sb.WriteString("(")
	for c := range cols {
		if c > 0 {
			sb.WriteString(", ")
		}
		fmt.Fprintf(sb, "$%d", row*cols+c+1)
	}
	sb.WriteString(")")
}

// marshalMetadata safely marshals metadata to JSON, returning "{}" for nil/empty maps.
func marshalMetadata(m map[string]any) ([]byte, error) {
	if len(m) == 0 {
		return []byte("{}"), nil
	}
	data, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return data, nil
}

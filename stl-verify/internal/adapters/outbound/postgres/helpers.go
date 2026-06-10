// Package postgres provides PostgreSQL implementations of repository interfaces.
package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"

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
	br := tx.SendBatch(ctx, batch)
	result := make(map[K]int64, len(rows))
	for _, row := range rows {
		var id int64
		if err := br.QueryRow().Scan(&id); err != nil {
			_ = br.Close()
			return nil, fmt.Errorf("upserting %s %v: %w", kind, key(row), err)
		}
		result[key(row)] = id
	}
	if err := br.Close(); err != nil {
		return nil, fmt.Errorf("closing %s batch: %w", kind, err)
	}
	return result, nil
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

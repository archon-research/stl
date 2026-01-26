// Package postgres provides PostgreSQL implementations of repository interfaces.
package postgres

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
)

// rollback rolls back the transaction and logs the error if it is not sql.ErrTxDone.
func rollback(tx *sql.Tx, logger *slog.Logger) {
	if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
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

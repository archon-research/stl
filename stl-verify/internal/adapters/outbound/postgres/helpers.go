// Package postgres provides PostgreSQL implementations of repository interfaces.
package postgres

import (
	"encoding/json"
	"log/slog"
	"math/big"
)

// bigIntToNumeric converts a *big.Int to a string for NUMERIC column storage.
// Returns nil if the input is nil.
func bigIntToNumeric(b *big.Int) any {
	if b == nil {
		return nil
	}
	return b.String()
}

// marshalMetadata safely marshals metadata to JSON, returning "{}" for nil/empty maps.
// This is safe because map[string]any can always be marshaled (unless it contains
// channels or functions, which entity metadata should never have).
func marshalMetadata(m map[string]any) []byte {
	if m == nil || len(m) == 0 {
		return []byte("{}")
	}
	data, err := json.Marshal(m)
	if err != nil {
		// This should never happen with map[string]any containing only JSON-safe types.
		// Log and return empty object rather than failing the batch.
		slog.Default().Warn("failed to marshal metadata, using empty object", "error", err)
		return []byte("{}")
	}
	return data
}

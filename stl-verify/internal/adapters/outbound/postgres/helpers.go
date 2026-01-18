// Package postgres provides PostgreSQL implementations of repository interfaces.
package postgres

import (
	"encoding/json"
	"math/big"
)

// bigIntToNumeric converts a *big.Int to a string for NUMERIC column storage.
// Returns nil if the input is nil.
// Postgres's NUMERIC type can handle arbitrary precision numbers as strings.
// This helper ensures that we store big.Int values correctly.
func bigIntToNumeric(b *big.Int) any {
	if b == nil {
		return nil
	}
	return b.String()
}

// marshalMetadata safely marshals metadata to JSON, returning "{}" for nil/empty maps.
// This is safe because map[string]any can always be marshaled (unless it contains
// channels or functions, which entity metadata should never have).
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

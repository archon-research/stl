package jsonutil

import (
	"bytes"
	"encoding/json"
)

// IsNullOrEmpty reports whether a JSON payload is empty or the literal four-byte
// token "null". JSON-RPC responses with {"result": null} decode to a non-nil
// 4-byte json.RawMessage, which fools naive == nil checks.
func IsNullOrEmpty(r json.RawMessage) bool {
	return len(r) == 0 || bytes.Equal(bytes.TrimSpace(r), []byte("null"))
}

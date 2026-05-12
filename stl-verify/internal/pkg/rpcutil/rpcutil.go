// Package rpcutil provides shared JSON-RPC 2.0 types and response helpers.
package rpcutil

import (
	"bytes"
	"encoding/json"
	"net/http"
)

// IsNullOrEmpty reports whether a JSON-RPC result payload is missing or the
// literal `null` token. Trims surrounding whitespace so pretty-printed
// upstream responses are handled correctly. Necessary because
// `encoding/json` materialises a wire `null` into a `json.RawMessage` as the
// 4-byte slice "null" — a plain `r == nil` check cannot distinguish that from
// real data.
func IsNullOrEmpty(r json.RawMessage) bool {
	if len(r) == 0 {
		return true
	}
	return bytes.Equal(bytes.TrimSpace(r), []byte("null"))
}

// Request represents a JSON-RPC 2.0 request.
type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	ID      json.RawMessage `json:"id"`
}

// WriteResult writes a JSON-RPC 2.0 success response.
func WriteResult(w http.ResponseWriter, id, result json.RawMessage) {
	_ = json.NewEncoder(w).Encode(map[string]json.RawMessage{
		"jsonrpc": json.RawMessage(`"2.0"`),
		"id":      id,
		"result":  result,
	})
}

// WriteError writes a JSON-RPC 2.0 error response.
func WriteError(w http.ResponseWriter, id json.RawMessage, code int, message string) {
	errJSON, _ := json.Marshal(map[string]any{"code": code, "message": message})
	_ = json.NewEncoder(w).Encode(map[string]json.RawMessage{
		"jsonrpc": json.RawMessage(`"2.0"`),
		"id":      id,
		"error":   json.RawMessage(errJSON),
	})
}

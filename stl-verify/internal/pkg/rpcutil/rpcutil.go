// Package rpcutil provides shared JSON-RPC 2.0 types and response helpers.
package rpcutil

import (
	"encoding/json"
	"net/http"
	"strings"
)

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

// IsUnfinalizedError returns true if the error is due to querying unfinalized block data.
// This is expected near chain tip, especially on chains with fast block times.
func IsUnfinalizedError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "unfinalized data")
}

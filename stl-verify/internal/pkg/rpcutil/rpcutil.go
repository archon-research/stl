// Package rpcutil provides shared JSON-RPC 2.0 types and response helpers.
package rpcutil

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
)

// ErrUpstreamNullResult signals that a JSON-RPC endpoint returned a literal
// `null` result for a request that, in normal operation, is expected to carry
// a body (block, receipts, traces, blobs). On Ethereum-style upstreams this
// typically appears during a propagation race: the new-head WebSocket
// announces a hash before the corresponding body has propagated through
// every HTTP RPC backend, so `eth_getBlockByHash` for that hash transiently
// resolves to `null` even though the chain has actually produced the block.
//
// The sentinel lives in `rpcutil` (rather than the alchemy adapter) so that
// services in the application layer can use `errors.Is` against it without
// taking a forbidden import on an adapter. Downstream retry policy
// (BackfillService.runRetryLoop) treats this as a recoverable error so the
// next fetch — once propagation completes — gets the real payload. Adapters
// do not retry inline; they surface the error so the caller can decide
// between retry, drop, or escalation.
var ErrUpstreamNullResult = errors.New("upstream returned null result")

// IsNullOrEmpty reports whether a JSON-RPC result payload is missing or the
// literal `null` token. Necessary because `encoding/json` materialises a wire
// `null` into a `json.RawMessage` as the 4-byte slice "null" — a plain
// `r == nil` check cannot distinguish that from real data. The TrimSpace
// guard covers the edge case of a hand-constructed RawMessage whose bytes
// include surrounding whitespace; in practice the standard JSON decoder
// strips whitespace before producing the RawMessage value, so this is belt
// and braces.
func IsNullOrEmpty(r json.RawMessage) bool {
	trimmed := bytes.TrimSpace(r)
	return len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null"))
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

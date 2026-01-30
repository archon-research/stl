// types.go defines JSON-RPC request/response types for Alchemy API communication.
//
// This file contains:
//   - jsonRPCRequest/jsonRPCResponse: Standard JSON-RPC 2.0 message structures
//   - subscriptionParams: WebSocket subscription notification parsing
//   - Utility functions for truncating hashes
package alchemy

import (
	"encoding/json"
	"fmt"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// jsonRPCRequest represents a JSON-RPC 2.0 request.
type jsonRPCRequest struct {
	JSONRPC string        `json:"jsonrpc"`
	ID      int           `json:"id"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
}

// jsonRPCResponse represents a JSON-RPC 2.0 response.
type jsonRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int             `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *jsonRPCError   `json:"error,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// jsonRPCError represents a JSON-RPC 2.0 error.
type jsonRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// Error implements the error interface.
func (e *jsonRPCError) Error() string {
	return fmt.Sprintf("RPC error %d: %s", e.Code, e.Message)
}

// subscriptionParams represents the params field for subscription notifications.
type subscriptionParams struct {
	Subscription string               `json:"subscription"`
	Result       outbound.BlockHeader `json:"result"`
}

// truncateHash shortens a hash for logging purposes.
func truncateHash(hash string) string {
	if len(hash) <= 14 {
		return hash
	}
	return hash[:8] + "..." + hash[len(hash)-6:]
}

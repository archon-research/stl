package alchemy

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/gorilla/websocket"
)

// LightBlock represents a minimal block for the in-memory chain.
// Used for efficient parent hash chain validation.
type LightBlock struct {
	Number     int64
	Hash       string
	ParentHash string
}

// Subscriber implements the BlockSubscriber port using Alchemy's WebSocket API.
// It automatically reconnects when the connection is lost.
type Subscriber struct {
	config        Config
	conn          *websocket.Conn
	mu            sync.RWMutex
	done          chan struct{}
	closed        bool
	headers       chan outbound.BlockHeader
	ctx           context.Context
	cancel        context.CancelFunc
	lastBlockTime atomic.Int64 // Unix timestamp of last received block

	// In-memory chain state for efficient reorg detection (Ponder-style)
	chainMu           sync.RWMutex
	unfinalizedBlocks []LightBlock // Linked chain of blocks waiting to be finalized
	finalizedBlock    *LightBlock  // Last finalized block (reorgs beyond this are unrecoverable)
}

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

// subscriptionParams represents the params field for subscription notifications.
type subscriptionParams struct {
	Subscription string               `json:"subscription"`
	Result       outbound.BlockHeader `json:"result"`
}

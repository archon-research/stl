// Implements the WebSocket JSON-RPC handler for eth_subscribe (newHeads) and block broadcast.
package mockchain

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"

	"github.com/archon-research/stl/stl-verify/internal/pkg/rpcutil"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/gorilla/websocket"
)

type jsonRPCResponse struct {
	JsonRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  string          `json:"result"`
}

type jsonRPCErrorObj struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type jsonRPCErrorResponse struct {
	JsonRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Error   jsonRPCErrorObj `json:"error"`
}

type jsonRPCNotificationParams struct {
	Subscription string               `json:"subscription"`
	Result       outbound.BlockHeader `json:"result"`
}

type jsonRPCNotification struct {
	JsonRPC string                    `json:"jsonrpc"`
	Method  string                    `json:"method"`
	Params  jsonRPCNotificationParams `json:"params"`
}

type wsHandler struct {
	mu       sync.Mutex
	conn     *websocket.Conn
	upgrader websocket.Upgrader
	subID    string
}

func newWSHandler() *wsHandler {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
	return &wsHandler{
		upgrader: upgrader,
		subID:    "0x1",
	}
}

func (h *wsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("mockchain: websocket upgrade failed", "error", err)
		return
	}
	defer conn.Close()

	h.mu.Lock()
	oldConn := h.conn
	h.conn = conn
	h.mu.Unlock()

	if oldConn != nil {
		_ = oldConn.Close()
	}

	for {
		var req rpcutil.Request
		if err := conn.ReadJSON(&req); err != nil {
			slog.Debug("mockchain: websocket read error", "error", err)
			h.clearConn(conn)
			return
		}
		if err := h.handleRequest(conn, req); err != nil {
			slog.Debug("mockchain: websocket handle error", "error", err)
			h.clearConn(conn)
			return
		}
	}
}

func (h *wsHandler) clearConn(conn *websocket.Conn) {
	h.mu.Lock()
	if h.conn == conn {
		h.conn = nil
	}
	h.mu.Unlock()
}

func (h *wsHandler) handleRequest(conn *websocket.Conn, req rpcutil.Request) error {
	switch req.Method {
	case "eth_subscribe":
		return h.handleSubscribe(conn, req)
	default:
		return h.writeError(conn, req.ID, rpcErrMethodNotFound, "method not found")
	}
}

func (h *wsHandler) handleSubscribe(conn *websocket.Conn, req rpcutil.Request) error {
	var params []json.RawMessage
	if err := json.Unmarshal(req.Params, &params); err != nil || len(params) == 0 {
		return h.writeError(conn, req.ID, rpcErrInvalidParams, "unsupported subscription type")
	}
	var sub string
	if err := json.Unmarshal(params[0], &sub); err != nil || sub != "newHeads" {
		return h.writeError(conn, req.ID, rpcErrInvalidParams, "unsupported subscription type")
	}
	resp := jsonRPCResponse{JsonRPC: "2.0", ID: req.ID, Result: h.subID}
	h.mu.Lock()
	err := conn.WriteJSON(resp)
	h.mu.Unlock()
	if err != nil {
		return fmt.Errorf("writing subscribe response: %w", err)
	}
	return nil
}

func (h *wsHandler) writeError(conn *websocket.Conn, id json.RawMessage, code int, msg string) error {
	errResp := jsonRPCErrorResponse{
		JsonRPC: "2.0",
		ID:      id,
		Error:   jsonRPCErrorObj{Code: code, Message: msg},
	}
	h.mu.Lock()
	err := conn.WriteJSON(errResp)
	h.mu.Unlock()
	if err != nil {
		return fmt.Errorf("writing error response: %w", err)
	}
	return nil
}

// ConnectedCount returns 1 if a WebSocket client is currently connected, 0 otherwise.
func (h *wsHandler) ConnectedCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.conn != nil {
		return 1
	}
	return 0
}

// Disconnect closes the active WebSocket connection without stopping the handler.
// The read loop in ServeHTTP will receive an error and clear the connection.
func (h *wsHandler) Disconnect() {
	h.mu.Lock()
	conn := h.conn
	h.conn = nil
	h.mu.Unlock()
	if conn != nil {
		if err := conn.Close(); err != nil {
			slog.Debug("mockchain: close disconnected connection", "error", err)
		}
	}
}

func (h *wsHandler) Broadcast(header outbound.BlockHeader) {
	notification := jsonRPCNotification{
		JsonRPC: "2.0",
		Method:  "eth_subscription",
		Params: jsonRPCNotificationParams{
			Subscription: h.subID,
			Result:       header,
		},
	}

	h.mu.Lock()
	if h.conn == nil {
		h.mu.Unlock()
		return
	}
	if err := h.conn.WriteJSON(notification); err != nil {
		slog.Warn("mockchain: broadcast write failed, closing connection", "error", err)
		conn := h.conn
		h.conn = nil
		h.mu.Unlock()
		if err := conn.Close(); err != nil {
			slog.Debug("mockchain: close failed-broadcast connection", "error", err)
		}
		return
	}
	h.mu.Unlock()
}

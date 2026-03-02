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
			h.clearConn(conn)
			return
		}
		if err := h.handleRequest(conn, req); err != nil {
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
		return h.writeError(conn, req.ID, -32601, "method not found")
	}
}

func (h *wsHandler) handleSubscribe(conn *websocket.Conn, req rpcutil.Request) error {
	var params []json.RawMessage
	if err := json.Unmarshal(req.Params, &params); err != nil || len(params) == 0 {
		return h.writeError(conn, req.ID, -32602, "unsupported subscription type")
	}
	var sub string
	if err := json.Unmarshal(params[0], &sub); err != nil || sub != "newHeads" {
		return h.writeError(conn, req.ID, -32602, "unsupported subscription type")
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
		conn := h.conn
		h.conn = nil
		h.mu.Unlock()
		_ = conn.Close()
		return
	}
	h.mu.Unlock()
}

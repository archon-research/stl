// Implements the WebSocket JSON-RPC handler for eth_subscribe (newHeads) and block broadcast.
package mockchain

import (
	"log/slog"
	"net/http"
	"sync"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/gorilla/websocket"
)

type jsonRPCRequest struct {
	ID     int           `json:"id"`
	Method string        `json:"method"`
	Params []interface{} `json:"params"`
}

type jsonRPCResponse struct {
	JsonRPC string `json:"jsonrpc"`
	ID      int    `json:"id"`
	Result  string `json:"result"`
}

type jsonRPCErrorObj struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type jsonRPCErrorResponse struct {
	JsonRPC string          `json:"jsonrpc"`
	ID      int             `json:"id"`
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
	writeMu  sync.Mutex
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

	h.mu.Lock()
	oldConn := h.conn
	h.conn = conn
	h.mu.Unlock()

	if oldConn != nil {
		h.writeMu.Lock()
		_ = oldConn.Close()
		h.writeMu.Unlock()
	}

	for {
		var req jsonRPCRequest
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

func (h *wsHandler) handleRequest(conn *websocket.Conn, req jsonRPCRequest) error {
	switch req.Method {
	case "eth_subscribe":
		return h.handleSubscribe(conn, req)
	default:
		return h.writeError(conn, req.ID, -32601, "method not found")
	}
}

func (h *wsHandler) handleSubscribe(conn *websocket.Conn, req jsonRPCRequest) error {
	if len(req.Params) == 0 || req.Params[0] != "newHeads" {
		return h.writeError(conn, req.ID, -32602, "unsupported subscription type")
	}
	resp := jsonRPCResponse{JsonRPC: "2.0", ID: req.ID, Result: h.subID}
	h.writeMu.Lock()
	err := conn.WriteJSON(resp)
	h.writeMu.Unlock()
	return err
}

func (h *wsHandler) writeError(conn *websocket.Conn, id, code int, msg string) error {
	errResp := jsonRPCErrorResponse{
		JsonRPC: "2.0",
		ID:      id,
		Error:   jsonRPCErrorObj{Code: code, Message: msg},
	}
	h.writeMu.Lock()
	err := conn.WriteJSON(errResp)
	h.writeMu.Unlock()
	return err
}

func (h *wsHandler) Broadcast(header outbound.BlockHeader) {
	h.mu.Lock()
	conn := h.conn
	h.mu.Unlock()

	if conn == nil {
		return
	}

	notification := jsonRPCNotification{
		JsonRPC: "2.0",
		Method:  "eth_subscription",
		Params: jsonRPCNotificationParams{
			Subscription: h.subID,
			Result:       header,
		},
	}

	h.writeMu.Lock()
	err := conn.WriteJSON(notification)
	h.writeMu.Unlock()

	if err != nil {
		h.mu.Lock()
		if h.conn == conn {
			h.conn = nil
		}
		h.mu.Unlock()
	}
}

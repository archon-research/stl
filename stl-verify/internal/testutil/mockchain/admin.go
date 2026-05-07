// admin.go implements the admin HTTP API for controlling the mock server at runtime.
//
// Endpoints (all JSON request/response):
//   - POST /start        — start the replayer
//   - POST /stop         — stop the replayer
//   - POST /speed        — change block emission interval (body: {"interval_ms": N})
//   - GET  /status       — server status snapshot
//   - POST /reorg        — trigger a chain reorg (query: ?depth=N)
//   - POST /disconnect   — disconnect the active WebSocket client
//   - POST /error        — set error injection mode (body: {"mode": "none"|"429"|"500"|"timeout"|"once429"|"once500"})
package mockchain

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

type adminHandler struct {
	replayer  *Replayer
	reorgCtrl *reorgController
	ws        *wsHandler
	rpc       *httpHandler
}

func newAdminHandler(replayer *Replayer, reorgCtrl *reorgController, ws *wsHandler, rpc *httpHandler) *adminHandler {
	return &adminHandler{
		replayer:  replayer,
		reorgCtrl: reorgCtrl,
		ws:        ws,
		rpc:       rpc,
	}
}

func (h *adminHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/start":
		h.handleStart(w, r)
	case "/stop":
		h.handleStop(w, r)
	case "/speed":
		h.handleSpeed(w, r)
	case "/status":
		h.handleStatus(w, r)
	case "/reorg":
		h.handleReorg(w, r)
	case "/disconnect":
		h.handleDisconnect(w, r)
	case "/error":
		h.handleError(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (h *adminHandler) handleStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := h.replayer.Start(); err != nil {
		status := http.StatusServiceUnavailable
		if errors.Is(err, errAlreadyRunning) {
			status = http.StatusConflict
		}
		http.Error(w, err.Error(), status)
		return
	}
	if err := writeJSON(w, map[string]bool{"ok": true}); err != nil {
		slog.Error("mockchain: admin writeJSON", "error", err)
	}
}

func (h *adminHandler) handleStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	last := h.replayer.Stop()
	if err := writeJSON(w, map[string]any{"last_block": last}); err != nil {
		slog.Error("mockchain: admin writeJSON", "error", err)
	}
}

func (h *adminHandler) handleSpeed(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		IntervalMs int64 `json:"interval_ms"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.IntervalMs <= 0 {
		http.Error(w, "body must be {\"interval_ms\": <positive integer>}", http.StatusBadRequest)
		return
	}
	if err := h.replayer.SetInterval(time.Duration(body.IntervalMs) * time.Millisecond); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	if err := writeJSON(w, map[string]bool{"ok": true}); err != nil {
		slog.Error("mockchain: admin writeJSON", "error", err)
	}
}

func (h *adminHandler) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	st := h.replayer.getStatus()
	if err := writeJSON(w, map[string]any{
		"running":           st.Running,
		"blocks_emitted":    st.BlocksEmitted,
		"template_index":    st.TemplateIndex,
		"connected_clients": h.ws.ConnectedCount(),
		"reorg_count":       h.reorgCtrl.reorgCount.Load(),
	}); err != nil {
		slog.Error("mockchain: admin writeJSON", "error", err)
	}
}

func (h *adminHandler) handleReorg(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	depthStr := r.URL.Query().Get("depth")
	if depthStr == "" {
		http.Error(w, "missing query parameter: depth", http.StatusBadRequest)
		return
	}
	depth, err := strconv.Atoi(depthStr)
	if err != nil || depth <= 0 {
		http.Error(w, "depth must be a positive integer", http.StatusBadRequest)
		return
	}
	if err := h.reorgCtrl.trigger(depth); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := writeJSON(w, map[string]bool{"ok": true}); err != nil {
		slog.Error("mockchain: admin writeJSON", "error", err)
	}
}

func (h *adminHandler) handleDisconnect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	h.ws.Disconnect()
	if err := writeJSON(w, map[string]bool{"ok": true}); err != nil {
		slog.Error("mockchain: admin writeJSON", "error", err)
	}
}

func (h *adminHandler) handleError(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		Mode string `json:"mode"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	mode, ok := parseErrorMode(body.Mode)
	if !ok {
		http.Error(w, "mode must be one of: none, 429, 500, timeout, once429, once500", http.StatusBadRequest)
		return
	}
	h.rpc.SetErrorMode(mode)
	if err := writeJSON(w, map[string]bool{"ok": true}); err != nil {
		slog.Error("mockchain: admin writeJSON", "error", err)
	}
}

// parseErrorMode maps a string name to an ErrorMode constant.
func parseErrorMode(s string) (ErrorMode, bool) {
	switch s {
	case "none":
		return ErrorModeNone, true
	case "429":
		return ErrorMode429, true
	case "500":
		return ErrorMode500, true
	case "timeout":
		return ErrorModeTimeout, true
	case "once429":
		return ErrorModeOnce429, true
	case "once500":
		return ErrorModeOnce500, true
	}
	return ErrorModeNone, false
}

// writeJSON encodes v as JSON and writes it to w with Content-Type application/json.
func writeJSON(w http.ResponseWriter, v any) error {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		return fmt.Errorf("encoding JSON response: %w", err)
	}
	return nil
}

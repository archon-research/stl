// httprpc.go implements the HTTP JSON-RPC handler for eth_getBlockByHash, eth_blockNumber,
// eth_getBlockReceipts, trace_block, and eth_getBlobSidecars. Supports both single and batch requests.
package mockchain

import (
	"encoding/json"
	"net/http"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

type httpHandler struct {
	store *DataStore
}

func newHTTPHandler(store *DataStore) *httpHandler {
	return &httpHandler{store: store}
}

func (h *httpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var raw json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	if len(raw) > 0 && raw[0] == '[' {
		h.handleBatch(w, raw)
		return
	}

	var req testutil.JSONRPCRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	result, rpcErr := h.dispatch(req)
	h.respond(w, req.ID, result, rpcErr)
}

func (h *httpHandler) handleBatch(w http.ResponseWriter, raw json.RawMessage) {
	var reqs []testutil.JSONRPCRequest
	if err := json.Unmarshal(raw, &reqs); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	type batchResponse struct {
		JSONRPC string           `json:"jsonrpc"`
		ID      any              `json:"id"`
		Result  json.RawMessage  `json:"result,omitempty"`
		Error   *jsonRPCErrorObj `json:"error,omitempty"`
	}

	responses := make([]batchResponse, len(reqs))
	for i, req := range reqs {
		result, rpcErr := h.dispatch(req)
		responses[i] = batchResponse{JSONRPC: "2.0", ID: req.ID}
		if rpcErr != nil {
			responses[i].Error = rpcErr
		} else {
			responses[i].Result = result
		}
	}

	out, err := json.Marshal(responses)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	_, _ = w.Write(out)
}

func (h *httpHandler) dispatch(req testutil.JSONRPCRequest) (json.RawMessage, *jsonRPCErrorObj) {
	switch req.Method {
	case "eth_getBlockByHash":
		return h.getBlockByHash(req)
	case "eth_blockNumber":
		return h.blockNumber()
	case "eth_getBlockReceipts":
		return h.getDataByHash(req, "receipts")
	case "trace_block":
		return h.getDataByHash(req, "traces")
	case "eth_getBlobSidecars":
		return h.getDataByHash(req, "blobs")
	default:
		return nil, &jsonRPCErrorObj{Code: -32601, Message: "method not found"}
	}
}

func (h *httpHandler) respond(w http.ResponseWriter, id json.RawMessage, result json.RawMessage, rpcErr *jsonRPCErrorObj) {
	if rpcErr != nil {
		testutil.WriteRPCError(w, id, rpcErr.Code, rpcErr.Message)
		return
	}
	testutil.WriteRPCResult(w, id, result)
}

func (h *httpHandler) findIndexByHash(hash string) (int, bool) {
	for i, hdr := range h.store.Headers() {
		if hdr.Hash == hash {
			return i, true
		}
	}
	return -1, false
}

func extractHashParam(req testutil.JSONRPCRequest) (string, bool) {
	var params []json.RawMessage
	if err := json.Unmarshal(req.Params, &params); err != nil || len(params) == 0 {
		return "", false
	}
	var s string
	if err := json.Unmarshal(params[0], &s); err != nil {
		return "", false
	}
	return s, true
}

func (h *httpHandler) getBlockByHash(req testutil.JSONRPCRequest) (json.RawMessage, *jsonRPCErrorObj) {
	hash, ok := extractHashParam(req)
	if !ok {
		return nil, &jsonRPCErrorObj{Code: -32602, Message: "missing or invalid hash parameter"}
	}

	idx, found := h.findIndexByHash(hash)
	if !found {
		return json.RawMessage("null"), nil
	}
	raw, ok := h.store.Get(idx, "block")
	if !ok {
		return json.RawMessage("null"), nil
	}
	return raw, nil
}

func (h *httpHandler) getDataByHash(req testutil.JSONRPCRequest, dataType string) (json.RawMessage, *jsonRPCErrorObj) {
	hash, ok := extractHashParam(req)
	if !ok {
		return nil, &jsonRPCErrorObj{Code: -32602, Message: "missing or invalid hash parameter"}
	}

	idx, found := h.findIndexByHash(hash)
	if !found {
		return json.RawMessage("null"), nil
	}
	raw, ok := h.store.Get(idx, dataType)
	if !ok {
		return json.RawMessage("null"), nil
	}
	return raw, nil
}

func (h *httpHandler) blockNumber() (json.RawMessage, *jsonRPCErrorObj) {
	headers := h.store.Headers()
	if len(headers) == 0 {
		return json.RawMessage(`"0x0"`), nil
	}
	num := headers[len(headers)-1].Number
	result, err := json.Marshal(num)
	if err != nil {
		return nil, &jsonRPCErrorObj{Code: -32603, Message: "internal error"}
	}
	return result, nil
}

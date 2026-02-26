// httprpc.go implements the HTTP JSON-RPC handler for eth_getBlockByHash and eth_blockNumber.
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
	var req jsonRPCRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	idJSON, err := json.Marshal(req.ID)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	switch req.Method {
	case "eth_getBlockByHash":
		result, rpcErr := h.getBlockByHash(req)
		h.respond(w, idJSON, result, rpcErr)
	case "eth_blockNumber":
		result, rpcErr := h.blockNumber()
		h.respond(w, idJSON, result, rpcErr)
	default:
		testutil.WriteRPCError(w, idJSON, -32601, "method not found")
	}
}

func (h *httpHandler) respond(w http.ResponseWriter, id json.RawMessage, result json.RawMessage, rpcErr *jsonRPCErrorObj) {
	if rpcErr != nil {
		testutil.WriteRPCError(w, id, rpcErr.Code, rpcErr.Message)
		return
	}
	testutil.WriteRPCResult(w, id, result)
}

func (h *httpHandler) getBlockByHash(req jsonRPCRequest) (json.RawMessage, *jsonRPCErrorObj) {
	if len(req.Params) == 0 {
		return nil, &jsonRPCErrorObj{Code: -32602, Message: "missing hash parameter"}
	}
	hash, ok := req.Params[0].(string)
	if !ok {
		return nil, &jsonRPCErrorObj{Code: -32602, Message: "invalid hash parameter"}
	}

	for i, header := range h.store.Headers() {
		if header.Hash == hash {
			raw, found := h.store.Get(i, "block")
			if !found {
				return json.RawMessage("null"), nil
			}
			return raw, nil
		}
	}
	return json.RawMessage("null"), nil
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

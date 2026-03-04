// httprpc.go implements the HTTP JSON-RPC handler for eth_getBlockByHash, eth_getBlockByNumber,
// eth_blockNumber, eth_getBlockReceipts, trace_block, and eth_getBlobSidecars.
// Supports both single and batch requests, and both hash and block-number params.
package mockchain

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/pkg/hexutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/rpcutil"
)

// Standard JSON-RPC 2.0 error codes.
const (
	rpcErrMethodNotFound = -32601
	rpcErrInvalidParams  = -32602
	rpcErrInternal       = -32603
)

type httpHandler struct {
	store    *DataStore
	replayer *Replayer
}

func newHTTPHandler(store *DataStore, replayer *Replayer) *httpHandler {
	return &httpHandler{store: store, replayer: replayer}
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

	var req rpcutil.Request
	if err := json.Unmarshal(raw, &req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	result, rpcErr := h.dispatch(req)
	h.respond(w, req.ID, result, rpcErr)
}

func (h *httpHandler) handleBatch(w http.ResponseWriter, raw json.RawMessage) {
	var reqs []rpcutil.Request
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
	_, _ = w.Write(out) // error not actionable: headers already written
}

func (h *httpHandler) dispatch(req rpcutil.Request) (json.RawMessage, *jsonRPCErrorObj) {
	switch req.Method {
	case "eth_getBlockByHash":
		return h.getBlockByHash(req)
	case "eth_blockNumber":
		return h.blockNumber()
	case "eth_getBlockByNumber":
		return h.getBlockByNumber(req)
	case "eth_getBlockReceipts":
		return h.getDataByHashOrNumber(req, "receipts")
	case "trace_block":
		return h.getDataByHashOrNumber(req, "traces")
	case "eth_getBlobSidecars":
		return h.getDataByHashOrNumber(req, "blobs")
	default:
		return nil, &jsonRPCErrorObj{Code: rpcErrMethodNotFound, Message: "method not found"}
	}
}

func (h *httpHandler) respond(w http.ResponseWriter, id json.RawMessage, result json.RawMessage, rpcErr *jsonRPCErrorObj) {
	if rpcErr != nil {
		rpcutil.WriteError(w, id, rpcErr.Code, rpcErr.Message)
		return
	}
	rpcutil.WriteResult(w, id, result)
}

// extractStringParam extracts the first element of a JSON params array as a string.
func extractStringParam(req rpcutil.Request) (string, bool) {
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

// isHashParam returns true if the param is a valid 32-byte hex hash.
func isHashParam(s string) bool {
	return common.IsHexHash(s)
}

func (h *httpHandler) getBlockByHash(req rpcutil.Request) (json.RawMessage, *jsonRPCErrorObj) {
	hash, ok := extractStringParam(req)
	if !ok {
		return nil, &jsonRPCErrorObj{Code: rpcErrInvalidParams, Message: "missing or invalid hash parameter"}
	}

	header, found := h.replayer.HeaderForHash(hash)
	if !found {
		return json.RawMessage("null"), nil
	}
	raw, err := json.Marshal(header)
	if err != nil {
		return nil, &jsonRPCErrorObj{Code: rpcErrInternal, Message: "internal error"}
	}
	return raw, nil
}

func (h *httpHandler) getBlockByNumber(req rpcutil.Request) (json.RawMessage, *jsonRPCErrorObj) {
	numStr, ok := extractStringParam(req)
	if !ok {
		return nil, &jsonRPCErrorObj{Code: rpcErrInvalidParams, Message: "missing or invalid block number parameter"}
	}
	blockNum, err := hexutil.ParseInt64(numStr)
	if err != nil {
		return nil, &jsonRPCErrorObj{Code: rpcErrInvalidParams, Message: "invalid block number"}
	}
	header, found := h.replayer.HeaderForNumber(blockNum)
	if !found {
		return json.RawMessage("null"), nil
	}
	raw, err := json.Marshal(header)
	if err != nil {
		return nil, &jsonRPCErrorObj{Code: rpcErrInternal, Message: "internal error"}
	}
	return raw, nil
}

// getDataByHashOrNumber serves eth_getBlockReceipts, trace_block, and eth_getBlobSidecars.
// The first param may be a 32-byte hash (66 chars) or a hex block number (shorter).
func (h *httpHandler) getDataByHashOrNumber(req rpcutil.Request, dataType string) (json.RawMessage, *jsonRPCErrorObj) {
	param, ok := extractStringParam(req)
	if !ok {
		return nil, &jsonRPCErrorObj{Code: rpcErrInvalidParams, Message: "missing or invalid parameter"}
	}

	var idx int
	var found bool
	if isHashParam(param) {
		idx, found = h.replayer.TemplateIndexForHash(param)
	} else {
		blockNum, err := hexutil.ParseInt64(param)
		if err != nil {
			return json.RawMessage("null"), nil
		}
		idx, found = h.replayer.TemplateIndexForNumber(blockNum)
	}

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
	num := h.replayer.CurrentBlockNumber()
	if num == 0 {
		return json.RawMessage(`"0x0"`), nil
	}
	result, err := json.Marshal("0x" + strconv.FormatInt(num, 16))
	if err != nil {
		return nil, &jsonRPCErrorObj{Code: rpcErrInternal, Message: "internal error"}
	}
	return result, nil
}

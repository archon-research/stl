// httprpc.go implements the HTTP JSON-RPC handler for eth_getBlockByHash, eth_getBlockByNumber,
// eth_blockNumber, eth_getBlockReceipts, trace_block, and eth_getBlobSidecars.
// Supports both single and batch requests, and both hash and block-number params.
package mockchain

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync/atomic"

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

// ErrorMode controls how the HTTP RPC handler behaves for error injection.
type ErrorMode int32

const (
	// ErrorModeNone is normal operation.
	ErrorModeNone ErrorMode = 0
	// ErrorMode429 always returns HTTP 429 Too Many Requests.
	ErrorMode429 ErrorMode = 1
	// ErrorMode500 always returns HTTP 500 Internal Server Error.
	ErrorMode500 ErrorMode = 2
	// ErrorModeTimeout blocks indefinitely, simulating a client-side timeout.
	ErrorModeTimeout ErrorMode = 3
	// ErrorModeOnce429 returns HTTP 429 once, then resets to ErrorModeNone.
	ErrorModeOnce429 ErrorMode = 4
	// ErrorModeOnce500 returns HTTP 500 once, then resets to ErrorModeNone.
	ErrorModeOnce500 ErrorMode = 5
)

type httpHandler struct {
	store     *DataStore
	replayer  *Replayer
	errorMode atomic.Int32
}

func newHTTPHandler(store *DataStore, replayer *Replayer) *httpHandler {
	return &httpHandler{store: store, replayer: replayer}
}

// SetErrorMode sets the error injection mode for subsequent requests.
func (h *httpHandler) SetErrorMode(mode ErrorMode) {
	h.errorMode.Store(int32(mode))
}

func (h *httpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Error injection: check mode before processing the request.
	if injected := h.handleErrorInjection(w, r); injected {
		return
	}

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

// handleErrorInjection checks the current error mode and writes the appropriate
// HTTP error response. Returns true if the request was handled by injection.
func (h *httpHandler) handleErrorInjection(w http.ResponseWriter, r *http.Request) bool {
	mode := ErrorMode(h.errorMode.Load())

	switch mode {
	case ErrorModeNone:
		return false

	case ErrorMode429:
		http.Error(w, "too many requests", http.StatusTooManyRequests)
		return true

	case ErrorMode500:
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return true

	case ErrorModeTimeout:
		// Block until the client disconnects or context is cancelled.
		<-r.Context().Done()
		return true

	case ErrorModeOnce429:
		// Reset to None on success; only one request gets the error.
		if h.errorMode.CompareAndSwap(int32(ErrorModeOnce429), int32(ErrorModeNone)) {
			http.Error(w, "too many requests", http.StatusTooManyRequests)
			return true
		}
		return false

	case ErrorModeOnce500:
		if h.errorMode.CompareAndSwap(int32(ErrorModeOnce500), int32(ErrorModeNone)) {
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return true
		}
		return false
	}

	return false
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

// getBlockByHash returns the full block JSON for a given hash, with number/hash/parentHash
// patched to match the derived values.
func (h *httpHandler) getBlockByHash(req rpcutil.Request) (json.RawMessage, *jsonRPCErrorObj) {
	hash, ok := extractStringParam(req)
	if !ok {
		return nil, &jsonRPCErrorObj{Code: rpcErrInvalidParams, Message: "missing or invalid hash parameter"}
	}

	// Check reorg blocks first — full block data is pre-patched during reorg creation.
	if raw, found := h.store.GetReorgBlock(hash, "block"); found {
		return raw, nil
	}
	// Fall back to reorg header if no full block data is stored.
	if reorgHeader, found := h.store.GetReorgHeader(hash); found {
		raw, err := json.Marshal(reorgHeader)
		if err != nil {
			return nil, &jsonRPCErrorObj{Code: rpcErrInternal, Message: "internal error"}
		}
		return raw, nil
	}

	// Canonical block: look up template index, get full block data, patch derived fields.
	idx, found := h.replayer.TemplateIndexForHash(hash)
	if !found {
		return json.RawMessage("null"), nil
	}
	header, found := h.replayer.HeaderForHash(hash)
	if !found {
		return json.RawMessage("null"), nil
	}
	raw, ok := h.store.Get(idx, "block")
	if !ok {
		return nil, &jsonRPCErrorObj{Code: rpcErrInternal, Message: "missing block data for template"}
	}
	patched, err := patchBlockJSON(raw, header.Number, header.Hash, header.ParentHash)
	if err != nil {
		return nil, &jsonRPCErrorObj{Code: rpcErrInternal, Message: "internal error"}
	}
	return patched, nil
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
	idx, found := h.replayer.TemplateIndexForNumber(blockNum)
	if !found {
		return json.RawMessage("null"), nil
	}
	raw, ok := h.store.Get(idx, "block")
	if !ok {
		return nil, &jsonRPCErrorObj{Code: rpcErrInternal, Message: "missing block data for template"}
	}
	patched, err := patchBlockJSON(raw, header.Number, header.Hash, header.ParentHash)
	if err != nil {
		return nil, &jsonRPCErrorObj{Code: rpcErrInternal, Message: "internal error"}
	}
	return patched, nil
}

// getDataByHashOrNumber serves eth_getBlockReceipts, trace_block, and eth_getBlobSidecars.
// The first param may be a 32-byte hash (66 chars) or a hex block number (shorter).
// Reorg blocks are checked first so that reorg walk-back requests are served correctly.
func (h *httpHandler) getDataByHashOrNumber(req rpcutil.Request, dataType string) (json.RawMessage, *jsonRPCErrorObj) {
	param, ok := extractStringParam(req)
	if !ok {
		return nil, &jsonRPCErrorObj{Code: rpcErrInvalidParams, Message: "missing or invalid parameter"}
	}

	if isHashParam(param) {
		// Check reorg blocks first.
		if raw, found := h.store.GetReorgBlock(param, dataType); found {
			return raw, nil
		}
		idx, found := h.replayer.TemplateIndexForHash(param)
		if !found {
			return json.RawMessage("null"), nil
		}
		raw, ok := h.store.Get(idx, dataType)
		if !ok {
			return json.RawMessage("null"), nil
		}
		return raw, nil
	}

	blockNum, err := hexutil.ParseInt64(param)
	if err != nil {
		return json.RawMessage("null"), nil
	}
	idx, found := h.replayer.TemplateIndexForNumber(blockNum)
	if !found {
		return json.RawMessage("null"), nil
	}
	raw, ok := h.store.Get(idx, dataType)
	if !ok {
		return json.RawMessage("null"), nil
	}
	return raw, nil
}

// patchBlockJSON overwrites the "number", "hash", and "parentHash" fields in raw block JSON
// while preserving all other fields (transactions, uncles, withdrawals, etc.).
// Only top-level keys are parsed; nested arrays stay as raw bytes.
func patchBlockJSON(raw json.RawMessage, number, hash, parentHash string) (json.RawMessage, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, fmt.Errorf("unmarshalling block JSON: %w", err)
	}
	var marshalErr error
	obj["number"], marshalErr = json.Marshal(number)
	if marshalErr != nil {
		return nil, fmt.Errorf("marshalling number: %w", marshalErr)
	}
	obj["hash"], marshalErr = json.Marshal(hash)
	if marshalErr != nil {
		return nil, fmt.Errorf("marshalling hash: %w", marshalErr)
	}
	obj["parentHash"], marshalErr = json.Marshal(parentHash)
	if marshalErr != nil {
		return nil, fmt.Errorf("marshalling parentHash: %w", marshalErr)
	}
	return json.Marshal(obj)
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

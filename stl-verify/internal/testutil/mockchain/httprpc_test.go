package mockchain

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// httpRPCResponse is a test-only decode target for JSON-RPC success responses over HTTP.
type httpRPCResponse struct {
	JsonRPC string          `json:"jsonrpc"`
	ID      int             `json:"id"`
	Result  json.RawMessage `json:"result"`
}

// rpcPost sends a JSON-RPC POST request to the handler and returns the recorder.
func rpcPost(t *testing.T, h *httpHandler, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

// newTestHTTPHandler returns a handler backed by a replayer that has already emitted
// all three template blocks. The emitted (derived) headers are returned for use in tests.
func newTestHTTPHandler(t *testing.T) (*httpHandler, []string) {
	t.Helper()
	ds := NewFixtureDataStore()
	var hashes []string
	r := NewReplayer(ds.Headers(), ds, func(h outbound.BlockHeader) {
		hashes = append(hashes, h.Hash)
	}, 0)
	emitOrFail(t, r)
	emitOrFail(t, r)
	emitOrFail(t, r)
	return newHTTPHandler(ds, r), hashes
}

// TestNewHTTPHandler verifies the constructor sets the store and replayer.
func TestNewHTTPHandler(t *testing.T) {
	ds := NewFixtureDataStore()
	r := NewReplayer(ds.Headers(), ds, func(_ outbound.BlockHeader) {}, 0)
	h := newHTTPHandler(ds, r)
	if h.store != ds {
		t.Error("expected store to be set")
	}
	if h.replayer != r {
		t.Error("expected replayer to be set")
	}
}

// TestHTTPHandler_GetBlockByHash verifies known derived hash, unknown hash, and missing params cases.
func TestHTTPHandler_GetBlockByHash(t *testing.T) {
	h, hashes := newTestHTTPHandler(t)
	knownHash := hashes[0]

	tests := []struct {
		name     string
		params   string
		wantNull bool
		wantCode int // 0 = success, non-zero = JSON-RPC error code
	}{
		{"known derived hash", `["` + knownHash + `",false]`, false, 0},
		{"unknown hash", `["0xdeadbeef",false]`, true, 0},
		{"missing params", `[]`, false, -32602},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := rpcPost(t, h, `{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByHash","params":`+tt.params+`}`)
			if tt.wantCode != 0 {
				var resp jsonRPCErrorResponse
				if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
					t.Fatalf("decode: %v", err)
				}
				if resp.Error.Code != tt.wantCode {
					t.Errorf("expected error code %d, got %d", tt.wantCode, resp.Error.Code)
				}
				return
			}
			var resp httpRPCResponse
			if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if tt.wantNull && string(resp.Result) != "null" {
				t.Errorf("expected null result, got %s", resp.Result)
			}
			if !tt.wantNull && (string(resp.Result) == "null" || len(resp.Result) == 0) {
				t.Error("expected non-null block result")
			}
		})
	}
}

// TestHTTPHandler_GetBlockByNumber verifies eth_getBlockByNumber returns the correct patched header.
func TestHTTPHandler_GetBlockByNumber(t *testing.T) {
	h, _ := newTestHTTPHandler(t)

	tests := []struct {
		name     string
		params   string
		wantNull bool
		wantCode int
	}{
		{"known block number", `["0x1",false]`, false, 0},
		{"unknown block number", `["0xffff",false]`, true, 0},
		{"missing params", `[]`, false, -32602},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := rpcPost(t, h, `{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":`+tt.params+`}`)
			if tt.wantCode != 0 {
				var resp jsonRPCErrorResponse
				if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
					t.Fatalf("decode: %v", err)
				}
				if resp.Error.Code != tt.wantCode {
					t.Errorf("expected error code %d, got %d", tt.wantCode, resp.Error.Code)
				}
				return
			}
			var resp httpRPCResponse
			if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if tt.wantNull && string(resp.Result) != "null" {
				t.Errorf("expected null result, got %s", resp.Result)
			}
			if !tt.wantNull && (string(resp.Result) == "null" || len(resp.Result) == 0) {
				t.Error("expected non-null block result")
			}
		})
	}
}

// TestHTTPHandler_GetBlockByNumber_HashMatches verifies that eth_getBlockByNumber returns a header
// whose Hash matches what eth_blockNumber and WebSocket broadcasts would produce.
func TestHTTPHandler_GetBlockByNumber_HashMatches(t *testing.T) {
	h, hashes := newTestHTTPHandler(t)

	w := rpcPost(t, h, `{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["0x1",false]}`)
	var resp httpRPCResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	var header outbound.BlockHeader
	if err := json.Unmarshal(resp.Result, &header); err != nil {
		t.Fatalf("decode header: %v", err)
	}
	if header.Hash != hashes[0] {
		t.Errorf("eth_getBlockByNumber returned hash %q, want %q", header.Hash, hashes[0])
	}
}

// TestHTTPHandler_BlockNumber verifies eth_blockNumber reflects the replayer's emitted state.
func TestHTTPHandler_BlockNumber(t *testing.T) {
	ds := NewFixtureDataStore()
	r := NewReplayer(ds.Headers(), ds, func(_ outbound.BlockHeader) {}, 0)

	// No emissions yet: expect "0x0".
	h := newHTTPHandler(ds, r)
	w := rpcPost(t, h, `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`)
	var resp httpRPCResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(resp.Result) != `"0x0"` {
		t.Errorf("expected \"0x0\" before any emission, got %s", resp.Result)
	}

	// After emitting all 3 templates (base=1), last block number = 3 = 0x3.
	emitOrFail(t, r)
	emitOrFail(t, r)
	emitOrFail(t, r)
	w = rpcPost(t, h, `{"jsonrpc":"2.0","id":2,"method":"eth_blockNumber","params":[]}`)
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(resp.Result) != `"0x3"` {
		t.Errorf("expected \"0x3\" after 3 emissions, got %s", resp.Result)
	}
}

// TestHTTPHandler_UnknownMethod verifies an unsupported method returns -32601.
func TestHTTPHandler_UnknownMethod(t *testing.T) {
	h, _ := newTestHTTPHandler(t)

	w := rpcPost(t, h, `{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}`)

	var resp jsonRPCErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error.Code != -32601 {
		t.Errorf("expected -32601, got %d", resp.Error.Code)
	}
}

// TestHTTPHandler_MethodNotAllowed verifies that non-POST requests return HTTP 405.
func TestHTTPHandler_MethodNotAllowed(t *testing.T) {
	h, _ := newTestHTTPHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}

// TestHTTPHandler_InvalidBody verifies malformed JSON returns HTTP 400.
func TestHTTPHandler_InvalidBody(t *testing.T) {
	h, _ := newTestHTTPHandler(t)

	w := rpcPost(t, h, "this is not json")

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

// TestPatchBlockJSON verifies that patchBlockJSON overwrites number/hash/parentHash
// while preserving all other fields.
func TestPatchBlockJSON(t *testing.T) {
	t.Run("patches fields and preserves transactions", func(t *testing.T) {
		raw := json.RawMessage(`{"number":"0x1","hash":"0xold","parentHash":"0xparent","transactions":["0xtx1","0xtx2"],"gasUsed":"0x100"}`)
		patched, err := patchBlockJSON(raw, "0xff", "0xnewhash", "0xnewparent")
		if err != nil {
			t.Fatalf("patchBlockJSON: %v", err)
		}
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(patched, &obj); err != nil {
			t.Fatalf("unmarshal patched: %v", err)
		}
		if string(obj["number"]) != `"0xff"` {
			t.Errorf("number = %s, want \"0xff\"", obj["number"])
		}
		if string(obj["hash"]) != `"0xnewhash"` {
			t.Errorf("hash = %s, want \"0xnewhash\"", obj["hash"])
		}
		if string(obj["parentHash"]) != `"0xnewparent"` {
			t.Errorf("parentHash = %s, want \"0xnewparent\"", obj["parentHash"])
		}
		if string(obj["transactions"]) != `["0xtx1","0xtx2"]` {
			t.Errorf("transactions not preserved: %s", obj["transactions"])
		}
		if string(obj["gasUsed"]) != `"0x100"` {
			t.Errorf("gasUsed not preserved: %s", obj["gasUsed"])
		}
	})

	t.Run("header-only JSON works", func(t *testing.T) {
		raw := json.RawMessage(`{"number":"0x1","hash":"0xold","parentHash":"0xparent"}`)
		patched, err := patchBlockJSON(raw, "0x2", "0xnew", "0xnewparent")
		if err != nil {
			t.Fatalf("patchBlockJSON: %v", err)
		}
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(patched, &obj); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if string(obj["number"]) != `"0x2"` {
			t.Errorf("number = %s, want \"0x2\"", obj["number"])
		}
	})

	t.Run("invalid JSON returns error", func(t *testing.T) {
		_, err := patchBlockJSON(json.RawMessage(`not json`), "0x1", "0x2", "0x3")
		if err == nil {
			t.Error("expected error for invalid JSON")
		}
	})
}

// TestHTTPHandler_GetBlockByHash_FullBlock verifies that eth_getBlockByHash returns full
// block data (with transactions key) from the DataStore, not just the header.
func TestHTTPHandler_GetBlockByHash_FullBlock(t *testing.T) {
	h, hashes := newTestHTTPHandler(t)
	knownHash := hashes[0]

	w := rpcPost(t, h, `{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByHash","params":["`+knownHash+`",true]}`)
	var resp httpRPCResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	var block map[string]json.RawMessage
	if err := json.Unmarshal(resp.Result, &block); err != nil {
		t.Fatalf("decode block: %v", err)
	}
	if _, ok := block["transactions"]; !ok {
		t.Error("expected block to contain 'transactions' key")
	}
	// Verify the hash was patched to match the derived hash.
	var hash string
	if err := json.Unmarshal(block["hash"], &hash); err != nil {
		t.Fatalf("decode hash: %v", err)
	}
	if hash != knownHash {
		t.Errorf("hash = %q, want %q", hash, knownHash)
	}
}

// TestHTTPHandler_GetBlockByNumber_FullBlock verifies that eth_getBlockByNumber returns full
// block data (with transactions key) from the DataStore, not just the header.
func TestHTTPHandler_GetBlockByNumber_FullBlock(t *testing.T) {
	h, hashes := newTestHTTPHandler(t)

	w := rpcPost(t, h, `{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["0x1",true]}`)
	var resp httpRPCResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	var block map[string]json.RawMessage
	if err := json.Unmarshal(resp.Result, &block); err != nil {
		t.Fatalf("decode block: %v", err)
	}
	if _, ok := block["transactions"]; !ok {
		t.Error("expected block to contain 'transactions' key")
	}
	// Verify the hash was patched to match the derived hash.
	var hash string
	if err := json.Unmarshal(block["hash"], &hash); err != nil {
		t.Fatalf("decode hash: %v", err)
	}
	if hash != hashes[0] {
		t.Errorf("hash = %q, want %q", hash, hashes[0])
	}
}

// TestHTTPHandler_DataByHash verifies eth_getBlockReceipts, trace_block, and eth_getBlobSidecars
// return stored data for a known derived hash and null for an unknown hash.
func TestHTTPHandler_DataByHash(t *testing.T) {
	h, hashes := newTestHTTPHandler(t)
	knownHash := hashes[0]

	tests := []struct {
		method   string
		params   string
		wantNull bool
		wantCode int // 0 = success, non-zero = JSON-RPC error code
	}{
		// eth_getBlockReceipts
		{"eth_getBlockReceipts", `["` + knownHash + `"]`, false, 0},
		{"eth_getBlockReceipts", `["0xdeadbeef"]`, true, 0},
		{"eth_getBlockReceipts", `[]`, false, -32602},
		// trace_block
		{"trace_block", `["` + knownHash + `"]`, false, 0},
		{"trace_block", `["0xdeadbeef"]`, true, 0},
		{"trace_block", `[]`, false, -32602},
		// eth_getBlobSidecars
		{"eth_getBlobSidecars", `["` + knownHash + `"]`, false, 0},
		{"eth_getBlobSidecars", `["0xdeadbeef"]`, true, 0},
		{"eth_getBlobSidecars", `[]`, false, -32602},
	}
	for _, tt := range tests {
		t.Run(tt.method+"/"+tt.params, func(t *testing.T) {
			w := rpcPost(t, h, `{"jsonrpc":"2.0","id":1,"method":"`+tt.method+`","params":`+tt.params+`}`)
			if tt.wantCode != 0 {
				var resp jsonRPCErrorResponse
				if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
					t.Fatalf("decode: %v", err)
				}
				if resp.Error.Code != tt.wantCode {
					t.Errorf("expected error code %d, got %d", tt.wantCode, resp.Error.Code)
				}
				return
			}
			var resp httpRPCResponse
			if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if tt.wantNull && string(resp.Result) != "null" {
				t.Errorf("expected null result, got %s", resp.Result)
			}
			if !tt.wantNull && (string(resp.Result) == "null" || len(resp.Result) == 0) {
				t.Error("expected non-null result")
			}
		})
	}
}

// TestHTTPHandler_DataByNumber verifies that eth_getBlockReceipts, trace_block, and
// eth_getBlobSidecars return data when given a block number instead of a hash.
func TestHTTPHandler_DataByNumber(t *testing.T) {
	h, _ := newTestHTTPHandler(t)

	methods := []string{"eth_getBlockReceipts", "trace_block", "eth_getBlobSidecars"}
	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			w := rpcPost(t, h, `{"jsonrpc":"2.0","id":1,"method":"`+method+`","params":["0x1"]}`)
			var resp httpRPCResponse
			if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if string(resp.Result) == "null" || len(resp.Result) == 0 {
				t.Errorf("%s by number returned null, expected data", method)
			}
		})
	}
}

// TestHTTPHandler_Batch verifies that a batch request returns a matching array of responses.
func TestHTTPHandler_Batch(t *testing.T) {
	h, hashes := newTestHTTPHandler(t)
	knownHash := hashes[0]

	body := `[` +
		`{"jsonrpc":"2.0","id":10,"method":"eth_blockNumber","params":[]}` +
		`,{"jsonrpc":"2.0","id":11,"method":"eth_getBlockByHash","params":["` + knownHash + `",false]}` +
		`]`

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var responses []struct {
		ID     int             `json:"id"`
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &responses); err != nil {
		t.Fatalf("decode batch response: %v", err)
	}
	if len(responses) != 2 {
		t.Fatalf("expected 2 responses, got %d", len(responses))
	}
	if responses[0].ID != 10 {
		t.Errorf("expected id 10 for first response, got %d", responses[0].ID)
	}
	if responses[1].ID != 11 {
		t.Errorf("expected id 11 for second response, got %d", responses[1].ID)
	}
	if string(responses[0].Result) == "" {
		t.Error("expected non-empty blockNumber result")
	}
	if string(responses[1].Result) == "null" || len(responses[1].Result) == 0 {
		t.Error("expected non-null block result for known hash")
	}
}

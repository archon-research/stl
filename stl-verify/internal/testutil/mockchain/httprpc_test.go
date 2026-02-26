package mockchain

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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

// TestNewHTTPHandler verifies the constructor sets the store.
func TestNewHTTPHandler(t *testing.T) {
	ds := NewTestDataStore()
	h := newHTTPHandler(ds)
	if h.store != ds {
		t.Error("expected store to be set")
	}
}

// TestHTTPHandler_GetBlockByHash verifies known hash, unknown hash, and missing params cases.
func TestHTTPHandler_GetBlockByHash(t *testing.T) {
	ds := NewTestDataStore()
	h := newHTTPHandler(ds)
	knownHash := ds.Headers()[0].Hash

	tests := []struct {
		name      string
		params    string
		wantNull  bool
		wantCode  int // 0 = success, non-zero = JSON-RPC error code
	}{
		{"known hash", `["` + knownHash + `",false]`, false, 0},
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

// TestHTTPHandler_BlockNumber verifies eth_blockNumber for both a populated and empty store.
func TestHTTPHandler_BlockNumber(t *testing.T) {
	populatedDS := NewTestDataStore()
	wantNumber := `"` + populatedDS.Headers()[populatedDS.Len()-1].Number + `"`

	tests := []struct {
		name       string
		store      *DataStore
		wantResult string
	}{
		{"populated store", populatedDS, wantNumber},
		{"empty store", NewDataStore(), `"0x0"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHTTPHandler(tt.store)
			w := rpcPost(t, h, `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`)
			var resp httpRPCResponse
			if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if string(resp.Result) != tt.wantResult {
				t.Errorf("expected %s, got %s", tt.wantResult, resp.Result)
			}
		})
	}
}

// TestHTTPHandler_UnknownMethod verifies an unsupported method returns -32601.
func TestHTTPHandler_UnknownMethod(t *testing.T) {
	ds := NewTestDataStore()
	h := newHTTPHandler(ds)

	w := rpcPost(t, h, `{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}`)

	var resp jsonRPCErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error.Code != -32601 {
		t.Errorf("expected -32601, got %d", resp.Error.Code)
	}
}

// TestHTTPHandler_InvalidBody verifies malformed JSON returns HTTP 400.
func TestHTTPHandler_InvalidBody(t *testing.T) {
	ds := NewTestDataStore()
	h := newHTTPHandler(ds)

	w := rpcPost(t, h, "this is not json")

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

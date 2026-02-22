package multicall

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rpc"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// startBatchRPCServer starts an HTTP server that handles JSON-RPC batch requests.
// handler is called for each individual request within the batch.
func startBatchRPCServer(t *testing.T, handler func(req testutil.JSONRPCRequest) (json.RawMessage, *rpcError)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")

		// Try batch first (array of requests)
		var batch []testutil.JSONRPCRequest
		if err := json.Unmarshal(body, &batch); err != nil {
			// Single request
			var single testutil.JSONRPCRequest
			if err := json.Unmarshal(body, &single); err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			batch = []testutil.JSONRPCRequest{single}
		}

		responses := make([]json.RawMessage, len(batch))
		for i, req := range batch {
			result, rpcErr := handler(req)
			if rpcErr != nil {
				errJSON, _ := json.Marshal(map[string]interface{}{
					"code":    rpcErr.Code,
					"message": rpcErr.Message,
				})
				resp, _ := json.Marshal(map[string]json.RawMessage{
					"jsonrpc": json.RawMessage(`"2.0"`),
					"id":      req.ID,
					"error":   errJSON,
				})
				responses[i] = resp
			} else {
				resp, _ := json.Marshal(map[string]json.RawMessage{
					"jsonrpc": json.RawMessage(`"2.0"`),
					"id":      req.ID,
					"result":  result,
				})
				responses[i] = resp
			}
		}

		_, _ = w.Write([]byte("["))
		for i, r := range responses {
			if i > 0 {
				_, _ = w.Write([]byte(","))
			}
			_, _ = w.Write(r)
		}
		_, _ = w.Write([]byte("]"))
	}))
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func dialTestRPC(t *testing.T, serverURL string) *rpc.Client {
	t.Helper()
	client, err := rpc.Dial(serverURL)
	if err != nil {
		t.Fatalf("dial rpc: %v", err)
	}
	t.Cleanup(client.Close)
	return client
}

func newTestDirectCaller(t *testing.T, serverURL string) *DirectCaller {
	t.Helper()
	dc, err := NewDirectCaller(dialTestRPC(t, serverURL))
	if err != nil {
		t.Fatalf("NewDirectCaller: %v", err)
	}
	return dc
}

// ---------------------------------------------------------------------------
// TestToBlockNumArg
// ---------------------------------------------------------------------------

func TestToBlockNumArg(t *testing.T) {
	tests := []struct {
		name    string
		number  *big.Int
		want    string
		wantErr bool
	}{
		{
			name:    "nil returns error",
			number:  nil,
			wantErr: true,
		},
		{
			name:   "zero",
			number: big.NewInt(0),
			want:   "0x0",
		},
		{
			name:   "positive",
			number: big.NewInt(12345678),
			want:   "0xbc614e",
		},
		{
			name:    "negative returns error",
			number:  big.NewInt(-1),
			wantErr: true,
		},
		{
			name:    "large negative returns error",
			number:  big.NewInt(-999999),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := toBlockNumArg(tt.number)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("toBlockNumArg(%v) = %q, want %q", tt.number, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestDirectCaller_Execute
// ---------------------------------------------------------------------------

func TestDirectCaller_Execute(t *testing.T) {
	target := common.HexToAddress("0x1111111111111111111111111111111111111111")
	callData := []byte{0xAB, 0xCD}

	t.Run("empty calls returns empty results", func(t *testing.T) {
		srv := startBatchRPCServer(t, func(req testutil.JSONRPCRequest) (json.RawMessage, *rpcError) {
			t.Fatal("no RPC call expected for empty calls")
			return nil, nil
		})
		defer srv.Close()

		dc := newTestDirectCaller(t, srv.URL)
		results, err := dc.Execute(context.Background(), nil, big.NewInt(100))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(results) != 0 {
			t.Errorf("results len = %d, want 0", len(results))
		}
	})

	t.Run("negative block number returns error", func(t *testing.T) {
		srv := startBatchRPCServer(t, func(req testutil.JSONRPCRequest) (json.RawMessage, *rpcError) {
			t.Fatal("no RPC call expected for negative block number")
			return nil, nil
		})
		defer srv.Close()

		dc := newTestDirectCaller(t, srv.URL)
		calls := []outbound.Call{{Target: target, CallData: callData}}
		_, err := dc.Execute(context.Background(), calls, big.NewInt(-5))
		if err == nil {
			t.Fatal("expected error for negative block number")
		}
		if !strings.Contains(err.Error(), "negative block number") {
			t.Errorf("error %q does not contain 'negative block number'", err)
		}
	})

	t.Run("single successful call", func(t *testing.T) {
		returnData := []byte{0x01, 0x02, 0x03}
		srv := startBatchRPCServer(t, func(req testutil.JSONRPCRequest) (json.RawMessage, *rpcError) {
			hex := fmt.Sprintf(`"0x%x"`, returnData)
			return json.RawMessage(hex), nil
		})
		defer srv.Close()

		dc := newTestDirectCaller(t, srv.URL)
		calls := []outbound.Call{{Target: target, AllowFailure: false, CallData: callData}}
		results, err := dc.Execute(context.Background(), calls, big.NewInt(100))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("results len = %d, want 1", len(results))
		}
		if !results[0].Success {
			t.Error("result[0].Success = false, want true")
		}
		if len(results[0].ReturnData) == 0 {
			t.Error("result[0].ReturnData is empty")
		}
	})

	t.Run("multiple calls all succeed", func(t *testing.T) {
		srv := startBatchRPCServer(t, func(req testutil.JSONRPCRequest) (json.RawMessage, *rpcError) {
			return json.RawMessage(`"0xdeadbeef"`), nil
		})
		defer srv.Close()

		dc := newTestDirectCaller(t, srv.URL)
		calls := []outbound.Call{
			{Target: target, AllowFailure: true, CallData: callData},
			{Target: common.HexToAddress("0x2222222222222222222222222222222222222222"), AllowFailure: true, CallData: []byte{0x01}},
			{Target: common.HexToAddress("0x3333333333333333333333333333333333333333"), AllowFailure: true, CallData: []byte{0x02}},
		}
		results, err := dc.Execute(context.Background(), calls, big.NewInt(200))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(results) != 3 {
			t.Fatalf("results len = %d, want 3", len(results))
		}
		for i, r := range results {
			if !r.Success {
				t.Errorf("results[%d].Success = false, want true", i)
			}
		}
	})

	t.Run("AllowFailure true — individual call error is tolerated", func(t *testing.T) {
		callCount := 0
		srv := startBatchRPCServer(t, func(req testutil.JSONRPCRequest) (json.RawMessage, *rpcError) {
			callCount++
			if callCount == 2 {
				return nil, &rpcError{Code: -32000, Message: "execution reverted"}
			}
			return json.RawMessage(`"0xaa"`), nil
		})
		defer srv.Close()

		dc := newTestDirectCaller(t, srv.URL)
		calls := []outbound.Call{
			{Target: target, AllowFailure: true, CallData: callData},
			{Target: target, AllowFailure: true, CallData: callData},
			{Target: target, AllowFailure: true, CallData: callData},
		}
		results, err := dc.Execute(context.Background(), calls, big.NewInt(300))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(results) != 3 {
			t.Fatalf("results len = %d, want 3", len(results))
		}
		if !results[0].Success {
			t.Error("results[0].Success = false, want true")
		}
		if results[1].Success {
			t.Error("results[1].Success = true, want false (reverted)")
		}
		if !results[2].Success {
			t.Error("results[2].Success = false, want true")
		}
	})

	t.Run("AllowFailure false — individual call error returns error", func(t *testing.T) {
		srv := startBatchRPCServer(t, func(req testutil.JSONRPCRequest) (json.RawMessage, *rpcError) {
			return nil, &rpcError{Code: -32000, Message: "execution reverted"}
		})
		defer srv.Close()

		dc := newTestDirectCaller(t, srv.URL)
		calls := []outbound.Call{
			{Target: target, AllowFailure: false, CallData: callData},
		}
		_, err := dc.Execute(context.Background(), calls, big.NewInt(400))
		if err == nil {
			t.Fatal("expected error when AllowFailure=false and call reverts")
		}
		if !strings.Contains(err.Error(), "direct call to") {
			t.Errorf("error %q does not contain 'direct call to'", err)
		}
	})

	t.Run("batch RPC transport error", func(t *testing.T) {
		// Server that closes connection immediately.
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hj, ok := w.(http.Hijacker)
			if !ok {
				http.Error(w, "hijack not supported", http.StatusInternalServerError)
				return
			}
			conn, _, _ := hj.Hijack()
			conn.Close()
		}))
		defer srv.Close()

		dc := newTestDirectCaller(t, srv.URL)
		calls := []outbound.Call{{Target: target, AllowFailure: true, CallData: callData}}
		_, err := dc.Execute(context.Background(), calls, big.NewInt(500))
		if err == nil {
			t.Fatal("expected error from transport failure")
		}
		if !strings.Contains(err.Error(), "batch eth_call failed") {
			t.Errorf("error %q does not contain 'batch eth_call failed'", err)
		}
	})

	t.Run("nil block number returns error", func(t *testing.T) {
		srv := startBatchRPCServer(t, func(req testutil.JSONRPCRequest) (json.RawMessage, *rpcError) {
			t.Fatal("no RPC call expected for nil block number")
			return nil, nil
		})
		defer srv.Close()

		dc := newTestDirectCaller(t, srv.URL)
		calls := []outbound.Call{{Target: target, AllowFailure: true, CallData: callData}}
		_, err := dc.Execute(context.Background(), calls, nil)
		if err == nil {
			t.Fatal("expected error for nil block number")
		}
		if !strings.Contains(err.Error(), "block number is required") {
			t.Errorf("error %q does not contain 'block number is required'", err)
		}
	})
}

// ---------------------------------------------------------------------------
// TestNewDirectCaller
// ---------------------------------------------------------------------------

func TestNewDirectCaller(t *testing.T) {
	t.Run("nil rpcClient returns error", func(t *testing.T) {
		_, err := NewDirectCaller(nil)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "rpcClient cannot be nil") {
			t.Errorf("error %q does not contain 'rpcClient cannot be nil'", err)
		}
	})

	t.Run("valid rpcClient succeeds", func(t *testing.T) {
		client := rpc.DialInProc(rpc.NewServer())
		defer client.Close()
		dc, err := NewDirectCaller(client)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if dc == nil {
			t.Fatal("expected non-nil DirectCaller")
		}
	})
}

// ---------------------------------------------------------------------------
// TestDirectCaller_Address
// ---------------------------------------------------------------------------

func TestDirectCaller_Address(t *testing.T) {
	client := rpc.DialInProc(rpc.NewServer())
	defer client.Close()
	dc, err := NewDirectCaller(client)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	addr := dc.Address()
	if addr != (common.Address{}) {
		t.Errorf("Address() = %s, want zero address", addr.Hex())
	}
}

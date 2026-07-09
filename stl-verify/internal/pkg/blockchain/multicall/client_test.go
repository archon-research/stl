package multicall

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// TestExecuteRecordsBatchSizeThroughExecute drives the instrumentation path via
// the real Execute method rather than calling recordBatch directly, so the
// Execute -> recordBatch wiring is exercised.
//
// The httptest server returns HTTP 500, which causes CallContract to fail after
// recordBatch has already run. We assert both that Execute returned an error and
// that the histogram captured count=1, sum=1.
func TestExecuteRecordsBatchSizeThroughExecute(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	tel, err := NewTelemetryWithProvider(mp, "mainnet")
	if err != nil {
		t.Fatalf("NewTelemetryWithProvider: %v", err)
	}

	// RPC endpoint that always fails so Execute reaches recordBatch then errors
	// at CallContract instead of needing a live node.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	rpcClient, err := rpc.DialHTTP(srv.URL)
	if err != nil {
		t.Fatalf("rpc.DialHTTP: %v", err)
	}
	t.Cleanup(rpcClient.Close)
	ethClient := ethclient.NewClient(rpcClient)

	mc, err := NewClient(ethClient, common.HexToAddress("0xcA11bde05977b3631167028862bE2a173976CA11"), WithTelemetry(tel))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	calls := []outbound.Call{{Target: common.Address{}, CallData: []byte{0x01, 0x02, 0x03, 0x04}}}
	_, execErr := mc.Execute(context.Background(), calls, big.NewInt(100))
	if execErr == nil {
		t.Fatal("expected Execute to fail against a 500 endpoint")
	}

	dp := histDataPoint(t, reader, "multicall.batch.size")
	if dp.Count != 1 || dp.Sum != 1 {
		t.Errorf("count=%d sum=%d, want 1 and 1 (recorded before the failing RPC)", dp.Count, dp.Sum)
	}
}

func TestRecordBatchNilTelemetryIsNoOp(t *testing.T) {
	c := &Client{}                         // no telemetry
	c.recordBatch(context.Background(), 5) // must not panic
}

// TestExecuteAtHashRecordsBatchSizeThroughExecuteAtHash mirrors
// TestExecuteRecordsBatchSizeThroughExecute for the hash-pinned path, so the
// telemetry wiring is verified for both entry points.
func TestExecuteAtHashRecordsBatchSizeThroughExecuteAtHash(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	tel, err := NewTelemetryWithProvider(mp, "mainnet")
	if err != nil {
		t.Fatalf("NewTelemetryWithProvider: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	rpcClient, err := rpc.DialHTTP(srv.URL)
	if err != nil {
		t.Fatalf("rpc.DialHTTP: %v", err)
	}
	t.Cleanup(rpcClient.Close)
	ethClient := ethclient.NewClient(rpcClient)

	mc, err := NewClient(ethClient, common.HexToAddress("0xcA11bde05977b3631167028862bE2a173976CA11"), WithTelemetry(tel))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	calls := []outbound.Call{{Target: common.Address{}, CallData: []byte{0x01, 0x02, 0x03, 0x04}}}
	_, execErr := mc.(*Client).ExecuteAtHash(context.Background(), calls, common.HexToHash("0xabc"))
	if execErr == nil {
		t.Fatal("expected ExecuteAtHash to fail against a 500 endpoint")
	}

	dp := histDataPoint(t, reader, "multicall.batch.size")
	if dp.Count != 1 || dp.Sum != 1 {
		t.Errorf("count=%d sum=%d, want 1 and 1 (recorded before the failing RPC)", dp.Count, dp.Sum)
	}
}

// TestExecuteAtHashPinsEthCallToTheExactBlockHash decodes the JSON-RPC request
// ExecuteAtHash sends and asserts the eth_call block parameter is the exact
// block hash that was passed ({"blockHash": ...}), never "latest" or a number.
// The server answers with a valid packed aggregate3 response so the
// success/unpack path is exercised too.
func TestExecuteAtHashPinsEthCallToTheExactBlockHash(t *testing.T) {
	blockHash := common.HexToHash("0x6afc23b52c9af7404f7c556802d29dbb0f1a2b34ce56ff8e78ad124f1a3cbd90")
	subcallReturn := []byte{0xbe, 0xef}

	type rpcRequest struct {
		ID     json.RawMessage   `json:"id"`
		Method string            `json:"method"`
		Params []json.RawMessage `json:"params"`
	}
	var mu sync.Mutex
	var captured []rpcRequest

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("reading request body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var req rpcRequest
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("unmarshalling JSON-RPC request %s: %v", body, err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		captured = append(captured, req)
		mu.Unlock()

		if len(req.Params) < 1 {
			t.Errorf("eth_call request has no params: %s", body)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var callArgs map[string]hexutil.Bytes
		if err := json.Unmarshal(req.Params[0], &callArgs); err != nil {
			t.Errorf("unmarshalling call args %s: %v", req.Params[0], err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		calldata := callArgs["input"]
		if calldata == nil {
			calldata = callArgs["data"]
		}
		result, err := testutil.HandleMulticall3(calldata, func(_ common.Address, _ []byte) ([]byte, bool) {
			return subcallReturn, true
		})
		if err != nil {
			t.Errorf("answering aggregate3: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":%q}`, req.ID, result)
	}))
	t.Cleanup(srv.Close)

	rpcClient, err := rpc.DialHTTP(srv.URL)
	if err != nil {
		t.Fatalf("rpc.DialHTTP: %v", err)
	}
	t.Cleanup(rpcClient.Close)

	mc, err := NewClient(ethclient.NewClient(rpcClient), common.HexToAddress("0xcA11bde05977b3631167028862bE2a173976CA11"))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	calls := []outbound.Call{{Target: common.HexToAddress("0x00000000000000000000000000000000000000AA"), AllowFailure: true, CallData: []byte{0xde, 0xad, 0xbe, 0xef}}}
	results, err := mc.(*Client).ExecuteAtHash(context.Background(), calls, blockHash)
	if err != nil {
		t.Fatalf("ExecuteAtHash: %v", err)
	}
	if len(results) != 1 || !results[0].Success || !bytes.Equal(results[0].ReturnData, subcallReturn) {
		t.Errorf("results = %+v, want one successful result with return data %#x", results, subcallReturn)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(captured) != 1 {
		t.Fatalf("captured %d JSON-RPC requests, want 1", len(captured))
	}
	req := captured[0]
	if req.Method != "eth_call" {
		t.Errorf("method = %q, want eth_call", req.Method)
	}
	if len(req.Params) != 2 {
		t.Fatalf("params length = %d, want 2 (call args + block parameter)", len(req.Params))
	}
	if string(req.Params[1]) == `"latest"` {
		t.Fatalf("block parameter = %s: the read is unpinned, want it pinned to the block hash", req.Params[1])
	}
	var pin struct {
		BlockHash *common.Hash `json:"blockHash"`
	}
	if err := json.Unmarshal(req.Params[1], &pin); err != nil {
		t.Fatalf("block parameter %s is not a block-hash object: %v", req.Params[1], err)
	}
	if pin.BlockHash == nil || *pin.BlockHash != blockHash {
		t.Errorf("block parameter = %s, want blockHash pinned to %s", req.Params[1], blockHash.Hex())
	}
}

// TestExecuteAtHashEmptyCallsReturnsEmpty mirrors the Execute contract: no
// calls means no RPC round trip and an empty (not nil) result.
func TestExecuteAtHashEmptyCallsReturnsEmpty(t *testing.T) {
	c := &Client{}
	results, err := c.ExecuteAtHash(context.Background(), nil, common.Hash{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("results len = %d, want 0", len(results))
	}
}

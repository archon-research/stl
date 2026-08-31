package alchemy

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

func logsTestClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	client, err := NewClient(ClientConfig{
		HTTPURL:        server.URL,
		MaxRetries:     2,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return client
}

func rpcErrorHandler(hits *atomic.Int64, code int, message string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		var req jsonRPCRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		_ = json.NewEncoder(w).Encode(jsonRPCResponse{
			JSONRPC: "2.0", ID: req.ID,
			Error: &jsonRPCError{Code: code, Message: message},
		})
	}
}

func capturedGetLogsFilter(t *testing.T, filter outbound.LogFilter) map[string]any {
	t.Helper()
	var captured []any
	client := logsTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		var req jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decoding request: %v", err)
		}
		if req.Method != "eth_getLogs" {
			t.Errorf("method = %q, want eth_getLogs", req.Method)
		}
		captured = req.Params
		_ = json.NewEncoder(w).Encode(jsonRPCResponse{JSONRPC: "2.0", ID: req.ID, Result: json.RawMessage(`[]`)})
	})

	if _, err := client.GetLogs(context.Background(), filter); err != nil {
		t.Fatalf("GetLogs: %v", err)
	}
	if len(captured) != 1 {
		t.Fatalf("params length = %d, want 1", len(captured))
	}
	raw, err := json.Marshal(captured[0])
	if err != nil {
		t.Fatalf("re-marshalling params: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshalling params: %v", err)
	}
	return got
}

func TestGetLogs_SendsHexBoundsAndAddress(t *testing.T) {
	got := capturedGetLogsFilter(t, outbound.LogFilter{
		Address:   common.HexToAddress(poolManagerTestAddr),
		FromBlock: 21688329,
		ToBlock:   21700000,
		Topic0:    common.HexToHash(modifyLiquidityTestTopic0),
	})

	if got["fromBlock"] != "0x14af009" {
		t.Errorf("fromBlock = %v, want 0x14af009", got["fromBlock"])
	}
	if got["toBlock"] != "0x14b1da0" {
		t.Errorf("toBlock = %v, want 0x14b1da0", got["toBlock"])
	}
	address, _ := got["address"].(string)
	if !strings.EqualFold(address, poolManagerTestAddr) {
		t.Errorf("address = %v, want %s", got["address"], poolManagerTestAddr)
	}
}

const (
	poolManagerTestAddr       = "0x000000000004444c5dc75cB358380D2e3dE08A90"
	modifyLiquidityTestTopic0 = "0xf208f4912782fd25c7f114ca3723a2d5dd6f3bcc3ac8db5af63baa85f711d5ec"
	poolATestID               = "0x1d5b2949ece8754c2d736991c62c5162bd144f497b2212182401b9bae77e2d76"
	poolBTestID               = "0xbc21dd4a44766fadfd6447f4b222a6185dcc2e6a3b15eb79e0cc637e30e7e97f"
)

func TestGetLogs_BuildsThePositionalTopicsArray(t *testing.T) {
	tests := []struct {
		name   string
		topic0 common.Hash
		topic1 []common.Hash
		// nil want means the topics key must be absent.
		want []any
	}{
		{
			name:   "topic0 and a topic1 or-set",
			topic0: common.HexToHash(modifyLiquidityTestTopic0),
			topic1: []common.Hash{common.HexToHash(poolATestID), common.HexToHash(poolBTestID)},
			want:   []any{modifyLiquidityTestTopic0, []any{poolATestID, poolBTestID}},
		},
		{
			name:   "topic0 only",
			topic0: common.HexToHash(modifyLiquidityTestTopic0),
			want:   []any{modifyLiquidityTestTopic0},
		},
		{
			name:   "topic1 only",
			topic1: []common.Hash{common.HexToHash(poolATestID)},
			want:   []any{nil, []any{poolATestID}},
		},
		{
			name: "neither",
			want: nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := capturedGetLogsFilter(t, outbound.LogFilter{
				Address:   common.HexToAddress(poolManagerTestAddr),
				FromBlock: 1,
				ToBlock:   2,
				Topic0:    tc.topic0,
				Topic1:    tc.topic1,
			})

			topics, present := got["topics"]
			if tc.want == nil {
				if present {
					t.Fatalf("topics = %v, want the key absent when nothing is constrained", topics)
				}
				return
			}
			if !present {
				t.Fatal("topics key is absent")
			}
			assertTopicsEqual(t, topics, tc.want)
		})
	}
}

func assertTopicsEqual(t *testing.T, got any, want []any) {
	t.Helper()
	entries, ok := got.([]any)
	if !ok {
		t.Fatalf("topics = %T, want a JSON array", got)
	}
	if len(entries) != len(want) {
		t.Fatalf("topics length = %d, want %d", len(entries), len(want))
	}
	for i, wantEntry := range want {
		switch expected := wantEntry.(type) {
		case nil:
			if entries[i] != nil {
				t.Errorf("topics[%d] = %v, want an explicit null placeholder", i, entries[i])
			}
		case string:
			actual, isString := entries[i].(string)
			if !isString || !strings.EqualFold(actual, expected) {
				t.Errorf("topics[%d] = %v, want %s", i, entries[i], expected)
			}
		case []any:
			orSet, isArray := entries[i].([]any)
			if !isArray {
				t.Fatalf("topics[%d] = %T, want a JSON array (the OR-set)", i, entries[i])
			}
			if len(orSet) != len(expected) {
				t.Fatalf("topics[%d] length = %d, want %d", i, len(orSet), len(expected))
			}
			for j, wantHash := range expected {
				actual, isString := orSet[j].(string)
				if !isString || !strings.EqualFold(actual, wantHash.(string)) {
					t.Errorf("topics[%d][%d] = %v, want %v", i, j, orSet[j], wantHash)
				}
			}
		}
	}
}

func TestGetLogs_DecodesReturnedLogs(t *testing.T) {
	client := logsTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		var req jsonRPCRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		_ = json.NewEncoder(w).Encode(jsonRPCResponse{JSONRPC: "2.0", ID: req.ID, Result: json.RawMessage(`[
			{"address":"0x000000000004444c5dc75cb358380d2e3de08a90",
			 "topics":["0xf208f4912782fd25c7f114ca3723a2d5dd6f3bcc3ac8db5af63baa85f711d5ec",
			           "0x1d5b2949ece8754c2d736991c62c5162bd144f497b2212182401b9bae77e2d76"],
			 "data":"0x00",
			 "blockHash":"0x2222222222222222222222222222222222222222222222222222222222222222",
			 "blockNumber":"0x14af009",
			 "transactionHash":"0x3333333333333333333333333333333333333333333333333333333333333333",
			 "transactionIndex":"0x1",
			 "logIndex":"0x7",
			 "removed":false}
		]`)})
	})

	logs, err := client.GetLogs(context.Background(), outbound.LogFilter{
		Address: common.HexToAddress("0x000000000004444c5dc75cB358380D2e3dE08A90"), FromBlock: 1, ToBlock: 2,
	})
	if err != nil {
		t.Fatalf("GetLogs: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("logs length = %d, want 1", len(logs))
	}
	got := logs[0]
	if got.LogIndex != "0x7" {
		t.Errorf("logIndex = %q, want 0x7", got.LogIndex)
	}
	if got.BlockNumber != "0x14af009" {
		t.Errorf("blockNumber = %q, want 0x14af009", got.BlockNumber)
	}
	if len(got.Topics) != 2 {
		t.Errorf("topics length = %d, want 2", len(got.Topics))
	}
	if got.TransactionHash != "0x3333333333333333333333333333333333333333333333333333333333333333" {
		t.Errorf("transactionHash = %q", got.TransactionHash)
	}
}

func TestGetLogs_RangeRefusalIsSentinelAndNotRetried(t *testing.T) {
	cases := []struct {
		name    string
		code    int
		message string
	}{
		{"alchemy response size", -32602, "Log response size exceeded. this block range should work: [0x14af009, 0x14b0000]"},
		{"infura result cap", -32005, "query returned more than 10000 results"},
		{"range cap", -32602, "eth_getLogs is limited to a 10000 range"},
		{"query timeout", -32603, "query timeout exceeded"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var hits atomic.Int64
			client := logsTestClient(t, rpcErrorHandler(&hits, tc.code, tc.message))

			_, err := client.GetLogs(context.Background(), outbound.LogFilter{
				Address: common.HexToAddress("0x1"), FromBlock: 1, ToBlock: 500000,
			})
			if !errors.Is(err, outbound.ErrLogRangeTooLarge) {
				t.Fatalf("error = %v, want it to wrap ErrLogRangeTooLarge", err)
			}
			if hits.Load() != 1 {
				t.Errorf("upstream hit %d times, want 1: a deterministic range refusal must not be retried", hits.Load())
			}
		})
	}
}

func TestGetLogs_UnrelatedRPCErrorIsRetriedAndNotTheRangeSentinel(t *testing.T) {
	var hits atomic.Int64
	client := logsTestClient(t, rpcErrorHandler(&hits, -32000, "header not found"))

	_, err := client.GetLogs(context.Background(), outbound.LogFilter{
		Address: common.HexToAddress("0x1"), FromBlock: 1, ToBlock: 2,
	})
	if err == nil {
		t.Fatal("expected an error")
	}
	if errors.Is(err, outbound.ErrLogRangeTooLarge) {
		t.Errorf("error = %v, want it NOT to wrap ErrLogRangeTooLarge", err)
	}
	if hits.Load() != 3 {
		t.Errorf("upstream hit %d times, want 3 (initial + 2 retries)", hits.Load())
	}
}

func TestGetLogs_RejectsInvertedRange(t *testing.T) {
	client := logsTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("GetLogs must reject an inverted range before dialling")
	})

	if _, err := client.GetLogs(context.Background(), outbound.LogFilter{
		Address: common.HexToAddress("0x1"), FromBlock: 10, ToBlock: 9,
	}); err == nil {
		t.Fatal("expected an error for fromBlock > toBlock")
	}
}

func TestGetLogs_RejectsNegativeBound(t *testing.T) {
	client := logsTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("GetLogs must reject a negative bound before dialling")
	})

	if _, err := client.GetLogs(context.Background(), outbound.LogFilter{
		Address: common.HexToAddress("0x1"), FromBlock: -1, ToBlock: 9,
	}); err == nil {
		t.Fatal("expected an error for a negative fromBlock")
	}
}

func TestGetLogs_MalformedResultIsAnError(t *testing.T) {
	client := logsTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		var req jsonRPCRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		_ = json.NewEncoder(w).Encode(jsonRPCResponse{JSONRPC: "2.0", ID: req.ID, Result: json.RawMessage(`{"not":"an array"}`)})
	})

	if _, err := client.GetLogs(context.Background(), outbound.LogFilter{
		Address: common.HexToAddress("0x1"), FromBlock: 1, ToBlock: 2,
	}); err == nil {
		t.Fatal("expected an error for a non-array result")
	}
}

func TestGetBlockHeaderByNumber_ParsesHeader(t *testing.T) {
	var captured []any
	client := logsTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		var req jsonRPCRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.Method != "eth_getBlockByNumber" {
			t.Errorf("method = %q, want eth_getBlockByNumber", req.Method)
		}
		captured = req.Params
		_ = json.NewEncoder(w).Encode(jsonRPCResponse{JSONRPC: "2.0", ID: req.ID, Result: json.RawMessage(
			`{"number":"0x14af009","hash":"0x2222222222222222222222222222222222222222222222222222222222222222","timestamp":"0x67c00000"}`)})
	})

	header, err := client.GetBlockHeaderByNumber(context.Background(), 21688329)
	if err != nil {
		t.Fatalf("GetBlockHeaderByNumber: %v", err)
	}
	if header.Hash != "0x2222222222222222222222222222222222222222222222222222222222222222" {
		t.Errorf("hash = %q", header.Hash)
	}
	if header.Timestamp != "0x67c00000" {
		t.Errorf("timestamp = %q", header.Timestamp)
	}
	if len(captured) != 2 || captured[0] != "0x14af009" || captured[1] != false {
		t.Errorf("params = %v, want [0x14af009 false]", captured)
	}
}

func TestGetBlockHeaderByNumber_RejectsNegativeBlock(t *testing.T) {
	client := logsTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("GetBlockHeaderByNumber must reject a negative block before dialling")
	})

	if _, err := client.GetBlockHeaderByNumber(context.Background(), -1); err == nil {
		t.Fatal("expected an error for a negative block number")
	}
}

func TestGetBlockHeaderByNumber_MalformedHeaderIsAnError(t *testing.T) {
	client := logsTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		var req jsonRPCRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		_ = json.NewEncoder(w).Encode(jsonRPCResponse{JSONRPC: "2.0", ID: req.ID, Result: json.RawMessage(`["not","an object"]`)})
	})

	if _, err := client.GetBlockHeaderByNumber(context.Background(), 1); err == nil {
		t.Fatal("expected an error for a non-object header")
	}
}

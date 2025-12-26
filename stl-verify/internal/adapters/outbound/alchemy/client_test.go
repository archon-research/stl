package alchemy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// --- Test: NewClient ---

func TestNewClient_CreatesClientWithURL(t *testing.T) {
	url := "https://eth-mainnet.g.alchemy.com/v2/demo"
	client := NewClient(url)

	if client == nil {
		t.Fatal("expected client, got nil")
	}
	if client.httpURL != url {
		t.Errorf("expected httpURL=%s, got %s", url, client.httpURL)
	}
	if client.httpClient == nil {
		t.Fatal("expected httpClient, got nil")
	}
	if client.httpClient.Timeout != 30*time.Second {
		t.Errorf("expected timeout=30s, got %v", client.httpClient.Timeout)
	}
}

// --- Test: GetBlockByNumber ---

func TestGetBlockByNumber_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("expected Content-Type=application/json, got %s", r.Header.Get("Content-Type"))
		}

		var req jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("failed to decode request: %v", err)
		}

		if req.Method != "eth_getBlockByNumber" {
			t.Errorf("expected method=eth_getBlockByNumber, got %s", req.Method)
		}
		if len(req.Params) != 2 {
			t.Errorf("expected 2 params, got %d", len(req.Params))
		}
		if req.Params[0] != "0x100" {
			t.Errorf("expected block number=0x100, got %v", req.Params[0])
		}

		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`{"number":"0x100","hash":"0xabc"}`),
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	result, err := client.GetBlockByNumber(ctx, 256, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result == nil {
		t.Fatal("expected result, got nil")
	}

	var block map[string]string
	if err := json.Unmarshal(result, &block); err != nil {
		t.Fatalf("failed to unmarshal result: %v", err)
	}

	if block["number"] != "0x100" {
		t.Errorf("expected number=0x100, got %s", block["number"])
	}
}

func TestGetBlockByNumber_RPCError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Error: &jsonRPCError{
				Code:    -32000,
				Message: "block not found",
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	_, err := client.GetBlockByNumber(ctx, 999999999, false)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "block not found") {
		t.Errorf("expected error to contain 'block not found', got %v", err)
	}
}

func TestGetBlockByNumber_HTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal server error"))
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	_, err := client.GetBlockByNumber(ctx, 256, false)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestGetBlockByNumber_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("not json"))
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	_, err := client.GetBlockByNumber(ctx, 256, false)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "failed to parse response") {
		t.Errorf("expected parse error, got %v", err)
	}
}

func TestGetBlockByNumber_ContextCancelled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		resp := jsonRPCResponse{JSONRPC: "2.0", ID: 1}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := client.GetBlockByNumber(ctx, 256, false)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// --- Test: GetBlockByHash ---

func TestGetBlockByHash_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req jsonRPCRequest
		json.NewDecoder(r.Body).Decode(&req)

		if req.Method != "eth_getBlockByHash" {
			t.Errorf("expected method=eth_getBlockByHash, got %s", req.Method)
		}

		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`{"number":"0x100","hash":"0xabc123","parentHash":"0xdef456"}`),
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	header, err := client.GetBlockByHash(ctx, "0xabc123", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if header == nil {
		t.Fatal("expected header, got nil")
	}
	if header.Number != "0x100" {
		t.Errorf("expected number=0x100, got %s", header.Number)
	}
	if header.Hash != "0xabc123" {
		t.Errorf("expected hash=0xabc123, got %s", header.Hash)
	}
}

func TestGetBlockByHash_NotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`null`),
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	_, err := client.GetBlockByHash(ctx, "0xnonexistent", false)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "block not found") {
		t.Errorf("expected 'block not found' error, got %v", err)
	}
}

func TestGetBlockByHash_InvalidBlockJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`{"number": 123}`), // number should be string
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	_, err := client.GetBlockByHash(ctx, "0xabc", false)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "failed to parse block") {
		t.Errorf("expected parse error, got %v", err)
	}
}

// --- Test: GetBlockReceipts ---

func TestGetBlockReceipts_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req jsonRPCRequest
		json.NewDecoder(r.Body).Decode(&req)

		if req.Method != "eth_getBlockReceipts" {
			t.Errorf("expected method=eth_getBlockReceipts, got %s", req.Method)
		}

		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`[{"transactionHash":"0x123","status":"0x1"}]`),
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	result, err := client.GetBlockReceipts(ctx, 256)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result == nil {
		t.Fatal("expected result, got nil")
	}

	var receipts []map[string]string
	if err := json.Unmarshal(result, &receipts); err != nil {
		t.Fatalf("failed to unmarshal result: %v", err)
	}

	if len(receipts) != 1 {
		t.Errorf("expected 1 receipt, got %d", len(receipts))
	}
}

// --- Test: GetBlockTraces ---

func TestGetBlockTraces_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req jsonRPCRequest
		json.NewDecoder(r.Body).Decode(&req)

		if req.Method != "trace_block" {
			t.Errorf("expected method=trace_block, got %s", req.Method)
		}

		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`[{"action":{"callType":"call"}}]`),
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	result, err := client.GetBlockTraces(ctx, 256)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result == nil {
		t.Fatal("expected result, got nil")
	}
}

// --- Test: GetBlobSidecars ---

func TestGetBlobSidecars_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req jsonRPCRequest
		json.NewDecoder(r.Body).Decode(&req)

		if req.Method != "eth_getBlobSidecars" {
			t.Errorf("expected method=eth_getBlobSidecars, got %s", req.Method)
		}

		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`[]`),
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	result, err := client.GetBlobSidecars(ctx, 256)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result == nil {
		t.Fatal("expected result, got nil")
	}
}

// --- Test: GetCurrentBlockNumber ---

func TestGetCurrentBlockNumber_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req jsonRPCRequest
		json.NewDecoder(r.Body).Decode(&req)

		if req.Method != "eth_blockNumber" {
			t.Errorf("expected method=eth_blockNumber, got %s", req.Method)
		}

		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`"0x1234"`),
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	blockNum, err := client.GetCurrentBlockNumber(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := int64(0x1234)
	if blockNum != expected {
		t.Errorf("expected blockNum=%d, got %d", expected, blockNum)
	}
}

func TestGetCurrentBlockNumber_InvalidResult(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`123`), // Should be string
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	_, err := client.GetCurrentBlockNumber(ctx)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "failed to parse block number") {
		t.Errorf("expected parse error, got %v", err)
	}
}

// --- Test: GetBlocksBatch ---

func TestGetBlocksBatch_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var requests []jsonRPCRequest
		json.NewDecoder(r.Body).Decode(&requests)

		// Should have 8 requests (2 blocks * 4 methods each)
		if len(requests) != 8 {
			t.Errorf("expected 8 requests, got %d", len(requests))
		}

		// Build responses
		responses := make([]jsonRPCResponse, len(requests))
		for i, req := range requests {
			responses[i] = jsonRPCResponse{
				JSONRPC: "2.0",
				ID:      req.ID,
				Result:  json.RawMessage(`{}`),
			}
		}

		json.NewEncoder(w).Encode(responses)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	results, err := client.GetBlocksBatch(ctx, []int64{100, 101}, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}

	if results[0].BlockNumber != 100 {
		t.Errorf("expected first block number=100, got %d", results[0].BlockNumber)
	}
	if results[1].BlockNumber != 101 {
		t.Errorf("expected second block number=101, got %d", results[1].BlockNumber)
	}
}

func TestGetBlocksBatch_EmptyInput(t *testing.T) {
	client := NewClient("http://localhost:8545")
	ctx := context.Background()

	results, err := client.GetBlocksBatch(ctx, []int64{}, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if results != nil {
		t.Errorf("expected nil results for empty input, got %v", results)
	}
}

func TestGetBlocksBatch_PartialErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var requests []jsonRPCRequest
		json.NewDecoder(r.Body).Decode(&requests)

		responses := make([]jsonRPCResponse, len(requests))
		for i, req := range requests {
			// Make some requests fail
			if req.ID == 1 || req.ID == 2 {
				responses[i] = jsonRPCResponse{
					JSONRPC: "2.0",
					ID:      req.ID,
					Error: &jsonRPCError{
						Code:    -32000,
						Message: "not found",
					},
				}
			} else {
				responses[i] = jsonRPCResponse{
					JSONRPC: "2.0",
					ID:      req.ID,
					Result:  json.RawMessage(`{}`),
				}
			}
		}

		json.NewEncoder(w).Encode(responses)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	results, err := client.GetBlocksBatch(ctx, []int64{100}, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should still return results, but some fields may be nil
	if len(results) != 1 {
		t.Errorf("expected 1 result, got %d", len(results))
	}

	// Block data should be present (ID 0), receipts/traces may be nil (IDs 1, 2 had errors)
	if results[0].Block == nil {
		t.Error("expected Block to be present")
	}
}

func TestGetBlocksBatch_HTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	_, err := client.GetBlocksBatch(ctx, []int64{100}, false)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestGetBlocksBatch_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("not json"))
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	_, err := client.GetBlocksBatch(ctx, []int64{100}, false)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "failed to parse batch response") {
		t.Errorf("expected parse error, got %v", err)
	}
}

// --- Test: Connection failures ---

func TestClient_ConnectionRefused(t *testing.T) {
	client := NewClient("http://localhost:19999") // Port that's not listening
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := client.GetBlockByNumber(ctx, 100, false)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "HTTP request failed") {
		t.Errorf("expected HTTP request error, got %v", err)
	}
}

func TestClient_InvalidURL(t *testing.T) {
	client := NewClient("://invalid-url")
	ctx := context.Background()

	_, err := client.GetBlockByNumber(ctx, 100, false)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

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

// testClient creates a client for testing with the given URL.
func testClient(t *testing.T, url string) *Client {
	t.Helper()
	client, err := NewClient(ClientConfig{HTTPURL: url})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	return client
}

// testClientWithBlobs creates a client for testing with traces and blobs enabled.
func testClientWithBlobs(t *testing.T, url string) *Client {
	t.Helper()
	client, err := NewClient(ClientConfig{HTTPURL: url, EnableTraces: true, EnableBlobs: true})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	return client
}

// --- Test: NewClient ---

func TestNewClient_CreatesClientWithConfig(t *testing.T) {
	url := "https://eth-mainnet.g.alchemy.com/v2/demo"
	client, err := NewClient(ClientConfig{HTTPURL: url})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if client == nil {
		t.Fatal("expected client, got nil")
	}
	if client.config.HTTPURL != url {
		t.Errorf("expected HTTPURL=%s, got %s", url, client.config.HTTPURL)
	}
	if client.httpClient == nil {
		t.Fatal("expected httpClient, got nil")
	}
	if client.httpClient.Timeout != 30*time.Second {
		t.Errorf("expected timeout=30s, got %v", client.httpClient.Timeout)
	}
}

func TestNewClient_EmptyURLReturnsError(t *testing.T) {
	_, err := NewClient(ClientConfig{})
	if err == nil {
		t.Fatal("expected error for empty URL, got nil")
	}
	if !strings.Contains(err.Error(), "HTTPURL is required") {
		t.Errorf("expected 'HTTPURL is required' error, got %v", err)
	}
}

func TestNewClient_CustomTimeout(t *testing.T) {
	client, err := NewClient(ClientConfig{
		HTTPURL: "https://example.com",
		Timeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if client.httpClient.Timeout != 10*time.Second {
		t.Errorf("expected timeout=10s, got %v", client.httpClient.Timeout)
	}
}

func TestClientConfigDefaults_ReturnsDefaults(t *testing.T) {
	defaults := ClientConfigDefaults()

	if defaults.Timeout != 30*time.Second {
		t.Errorf("expected default timeout=30s, got %v", defaults.Timeout)
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
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Errorf("failed to encode response: %v", err)
		}
	}))
	defer server.Close()

	client := testClient(t, server.URL)
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
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Errorf("failed to encode response: %v", err)
		}
	}))
	defer server.Close()

	client := testClient(t, server.URL)
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
		if _, err := w.Write([]byte("internal server error")); err != nil {
			t.Errorf("failed to write response: %v", err)
		}
	}))
	defer server.Close()

	client := testClient(t, server.URL)
	ctx := context.Background()

	_, err := client.GetBlockByNumber(ctx, 256, false)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestGetBlockByNumber_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := w.Write([]byte("not json")); err != nil {
			t.Errorf("failed to write response: %v", err)
		}
	}))
	defer server.Close()

	client := testClient(t, server.URL)
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
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Errorf("failed to encode response: %v", err)
		}
	}))
	defer server.Close()

	client := testClient(t, server.URL)
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
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("failed to decode request: %v", err)
		}

		if req.Method != "eth_getBlockByHash" {
			t.Errorf("expected method=eth_getBlockByHash, got %s", req.Method)
		}

		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`{"number":"0x100","hash":"0xabc123","parentHash":"0xdef456"}`),
		}
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Errorf("failed to encode response: %v", err)
		}
	}))
	defer server.Close()

	client := testClient(t, server.URL)
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
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Errorf("failed to encode response: %v", err)
		}
	}))
	defer server.Close()

	client := testClient(t, server.URL)
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
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Errorf("failed to encode response: %v", err)
		}
	}))
	defer server.Close()

	client := testClient(t, server.URL)
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
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("failed to decode request: %v", err)
		}

		if req.Method != "eth_getBlockReceipts" {
			t.Errorf("expected method=eth_getBlockReceipts, got %s", req.Method)
		}

		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`[{"transactionHash":"0x123","status":"0x1"}]`),
		}
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Errorf("failed to encode response: %v", err)
		}
	}))
	defer server.Close()

	client := testClient(t, server.URL)
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
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("failed to decode request: %v", err)
		}

		if req.Method != "trace_block" {
			t.Errorf("expected method=trace_block, got %s", req.Method)
		}

		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`[{"action":{"callType":"call"}}]`),
		}
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Errorf("failed to encode response: %v", err)
		}
	}))
	defer server.Close()

	client := testClient(t, server.URL)
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
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("failed to decode request: %v", err)
		}

		if req.Method != "eth_getBlobSidecars" {
			t.Errorf("expected method=eth_getBlobSidecars, got %s", req.Method)
		}

		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`[]`),
		}
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Errorf("failed to encode response: %v", err)
		}
	}))
	defer server.Close()

	client := testClient(t, server.URL)
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
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("failed to decode request: %v", err)
		}

		if req.Method != "eth_blockNumber" {
			t.Errorf("expected method=eth_blockNumber, got %s", req.Method)
		}

		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`"0x1234"`),
		}
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Errorf("failed to encode response: %v", err)
		}
	}))
	defer server.Close()

	client := testClient(t, server.URL)
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
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Errorf("failed to encode response: %v", err)
		}
	}))
	defer server.Close()

	client := testClient(t, server.URL)
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
		if err := json.NewDecoder(r.Body).Decode(&requests); err != nil {
			t.Errorf("failed to decode requests: %v", err)
		}

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

		if err := json.NewEncoder(w).Encode(responses); err != nil {
			t.Errorf("failed to encode responses: %v", err)
		}
	}))
	defer server.Close()

	client := testClientWithBlobs(t, server.URL)
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
	client := testClient(t, "http://localhost:8545")
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
		if err := json.NewDecoder(r.Body).Decode(&requests); err != nil {
			t.Errorf("failed to decode requests: %v", err)
		}

		responses := make([]jsonRPCResponse, len(requests))
		for i, req := range requests {
			// Make receipts (ID 1) and traces (ID 2) fail
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

		if err := json.NewEncoder(w).Encode(responses); err != nil {
			t.Errorf("failed to encode responses: %v", err)
		}
	}))
	defer server.Close()

	client := testClientWithBlobs(t, server.URL)
	ctx := context.Background()

	results, err := client.GetBlocksBatch(ctx, []int64{100}, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	// Block data should be present (ID 0)
	if results[0].Block == nil {
		t.Error("expected Block to be present")
	}
	if results[0].BlockErr != nil {
		t.Errorf("expected no BlockErr, got %v", results[0].BlockErr)
	}

	// Receipts should have error (ID 1)
	if results[0].Receipts != nil {
		t.Error("expected Receipts to be nil")
	}
	if results[0].ReceiptsErr == nil {
		t.Error("expected ReceiptsErr to be set")
	} else if !strings.Contains(results[0].ReceiptsErr.Error(), "not found") {
		t.Errorf("expected 'not found' in error, got %v", results[0].ReceiptsErr)
	}

	// Traces should have error (ID 2)
	if results[0].Traces != nil {
		t.Error("expected Traces to be nil")
	}
	if results[0].TracesErr == nil {
		t.Error("expected TracesErr to be set")
	}

	// Blobs should be present (ID 3)
	if results[0].Blobs == nil {
		t.Error("expected Blobs to be present")
	}
	if results[0].BlobsErr != nil {
		t.Errorf("expected no BlobsErr, got %v", results[0].BlobsErr)
	}

	// HasErrors should return true
	if !results[0].HasErrors() {
		t.Error("expected HasErrors() to return true")
	}
}

func TestGetBlocksBatch_AllSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var requests []jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&requests); err != nil {
			t.Errorf("failed to decode requests: %v", err)
		}

		responses := make([]jsonRPCResponse, len(requests))
		for i, req := range requests {
			responses[i] = jsonRPCResponse{
				JSONRPC: "2.0",
				ID:      req.ID,
				Result:  json.RawMessage(`{"data": "test"}`),
			}
		}

		if err := json.NewEncoder(w).Encode(responses); err != nil {
			t.Errorf("failed to encode responses: %v", err)
		}
	}))
	defer server.Close()

	client := testClientWithBlobs(t, server.URL)
	ctx := context.Background()

	results, err := client.GetBlocksBatch(ctx, []int64{100}, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	// All data should be present, no errors
	if results[0].Block == nil {
		t.Error("expected Block to be present")
	}
	if results[0].Receipts == nil {
		t.Error("expected Receipts to be present")
	}
	if results[0].Traces == nil {
		t.Error("expected Traces to be present")
	}
	if results[0].Blobs == nil {
		t.Error("expected Blobs to be present")
	}

	// No errors
	if results[0].HasErrors() {
		t.Errorf("expected no errors, got Block=%v Receipts=%v Traces=%v Blobs=%v",
			results[0].BlockErr, results[0].ReceiptsErr, results[0].TracesErr, results[0].BlobsErr)
	}
}

func TestGetBlocksBatch_AllErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var requests []jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&requests); err != nil {
			t.Errorf("failed to decode requests: %v", err)
		}

		responses := make([]jsonRPCResponse, len(requests))
		for i, req := range requests {
			responses[i] = jsonRPCResponse{
				JSONRPC: "2.0",
				ID:      req.ID,
				Error: &jsonRPCError{
					Code:    -32602,
					Message: "invalid block number",
				},
			}
		}

		if err := json.NewEncoder(w).Encode(responses); err != nil {
			t.Errorf("failed to encode responses: %v", err)
		}
	}))
	defer server.Close()

	client := testClientWithBlobs(t, server.URL)
	ctx := context.Background()

	results, err := client.GetBlocksBatch(ctx, []int64{999999999}, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err) // Batch itself should succeed
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	// All data should have errors
	if results[0].BlockErr == nil {
		t.Error("expected BlockErr to be set")
	}
	if results[0].ReceiptsErr == nil {
		t.Error("expected ReceiptsErr to be set")
	}
	if results[0].TracesErr == nil {
		t.Error("expected TracesErr to be set")
	}
	if results[0].BlobsErr == nil {
		t.Error("expected BlobsErr to be set")
	}

	// Errors should include the error code
	if !strings.Contains(results[0].BlockErr.Error(), "-32602") {
		t.Errorf("expected error code in message, got %v", results[0].BlockErr)
	}

	if !results[0].HasErrors() {
		t.Error("expected HasErrors() to return true")
	}
}

func TestGetBlocksBatch_HTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	client := testClient(t, server.URL)
	ctx := context.Background()

	_, err := client.GetBlocksBatch(ctx, []int64{100}, false)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestGetBlocksBatch_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := w.Write([]byte("not json")); err != nil {
			t.Errorf("failed to write response: %v", err)
		}
	}))
	defer server.Close()

	client := testClient(t, server.URL)
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
	client := testClient(t, "http://localhost:19999") // Port that's not listening
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := client.GetBlockByNumber(ctx, 100, false)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	// Error could be connection refused, context deadline, or retry exhausted
	if !strings.Contains(err.Error(), "request failed") &&
		!strings.Contains(err.Error(), "context") &&
		!strings.Contains(err.Error(), "retries") {
		t.Errorf("expected connection/timeout/retry error, got %v", err)
	}
}

func TestClient_InvalidURL(t *testing.T) {
	// Client still accepts the URL since we only validate non-empty
	// The error will occur when making the actual request
	client := testClient(t, "://invalid-url")
	ctx := context.Background()

	_, err := client.GetBlockByNumber(ctx, 100, false)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// --- Test: Retry Logic ---

func TestClient_RetriesOn5xxErrors(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`"0x100"`),
		}
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Errorf("failed to encode response: %v", err)
		}
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		HTTPURL:        server.URL,
		MaxRetries:     3,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		BackoffFactor:  2.0,
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	_, err = client.GetCurrentBlockNumber(context.Background())
	if err != nil {
		t.Fatalf("expected success after retries, got %v", err)
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

func TestClient_RetriesOn429RateLimitError(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 2 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`"0x100"`),
		}
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Errorf("failed to encode response: %v", err)
		}
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		HTTPURL:        server.URL,
		MaxRetries:     3,
		InitialBackoff: 1 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	_, err = client.GetCurrentBlockNumber(context.Background())
	if err != nil {
		t.Fatalf("expected success after retry, got %v", err)
	}
	if attempts != 2 {
		t.Errorf("expected 2 attempts, got %d", attempts)
	}
}

func TestClient_RetriesOnRPCError(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts <= 3 {
			resp := jsonRPCResponse{
				JSONRPC: "2.0",
				ID:      1,
				Error: &jsonRPCError{
					Code:    -32000,
					Message: "execution reverted",
				},
			}
			if err := json.NewEncoder(w).Encode(resp); err != nil {
				t.Errorf("failed to encode response: %v", err)
			}
			return
		}
		// Success on 4th attempt
		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`"0x100"`),
		}
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Errorf("failed to encode response: %v", err)
		}
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		HTTPURL:        server.URL,
		MaxRetries:     3,
		InitialBackoff: 1 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	_, err = client.GetCurrentBlockNumber(context.Background())
	if err != nil {
		t.Fatalf("expected success after retries, got %v", err)
	}
	if attempts != 4 { // 1 initial + 3 retries = 4 total
		t.Errorf("expected 4 attempts, got %d", attempts)
	}
}

func TestClient_RetriesOnParseError(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 2 {
			if _, err := w.Write([]byte("invalid json")); err != nil {
				t.Errorf("failed to write response: %v", err)
			}
			return
		}
		// Success on 2nd attempt
		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`"0x100"`),
		}
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Errorf("failed to encode response: %v", err)
		}
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		HTTPURL:        server.URL,
		MaxRetries:     3,
		InitialBackoff: 1 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	_, err = client.GetCurrentBlockNumber(context.Background())
	if err != nil {
		t.Fatalf("expected success after retry, got %v", err)
	}
	if attempts != 2 {
		t.Errorf("expected 2 attempts, got %d", attempts)
	}
}

func TestClient_ExhaustsRetries(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		HTTPURL:        server.URL,
		MaxRetries:     3,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	_, err = client.GetCurrentBlockNumber(context.Background())
	if err == nil {
		t.Fatal("expected error after retries exhausted")
	}
	if !strings.Contains(err.Error(), "after 3 retries") {
		t.Errorf("expected 'after 3 retries' in error, got %v", err)
	}
	if attempts != 4 { // 1 initial + 3 retries
		t.Errorf("expected 4 attempts, got %d", attempts)
	}
}

func TestClient_RespectsContextCancellation(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		HTTPURL:        server.URL,
		MaxRetries:     10,
		InitialBackoff: 100 * time.Millisecond, // Long backoff
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err = client.GetCurrentBlockNumber(ctx)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected context error")
	}
	// Should cancel quickly, not wait for all retries
	if elapsed > 200*time.Millisecond {
		t.Errorf("expected quick cancellation, took %v", elapsed)
	}
}

func TestClient_BatchRetriesOn5xxErrors(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 2 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		// Return batch response
		responses := []jsonRPCResponse{
			{JSONRPC: "2.0", ID: 0, Result: json.RawMessage(`{}`)},
			{JSONRPC: "2.0", ID: 1, Result: json.RawMessage(`{}`)},
			{JSONRPC: "2.0", ID: 2, Result: json.RawMessage(`{}`)},
			{JSONRPC: "2.0", ID: 3, Result: json.RawMessage(`{}`)},
		}
		if err := json.NewEncoder(w).Encode(responses); err != nil {
			t.Errorf("failed to encode responses: %v", err)
		}
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		HTTPURL:        server.URL,
		MaxRetries:     3,
		InitialBackoff: 1 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	_, err = client.GetBlocksBatch(context.Background(), []int64{100}, false)
	if err != nil {
		t.Fatalf("expected success after retry, got %v", err)
	}
	if attempts != 2 {
		t.Errorf("expected 2 attempts, got %d", attempts)
	}
}

func TestClientConfigDefaults_IncludesRetryConfig(t *testing.T) {
	defaults := ClientConfigDefaults()

	if defaults.MaxRetries != 3 {
		t.Errorf("expected MaxRetries=3, got %d", defaults.MaxRetries)
	}
	if defaults.InitialBackoff != 100*time.Millisecond {
		t.Errorf("expected InitialBackoff=100ms, got %v", defaults.InitialBackoff)
	}
	if defaults.MaxBackoff != 5*time.Second {
		t.Errorf("expected MaxBackoff=5s, got %v", defaults.MaxBackoff)
	}
	if defaults.BackoffFactor != 2.0 {
		t.Errorf("expected BackoffFactor=2.0, got %v", defaults.BackoffFactor)
	}
}

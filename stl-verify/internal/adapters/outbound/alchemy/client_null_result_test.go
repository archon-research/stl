package alchemy

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/pkg/rpcutil"
)

// batchTestServer wires up an httptest.Server that responds to a JSON-RPC batch
// using the per-id results provided. Missing ids respond with a generic valid result.
func batchTestServer(t *testing.T, perID map[int]json.RawMessage) *httptest.Server {
	t.Helper()
	defaultResult := json.RawMessage(`{"hash":"0xabc","timestamp":"0x67c00000"}`)
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var requests []jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&requests); err != nil {
			t.Errorf("failed to decode batch request: %v", err)
		}
		responses := make([]jsonRPCResponse, len(requests))
		for i, req := range requests {
			resp := jsonRPCResponse{JSONRPC: "2.0", ID: req.ID}
			if v, ok := perID[req.ID]; ok {
				resp.Result = v
			} else {
				resp.Result = defaultResult
			}
			responses[i] = resp
		}
		if err := json.NewEncoder(w).Encode(responses); err != nil {
			t.Errorf("failed to encode batch response: %v", err)
		}
	}))
}

// singleCallTestServer wires up an httptest.Server that responds to single
// (non-batch) JSON-RPC requests, using the per-method results provided.
func singleCallTestServer(t *testing.T, perMethod map[string]json.RawMessage) *httptest.Server {
	t.Helper()
	defaultResult := json.RawMessage(`{"hash":"0xabc","timestamp":"0x67c00000"}`)
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("failed to decode request: %v", err)
		}
		resp := jsonRPCResponse{JSONRPC: "2.0", ID: req.ID}
		if v, ok := perMethod[req.Method]; ok {
			resp.Result = v
		} else {
			resp.Result = defaultResult
		}
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Errorf("failed to encode response: %v", err)
		}
	}))
}

// ---------------------------------------------------------------------------
// Batched fetcher
// ---------------------------------------------------------------------------

func TestGetBlockDataByHash_Batched_NullBlock(t *testing.T) {
	server := batchTestServer(t, map[int]json.RawMessage{
		0: json.RawMessage("null"), // eth_getBlockByHash returns null
	})
	defer server.Close()

	client, err := NewClient(ClientConfig{HTTPURL: server.URL, EnableTraces: true, EnableBlobs: true})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	bd, err := client.GetBlockDataByHash(context.Background(), 100, "0xabc", true)
	if err != nil {
		t.Fatalf("GetBlockDataByHash returned transport error: %v", err)
	}

	if bd.BlockErr == nil {
		t.Fatalf("expected BlockErr to be set when result is null, got nil; Block=%q", string(bd.Block))
	}
	if !errors.Is(bd.BlockErr, rpcutil.ErrUpstreamNullResult) {
		t.Errorf("expected BlockErr to wrap rpcutil.ErrUpstreamNullResult, got %v", bd.BlockErr)
	}
	if !rpcutil.IsNullOrEmpty(bd.Block) {
		t.Errorf("expected Block to be empty/null after null response, got %q", string(bd.Block))
	}
	if !strings.Contains(bd.BlockErr.Error(), "eth_getBlockByHash") {
		t.Errorf("expected BlockErr to identify the RPC method, got %v", bd.BlockErr)
	}
}

func TestGetBlockDataByHash_Batched_NullReceipts(t *testing.T) {
	server := batchTestServer(t, map[int]json.RawMessage{
		1: json.RawMessage("null"), // eth_getBlockReceipts returns null
	})
	defer server.Close()

	client, err := NewClient(ClientConfig{HTTPURL: server.URL, EnableTraces: true, EnableBlobs: true})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	bd, err := client.GetBlockDataByHash(context.Background(), 100, "0xabc", true)
	if err != nil {
		t.Fatalf("GetBlockDataByHash returned transport error: %v", err)
	}
	if bd.ReceiptsErr == nil {
		t.Fatalf("expected ReceiptsErr to be set when result is null, got nil; Receipts=%q", string(bd.Receipts))
	}
	if !errors.Is(bd.ReceiptsErr, rpcutil.ErrUpstreamNullResult) {
		t.Errorf("expected ReceiptsErr to wrap rpcutil.ErrUpstreamNullResult, got %v", bd.ReceiptsErr)
	}
	if !rpcutil.IsNullOrEmpty(bd.Receipts) {
		t.Errorf("expected Receipts to be empty/null, got %q", string(bd.Receipts))
	}
	if !strings.Contains(bd.ReceiptsErr.Error(), "eth_getBlockReceipts") {
		t.Errorf("expected ReceiptsErr to identify the RPC method, got %v", bd.ReceiptsErr)
	}
	if bd.BlockErr != nil {
		t.Errorf("expected BlockErr to be nil (only receipts were null), got %v", bd.BlockErr)
	}
}

func TestGetBlockDataByHash_Batched_NullTraces(t *testing.T) {
	server := batchTestServer(t, map[int]json.RawMessage{
		2: json.RawMessage("null"), // trace_block returns null
	})
	defer server.Close()

	client, err := NewClient(ClientConfig{HTTPURL: server.URL, EnableTraces: true})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	bd, err := client.GetBlockDataByHash(context.Background(), 100, "0xabc", true)
	if err != nil {
		t.Fatalf("GetBlockDataByHash returned transport error: %v", err)
	}
	if bd.TracesErr == nil {
		t.Fatalf("expected TracesErr to be set when result is null, got nil; Traces=%q", string(bd.Traces))
	}
	if !errors.Is(bd.TracesErr, rpcutil.ErrUpstreamNullResult) {
		t.Errorf("expected TracesErr to wrap rpcutil.ErrUpstreamNullResult, got %v", bd.TracesErr)
	}
	if !rpcutil.IsNullOrEmpty(bd.Traces) {
		t.Errorf("expected Traces to be empty/null, got %q", string(bd.Traces))
	}
	if !strings.Contains(bd.TracesErr.Error(), "trace_block") {
		t.Errorf("expected TracesErr to identify the RPC method, got %v", bd.TracesErr)
	}
}

func TestGetBlockDataByHash_Batched_NullBlobs(t *testing.T) {
	server := batchTestServer(t, map[int]json.RawMessage{
		3: json.RawMessage("null"), // eth_getBlobSidecars returns null
	})
	defer server.Close()

	client, err := NewClient(ClientConfig{HTTPURL: server.URL, EnableTraces: true, EnableBlobs: true})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	bd, err := client.GetBlockDataByHash(context.Background(), 100, "0xabc", true)
	if err != nil {
		t.Fatalf("GetBlockDataByHash returned transport error: %v", err)
	}
	if bd.BlobsErr == nil {
		t.Fatalf("expected BlobsErr to be set when result is null, got nil; Blobs=%q", string(bd.Blobs))
	}
	if !errors.Is(bd.BlobsErr, rpcutil.ErrUpstreamNullResult) {
		t.Errorf("expected BlobsErr to wrap rpcutil.ErrUpstreamNullResult, got %v", bd.BlobsErr)
	}
	if !rpcutil.IsNullOrEmpty(bd.Blobs) {
		t.Errorf("expected Blobs to be empty/null, got %q", string(bd.Blobs))
	}
	if !strings.Contains(bd.BlobsErr.Error(), "eth_getBlobSidecars") {
		t.Errorf("expected BlobsErr to identify the RPC method, got %v", bd.BlobsErr)
	}
}

// ---------------------------------------------------------------------------
// Parallel fetcher
// ---------------------------------------------------------------------------

func TestGetBlockDataByHash_Parallel_NullBlock(t *testing.T) {
	server := singleCallTestServer(t, map[string]json.RawMessage{
		"eth_getBlockByHash": json.RawMessage("null"),
	})
	defer server.Close()

	client, err := NewClient(ClientConfig{HTTPURL: server.URL, ParallelRPC: true, EnableTraces: true, EnableBlobs: true})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	bd, err := client.GetBlockDataByHash(context.Background(), 100, "0xabc", true)
	if err != nil {
		t.Fatalf("GetBlockDataByHash returned transport error: %v", err)
	}
	if bd.BlockErr == nil {
		t.Fatalf("expected BlockErr to be set when result is null, got nil; Block=%q", string(bd.Block))
	}
	if !errors.Is(bd.BlockErr, rpcutil.ErrUpstreamNullResult) {
		t.Errorf("expected BlockErr to wrap rpcutil.ErrUpstreamNullResult, got %v", bd.BlockErr)
	}
	if !rpcutil.IsNullOrEmpty(bd.Block) {
		t.Errorf("expected Block to be empty/null after null response, got %q", string(bd.Block))
	}
	if !strings.Contains(bd.BlockErr.Error(), "eth_getBlockByHash") {
		t.Errorf("expected BlockErr to identify the RPC method, got %v", bd.BlockErr)
	}
}

func TestGetBlockDataByHash_Parallel_NullReceipts(t *testing.T) {
	server := singleCallTestServer(t, map[string]json.RawMessage{
		"eth_getBlockReceipts": json.RawMessage("null"),
	})
	defer server.Close()

	client, err := NewClient(ClientConfig{HTTPURL: server.URL, ParallelRPC: true})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	bd, err := client.GetBlockDataByHash(context.Background(), 100, "0xabc", true)
	if err != nil {
		t.Fatalf("GetBlockDataByHash returned transport error: %v", err)
	}
	if bd.ReceiptsErr == nil {
		t.Fatalf("expected ReceiptsErr to be set when result is null, got nil; Receipts=%q", string(bd.Receipts))
	}
	if !errors.Is(bd.ReceiptsErr, rpcutil.ErrUpstreamNullResult) {
		t.Errorf("expected ReceiptsErr to wrap rpcutil.ErrUpstreamNullResult, got %v", bd.ReceiptsErr)
	}
	if bd.BlockErr != nil {
		t.Errorf("expected BlockErr to be nil (only receipts were null), got %v", bd.BlockErr)
	}
}

func TestGetBlockDataByHash_Parallel_NullTraces(t *testing.T) {
	server := singleCallTestServer(t, map[string]json.RawMessage{
		"trace_block": json.RawMessage("null"),
	})
	defer server.Close()

	client, err := NewClient(ClientConfig{HTTPURL: server.URL, ParallelRPC: true, EnableTraces: true})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	bd, err := client.GetBlockDataByHash(context.Background(), 100, "0xabc", true)
	if err != nil {
		t.Fatalf("GetBlockDataByHash returned transport error: %v", err)
	}
	if bd.TracesErr == nil {
		t.Fatalf("expected TracesErr to be set when result is null, got nil; Traces=%q", string(bd.Traces))
	}
	if !errors.Is(bd.TracesErr, rpcutil.ErrUpstreamNullResult) {
		t.Errorf("expected TracesErr to wrap rpcutil.ErrUpstreamNullResult, got %v", bd.TracesErr)
	}
}

func TestGetBlockDataByHash_Parallel_NullBlobs(t *testing.T) {
	server := singleCallTestServer(t, map[string]json.RawMessage{
		"eth_getBlobSidecars": json.RawMessage("null"),
	})
	defer server.Close()

	client, err := NewClient(ClientConfig{HTTPURL: server.URL, ParallelRPC: true, EnableBlobs: true})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	bd, err := client.GetBlockDataByHash(context.Background(), 100, "0xabc", true)
	if err != nil {
		t.Fatalf("GetBlockDataByHash returned transport error: %v", err)
	}
	if bd.BlobsErr == nil {
		t.Fatalf("expected BlobsErr to be set when result is null, got nil; Blobs=%q", string(bd.Blobs))
	}
	if !errors.Is(bd.BlobsErr, rpcutil.ErrUpstreamNullResult) {
		t.Errorf("expected BlobsErr to wrap rpcutil.ErrUpstreamNullResult, got %v", bd.BlobsErr)
	}
}

// ---------------------------------------------------------------------------
// Sanity: valid responses do not trigger the null guard
// ---------------------------------------------------------------------------

func TestGetBlockDataByHash_Batched_ValidResponseUnaffected(t *testing.T) {
	server := batchTestServer(t, nil)
	defer server.Close()

	client, err := NewClient(ClientConfig{HTTPURL: server.URL})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	bd, err := client.GetBlockDataByHash(context.Background(), 100, "0xabc", true)
	if err != nil {
		t.Fatalf("GetBlockDataByHash returned transport error: %v", err)
	}
	if bd.BlockErr != nil || bd.ReceiptsErr != nil {
		t.Errorf("expected no errors for valid responses, got block=%v receipts=%v", bd.BlockErr, bd.ReceiptsErr)
	}
	if rpcutil.IsNullOrEmpty(bd.Block) || rpcutil.IsNullOrEmpty(bd.Receipts) {
		t.Errorf("expected populated Block/Receipts, got block=%q receipts=%q", string(bd.Block), string(bd.Receipts))
	}
}

func TestGetBlockDataByHash_Parallel_ValidResponseUnaffected(t *testing.T) {
	server := singleCallTestServer(t, nil)
	defer server.Close()

	client, err := NewClient(ClientConfig{HTTPURL: server.URL, ParallelRPC: true})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	bd, err := client.GetBlockDataByHash(context.Background(), 100, "0xabc", true)
	if err != nil {
		t.Fatalf("GetBlockDataByHash returned transport error: %v", err)
	}
	if bd.BlockErr != nil || bd.ReceiptsErr != nil {
		t.Errorf("expected no errors for valid responses, got block=%v receipts=%v", bd.BlockErr, bd.ReceiptsErr)
	}
	if rpcutil.IsNullOrEmpty(bd.Block) || rpcutil.IsNullOrEmpty(bd.Receipts) {
		t.Errorf("expected populated Block/Receipts, got block=%q receipts=%q", string(bd.Block), string(bd.Receipts))
	}
}

// ---------------------------------------------------------------------------
// GetBlocksBatch / GetBlocksAndReceiptsBatch / GetTracesBatch
// (the by-number bulk paths reached by BackfillService.processBatch)
// ---------------------------------------------------------------------------

func TestGetBlocksBatch_NullBlock(t *testing.T) {
	// ID layout for GetBlocksBatch is i*4 + offset, where offset is
	// 0=block, 1=receipts, 2=traces, 3=blobs. Only block 0's eth_getBlockByNumber
	// (ID 0) is set to null here.
	server := batchTestServer(t, map[int]json.RawMessage{
		0: json.RawMessage("null"),
	})
	defer server.Close()

	client, err := NewClient(ClientConfig{HTTPURL: server.URL})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	results, err := client.GetBlocksBatch(context.Background(), []int64{100}, false)
	if err != nil {
		t.Fatalf("GetBlocksBatch returned transport error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].BlockErr == nil {
		t.Fatalf("expected BlockErr to be set, got nil; Block=%q", string(results[0].Block))
	}
	if !errors.Is(results[0].BlockErr, rpcutil.ErrUpstreamNullResult) {
		t.Errorf("expected BlockErr to wrap rpcutil.ErrUpstreamNullResult, got %v", results[0].BlockErr)
	}
	if !rpcutil.IsNullOrEmpty(results[0].Block) {
		t.Errorf("expected Block to be empty/null, got %q", string(results[0].Block))
	}
	if !strings.Contains(results[0].BlockErr.Error(), "eth_getBlockByNumber") {
		t.Errorf("expected error to identify the RPC method, got %v", results[0].BlockErr)
	}
}

func TestGetBlocksBatch_NullReceipts(t *testing.T) {
	server := batchTestServer(t, map[int]json.RawMessage{
		1: json.RawMessage("null"), // receipts of block 0
	})
	defer server.Close()

	client, err := NewClient(ClientConfig{HTTPURL: server.URL})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	results, err := client.GetBlocksBatch(context.Background(), []int64{100}, false)
	if err != nil {
		t.Fatalf("GetBlocksBatch returned transport error: %v", err)
	}
	if !errors.Is(results[0].ReceiptsErr, rpcutil.ErrUpstreamNullResult) {
		t.Errorf("expected ReceiptsErr to wrap rpcutil.ErrUpstreamNullResult, got %v", results[0].ReceiptsErr)
	}
	if results[0].BlockErr != nil {
		t.Errorf("expected BlockErr to be nil (only receipts were null), got %v", results[0].BlockErr)
	}
}

func TestGetBlocksAndReceiptsBatch_NullBlock(t *testing.T) {
	// IDs are i*2 for block, i*2+1 for receipts.
	server := batchTestServer(t, map[int]json.RawMessage{
		0: json.RawMessage("null"),
	})
	defer server.Close()

	client, err := NewClient(ClientConfig{HTTPURL: server.URL})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	results, err := client.GetBlocksAndReceiptsBatch(context.Background(), []int64{100}, false)
	if err != nil {
		t.Fatalf("transport error: %v", err)
	}
	if !errors.Is(results[0].BlockErr, rpcutil.ErrUpstreamNullResult) {
		t.Errorf("expected BlockErr to wrap rpcutil.ErrUpstreamNullResult, got %v", results[0].BlockErr)
	}
}

func TestGetTracesBatch_NullTrace(t *testing.T) {
	server := batchTestServer(t, map[int]json.RawMessage{
		0: json.RawMessage("null"),
	})
	defer server.Close()

	client, err := NewClient(ClientConfig{HTTPURL: server.URL})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	traces, errs := client.GetTracesBatch(context.Background(), []int64{100})
	if traces[100] != nil {
		t.Errorf("expected traces[100] to be nil, got %q", string(traces[100]))
	}
	if !errors.Is(errs[100], rpcutil.ErrUpstreamNullResult) {
		t.Errorf("expected errs[100] to wrap rpcutil.ErrUpstreamNullResult, got %v", errs[100])
	}
}

// ---------------------------------------------------------------------------
// Single-call methods now consistently surface rpcutil.ErrUpstreamNullResult
// ---------------------------------------------------------------------------

func nullResultServer(t *testing.T) *httptest.Server {
	t.Helper()
	return singleCallTestServer(t, map[string]json.RawMessage{
		"eth_getBlockByNumber": json.RawMessage("null"),
		"eth_getBlockByHash":   json.RawMessage("null"),
		"eth_getBlockReceipts": json.RawMessage("null"),
		"trace_block":          json.RawMessage("null"),
		"eth_getBlobSidecars":  json.RawMessage("null"),
	})
}

func TestGetBlockByNumber_NullResult(t *testing.T) {
	server := nullResultServer(t)
	defer server.Close()
	client, err := NewClient(ClientConfig{HTTPURL: server.URL})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	raw, err := client.GetBlockByNumber(context.Background(), 100, false)
	if raw != nil {
		t.Errorf("expected nil result, got %q", string(raw))
	}
	if !errors.Is(err, rpcutil.ErrUpstreamNullResult) {
		t.Errorf("expected rpcutil.ErrUpstreamNullResult, got %v", err)
	}
}

func TestGetFullBlockByHash_NullResult(t *testing.T) {
	server := nullResultServer(t)
	defer server.Close()
	client, err := NewClient(ClientConfig{HTTPURL: server.URL})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	raw, err := client.GetFullBlockByHash(context.Background(), "0xabc", false)
	if raw != nil {
		t.Errorf("expected nil result, got %q", string(raw))
	}
	if !errors.Is(err, rpcutil.ErrUpstreamNullResult) {
		t.Errorf("expected rpcutil.ErrUpstreamNullResult, got %v", err)
	}
}

func TestGetBlockReceipts_NullResult(t *testing.T) {
	server := nullResultServer(t)
	defer server.Close()
	client, err := NewClient(ClientConfig{HTTPURL: server.URL})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if _, err := client.GetBlockReceipts(context.Background(), 100); !errors.Is(err, rpcutil.ErrUpstreamNullResult) {
		t.Errorf("expected rpcutil.ErrUpstreamNullResult, got %v", err)
	}
	if _, err := client.GetBlockReceiptsByHash(context.Background(), "0xabc"); !errors.Is(err, rpcutil.ErrUpstreamNullResult) {
		t.Errorf("expected rpcutil.ErrUpstreamNullResult from ByHash variant, got %v", err)
	}
}

func TestGetBlockTraces_NullResult(t *testing.T) {
	server := nullResultServer(t)
	defer server.Close()
	client, err := NewClient(ClientConfig{HTTPURL: server.URL})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if _, err := client.GetBlockTraces(context.Background(), 100); !errors.Is(err, rpcutil.ErrUpstreamNullResult) {
		t.Errorf("expected rpcutil.ErrUpstreamNullResult, got %v", err)
	}
	if _, err := client.GetBlockTracesByHash(context.Background(), "0xabc"); !errors.Is(err, rpcutil.ErrUpstreamNullResult) {
		t.Errorf("expected rpcutil.ErrUpstreamNullResult from ByHash variant, got %v", err)
	}
}

func TestGetBlobSidecars_NullResult(t *testing.T) {
	server := nullResultServer(t)
	defer server.Close()
	client, err := NewClient(ClientConfig{HTTPURL: server.URL})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if _, err := client.GetBlobSidecars(context.Background(), 100); !errors.Is(err, rpcutil.ErrUpstreamNullResult) {
		t.Errorf("expected rpcutil.ErrUpstreamNullResult, got %v", err)
	}
	if _, err := client.GetBlobSidecarsByHash(context.Background(), "0xabc"); !errors.Is(err, rpcutil.ErrUpstreamNullResult) {
		t.Errorf("expected rpcutil.ErrUpstreamNullResult from ByHash variant, got %v", err)
	}
}

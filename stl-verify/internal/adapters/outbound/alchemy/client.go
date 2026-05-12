// client.go implements the BlockchainClient interface using Alchemy's HTTP JSON-RPC API.
// It provides methods for fetching blocks, receipts, traces, and blob sidecars with:
//   - Automatic retry logic with exponential backoff for transient failures
//   - Configurable timeouts and backoff parameters
//   - OpenTelemetry instrumentation for metrics and tracing
//   - Batch request support for efficient bulk data fetching
package alchemy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/hexutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/rpcutil"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/trace"
)

// Compile-time check that Client implements outbound.BlockchainClient
var _ outbound.BlockchainClient = (*Client)(nil)

// extractResult decodes a single JSON-RPC response into its raw bytes or an
// error appropriate for further wrapping by the caller. It captures the
// uniform handling pattern used by every multi-call fetcher in this file:
//
//   - transport error (non-nil callErr)  → propagated as-is
//   - missing response (resp == nil)     → "missing response for <method> <subject>"
//   - RPC error returned by the server   → "<method> <subject>: <rpcError>"
//   - literal JSON null result           → wrapped [rpcutil.ErrUpstreamNullResult]
//   - otherwise                          → the raw result bytes
//
// `subject` is whatever uniquely identifies the call to the operator reading
// logs — a block hash for ByHash variants, a decimal block number for
// ByNumber variants. The wrapped error preserves the underlying cause so
// callers can use errors.Is / errors.Unwrap.
func extractResult(resp *jsonRPCResponse, callErr error, method, subject string) (json.RawMessage, error) {
	if callErr != nil {
		return nil, callErr
	}
	if resp == nil {
		return nil, fmt.Errorf("missing response for %s %s", method, subject)
	}
	if resp.Error != nil {
		return nil, fmt.Errorf("%s %s: %w", method, subject, resp.Error)
	}
	if rpcutil.IsNullOrEmpty(resp.Result) {
		return nil, fmt.Errorf("%s %s: %w", method, subject, rpcutil.ErrUpstreamNullResult)
	}
	return resp.Result, nil
}

// ClientConfig holds configuration for the HTTP RPC client.
type ClientConfig struct {
	// HTTPURL is the Alchemy HTTP JSON-RPC endpoint URL.
	HTTPURL string

	// Timeout is the maximum time to wait for a single HTTP request.
	Timeout time.Duration

	// MaxRetries is the maximum number of retry attempts for transient failures.
	// Set to 0 to disable retries.
	MaxRetries int

	// InitialBackoff is the initial delay before the first retry.
	InitialBackoff time.Duration

	// MaxBackoff is the maximum delay between retries.
	MaxBackoff time.Duration

	// BackoffFactor is the multiplier applied to backoff after each retry.
	BackoffFactor float64

	// EnableTraces enables fetching execution traces (trace_block on supported nodes).
	EnableTraces bool

	// EnableBlobs enables fetching blob sidecars (post-Dencun blocks on supported nodes).
	EnableBlobs bool

	// ParallelRPC uses separate goroutines for each RPC call instead of batching.
	// This uses more credits but may be faster due to parallel execution.
	ParallelRPC bool

	// Logger is the structured logger for the client.
	Logger *slog.Logger

	// Telemetry is the optional OpenTelemetry instrumentation.
	// If nil, no metrics or traces are recorded.
	Telemetry *Telemetry

	// HTTPClient is an optional custom HTTP client.
	// If nil, a default client with OpenTelemetry instrumentation is used.
	// Use this to configure custom transport settings like connection pooling.
	HTTPClient *http.Client
}

// ClientConfigDefaults returns a config with default values.
func ClientConfigDefaults() ClientConfig {
	return ClientConfig{
		Timeout:        30 * time.Second,
		MaxRetries:     3,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     5 * time.Second,
		BackoffFactor:  2.0,
		Logger:         slog.Default(),
	}
}

// Client implements BlockchainClient using Alchemy's HTTP JSON-RPC API.
type Client struct {
	config     ClientConfig
	httpClient *http.Client
	logger     *slog.Logger
	telemetry  *Telemetry
}

// NewClient creates a new Alchemy HTTP RPC client.
func NewClient(config ClientConfig) (*Client, error) {
	if config.HTTPURL == "" {
		return nil, errors.New("HTTPURL is required")
	}

	// Apply defaults for zero values
	defaults := ClientConfigDefaults()
	if config.Timeout == 0 {
		config.Timeout = defaults.Timeout
	}
	if config.MaxRetries == 0 {
		config.MaxRetries = defaults.MaxRetries
	}
	if config.InitialBackoff == 0 {
		config.InitialBackoff = defaults.InitialBackoff
	}
	if config.MaxBackoff == 0 {
		config.MaxBackoff = defaults.MaxBackoff
	}
	if config.BackoffFactor == 0 {
		config.BackoffFactor = defaults.BackoffFactor
	}
	if config.Logger == nil {
		config.Logger = defaults.Logger
	}

	// Use provided HTTP client or create a default one
	httpClient := config.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: config.Timeout,
			Transport: otelhttp.NewTransport(http.DefaultTransport,
				otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
					return "alchemy.http"
				}),
			),
		}
	}

	return &Client{
		config:     config,
		httpClient: httpClient,
		logger:     config.Logger.With("component", "alchemy-client"),
		telemetry:  config.Telemetry,
	}, nil
}

// callSingle dispatches a single JSON-RPC request and decodes the result
// through extractResult, so every single-call fetcher gets the same VEC-242
// null-result handling as the multi-call ones. The returned error wraps
// [rpcutil.ErrUpstreamNullResult] when the upstream replied with literal JSON null.
func (c *Client) callSingle(ctx context.Context, method, subject string, params []any) (json.RawMessage, error) {
	req := jsonRPCRequest{JSONRPC: "2.0", ID: 1, Method: method, Params: params}
	resp, err := c.call(ctx, req)
	return extractResult(resp, err, method, subject)
}

// GetBlockByNumber fetches a block by its number. A literal JSON null response
// surfaces as a wrapped [rpcutil.ErrUpstreamNullResult] (e.g. transient propagation
// races near the chain tip).
func (c *Client) GetBlockByNumber(ctx context.Context, blockNum int64, fullTx bool) (json.RawMessage, error) {
	hexNum := fmt.Sprintf("0x%x", blockNum)
	return c.callSingle(ctx, "eth_getBlockByNumber", strconv.FormatInt(blockNum, 10), []any{hexNum, fullTx})
}

// GetBlockByHash fetches a block header by its hash. A literal JSON null
// response surfaces as a wrapped [rpcutil.ErrUpstreamNullResult].
func (c *Client) GetBlockByHash(ctx context.Context, hash string, fullTx bool) (*outbound.BlockHeader, error) {
	raw, err := c.callSingle(ctx, "eth_getBlockByHash", hash, []any{hash, fullTx})
	if err != nil {
		return nil, err
	}
	var header outbound.BlockHeader
	if err := json.Unmarshal(raw, &header); err != nil {
		return nil, fmt.Errorf("failed to parse block: %w", err)
	}
	return &header, nil
}

// GetFullBlockByHash fetches full block JSON by hash. A literal JSON null
// response surfaces as a wrapped [rpcutil.ErrUpstreamNullResult].
func (c *Client) GetFullBlockByHash(ctx context.Context, hash string, fullTx bool) (json.RawMessage, error) {
	return c.callSingle(ctx, "eth_getBlockByHash", hash, []any{hash, fullTx})
}

// GetBlockReceipts fetches all transaction receipts for a block by number.
func (c *Client) GetBlockReceipts(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	hexNum := fmt.Sprintf("0x%x", blockNum)
	return c.callSingle(ctx, "eth_getBlockReceipts", strconv.FormatInt(blockNum, 10), []any{hexNum})
}

// GetBlockReceiptsByHash fetches all transaction receipts for a block by hash.
// Use this to prevent TOCTOU race conditions during reorgs.
func (c *Client) GetBlockReceiptsByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	return c.callSingle(ctx, "eth_getBlockReceipts", hash, []any{hash})
}

// GetBlockTraces fetches execution traces for a block by number.
func (c *Client) GetBlockTraces(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	hexNum := fmt.Sprintf("0x%x", blockNum)
	return c.callSingle(ctx, "trace_block", strconv.FormatInt(blockNum, 10), []any{hexNum})
}

// GetBlockTracesByHash fetches execution traces for a block by hash.
// Use this to prevent TOCTOU race conditions during reorgs.
func (c *Client) GetBlockTracesByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	return c.callSingle(ctx, "trace_block", hash, []any{hash})
}

// GetBlobSidecars fetches blob sidecars for a block by number.
func (c *Client) GetBlobSidecars(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	hexNum := fmt.Sprintf("0x%x", blockNum)
	return c.callSingle(ctx, "eth_getBlobSidecars", strconv.FormatInt(blockNum, 10), []any{hexNum})
}

// GetBlobSidecarsByHash fetches blob sidecars for a block by hash.
// Use this to prevent TOCTOU race conditions during reorgs.
func (c *Client) GetBlobSidecarsByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	return c.callSingle(ctx, "eth_getBlobSidecars", hash, []any{hash})
}

// GetBlockDataByHash fetches all data for a single block by hash.
// This is TOCTOU-safe - fetching by hash ensures we get data for the exact block we want.
// When ParallelRPC is enabled, each RPC call runs in a separate goroutine for lower latency.
// Otherwise, all calls are batched into a single HTTP request.
func (c *Client) GetBlockDataByHash(ctx context.Context, blockNum int64, hash string, fullTx bool) (outbound.BlockData, error) {
	if c.config.ParallelRPC {
		return c.getBlockDataByHashParallel(ctx, blockNum, hash, fullTx)
	}
	return c.getBlockDataByHashBatched(ctx, blockNum, hash, fullTx)
}

// getBlockDataByHashParallel fetches block data using parallel goroutines for each RPC call.
// This uses more API credits but may be faster due to parallel network requests.
func (c *Client) getBlockDataByHashParallel(ctx context.Context, blockNum int64, hash string, fullTx bool) (outbound.BlockData, error) {
	// Create a parent span for the parallel fetch operation
	// This ensures all child RPC spans are properly linked
	var span trace.Span
	if c.telemetry != nil {
		ctx, span = c.telemetry.StartSpan(ctx, "parallel_fetch")
		defer span.End()
	}

	result := outbound.BlockData{BlockNumber: blockNum}

	var wg sync.WaitGroup
	var mu sync.Mutex

	// Fetch block
	wg.Go(func() {
		req := jsonRPCRequest{JSONRPC: "2.0", ID: 0, Method: "eth_getBlockByHash", Params: []any{hash, fullTx}}
		resp, err := c.call(ctx, req)
		raw, err := extractResult(resp, err, req.Method, hash)
		mu.Lock()
		result.Block, result.BlockErr = raw, err
		mu.Unlock()
	})

	// Fetch receipts
	wg.Go(func() {
		req := jsonRPCRequest{JSONRPC: "2.0", ID: 1, Method: "eth_getBlockReceipts", Params: []any{hash}}
		resp, err := c.call(ctx, req)
		raw, err := extractResult(resp, err, req.Method, hash)
		mu.Lock()
		result.Receipts, result.ReceiptsErr = raw, err
		mu.Unlock()
	})

	// Fetch traces (only if enabled)
	if c.config.EnableTraces {
		wg.Go(func() {
			req := jsonRPCRequest{JSONRPC: "2.0", ID: 2, Method: "trace_block", Params: []any{hash}}
			resp, err := c.call(ctx, req)
			raw, err := extractResult(resp, err, req.Method, hash)
			mu.Lock()
			result.Traces, result.TracesErr = raw, err
			mu.Unlock()
		})
	}

	// Fetch blobs (only if enabled)
	if c.config.EnableBlobs {
		wg.Go(func() {
			req := jsonRPCRequest{JSONRPC: "2.0", ID: 3, Method: "eth_getBlobSidecars", Params: []any{hash}}
			resp, err := c.call(ctx, req)
			raw, err := extractResult(resp, err, req.Method, hash)
			mu.Lock()
			result.Blobs, result.BlobsErr = raw, err
			mu.Unlock()
		})
	}

	wg.Wait()
	return result, nil
}

// getBlockDataByHashBatched fetches block data using a single batched RPC call.
// This uses fewer API credits but all calls share a single HTTP round-trip.
func (c *Client) getBlockDataByHashBatched(ctx context.Context, blockNum int64, hash string, fullTx bool) (outbound.BlockData, error) {
	// Build batch request: 2-4 calls (block, receipts, optionally traces, optionally blobs)
	callsCount := 2
	if c.config.EnableTraces {
		callsCount++
	}
	if c.config.EnableBlobs {
		callsCount++
	}
	requests := make([]jsonRPCRequest, 0, callsCount)

	requests = append(requests,
		jsonRPCRequest{JSONRPC: "2.0", ID: 0, Method: "eth_getBlockByHash", Params: []any{hash, fullTx}},
		jsonRPCRequest{JSONRPC: "2.0", ID: 1, Method: "eth_getBlockReceipts", Params: []any{hash}},
	)
	if c.config.EnableTraces {
		requests = append(requests,
			jsonRPCRequest{JSONRPC: "2.0", ID: 2, Method: "trace_block", Params: []any{hash}},
		)
	}
	if c.config.EnableBlobs {
		requests = append(requests,
			jsonRPCRequest{JSONRPC: "2.0", ID: 3, Method: "eth_getBlobSidecars", Params: []any{hash}},
		)
	}

	responses, err := c.callBatch(ctx, requests)
	if err != nil {
		return outbound.BlockData{}, err
	}

	// Build a map of ID -> response for easy lookup
	respMap := make(map[int]*jsonRPCResponse, len(responses))
	for i := range responses {
		respMap[responses[i].ID] = &responses[i]
	}

	// Assemble result. extractResult covers all four error shapes (missing
	// response, RPC error, JSON null, transport error) uniformly.
	result := outbound.BlockData{BlockNumber: blockNum}
	result.Block, result.BlockErr = extractResult(respMap[0], nil, "eth_getBlockByHash", hash)
	result.Receipts, result.ReceiptsErr = extractResult(respMap[1], nil, "eth_getBlockReceipts", hash)
	if c.config.EnableTraces {
		result.Traces, result.TracesErr = extractResult(respMap[2], nil, "trace_block", hash)
	}
	if c.config.EnableBlobs {
		result.Blobs, result.BlobsErr = extractResult(respMap[3], nil, "eth_getBlobSidecars", hash)
	}

	return result, nil
}

// GetCurrentBlockNumber fetches the latest block number.
func (c *Client) GetCurrentBlockNumber(ctx context.Context) (int64, error) {
	req := jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "eth_blockNumber",
		Params:  []any{},
	}

	resp, err := c.call(ctx, req)
	if err != nil {
		return 0, err
	}

	var result string
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		return 0, fmt.Errorf("failed to parse block number: %w", err)
	}

	return hexutil.ParseInt64(result)
}

// GetBlocksBatch fetches all data for multiple blocks in a single batched RPC call.
func (c *Client) GetBlocksBatch(ctx context.Context, blockNums []int64, fullTx bool) ([]outbound.BlockData, error) {
	if len(blockNums) == 0 {
		return nil, nil
	}

	// Build batch request: 2-4 calls per block (block, receipts, optionally traces, optionally blobs)
	callsPerBlock := 2
	if c.config.EnableTraces {
		callsPerBlock++
	}
	if c.config.EnableBlobs {
		callsPerBlock++
	}
	requests := make([]jsonRPCRequest, 0, len(blockNums)*callsPerBlock)
	for i, blockNum := range blockNums {
		hexNum := fmt.Sprintf("0x%x", blockNum)
		baseID := i * 4 // Keep consistent IDs for response mapping

		requests = append(requests,
			jsonRPCRequest{JSONRPC: "2.0", ID: baseID, Method: "eth_getBlockByNumber", Params: []any{hexNum, fullTx}},
			jsonRPCRequest{JSONRPC: "2.0", ID: baseID + 1, Method: "eth_getBlockReceipts", Params: []any{hexNum}},
		)
		if c.config.EnableTraces {
			requests = append(requests,
				jsonRPCRequest{JSONRPC: "2.0", ID: baseID + 2, Method: "trace_block", Params: []any{hexNum}},
			)
		}
		if c.config.EnableBlobs {
			requests = append(requests,
				jsonRPCRequest{JSONRPC: "2.0", ID: baseID + 3, Method: "eth_getBlobSidecars", Params: []any{hexNum}},
			)
		}
	}

	responses, err := c.callBatch(ctx, requests)
	if err != nil {
		return nil, err
	}

	// Build a map of ID -> response for easy lookup
	respMap := make(map[int]*jsonRPCResponse, len(responses))
	for i := range responses {
		respMap[responses[i].ID] = &responses[i]
	}

	// Assemble results. extractResult applies the null/RPC-error/missing-response
	// decoding uniformly so the gap-fill path enjoys the same VEC-242 protection
	// as the by-hash path.
	results := make([]outbound.BlockData, len(blockNums))
	for i, blockNum := range blockNums {
		baseID := i * 4
		subject := strconv.FormatInt(blockNum, 10)
		results[i] = outbound.BlockData{BlockNumber: blockNum}
		results[i].Block, results[i].BlockErr = extractResult(respMap[baseID], nil, "eth_getBlockByNumber", subject)
		results[i].Receipts, results[i].ReceiptsErr = extractResult(respMap[baseID+1], nil, "eth_getBlockReceipts", subject)
		if c.config.EnableTraces {
			results[i].Traces, results[i].TracesErr = extractResult(respMap[baseID+2], nil, "trace_block", subject)
		}
		if c.config.EnableBlobs {
			results[i].Blobs, results[i].BlobsErr = extractResult(respMap[baseID+3], nil, "eth_getBlobSidecars", subject)
		}
	}

	return results, nil
}

// GetBlocksAndReceiptsBatch fetches blocks and receipts (without traces) for multiple blocks.
// This is the "fast phase" of two-phase bulk downloading - blocks and receipts are cheap queries.
// Use this when you want to fetch traces separately with GetTracesBatch.
func (c *Client) GetBlocksAndReceiptsBatch(ctx context.Context, blockNums []int64, fullTx bool) ([]outbound.BlockData, error) {
	if len(blockNums) == 0 {
		return nil, nil
	}

	// Build batch request: 2 calls per block (block, receipts)
	requests := make([]jsonRPCRequest, 0, len(blockNums)*2)
	for i, blockNum := range blockNums {
		hexNum := fmt.Sprintf("0x%x", blockNum)
		baseID := i * 2

		requests = append(requests,
			jsonRPCRequest{JSONRPC: "2.0", ID: baseID, Method: "eth_getBlockByNumber", Params: []any{hexNum, fullTx}},
			jsonRPCRequest{JSONRPC: "2.0", ID: baseID + 1, Method: "eth_getBlockReceipts", Params: []any{hexNum}},
		)
	}

	responses, err := c.callBatch(ctx, requests)
	if err != nil {
		return nil, err
	}

	// Build a map of ID -> response for easy lookup
	respMap := make(map[int]*jsonRPCResponse, len(responses))
	for i := range responses {
		respMap[responses[i].ID] = &responses[i]
	}

	// Assemble results.
	results := make([]outbound.BlockData, len(blockNums))
	for i, blockNum := range blockNums {
		baseID := i * 2
		subject := strconv.FormatInt(blockNum, 10)
		results[i] = outbound.BlockData{BlockNumber: blockNum}
		results[i].Block, results[i].BlockErr = extractResult(respMap[baseID], nil, "eth_getBlockByNumber", subject)
		results[i].Receipts, results[i].ReceiptsErr = extractResult(respMap[baseID+1], nil, "eth_getBlockReceipts", subject)
	}

	return results, nil
}

// GetTracesBatch fetches traces for multiple blocks in a single batched RPC call.
// This is the "slow phase" of two-phase bulk downloading - traces re-execute all transactions.
// Returns a map of block number -> trace data and a map of block number -> error.
func (c *Client) GetTracesBatch(ctx context.Context, blockNums []int64) (map[int64]json.RawMessage, map[int64]error) {
	if len(blockNums) == 0 {
		return nil, nil
	}

	requests := make([]jsonRPCRequest, len(blockNums))
	for i, blockNum := range blockNums {
		hexNum := fmt.Sprintf("0x%x", blockNum)
		requests[i] = jsonRPCRequest{JSONRPC: "2.0", ID: i, Method: "trace_block", Params: []any{hexNum}}
	}

	responses, err := c.callBatch(ctx, requests)
	if err != nil {
		// Return error for all blocks
		errs := make(map[int64]error, len(blockNums))
		for _, blockNum := range blockNums {
			errs[blockNum] = err
		}
		return nil, errs
	}

	// Build a map of ID -> response for easy lookup
	respMap := make(map[int]*jsonRPCResponse, len(responses))
	for i := range responses {
		respMap[responses[i].ID] = &responses[i]
	}

	traces := make(map[int64]json.RawMessage, len(blockNums))
	errs := make(map[int64]error)

	for i, blockNum := range blockNums {
		raw, err := extractResult(respMap[i], nil, "trace_block", strconv.FormatInt(blockNum, 10))
		if err != nil {
			errs[blockNum] = err
			continue
		}
		traces[blockNum] = raw
	}

	return traces, errs
}

// callBatch makes a batched HTTP JSON-RPC call to the Alchemy API with retry.
func (c *Client) callBatch(ctx context.Context, requests []jsonRPCRequest) ([]jsonRPCResponse, error) {
	// Start span if telemetry is enabled
	if c.telemetry != nil {
		var span trace.Span
		ctx, span = c.telemetry.StartSpan(ctx, "batch")
		defer span.End()
		c.telemetry.RecordBatchSize(ctx, len(requests))
	}

	start := time.Now()
	reqBytes, err := json.Marshal(requests)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal batch request: %w", err)
	}

	var rpcResponses []jsonRPCResponse
	err = c.doWithRetry(ctx, "batch", func() error {
		// Reset responses to avoid leftover data from previous attempts
		rpcResponses = nil

		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.config.HTTPURL, bytes.NewReader(reqBytes))
		if err != nil {
			return &nonRetryableError{err: fmt.Errorf("failed to create request: %w", err)}
		}
		httpReq.Header.Set("Content-Type", "application/json")

		httpResp, err := c.httpClient.Do(httpReq)
		if err != nil {
			return fmt.Errorf("HTTP request failed: %w", err)
		}
		defer func() {
			if err := httpResp.Body.Close(); err != nil {
				c.logger.Warn("failed to close HTTP response body", "error", err)
			}
		}()

		// Check for retryable HTTP status codes
		if httpResp.StatusCode >= 500 || httpResp.StatusCode == 429 {
			return fmt.Errorf("HTTP %d: server error", httpResp.StatusCode)
		}

		respBytes, err := io.ReadAll(httpResp.Body)
		if err != nil {
			return fmt.Errorf("failed to read response: %w", err)
		}

		if err := json.Unmarshal(respBytes, &rpcResponses); err != nil {
			return fmt.Errorf("failed to parse batch response: %w", err)
		}

		return nil
	})

	// Record metrics if telemetry is enabled
	if c.telemetry != nil {
		c.telemetry.RecordRequest(ctx, "batch", time.Since(start), err)
	}

	if err != nil {
		return nil, err
	}
	return rpcResponses, nil
}

// call makes an HTTP JSON-RPC call to the Alchemy API with retry.
func (c *Client) call(ctx context.Context, req jsonRPCRequest) (*jsonRPCResponse, error) {
	// Start span if telemetry is enabled
	if c.telemetry != nil {
		var span trace.Span
		ctx, span = c.telemetry.StartSpan(ctx, req.Method)
		defer span.End()
	}

	start := time.Now()
	reqBytes, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	var rpcResp jsonRPCResponse
	err = c.doWithRetry(ctx, req.Method, func() error {
		// Reset response to avoid leftover error field from previous attempts
		rpcResp = jsonRPCResponse{}

		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.config.HTTPURL, bytes.NewReader(reqBytes))
		if err != nil {
			return &nonRetryableError{err: fmt.Errorf("failed to create request: %w", err)}
		}
		httpReq.Header.Set("Content-Type", "application/json")

		httpResp, err := c.httpClient.Do(httpReq)
		if err != nil {
			return fmt.Errorf("HTTP request failed: %w", err)
		}
		defer func() {
			if err := httpResp.Body.Close(); err != nil {
				c.logger.Warn("failed to close HTTP response body", "error", err)
			}
		}()

		// Check for retryable HTTP status codes
		if httpResp.StatusCode >= 500 || httpResp.StatusCode == 429 {
			return fmt.Errorf("HTTP %d: server error", httpResp.StatusCode)
		}

		respBytes, err := io.ReadAll(httpResp.Body)
		if err != nil {
			return fmt.Errorf("failed to read response: %w", err)
		}

		if err := json.Unmarshal(respBytes, &rpcResp); err != nil {
			return fmt.Errorf("failed to parse response: %w", err)
		}

		if rpcResp.Error != nil {
			return fmt.Errorf("RPC error: %s", rpcResp.Error.Message)
		}

		return nil
	})

	// Record metrics if telemetry is enabled
	if c.telemetry != nil {
		c.telemetry.RecordRequest(ctx, req.Method, time.Since(start), err)
	}

	if err != nil {
		return nil, err
	}
	return &rpcResp, nil
}

// nonRetryableError wraps errors that should not be retried.
type nonRetryableError struct {
	err error
}

func (e *nonRetryableError) Error() string {
	return e.err.Error()
}

func (e *nonRetryableError) Unwrap() error {
	return e.err
}

// doWithRetry executes fn with exponential backoff retry for transient failures.
// Returns immediately on non-retryable errors or context cancellation.
func (c *Client) doWithRetry(ctx context.Context, method string, fn func() error) error {
	backoff := c.config.InitialBackoff
	var lastErr error

	for attempt := 0; attempt <= c.config.MaxRetries; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}

		// Check if error is non-retryable
		var nonRetryable *nonRetryableError
		if errors.As(err, &nonRetryable) {
			return nonRetryable.err
		}

		lastErr = err

		// Don't sleep after the last attempt
		if attempt == c.config.MaxRetries {
			break
		}

		// Record retry metric if telemetry is enabled
		if c.telemetry != nil {
			c.telemetry.RecordRetry(ctx, method, attempt+1)
		}

		c.logger.Warn("request failed, retrying",
			"method", method,
			"attempt", attempt+1,
			"maxRetries", c.config.MaxRetries,
			"backoff", backoff,
			"error", err)

		// Wait before retry, respecting context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}

		// Increase backoff for next attempt
		backoff = min(time.Duration(float64(backoff)*c.config.BackoffFactor), c.config.MaxBackoff)
	}

	c.logger.Error("request failed after all retries",
		"method", method,
		"maxRetries", c.config.MaxRetries,
		"error", lastErr)

	return fmt.Errorf("after %d retries: %w", c.config.MaxRetries, lastErr)
}

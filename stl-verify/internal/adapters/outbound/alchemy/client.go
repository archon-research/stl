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
	"sync"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/hexutil"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/trace"
)

// Compile-time check that Client implements outbound.BlockchainClient
var _ outbound.BlockchainClient = (*Client)(nil)

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

	return &Client{
		config: config,
		httpClient: &http.Client{
			Timeout: config.Timeout,
			Transport: otelhttp.NewTransport(http.DefaultTransport,
				otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
					return "alchemy.http"
				}),
			),
		},
		logger:    config.Logger.With("component", "alchemy-client"),
		telemetry: config.Telemetry,
	}, nil
}

// GetBlockByNumber fetches a block by its number.
func (c *Client) GetBlockByNumber(ctx context.Context, blockNum int64, fullTx bool) (json.RawMessage, error) {
	hexNum := fmt.Sprintf("0x%x", blockNum)
	req := jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "eth_getBlockByNumber",
		Params:  []interface{}{hexNum, fullTx},
	}

	resp, err := c.call(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.Result, nil
}

// GetBlockByHash fetches a block by its hash.
func (c *Client) GetBlockByHash(ctx context.Context, hash string, fullTx bool) (*outbound.BlockHeader, error) {
	req := jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "eth_getBlockByHash",
		Params:  []interface{}{hash, fullTx},
	}

	resp, err := c.call(ctx, req)
	if err != nil {
		return nil, err
	}

	if resp.Result == nil || string(resp.Result) == "null" {
		return nil, fmt.Errorf("block not found: %s", hash)
	}

	var header outbound.BlockHeader
	if err := json.Unmarshal(resp.Result, &header); err != nil {
		return nil, fmt.Errorf("failed to parse block: %w", err)
	}

	return &header, nil
}

// GetFullBlockByHash fetches full block JSON by hash.
func (c *Client) GetFullBlockByHash(ctx context.Context, hash string, fullTx bool) (json.RawMessage, error) {
	req := jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "eth_getBlockByHash",
		Params:  []interface{}{hash, fullTx},
	}

	resp, err := c.call(ctx, req)
	if err != nil {
		return nil, err
	}

	if resp.Result == nil || string(resp.Result) == "null" {
		return nil, fmt.Errorf("block not found: %s", hash)
	}

	return resp.Result, nil
}

// GetBlockReceipts fetches all transaction receipts for a block by number.
func (c *Client) GetBlockReceipts(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	hexNum := fmt.Sprintf("0x%x", blockNum)
	req := jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "eth_getBlockReceipts",
		Params:  []interface{}{hexNum},
	}

	resp, err := c.call(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.Result, nil
}

// GetBlockReceiptsByHash fetches all transaction receipts for a block by hash.
// Use this to prevent TOCTOU race conditions during reorgs.
func (c *Client) GetBlockReceiptsByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	req := jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "eth_getBlockReceipts",
		Params:  []interface{}{hash},
	}

	resp, err := c.call(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.Result, nil
}

// GetBlockTraces fetches execution traces for a block by number.
func (c *Client) GetBlockTraces(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	hexNum := fmt.Sprintf("0x%x", blockNum)
	req := jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "trace_block",
		Params:  []interface{}{hexNum},
	}

	resp, err := c.call(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.Result, nil
}

// GetBlockTracesByHash fetches execution traces for a block by hash.
// Use this to prevent TOCTOU race conditions during reorgs.
func (c *Client) GetBlockTracesByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	req := jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "trace_block",
		Params:  []interface{}{hash},
	}

	resp, err := c.call(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.Result, nil
}

// GetBlobSidecars fetches blob sidecars for a block by number.
func (c *Client) GetBlobSidecars(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	hexNum := fmt.Sprintf("0x%x", blockNum)
	req := jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "eth_getBlobSidecars",
		Params:  []interface{}{hexNum},
	}

	resp, err := c.call(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.Result, nil
}

// GetBlobSidecarsByHash fetches blob sidecars for a block by hash.
// Use this to prevent TOCTOU race conditions during reorgs.
func (c *Client) GetBlobSidecarsByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	req := jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "eth_getBlobSidecars",
		Params:  []interface{}{hash},
	}

	resp, err := c.call(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.Result, nil
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
	wg.Add(1)
	go func() {
		defer wg.Done()
		req := jsonRPCRequest{JSONRPC: "2.0", ID: 0, Method: "eth_getBlockByHash", Params: []interface{}{hash, fullTx}}
		resp, err := c.call(ctx, req)
		mu.Lock()
		defer mu.Unlock()
		if err != nil {
			result.BlockErr = err
		} else if resp.Error != nil {
			result.BlockErr = fmt.Errorf("RPC error for block %s: %s (code: %d)", hash, resp.Error.Message, resp.Error.Code)
		} else {
			result.Block = resp.Result
		}
	}()

	// Fetch receipts
	wg.Add(1)
	go func() {
		defer wg.Done()
		req := jsonRPCRequest{JSONRPC: "2.0", ID: 1, Method: "eth_getBlockReceipts", Params: []interface{}{hash}}
		resp, err := c.call(ctx, req)
		mu.Lock()
		defer mu.Unlock()
		if err != nil {
			result.ReceiptsErr = err
		} else if resp.Error != nil {
			result.ReceiptsErr = fmt.Errorf("RPC error for block %s receipts: %s (code: %d)", hash, resp.Error.Message, resp.Error.Code)
		} else {
			result.Receipts = resp.Result
		}
	}()

	// Fetch traces
	wg.Add(1)
	go func() {
		defer wg.Done()
		req := jsonRPCRequest{JSONRPC: "2.0", ID: 2, Method: "trace_block", Params: []interface{}{hash}}
		resp, err := c.call(ctx, req)
		mu.Lock()
		defer mu.Unlock()
		if err != nil {
			result.TracesErr = err
		} else if resp.Error != nil {
			result.TracesErr = fmt.Errorf("RPC error for block %s traces: %s (code: %d)", hash, resp.Error.Message, resp.Error.Code)
		} else {
			result.Traces = resp.Result
		}
	}()

	// Fetch blobs (only if enabled)
	if c.config.EnableBlobs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := jsonRPCRequest{JSONRPC: "2.0", ID: 3, Method: "eth_getBlobSidecars", Params: []interface{}{hash}}
			resp, err := c.call(ctx, req)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				result.BlobsErr = err
			} else if resp.Error != nil {
				result.BlobsErr = fmt.Errorf("RPC error for block %s blobs: %s (code: %d)", hash, resp.Error.Message, resp.Error.Code)
			} else {
				result.Blobs = resp.Result
			}
		}()
	}

	wg.Wait()
	return result, nil
}

// getBlockDataByHashBatched fetches block data using a single batched RPC call.
// This uses fewer API credits but all calls share a single HTTP round-trip.
func (c *Client) getBlockDataByHashBatched(ctx context.Context, blockNum int64, hash string, fullTx bool) (outbound.BlockData, error) {
	// Build batch request: 3-4 calls (block, receipts, traces, and optionally blobs)
	callsCount := 3
	if c.config.EnableBlobs {
		callsCount = 4
	}
	requests := make([]jsonRPCRequest, 0, callsCount)

	requests = append(requests,
		jsonRPCRequest{JSONRPC: "2.0", ID: 0, Method: "eth_getBlockByHash", Params: []interface{}{hash, fullTx}},
		jsonRPCRequest{JSONRPC: "2.0", ID: 1, Method: "eth_getBlockReceipts", Params: []interface{}{hash}},
		jsonRPCRequest{JSONRPC: "2.0", ID: 2, Method: "trace_block", Params: []interface{}{hash}},
	)
	if c.config.EnableBlobs {
		requests = append(requests,
			jsonRPCRequest{JSONRPC: "2.0", ID: 3, Method: "eth_getBlobSidecars", Params: []interface{}{hash}},
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

	// Assemble result
	result := outbound.BlockData{BlockNumber: blockNum}

	// Block data
	if resp := respMap[0]; resp != nil {
		if resp.Error != nil {
			result.BlockErr = fmt.Errorf("RPC error for block %s: %s (code: %d)", hash, resp.Error.Message, resp.Error.Code)
		} else {
			result.Block = resp.Result
		}
	} else {
		result.BlockErr = fmt.Errorf("missing response for block %d", blockNum)
	}

	// Receipts
	if resp := respMap[1]; resp != nil {
		if resp.Error != nil {
			result.ReceiptsErr = fmt.Errorf("RPC error for block %s receipts: %s (code: %d)", hash, resp.Error.Message, resp.Error.Code)
		} else {
			result.Receipts = resp.Result
		}
	} else {
		result.ReceiptsErr = fmt.Errorf("missing response for receipts of block %d", blockNum)
	}

	// Traces
	if resp := respMap[2]; resp != nil {
		if resp.Error != nil {
			result.TracesErr = fmt.Errorf("RPC error for block %s traces: %s (code: %d)", hash, resp.Error.Message, resp.Error.Code)
		} else {
			result.Traces = resp.Result
		}
	} else {
		result.TracesErr = fmt.Errorf("missing response for traces of block %d", blockNum)
	}

	// Blobs (only if enabled)
	if c.config.EnableBlobs {
		if resp := respMap[3]; resp != nil {
			if resp.Error != nil {
				result.BlobsErr = fmt.Errorf("RPC error for block %s blobs: %s (code: %d)", hash, resp.Error.Message, resp.Error.Code)
			} else {
				result.Blobs = resp.Result
			}
		} else {
			result.BlobsErr = fmt.Errorf("missing response for blobs of block %d", blockNum)
		}
	}

	return result, nil
}

// GetCurrentBlockNumber fetches the latest block number.
func (c *Client) GetCurrentBlockNumber(ctx context.Context) (int64, error) {
	req := jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "eth_blockNumber",
		Params:  []interface{}{},
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

	// Build batch request: 3-4 calls per block (block, receipts, traces, and optionally blobs)
	callsPerBlock := 3
	if c.config.EnableBlobs {
		callsPerBlock = 4
	}
	requests := make([]jsonRPCRequest, 0, len(blockNums)*callsPerBlock)
	for i, blockNum := range blockNums {
		hexNum := fmt.Sprintf("0x%x", blockNum)
		baseID := i * 4 // Keep consistent IDs for response mapping

		requests = append(requests,
			jsonRPCRequest{JSONRPC: "2.0", ID: baseID, Method: "eth_getBlockByNumber", Params: []interface{}{hexNum, fullTx}},
			jsonRPCRequest{JSONRPC: "2.0", ID: baseID + 1, Method: "eth_getBlockReceipts", Params: []interface{}{hexNum}},
			jsonRPCRequest{JSONRPC: "2.0", ID: baseID + 2, Method: "trace_block", Params: []interface{}{hexNum}},
		)
		if c.config.EnableBlobs {
			requests = append(requests,
				jsonRPCRequest{JSONRPC: "2.0", ID: baseID + 3, Method: "eth_getBlobSidecars", Params: []interface{}{hexNum}},
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

	// Assemble results
	results := make([]outbound.BlockData, len(blockNums))
	for i, blockNum := range blockNums {
		baseID := i * 4
		results[i] = outbound.BlockData{BlockNumber: blockNum}

		// Block data
		if resp := respMap[baseID]; resp != nil {
			if resp.Error != nil {
				results[i].BlockErr = fmt.Errorf("RPC error for block %d: %s (code: %d)", blockNum, resp.Error.Message, resp.Error.Code)
			} else {
				results[i].Block = resp.Result
			}
		} else {
			results[i].BlockErr = fmt.Errorf("missing response for block %d", blockNum)
		}

		// Receipts
		if resp := respMap[baseID+1]; resp != nil {
			if resp.Error != nil {
				results[i].ReceiptsErr = fmt.Errorf("RPC error for block %d receipts: %s (code: %d)", blockNum, resp.Error.Message, resp.Error.Code)
			} else {
				results[i].Receipts = resp.Result
			}
		} else {
			results[i].ReceiptsErr = fmt.Errorf("missing response for receipts of block %d", blockNum)
		}

		// Traces
		if resp := respMap[baseID+2]; resp != nil {
			if resp.Error != nil {
				results[i].TracesErr = fmt.Errorf("RPC error for block %d traces: %s (code: %d)", blockNum, resp.Error.Message, resp.Error.Code)
			} else {
				results[i].Traces = resp.Result
			}
		} else {
			results[i].TracesErr = fmt.Errorf("missing response for traces of block %d", blockNum)
		}

		// Blobs (only if enabled)
		if c.config.EnableBlobs {
			if resp := respMap[baseID+3]; resp != nil {
				if resp.Error != nil {
					results[i].BlobsErr = fmt.Errorf("RPC error for block %d blobs: %s (code: %d)", blockNum, resp.Error.Message, resp.Error.Code)
				} else {
					results[i].Blobs = resp.Result
				}
			} else {
				results[i].BlobsErr = fmt.Errorf("missing response for blobs of block %d", blockNum)
			}
		}
	}

	return results, nil
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
		backoff = time.Duration(float64(backoff) * c.config.BackoffFactor)
		if backoff > c.config.MaxBackoff {
			backoff = c.config.MaxBackoff
		}
	}

	c.logger.Error("request failed after all retries",
		"method", method,
		"maxRetries", c.config.MaxRetries,
		"error", lastErr)

	return fmt.Errorf("after %d retries: %w", c.config.MaxRetries, lastErr)
}

package alchemy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that Client implements outbound.BlockchainClient
var _ outbound.BlockchainClient = (*Client)(nil)

// ClientConfig holds configuration for the HTTP RPC client.
type ClientConfig struct {
	// HTTPURL is the Alchemy HTTP JSON-RPC endpoint URL.
	HTTPURL string

	// Timeout is the maximum time to wait for a single HTTP request.
	Timeout time.Duration
}

// ClientConfigDefaults returns a config with default values.
func ClientConfigDefaults() ClientConfig {
	return ClientConfig{
		Timeout: 30 * time.Second,
	}
}

// Client implements BlockchainClient using Alchemy's HTTP JSON-RPC API.
type Client struct {
	config     ClientConfig
	httpClient *http.Client
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

	return &Client{
		config: config,
		httpClient: &http.Client{
			Timeout: config.Timeout,
		},
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

// GetBlockReceipts fetches all transaction receipts for a block.
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

// GetBlockTraces fetches execution traces for a block.
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

// GetBlobSidecars fetches blob sidecars for a block.
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

	return parseBlockNumber(result)
}

// GetBlocksBatch fetches all data for multiple blocks in a single batched RPC call.
func (c *Client) GetBlocksBatch(ctx context.Context, blockNums []int64, fullTx bool) ([]outbound.BlockData, error) {
	if len(blockNums) == 0 {
		return nil, nil
	}

	// Build batch request: 4 calls per block (block, receipts, traces, blobs)
	requests := make([]jsonRPCRequest, 0, len(blockNums)*4)
	for i, blockNum := range blockNums {
		hexNum := fmt.Sprintf("0x%x", blockNum)
		baseID := i * 4

		requests = append(requests,
			jsonRPCRequest{JSONRPC: "2.0", ID: baseID, Method: "eth_getBlockByNumber", Params: []interface{}{hexNum, fullTx}},
			jsonRPCRequest{JSONRPC: "2.0", ID: baseID + 1, Method: "eth_getBlockReceipts", Params: []interface{}{hexNum}},
			jsonRPCRequest{JSONRPC: "2.0", ID: baseID + 2, Method: "trace_block", Params: []interface{}{hexNum}},
			jsonRPCRequest{JSONRPC: "2.0", ID: baseID + 3, Method: "eth_getBlobSidecars", Params: []interface{}{hexNum}},
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

	// Assemble results
	results := make([]outbound.BlockData, len(blockNums))
	for i, blockNum := range blockNums {
		baseID := i * 4
		results[i] = outbound.BlockData{BlockNumber: blockNum}

		if resp := respMap[baseID]; resp != nil && resp.Error == nil {
			results[i].Block = resp.Result
		}
		if resp := respMap[baseID+1]; resp != nil && resp.Error == nil {
			results[i].Receipts = resp.Result
		}
		if resp := respMap[baseID+2]; resp != nil && resp.Error == nil {
			results[i].Traces = resp.Result
		}
		if resp := respMap[baseID+3]; resp != nil && resp.Error == nil {
			results[i].Blobs = resp.Result
		}
	}

	return results, nil
}

// callBatch makes a batched HTTP JSON-RPC call to the Alchemy API.
func (c *Client) callBatch(ctx context.Context, requests []jsonRPCRequest) ([]jsonRPCResponse, error) {
	reqBytes, err := json.Marshal(requests)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal batch request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.config.HTTPURL, bytes.NewReader(reqBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer httpResp.Body.Close()

	respBytes, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	var rpcResponses []jsonRPCResponse
	if err := json.Unmarshal(respBytes, &rpcResponses); err != nil {
		return nil, fmt.Errorf("failed to parse batch response: %w", err)
	}

	return rpcResponses, nil
}

// call makes an HTTP JSON-RPC call to the Alchemy API.
func (c *Client) call(ctx context.Context, req jsonRPCRequest) (*jsonRPCResponse, error) {
	reqBytes, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.config.HTTPURL, bytes.NewReader(reqBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer httpResp.Body.Close()

	respBytes, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	var rpcResp jsonRPCResponse
	if err := json.Unmarshal(respBytes, &rpcResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	if rpcResp.Error != nil {
		return nil, fmt.Errorf("RPC error: %s", rpcResp.Error.Message)
	}

	return &rpcResp, nil
}

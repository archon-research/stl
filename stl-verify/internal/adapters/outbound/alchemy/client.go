package alchemy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that Client implements outbound.BlockchainClient
var _ outbound.BlockchainClient = (*Client)(nil)

// Client implements BlockchainClient using Alchemy's HTTP JSON-RPC API.
type Client struct {
	httpURL    string
	httpClient *http.Client
}

// NewClient creates a new Alchemy HTTP RPC client.
func NewClient(httpURL string) *Client {
	return &Client{
		httpURL: httpURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
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

// call makes an HTTP JSON-RPC call to the Alchemy API.
func (c *Client) call(ctx context.Context, req jsonRPCRequest) (*jsonRPCResponse, error) {
	reqBytes, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.httpURL, bytes.NewReader(reqBytes))
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

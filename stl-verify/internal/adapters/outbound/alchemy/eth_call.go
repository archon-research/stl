package alchemy

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// ethCallMsg is the parameter object for eth_call.
type ethCallMsg struct {
	To   string `json:"to"`
	Data string `json:"data"`
}

// CallContract executes a read-only contract call via eth_call at the latest block.
// Returns the raw ABI-encoded response bytes.
func (c *Client) CallContract(ctx context.Context, to string, data []byte) ([]byte, error) {
	req := jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "eth_call",
		Params: []any{
			ethCallMsg{
				To:   to,
				Data: "0x" + hex.EncodeToString(data),
			},
			"latest",
		},
	}

	resp, err := c.call(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("eth_call to %s: %w", to, err)
	}

	// Result is a hex-encoded string e.g. "0xabcd..."
	var hexResult string
	if err := json.Unmarshal(resp.Result, &hexResult); err != nil {
		return nil, fmt.Errorf("parse eth_call result: %w", err)
	}

	result, err := hex.DecodeString(strings.TrimPrefix(hexResult, "0x"))
	if err != nil {
		return nil, fmt.Errorf("decode eth_call result: %w", err)
	}

	return result, nil
}

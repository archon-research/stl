package alchemy

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

var _ outbound.LogScanClient = (*Client)(nil)

// Matching on prose is unavoidable: JSON-RPC error codes are reused for
// unrelated failures (-32602 is also a malformed filter), so the code alone
// cannot separate "narrow the range and retry" from "the request is wrong".
var rangeRefusalPhrases = []string{
	"log response size exceeded",
	"response size exceeded",
	"query returned more than",
	"more than 10000 results",
	"limited to a 10000 range",
	"block range is too large",
	"block range too large",
	"query timeout exceeded",
	"exceed maximum block range",
}

func (c *Client) GetLogs(ctx context.Context, filter outbound.LogFilter) ([]outbound.FilteredLog, error) {
	params, err := getLogsParams(filter)
	if err != nil {
		return nil, err
	}

	subject := fmt.Sprintf("%d-%d", filter.FromBlock, filter.ToBlock)
	req := jsonRPCRequest{JSONRPC: "2.0", ID: 1, Method: "eth_getLogs", Params: []any{params}}
	resp, callErr := c.callClassified(ctx, req, classifyGetLogsError)
	raw, err := extractResult(resp, callErr, "eth_getLogs", subject)
	if err != nil {
		return nil, err
	}

	var logs []outbound.FilteredLog
	if err := json.Unmarshal(raw, &logs); err != nil {
		return nil, fmt.Errorf("eth_getLogs %s: parsing logs: %w", subject, err)
	}
	return logs, nil
}

func getLogsParams(filter outbound.LogFilter) (map[string]any, error) {
	if filter.FromBlock < 0 || filter.ToBlock < 0 {
		return nil, fmt.Errorf("eth_getLogs: block bounds must be non-negative, got [%d, %d]", filter.FromBlock, filter.ToBlock)
	}
	if filter.FromBlock > filter.ToBlock {
		return nil, fmt.Errorf("eth_getLogs: fromBlock %d is above toBlock %d", filter.FromBlock, filter.ToBlock)
	}

	params := map[string]any{
		"fromBlock": "0x" + strconv.FormatInt(filter.FromBlock, 16),
		"toBlock":   "0x" + strconv.FormatInt(filter.ToBlock, 16),
	}
	if filter.Address != (common.Address{}) {
		params["address"] = filter.Address.Hex()
	}
	if topics := getLogsTopics(filter); len(topics) > 0 {
		params["topics"] = topics
	}
	return params, nil
}

// A constrained topic1 under an unconstrained topic0 needs an explicit JSON
// null placeholder, which is what the nil entry serialises to.
func getLogsTopics(filter outbound.LogFilter) []any {
	var topics []any
	switch {
	case filter.Topic0 != (common.Hash{}):
		topics = append(topics, filter.Topic0.Hex())
	case len(filter.Topic1) > 0:
		topics = append(topics, nil)
	default:
		return nil
	}
	if len(filter.Topic1) == 0 {
		return topics
	}

	orSet := make([]string, len(filter.Topic1))
	for i, t := range filter.Topic1 {
		orSet[i] = t.Hex()
	}
	return append(topics, orSet)
}

func classifyGetLogsError(rpcErr *jsonRPCError) error {
	if !isRangeRefusal(rpcErr.Message) {
		return fmt.Errorf("RPC error: %s", rpcErr.Message)
	}
	return &nonRetryableError{err: fmt.Errorf("eth_getLogs refused (%s): %w", rpcErr.Message, outbound.ErrLogRangeTooLarge)}
}

func isRangeRefusal(message string) bool {
	lowered := strings.ToLower(message)
	for _, phrase := range rangeRefusalPhrases {
		if strings.Contains(lowered, phrase) {
			return true
		}
	}
	return false
}

func (c *Client) GetBlockHeaderByNumber(ctx context.Context, blockNumber int64) (*outbound.BlockHeader, error) {
	if blockNumber < 0 {
		return nil, fmt.Errorf("eth_getBlockByNumber: block number must be non-negative, got %d", blockNumber)
	}
	hexNum := "0x" + strconv.FormatInt(blockNumber, 16)
	raw, err := c.callSingle(ctx, "eth_getBlockByNumber", strconv.FormatInt(blockNumber, 10), []any{hexNum, false})
	if err != nil {
		return nil, err
	}
	var header outbound.BlockHeader
	if err := json.Unmarshal(raw, &header); err != nil {
		return nil, fmt.Errorf("eth_getBlockByNumber %d: parsing header: %w", blockNumber, err)
	}
	return &header, nil
}

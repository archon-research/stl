package multicall

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// DirectCaller implements outbound.Multicaller by making individual eth_call
// per target using JSON-RPC batching instead of the Multicall3 contract. This
// is needed for contracts like Chronicle that use toll/whitelist access control
// where msg.sender must be address(0) (the default for eth_call).
//
// All calls are sent in a single HTTP request via rpc.BatchCallContext,
// minimizing round-trip latency while keeping each call's msg.sender as address(0).
type DirectCaller struct {
	rpcClient *rpc.Client
}

// NewDirectCaller creates a new DirectCaller from an ethclient.
func NewDirectCaller(rpcClient *rpc.Client) *DirectCaller {
	return &DirectCaller{rpcClient: rpcClient}
}

// ethCallArg mirrors go-ethereum's internal callMsg JSON encoding for eth_call.
type ethCallArg struct {
	To   string `json:"to"`
	Data string `json:"data"`
}

// Execute sends all calls in a single JSON-RPC batch request.
func (c *DirectCaller) Execute(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
	if len(calls) == 0 {
		return []outbound.Result{}, nil
	}

	blockArg := toBlockNumArg(blockNumber)

	elems := make([]rpc.BatchElem, len(calls))
	hexResults := make([]hexutil.Bytes, len(calls))

	for i, call := range calls {
		hexResults[i] = nil
		elems[i] = rpc.BatchElem{
			Method: "eth_call",
			Args: []interface{}{
				ethCallArg{
					To:   call.Target.Hex(),
					Data: "0x" + hex.EncodeToString(call.CallData),
				},
				blockArg,
			},
			Result: &hexResults[i],
		}
	}

	if err := c.rpcClient.BatchCallContext(ctx, elems); err != nil {
		return nil, fmt.Errorf("batch eth_call failed: %w", err)
	}

	results := make([]outbound.Result, len(calls))
	for i, elem := range elems {
		if elem.Error != nil {
			if !calls[i].AllowFailure {
				return nil, fmt.Errorf("direct call to %s failed: %w", calls[i].Target.Hex(), elem.Error)
			}
			results[i] = outbound.Result{Success: false}
			continue
		}
		results[i] = outbound.Result{
			Success:    true,
			ReturnData: hexResults[i],
		}
	}

	return results, nil
}

// Address returns a zero address since DirectCaller doesn't use a contract.
func (c *DirectCaller) Address() common.Address {
	return common.Address{}
}

func toBlockNumArg(number *big.Int) string {
	if number == nil {
		return "latest"
	}
	if number.Sign() >= 0 {
		return hexutil.EncodeBig(number)
	}
	return "latest"
}

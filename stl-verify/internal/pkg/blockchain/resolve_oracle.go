package blockchain

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// resolveViaPoolAddressProvider calls getPriceOracle() on a PoolAddressesProvider contract
// at the given block to discover the oracle address. Shared by SparkLend and Aave resolvers.
func resolveViaPoolAddressProvider(
	ctx context.Context,
	mc outbound.Multicaller,
	providerAddr common.Address,
	providerABI *abi.ABI,
	blockNumber int64,
) (common.Address, error) {
	callData, err := providerABI.Pack("getPriceOracle")
	if err != nil {
		return common.Address{}, fmt.Errorf("packing getPriceOracle: %w", err)
	}

	block := new(big.Int).SetInt64(blockNumber)
	results, err := mc.Execute(ctx, []outbound.Call{
		{Target: providerAddr, AllowFailure: false, CallData: callData},
	}, block)
	if err != nil {
		return common.Address{}, fmt.Errorf("executing multicall at block %d: %w", blockNumber, err)
	}

	if len(results) != 1 {
		return common.Address{}, fmt.Errorf("expected 1 result, got %d", len(results))
	}
	if !results[0].Success {
		return common.Address{}, fmt.Errorf("getPriceOracle call failed at block %d", blockNumber)
	}

	unpacked, err := providerABI.Unpack("getPriceOracle", results[0].ReturnData)
	if err != nil {
		return common.Address{}, fmt.Errorf("unpacking getPriceOracle at block %d: %w", blockNumber, err)
	}

	addr, ok := unpacked[0].(common.Address)
	if !ok {
		return common.Address{}, fmt.Errorf("unexpected return type from getPriceOracle")
	}

	return addr, nil
}

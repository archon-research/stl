package blockchain

import (
	"context"
	"fmt"
	"math"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// FetchOraclePrices fetches asset prices from an oracle contract via a single multicall.
// Returns raw prices (one per asset) directly.
func FetchOraclePrices(
	ctx context.Context,
	multicaller outbound.Multicaller,
	oracleABI *abi.ABI,
	oracleAddr common.Address,
	assets []common.Address,
	blockNum int64,
) ([]*big.Int, error) {
	block := new(big.Int).SetInt64(blockNum)

	// Pack getAssetsPrices(assets[]) call
	getAssetsPricesData, err := oracleABI.Pack("getAssetsPrices", assets)
	if err != nil {
		return nil, fmt.Errorf("packing getAssetsPrices: %w", err)
	}

	calls := []outbound.Call{
		{Target: oracleAddr, AllowFailure: false, CallData: getAssetsPricesData},
	}

	results, err := multicaller.Execute(ctx, calls, block)
	if err != nil {
		return nil, fmt.Errorf("executing multicall at block %d: %w", blockNum, err)
	}

	if len(results) != 1 {
		return nil, fmt.Errorf("expected 1 multicall result, got %d", len(results))
	}

	if !results[0].Success {
		return nil, fmt.Errorf("getAssetsPrices call failed at block %d", blockNum)
	}

	prices, err := unpackAssetsPrices(oracleABI, results[0].ReturnData)
	if err != nil {
		return nil, fmt.Errorf("unpacking getAssetsPrices at block %d: %w", blockNum, err)
	}

	return prices, nil
}

func unpackAssetsPrices(oracleABI *abi.ABI, data []byte) ([]*big.Int, error) {
	unpacked, err := oracleABI.Unpack("getAssetsPrices", data)
	if err != nil {
		return nil, err
	}
	return unpacked[0].([]*big.Int), nil
}

// ConvertOraclePriceToUSD converts a raw oracle price to USD.
// decimals specifies the oracle's price precision (e.g., 8 for Chainlink/Aave: 1e8 = $1.00).
func ConvertOraclePriceToUSD(rawPrice *big.Int, decimals int) float64 {
	if rawPrice == nil || rawPrice.Sign() == 0 {
		return 0
	}
	f := new(big.Float).SetInt(rawPrice)
	divisor := new(big.Float).SetFloat64(math.Pow10(decimals))
	result, _ := new(big.Float).Quo(f, divisor).Float64()
	return result
}

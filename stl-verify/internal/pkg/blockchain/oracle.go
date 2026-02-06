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

// OraclePriceResult holds the result of fetching oracle prices for a single block.
type OraclePriceResult struct {
	OracleAddress common.Address
	Prices        []*big.Int
}

// FetchOraclePrices batches oracle address lookup + price fetch into one multicall.
// Returns the oracle address, raw prices (one per asset), and error.
//
// Flow:
//  1. Pack call 1: PoolAddressProvider.getPriceOracle() → verify oracle address
//  2. Pack call 2: Oracle.getAssetsPrices(assets[]) using cachedOracleAddr
//  3. Execute both via multicall — single RPC call
//  4. If oracle address matches cache: use prices from call 2 (99.99% of the time)
//  5. If different: re-fetch prices with new oracle address (extremely rare)
func FetchOraclePrices(
	ctx context.Context,
	multicaller outbound.Multicaller,
	providerABI, oracleABI *abi.ABI,
	providerAddr, cachedOracleAddr common.Address,
	assets []common.Address,
	blockNum int64,
) (*OraclePriceResult, error) {
	block := new(big.Int).SetInt64(blockNum)

	// Pack getPriceOracle() call
	getPriceOracleData, err := providerABI.Pack("getPriceOracle")
	if err != nil {
		return nil, fmt.Errorf("packing getPriceOracle: %w", err)
	}

	// Pack getAssetsPrices(assets[]) call using cached oracle address
	getAssetsPricesData, err := oracleABI.Pack("getAssetsPrices", assets)
	if err != nil {
		return nil, fmt.Errorf("packing getAssetsPrices: %w", err)
	}

	calls := []outbound.Call{
		{Target: providerAddr, AllowFailure: false, CallData: getPriceOracleData},
		{Target: cachedOracleAddr, AllowFailure: false, CallData: getAssetsPricesData},
	}

	results, err := multicaller.Execute(ctx, calls, block)
	if err != nil {
		return nil, fmt.Errorf("executing multicall at block %d: %w", blockNum, err)
	}

	if len(results) != 2 {
		return nil, fmt.Errorf("expected 2 multicall results, got %d", len(results))
	}

	// Unpack oracle address
	if !results[0].Success {
		return nil, fmt.Errorf("getPriceOracle call failed at block %d", blockNum)
	}
	oracleAddrResult, err := providerABI.Unpack("getPriceOracle", results[0].ReturnData)
	if err != nil {
		return nil, fmt.Errorf("unpacking getPriceOracle at block %d: %w", blockNum, err)
	}
	currentOracleAddr := oracleAddrResult[0].(common.Address)

	// If oracle address matches cache, use prices from call 2
	if currentOracleAddr == cachedOracleAddr && results[1].Success {
		prices, err := unpackAssetsPrices(oracleABI, results[1].ReturnData)
		if err != nil {
			return nil, fmt.Errorf("unpacking getAssetsPrices at block %d: %w", blockNum, err)
		}
		return &OraclePriceResult{
			OracleAddress: currentOracleAddr,
			Prices:        prices,
		}, nil
	}

	// Oracle address changed (extremely rare — governance-driven).
	// Re-fetch prices with the new oracle address.
	getAssetsPricesData2, err := oracleABI.Pack("getAssetsPrices", assets)
	if err != nil {
		return nil, fmt.Errorf("packing getAssetsPrices for new oracle: %w", err)
	}

	retryResults, err := multicaller.Execute(ctx, []outbound.Call{
		{Target: currentOracleAddr, AllowFailure: false, CallData: getAssetsPricesData2},
	}, block)
	if err != nil {
		return nil, fmt.Errorf("executing retry multicall at block %d: %w", blockNum, err)
	}

	if len(retryResults) != 1 || !retryResults[0].Success {
		return nil, fmt.Errorf("getAssetsPrices retry failed at block %d", blockNum)
	}

	prices, err := unpackAssetsPrices(oracleABI, retryResults[0].ReturnData)
	if err != nil {
		return nil, fmt.Errorf("unpacking retry getAssetsPrices at block %d: %w", blockNum, err)
	}

	return &OraclePriceResult{
		OracleAddress: currentOracleAddr,
		Prices:        prices,
	}, nil
}

func unpackAssetsPrices(oracleABI *abi.ABI, data []byte) ([]*big.Int, error) {
	unpacked, err := oracleABI.Unpack("getAssetsPrices", data)
	if err != nil {
		return nil, err
	}
	return unpacked[0].([]*big.Int), nil
}

// ConvertOraclePriceToUSD converts a raw oracle price to USD.
// Aave/SparkLend oracles return prices with 8 decimals (1e8 = $1.00).
func ConvertOraclePriceToUSD(rawPrice *big.Int) float64 {
	if rawPrice == nil || rawPrice.Sign() == 0 {
		return 0
	}
	f := new(big.Float).SetInt(rawPrice)
	divisor := new(big.Float).SetFloat64(math.Pow10(8))
	result, _ := new(big.Float).Quo(f, divisor).Float64()
	return result
}

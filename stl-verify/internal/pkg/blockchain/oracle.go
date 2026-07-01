package blockchain

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// executeOracleState runs calls pinned to blockHash via ExecuteAtHash when
// blockHash is non-zero, otherwise falls back to Execute pinned to blockNum.
// Oracle/feed prices are versioned per-block state: after a reorg an archive
// node answers eth_call-by-number with the new canonical price, which can
// silently disagree with the reorged (older-version) block being processed.
// The zero-hash fallback exists for the oracle backfill service, which replays
// already-settled historical blocks with no live fork ambiguity and has no
// BlockEvent to source a hash from. See VEC-471.
func executeOracleState(ctx context.Context, multicaller outbound.Multicaller, calls []outbound.Call, blockNum int64, blockHash common.Hash) ([]outbound.Result, error) {
	if blockHash != (common.Hash{}) {
		return multicaller.ExecuteAtHash(ctx, calls, blockHash)
	}
	return multicaller.Execute(ctx, calls, new(big.Int).SetInt64(blockNum))
}

// FetchOraclePrices fetches asset prices from an oracle contract via a single
// multicall, pinned to blockHash (see executeOracleState).
// Returns raw prices (one per asset) directly.
func FetchOraclePrices(
	ctx context.Context,
	multicaller outbound.Multicaller,
	oracleABI *abi.ABI,
	oracleAddr common.Address,
	assets []common.Address,
	blockNum int64,
	blockHash common.Hash,
) ([]*big.Int, error) {
	// Pack getAssetsPrices(assets[]) call
	getAssetsPricesData, err := oracleABI.Pack("getAssetsPrices", assets)
	if err != nil {
		return nil, fmt.Errorf("packing getAssetsPrices: %w", err)
	}

	calls := []outbound.Call{
		{Target: oracleAddr, AllowFailure: false, CallData: getAssetsPricesData},
	}

	results, err := executeOracleState(ctx, multicaller, calls, blockNum, blockHash)
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

// AssetPriceResult holds the result of an individual getAssetPrice call.
type AssetPriceResult struct {
	Price   *big.Int
	Success bool
}

// FetchOraclePricesIndividual fetches asset prices from an oracle contract using
// N individual getAssetPrice(address) calls, each with AllowFailure: true.
// This is still a single multicall RPC round-trip, but each token's price lookup
// can fail independently. Tokens without price sources at historical blocks return
// Success: false and are silently skipped by the caller.
//
// Number-pinned intentionally: this is only called by the oracle backfill
// service, which replays already-settled historical blocks with no live fork
// ambiguity — the reorg-correctness concern behind ExecuteAtHash (VEC-471)
// doesn't apply. See FetchOraclePrices for the hash-pinned live path.
func FetchOraclePricesIndividual(
	ctx context.Context,
	multicaller outbound.Multicaller,
	oracleABI *abi.ABI,
	oracleAddr common.Address,
	assets []common.Address,
	blockNum int64,
) ([]AssetPriceResult, error) {
	if len(assets) == 0 {
		return nil, nil
	}

	block := new(big.Int).SetInt64(blockNum)

	calls := make([]outbound.Call, len(assets))
	for i, asset := range assets {
		callData, err := oracleABI.Pack("getAssetPrice", asset)
		if err != nil {
			return nil, fmt.Errorf("packing getAssetPrice for asset %d: %w", i, err)
		}
		calls[i] = outbound.Call{
			Target:       oracleAddr,
			AllowFailure: true,
			CallData:     callData,
		}
	}

	results, err := multicaller.Execute(ctx, calls, block)
	if err != nil {
		return nil, fmt.Errorf("executing multicall at block %d: %w", blockNum, err)
	}

	if len(results) != len(assets) {
		return nil, fmt.Errorf("expected %d multicall results, got %d", len(assets), len(results))
	}

	out := make([]AssetPriceResult, len(results))
	for i, r := range results {
		if !r.Success {
			continue // AllowFailure: call reverted, leave Success=false
		}
		price, err := unpackAssetPrice(oracleABI, r.ReturnData)
		if err != nil {
			slog.Warn("failed to unpack asset price, treating as failure",
				"asset", assets[i].Hex(),
				"block", blockNum,
				"error", err)
			continue
		}
		out[i] = AssetPriceResult{Price: price, Success: true}
	}

	return out, nil
}

func unpackAssetPrice(oracleABI *abi.ABI, data []byte) (*big.Int, error) {
	unpacked, err := oracleABI.Unpack("getAssetPrice", data)
	if err != nil {
		return nil, err
	}
	return unpacked[0].(*big.Int), nil
}

// ScaleByDecimals converts a raw oracle price from fixed-point to float64.
// decimals specifies the oracle's price precision (e.g., 8 for Chainlink/Aave: 1e8 = $1.00).
func ScaleByDecimals(rawPrice *big.Int, decimals int) float64 {
	if rawPrice == nil || rawPrice.Sign() == 0 {
		return 0
	}
	f := new(big.Float).SetInt(rawPrice)
	divisor := new(big.Float).SetFloat64(math.Pow10(decimals))
	result, _ := new(big.Float).Quo(f, divisor).Float64()
	return result
}

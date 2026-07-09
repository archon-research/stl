package blockchain

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// curveVirtualPriceDecimals is the fixed 1e18 scale of get_virtual_price()
// across all Curve StableSwap pools, independent of coin decimals.
const curveVirtualPriceDecimals = 18

// CurveLPNGPoolConfig describes one Curve StableSwap-NG pool whose LP token is
// priced from the pool's virtual price and its coins' USD feeds. NG pools are
// their own LP ERC-20, so TokenID names the LP token deployed at PoolAddress.
// CoinFeeds carries one Chainlink-compatible USD feed per pool coin; at least
// two are required or the min() in the price formula guards nothing. Every
// CoinFeeds element carries the LP token id in FeedConfig.TokenID by
// construction (the unit prices exactly one token); a feed's coin identity is
// its position in CoinFeeds, so TokenID must not be used to key a per-coin map.
type CurveLPNGPoolConfig struct {
	TokenID     int64
	PoolAddress common.Address
	CoinFeeds   []FeedConfig
}

// FetchCurveLPNGPrices prices one NG pool's LP token in USD via a single
// hash-pinned multicall: get_virtual_price() on the pool plus
// latestRoundData() on every coin feed.
//
//	price = (virtual_price / 1e18) * min(coin USD prices)
//
// virtual_price only grows with accrued fees and is invariant to balance
// manipulation; min() bounds the LP value by the cheapest coin, because a
// depeg lets arbitrage drain the pool toward that coin and any higher
// per-coin price would overstate what LP shares can redeem. Every failed or
// non-positive sub-result is a hard error rather than a soft skip: the unit
// prices exactly one token, so there is no partial success to salvage, and
// erroring (instead of persisting 0) leaves the block unacked for retry.
func FetchCurveLPNGPrices(
	ctx context.Context,
	multicaller outbound.Multicaller,
	poolABI *abi.ABI,
	feedABI *abi.ABI,
	pool CurveLPNGPoolConfig,
	blockNum int64,
	blockHash common.Hash,
) ([]FeedPriceResult, error) {
	if len(pool.CoinFeeds) < 2 {
		return nil, fmt.Errorf("curve pool %s (tokenID %d): needs at least 2 coin feeds, got %d",
			pool.PoolAddress.Hex(), pool.TokenID, len(pool.CoinFeeds))
	}
	if blockHash == (common.Hash{}) {
		return nil, fmt.Errorf("curve pool %s (tokenID %d): missing block hash for hash-pinned read at block %d",
			pool.PoolAddress.Hex(), pool.TokenID, blockNum)
	}

	calls, err := buildCurveLPNGCalls(poolABI, feedABI, pool)
	if err != nil {
		return nil, err
	}

	results, err := multicaller.ExecuteAtHash(ctx, calls, blockHash)
	if err != nil {
		return nil, fmt.Errorf("executing multicall at block %d: %w", blockNum, err)
	}
	if len(results) != len(calls) {
		return nil, fmt.Errorf("expected %d multicall results, got %d", len(calls), len(results))
	}

	priceUSD, err := curveLPNGPriceUSD(poolABI, feedABI, pool, results, blockNum)
	if err != nil {
		return nil, err
	}

	return []FeedPriceResult{{TokenID: pool.TokenID, Price: priceUSD, Success: true}}, nil
}

// buildCurveLPNGCalls batches get_virtual_price() on the pool followed by
// latestRoundData() on each coin feed, in CoinFeeds order. AllowFailure keeps
// a reverting sub-call from aborting the whole multicall, so the caller can
// attribute the failure to the specific contract instead of surfacing an
// opaque aggregate revert; every failure is still a hard error there.
func buildCurveLPNGCalls(poolABI, feedABI *abi.ABI, pool CurveLPNGPoolConfig) ([]outbound.Call, error) {
	virtualPriceData, err := poolABI.Pack("get_virtual_price")
	if err != nil {
		return nil, fmt.Errorf("packing get_virtual_price: %w", err)
	}
	roundData, err := feedABI.Pack("latestRoundData")
	if err != nil {
		return nil, fmt.Errorf("packing latestRoundData: %w", err)
	}

	calls := make([]outbound.Call, 0, 1+len(pool.CoinFeeds))
	calls = append(calls, outbound.Call{Target: pool.PoolAddress, AllowFailure: true, CallData: virtualPriceData})
	for _, feed := range pool.CoinFeeds {
		calls = append(calls, outbound.Call{Target: feed.FeedAddress, AllowFailure: true, CallData: roundData})
	}
	return calls, nil
}

// curveLPNGPriceUSD derives the LP token's USD price from the multicall
// results: results[0] is the pool's get_virtual_price, results[1:] the coin
// feeds in CoinFeeds order.
func curveLPNGPriceUSD(poolABI, feedABI *abi.ABI, pool CurveLPNGPoolConfig, results []outbound.Result, blockNum int64) (float64, error) {
	virtualPrice, err := virtualPriceFromResult(poolABI, pool, results[0], blockNum)
	if err != nil {
		return 0, err
	}

	minCoinUSD, err := minCoinPriceUSD(feedABI, pool, results[1:], blockNum)
	if err != nil {
		return 0, err
	}

	return ScaleByDecimals(virtualPrice, curveVirtualPriceDecimals) * minCoinUSD, nil
}

func virtualPriceFromResult(poolABI *abi.ABI, pool CurveLPNGPoolConfig, result outbound.Result, blockNum int64) (*big.Int, error) {
	if !result.Success {
		return nil, fmt.Errorf("get_virtual_price call failed for pool %s (tokenID %d) at block %d",
			pool.PoolAddress.Hex(), pool.TokenID, blockNum)
	}
	virtualPrice, err := unpackVirtualPrice(poolABI, result.ReturnData)
	if err != nil {
		return nil, fmt.Errorf("unpacking get_virtual_price for pool %s at block %d: %w",
			pool.PoolAddress.Hex(), blockNum, err)
	}
	if virtualPrice.Sign() <= 0 {
		return nil, fmt.Errorf("non-positive virtual price %s for pool %s (tokenID %d) at block %d",
			virtualPrice, pool.PoolAddress.Hex(), pool.TokenID, blockNum)
	}
	return virtualPrice, nil
}

func minCoinPriceUSD(feedABI *abi.ABI, pool CurveLPNGPoolConfig, feedResults []outbound.Result, blockNum int64) (float64, error) {
	var minUSD float64
	for i, result := range feedResults {
		feed := pool.CoinFeeds[i]
		if !result.Success {
			return 0, fmt.Errorf("coin feed %s call failed for pool %s (tokenID %d) at block %d",
				feed.FeedAddress.Hex(), pool.PoolAddress.Hex(), pool.TokenID, blockNum)
		}
		answer, err := unpackLatestRoundData(feedABI, result.ReturnData)
		if err != nil {
			return 0, fmt.Errorf("unpacking latestRoundData for coin feed %s at block %d: %w",
				feed.FeedAddress.Hex(), blockNum, err)
		}
		if answer.Sign() <= 0 {
			return 0, fmt.Errorf("coin feed %s returned non-positive answer %s for pool %s at block %d",
				feed.FeedAddress.Hex(), answer, pool.PoolAddress.Hex(), blockNum)
		}
		coinUSD := ScaleByDecimals(answer, feed.FeedDecimals)
		if i == 0 || coinUSD < minUSD {
			minUSD = coinUSD
		}
	}
	return minUSD, nil
}

func unpackVirtualPrice(poolABI *abi.ABI, data []byte) (*big.Int, error) {
	unpacked, err := poolABI.Unpack("get_virtual_price", data)
	if err != nil {
		return nil, err
	}
	virtualPrice, ok := unpacked[0].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("get_virtual_price: expected *big.Int, got %T", unpacked[0])
	}
	return virtualPrice, nil
}

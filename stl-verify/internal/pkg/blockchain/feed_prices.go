package blockchain

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// FeedConfig describes a single price feed for multicall batching.
type FeedConfig struct {
	TokenID       int64
	FeedAddress   common.Address
	FeedDecimals  int
	QuoteCurrency string // "USD", "ETH", or "BTC"
}

// FeedPriceResult holds the result of a latestRoundData() call for one feed.
type FeedPriceResult struct {
	TokenID int64
	Price   float64 // decimal-converted price in the feed's quote currency
	Success bool
}

// FetchFeedPrices calls latestRoundData() on each feed via a single multicall.
// Each call uses AllowFailure: true so individual feeds can fail independently.
// Returns prices in the feed's native quote currency (not necessarily USD).
// Quote currency conversion (ETH→USD, BTC→USD) is handled by the caller.
func FetchFeedPrices(
	ctx context.Context,
	multicaller outbound.Multicaller,
	feedABI *abi.ABI,
	feeds []FeedConfig,
	blockNum int64,
) ([]FeedPriceResult, error) {
	if len(feeds) == 0 {
		return nil, nil
	}

	block := new(big.Int).SetInt64(blockNum)

	callData, err := feedABI.Pack("latestRoundData")
	if err != nil {
		return nil, fmt.Errorf("packing latestRoundData: %w", err)
	}

	calls := make([]outbound.Call, len(feeds))
	for i, feed := range feeds {
		calls[i] = outbound.Call{
			Target:       feed.FeedAddress,
			AllowFailure: true,
			CallData:     callData,
		}
	}

	results, err := multicaller.Execute(ctx, calls, block)
	if err != nil {
		return nil, fmt.Errorf("executing multicall at block %d: %w", blockNum, err)
	}

	if len(results) != len(feeds) {
		return nil, fmt.Errorf("expected %d multicall results, got %d", len(feeds), len(results))
	}

	out := make([]FeedPriceResult, len(feeds))
	for i, r := range results {
		out[i].TokenID = feeds[i].TokenID

		if !r.Success {
			continue // feed reverted (e.g., not deployed at this block)
		}

		answer, updatedAt, err := unpackLatestRoundData(feedABI, r.ReturnData)
		if err != nil {
			return nil, fmt.Errorf("unpacking latestRoundData for feed %d at block %d: %w",
				i, blockNum, err)
		}

		if answer.Sign() <= 0 {
			continue // inactive or corrupt feed
		}
		if updatedAt.Sign() == 0 {
			continue // round not complete
		}

		out[i].Price = ConvertOraclePriceToUSD(answer, feeds[i].FeedDecimals)
		out[i].Success = true
	}

	return out, nil
}

func unpackLatestRoundData(feedABI *abi.ABI, data []byte) (*big.Int, *big.Int, error) {
	unpacked, err := feedABI.Unpack("latestRoundData", data)
	if err != nil {
		return nil, nil, err
	}
	// latestRoundData returns: (uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound)
	answer := unpacked[1].(*big.Int)
	updatedAt := unpacked[3].(*big.Int)
	return answer, updatedAt, nil
}

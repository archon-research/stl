package blockchain

import (
	"context"
	"fmt"
	"log/slog"
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
// Feeds that revert on latestRoundData() (e.g. Aave SynchronicityPriceAdapters)
// are automatically retried with latestAnswer().
// Each call uses AllowFailure: true so individual feeds can fail independently.
// Returns prices in the feed's native quote currency (not necessarily USD).
// Quote currency conversion (ETH→USD, BTC→USD) is handled by the caller.
func FetchFeedPrices(
	ctx context.Context,
	multicaller outbound.Multicaller,
	feedABI *abi.ABI,
	feeds []FeedConfig,
	blockNum int64,
	logger *slog.Logger,
) ([]FeedPriceResult, error) {
	if len(feeds) == 0 {
		return nil, nil
	}

	block := new(big.Int).SetInt64(blockNum)

	out, failedIdx, err := fetchWithLatestRoundData(ctx, multicaller, feedABI, feeds, block, blockNum, logger)
	if err != nil {
		return nil, err
	}

	if len(failedIdx) > 0 {
		if err := retryWithLatestAnswer(ctx, multicaller, feedABI, feeds, block, blockNum, logger, out, failedIdx); err != nil {
			return nil, err
		}
	}

	return out, nil
}

// fetchWithLatestRoundData calls latestRoundData() on all feeds. Returns
// the results slice plus the indices of feeds that reverted.
func fetchWithLatestRoundData(
	ctx context.Context,
	multicaller outbound.Multicaller,
	feedABI *abi.ABI,
	feeds []FeedConfig,
	block *big.Int,
	blockNum int64,
	logger *slog.Logger,
) ([]FeedPriceResult, []int, error) {
	callData, err := feedABI.Pack("latestRoundData")
	if err != nil {
		return nil, nil, fmt.Errorf("packing latestRoundData: %w", err)
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
		return nil, nil, fmt.Errorf("executing multicall at block %d: %w", blockNum, err)
	}

	if len(results) != len(feeds) {
		return nil, nil, fmt.Errorf("expected %d multicall results, got %d", len(feeds), len(results))
	}

	out := make([]FeedPriceResult, len(feeds))
	var failedIdx []int
	for i, r := range results {
		out[i].TokenID = feeds[i].TokenID

		if !r.Success {
			failedIdx = append(failedIdx, i)
			continue
		}

		answer, updatedAt, err := unpackLatestRoundData(feedABI, r.ReturnData)
		if err != nil {
			return nil, nil, fmt.Errorf("unpacking latestRoundData for feed %d at block %d: %w",
				i, blockNum, err)
		}

		if answer.Sign() <= 0 {
			logger.Warn("feed returned non-positive answer",
				"feedIndex", i, "tokenID", feeds[i].TokenID, "block", blockNum)
			continue
		}
		if updatedAt.Sign() == 0 {
			logger.Warn("feed round not complete",
				"feedIndex", i, "tokenID", feeds[i].TokenID, "block", blockNum)
			continue
		}

		out[i].Price = ConvertOraclePriceToUSD(answer, feeds[i].FeedDecimals)
		out[i].Success = true
	}

	return out, failedIdx, nil
}

// retryWithLatestAnswer retries failed feeds using latestAnswer().
// Some adapters (e.g. Aave SynchronicityPriceAdapter for sDAI/USD) only
// implement latestAnswer() and not the full latestRoundData() interface.
func retryWithLatestAnswer(
	ctx context.Context,
	multicaller outbound.Multicaller,
	feedABI *abi.ABI,
	feeds []FeedConfig,
	block *big.Int,
	blockNum int64,
	logger *slog.Logger,
	out []FeedPriceResult,
	failedIdx []int,
) error {
	callData, err := feedABI.Pack("latestAnswer")
	if err != nil {
		return fmt.Errorf("packing latestAnswer: %w", err)
	}

	calls := make([]outbound.Call, len(failedIdx))
	for j, i := range failedIdx {
		calls[j] = outbound.Call{
			Target:       feeds[i].FeedAddress,
			AllowFailure: true,
			CallData:     callData,
		}
	}

	results, err := multicaller.Execute(ctx, calls, block)
	if err != nil {
		return fmt.Errorf("executing latestAnswer multicall at block %d: %w", blockNum, err)
	}

	if len(results) != len(failedIdx) {
		return fmt.Errorf("latestAnswer: expected %d results, got %d", len(failedIdx), len(results))
	}

	for j, r := range results {
		i := failedIdx[j]
		if !r.Success {
			continue
		}

		answer, err := unpackLatestAnswer(feedABI, r.ReturnData)
		if err != nil {
			return fmt.Errorf("unpacking latestAnswer for feed %d at block %d: %w", i, blockNum, err)
		}

		if answer.Sign() <= 0 {
			logger.Warn("latestAnswer returned non-positive value",
				"feedIndex", i, "tokenID", feeds[i].TokenID, "block", blockNum)
			continue
		}

		logger.Debug("feed recovered via latestAnswer",
			"feedIndex", i, "tokenID", feeds[i].TokenID, "block", blockNum)
		out[i].Price = ConvertOraclePriceToUSD(answer, feeds[i].FeedDecimals)
		out[i].Success = true
	}

	return nil
}

func unpackLatestAnswer(feedABI *abi.ABI, data []byte) (*big.Int, error) {
	unpacked, err := feedABI.Unpack("latestAnswer", data)
	if err != nil {
		return nil, err
	}
	return unpacked[0].(*big.Int), nil
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

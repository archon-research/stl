package blockchain

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"strings"

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

func validateFeeds(feeds []FeedConfig) error {
	for i, f := range feeds {
		if f.FeedAddress == (common.Address{}) {
			return fmt.Errorf("feed %d (tokenID %d): zero feed address", i, f.TokenID)
		}
		if f.FeedDecimals <= 0 {
			return fmt.Errorf("feed %d (tokenID %d): invalid feed decimals %d", i, f.TokenID, f.FeedDecimals)
		}
	}
	return nil
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
//
// feedABI is passed as a parameter rather than stored in FeedConfig because
// all feeds share the same ABI (AggregatorV3Interface).
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

	if err := validateFeeds(feeds); err != nil {
		return nil, fmt.Errorf("invalid feed config: %w", err)
	}

	block := new(big.Int).SetInt64(blockNum)

	out, failedFeedResults, err := fetchWithLatestRoundData(ctx, multicaller, feedABI, feeds, block, blockNum, logger)
	if err != nil {
		return nil, err
	}

	if len(failedFeedResults) > 0 {
		if err := retryWithLatestAnswer(ctx, multicaller, feedABI, feeds, block, blockNum, logger, out, failedFeedResults); err != nil {
			return nil, err
		}
	}

	logFeedFailures(out, feeds, blockNum, logger)

	return out, nil
}

// logFeedFailures logs individual feed failures and emits an error-level message
// when all feeds for an oracle have failed.
func logFeedFailures(results []FeedPriceResult, feeds []FeedConfig, blockNum int64, logger *slog.Logger) {
	var failCount int
	for i, r := range results {
		if !r.Success {
			failCount++
			logger.Warn("feed call failed",
				"tokenID", r.TokenID,
				"feedAddress", feeds[i].FeedAddress.Hex(),
				"block", blockNum)
		}
	}
	if failCount == len(results) && len(results) > 0 {
		logger.Error("all feeds failed, check configuration",
			"block", blockNum,
			"feedCount", len(results))
	}
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
	var failedFeedResults []int
	for i, r := range results {
		out[i].TokenID = feeds[i].TokenID

		if !r.Success {
			failedFeedResults = append(failedFeedResults, i)
			continue
		}

		answer, err := unpackLatestRoundData(feedABI, r.ReturnData)
		if err != nil {
			return nil, nil, fmt.Errorf("unpacking latestRoundData for feed %d at block %d: %w",
				i, blockNum, err)
		}

		if answer.Sign() <= 0 {
			logger.Warn("feed returned non-positive answer, will retry with latestAnswer",
				"feedIndex", i, "tokenID", feeds[i].TokenID,
				"feedAddress", feeds[i].FeedAddress.Hex(), "block", blockNum)
			failedFeedResults = append(failedFeedResults, i)
			continue
		}

		out[i].Price = ScaleByDecimals(answer, feeds[i].FeedDecimals)
		out[i].Success = true
	}

	return out, failedFeedResults, nil
}

// retryWithLatestAnswer retries failed feeds using latestAnswer().
// Some adapters (e.g. Aave SynchronicityPriceAdapter for sDAI/USD) only
// implement latestAnswer() and not the full latestRoundData() interface.
//
// Limitation: latestAnswer() returns only int256 (the price). Unlike
// latestRoundData(), it does not expose updatedAt, so round-completeness
// and staleness cannot be verified for feeds that only support this method.
func retryWithLatestAnswer(
	ctx context.Context,
	multicaller outbound.Multicaller,
	feedABI *abi.ABI,
	feeds []FeedConfig,
	block *big.Int,
	blockNum int64,
	logger *slog.Logger,
	out []FeedPriceResult,
	failedFeedResults []int,
) error {
	callData, err := feedABI.Pack("latestAnswer")
	if err != nil {
		return fmt.Errorf("packing latestAnswer: %w", err)
	}

	calls := make([]outbound.Call, len(failedFeedResults))
	for j, i := range failedFeedResults {
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

	if len(results) != len(failedFeedResults) {
		return fmt.Errorf("latestAnswer: expected %d results, got %d", len(failedFeedResults), len(results))
	}

	for j, r := range results {
		i := failedFeedResults[j]
		if !r.Success {
			continue
		}

		answer, err := unpackLatestAnswer(feedABI, r.ReturnData)
		if err != nil {
			return fmt.Errorf("unpacking latestAnswer for feed %d at block %d: %w", i, blockNum, err)
		}

		if answer.Sign() <= 0 {
			logger.Warn("latestAnswer returned non-positive value",
				"feedIndex", i, "tokenID", feeds[i].TokenID,
				"feedAddress", feeds[i].FeedAddress.Hex(), "block", blockNum)
			continue
		}

		logger.Debug("feed using latestAnswer fallback",
			"feedIndex", i, "tokenID", feeds[i].TokenID, "block", blockNum)
		out[i].Price = ScaleByDecimals(answer, feeds[i].FeedDecimals)
		out[i].Success = true
	}

	return nil
}

func unpackLatestAnswer(feedABI *abi.ABI, data []byte) (*big.Int, error) {
	unpacked, err := feedABI.Unpack("latestAnswer", data)
	if err != nil {
		return nil, err
	}
	answer, ok := unpacked[0].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("latestAnswer: expected *big.Int, got %T", unpacked[0])
	}
	return answer, nil
}

func unpackLatestRoundData(feedABI *abi.ABI, data []byte) (*big.Int, error) {
	unpacked, err := feedABI.Unpack("latestRoundData", data)
	if err != nil {
		return nil, err
	}
	// latestRoundData returns: (uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound)
	answer, ok := unpacked[1].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("latestRoundData answer: expected *big.Int, got %T", unpacked[1])
	}
	return answer, nil
}

// ValidateFeedDecimals calls decimals() on each feed and compares the on-chain
// result against the configured FeedDecimals. A mismatch would cause prices to
// be off by orders of magnitude, so any mismatch is a hard error.
// Feeds where decimals() reverts are logged as warnings and skipped (the feed
// may not exist at the given block).
func ValidateFeedDecimals(
	ctx context.Context,
	multicaller outbound.Multicaller,
	feedABI *abi.ABI,
	feeds []FeedConfig,
	blockNum int64,
	logger *slog.Logger,
) error {
	if len(feeds) == 0 {
		return nil
	}

	callData, err := feedABI.Pack("decimals")
	if err != nil {
		return fmt.Errorf("packing decimals(): %w", err)
	}

	calls := make([]outbound.Call, len(feeds))
	for i, feed := range feeds {
		calls[i] = outbound.Call{
			Target:       feed.FeedAddress,
			AllowFailure: true,
			CallData:     callData,
		}
	}

	block := new(big.Int).SetInt64(blockNum)
	results, err := multicaller.Execute(ctx, calls, block)
	if err != nil {
		return fmt.Errorf("executing decimals() multicall at block %d: %w", blockNum, err)
	}

	if len(results) != len(feeds) {
		return fmt.Errorf("decimals() multicall: expected %d results, got %d", len(feeds), len(results))
	}

	var mismatches []string
	for i, r := range results {
		if !r.Success {
			logger.Warn("decimals() call reverted, skipping validation",
				"feedIndex", i,
				"tokenID", feeds[i].TokenID,
				"feedAddress", feeds[i].FeedAddress.Hex(),
				"block", blockNum)
			continue
		}

		onChain, err := unpackDecimals(feedABI, r.ReturnData)
		if err != nil {
			logger.Warn("failed to unpack decimals(), skipping validation",
				"feedIndex", i,
				"tokenID", feeds[i].TokenID,
				"feedAddress", feeds[i].FeedAddress.Hex(),
				"block", blockNum,
				"error", err)
			continue
		}

		if int(onChain) != feeds[i].FeedDecimals {
			mismatches = append(mismatches,
				fmt.Sprintf("feed %s (tokenID %d): on-chain decimals=%d, configured=%d",
					feeds[i].FeedAddress.Hex(), feeds[i].TokenID, onChain, feeds[i].FeedDecimals))
		}
	}

	if len(mismatches) > 0 {
		return fmt.Errorf("feed decimals mismatch — prices would be wrong:\n  %s", strings.Join(mismatches, "\n  "))
	}

	return nil
}

func unpackDecimals(feedABI *abi.ABI, data []byte) (uint8, error) {
	unpacked, err := feedABI.Unpack("decimals", data)
	if err != nil {
		return 0, err
	}
	decimals, ok := unpacked[0].(uint8)
	if !ok {
		return 0, fmt.Errorf("decimals: expected uint8, got %T", unpacked[0])
	}
	return decimals, nil
}

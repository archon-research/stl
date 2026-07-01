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

// ERC4626VaultConfig describes one ERC-4626 vault priced via share→underlying
// conversion. The vault's USD price is convertToAssets(1 share) scaled to a
// per-share underlying amount, times the underlying token's USD feed price.
type ERC4626VaultConfig struct {
	TokenID            int64
	VaultAddress       common.Address
	ShareDecimals      int            // decimals of the vault share token (input scalar for convertToAssets)
	UnderlyingFeed     common.Address // Chainlink-compatible USD feed for the underlying token
	UnderlyingDecimals int            // decimals of the underlying token (output scale of convertToAssets)
	FeedDecimals       int            // decimals of the underlying USD feed
}

// FetchERC4626SharePrices prices each vault share in USD via a single multicall.
// Per vault it batches convertToAssets(10^ShareDecimals) on the vault and
// latestRoundData() on the underlying USD feed. Both calls use AllowFailure so
// vaults fail independently; a vault whose vault call or underlying feed fails
// (or returns a non-positive feed answer) is reported Success: false rather than
// producing a wrong price. If every vault fails it returns an error so the caller
// retries instead of treating the block as a successful no-op.
func FetchERC4626SharePrices(
	ctx context.Context,
	multicaller outbound.Multicaller,
	shareABI *abi.ABI,
	feedABI *abi.ABI,
	vaults []ERC4626VaultConfig,
	blockNum int64,
	logger *slog.Logger,
) ([]FeedPriceResult, error) {
	if len(vaults) == 0 {
		return nil, nil
	}

	calls, err := buildERC4626Calls(shareABI, feedABI, vaults)
	if err != nil {
		return nil, err
	}

	block := new(big.Int).SetInt64(blockNum)
	results, err := multicaller.Execute(ctx, calls, block)
	if err != nil {
		return nil, fmt.Errorf("executing multicall at block %d: %w", blockNum, err)
	}
	if len(results) != len(calls) {
		return nil, fmt.Errorf("expected %d multicall results, got %d", len(calls), len(results))
	}

	out := make([]FeedPriceResult, len(vaults))
	var failCount int
	for i, vault := range vaults {
		out[i].TokenID = vault.TokenID

		priceUSD, ok, err := sharePriceUSD(shareABI, feedABI, vault, results[2*i], results[2*i+1], blockNum, logger)
		if err != nil {
			return nil, err
		}
		if !ok {
			failCount++
			continue
		}
		out[i].Price = priceUSD
		out[i].Success = true
	}

	// A total failure is never a legitimate no-op: every vault reverting (or its
	// feed reverting) at a block means the price is unknown, not unchanged. With a
	// single seeded vault this is the common case, so returning nil here would let
	// the worker delete the SQS message / backfill mark the block done and never
	// retry, silently dropping the price. Partial success stays a soft skip.
	if failCount == len(vaults) {
		return nil, fmt.Errorf("all %d erc4626 vaults failed at block %d, check configuration", len(vaults), blockNum)
	}

	return out, nil
}

// ERC4626UnderlyingFeeds projects each vault's underlying USD feed into a
// FeedConfig so ValidateFeedDecimals can verify on-chain feed decimals match the
// seeded feed_decimals (a mismatch mis-scales share prices).
func ERC4626UnderlyingFeeds(vaults []ERC4626VaultConfig) []FeedConfig {
	feeds := make([]FeedConfig, len(vaults))
	for i, v := range vaults {
		feeds[i] = FeedConfig{
			TokenID:       v.TokenID,
			FeedAddress:   v.UnderlyingFeed,
			FeedDecimals:  v.FeedDecimals,
			QuoteCurrency: "USD",
		}
	}
	return feeds
}

func buildERC4626Calls(shareABI, feedABI *abi.ABI, vaults []ERC4626VaultConfig) ([]outbound.Call, error) {
	roundData, err := feedABI.Pack("latestRoundData")
	if err != nil {
		return nil, fmt.Errorf("packing latestRoundData: %w", err)
	}

	calls := make([]outbound.Call, 0, len(vaults)*2)
	for _, vault := range vaults {
		oneShare := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(vault.ShareDecimals)), nil)
		convertData, err := shareABI.Pack("convertToAssets", oneShare)
		if err != nil {
			return nil, fmt.Errorf("packing convertToAssets for token %d: %w", vault.TokenID, err)
		}
		calls = append(calls,
			outbound.Call{Target: vault.VaultAddress, AllowFailure: true, CallData: convertData},
			outbound.Call{Target: vault.UnderlyingFeed, AllowFailure: true, CallData: roundData},
		)
	}
	return calls, nil
}

// sharePriceUSD derives one vault share's USD price from its convertToAssets and
// underlying-feed multicall results. ok is false when either call is unusable, in
// which case the vault is skipped (logged at warn). A genuine decode error of a
// successful call is returned as an error so a malformed response is never
// silently dropped.
func sharePriceUSD(
	shareABI, feedABI *abi.ABI,
	vault ERC4626VaultConfig,
	convertResult, feedResult outbound.Result,
	blockNum int64,
	logger *slog.Logger,
) (float64, bool, error) {
	if !convertResult.Success {
		logger.Warn("convertToAssets call failed",
			"tokenID", vault.TokenID, "vault", vault.VaultAddress.Hex(), "block", blockNum)
		return 0, false, nil
	}
	if !feedResult.Success {
		logger.Warn("underlying feed call failed",
			"tokenID", vault.TokenID, "feed", vault.UnderlyingFeed.Hex(), "block", blockNum)
		return 0, false, nil
	}

	assets, err := unpackConvertToAssets(shareABI, convertResult.ReturnData)
	if err != nil {
		return 0, false, fmt.Errorf("unpacking convertToAssets for token %d at block %d: %w", vault.TokenID, blockNum, err)
	}

	answer, err := unpackLatestRoundData(feedABI, feedResult.ReturnData)
	if err != nil {
		return 0, false, fmt.Errorf("unpacking latestRoundData for token %d at block %d: %w", vault.TokenID, blockNum, err)
	}
	if answer.Sign() <= 0 {
		logger.Warn("underlying feed returned non-positive answer",
			"tokenID", vault.TokenID, "feed", vault.UnderlyingFeed.Hex(), "block", blockNum)
		return 0, false, nil
	}

	shareRatio := ScaleByDecimals(assets, vault.UnderlyingDecimals)
	underlyingUSD := ScaleByDecimals(answer, vault.FeedDecimals)
	return shareRatio * underlyingUSD, true, nil
}

func unpackConvertToAssets(shareABI *abi.ABI, data []byte) (*big.Int, error) {
	unpacked, err := shareABI.Unpack("convertToAssets", data)
	if err != nil {
		return nil, err
	}
	assets, ok := unpacked[0].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("convertToAssets: expected *big.Int, got %T", unpacked[0])
	}
	return assets, nil
}

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

// FetchERC4626SharePrices prices each vault share in USD via a single multicall,
// pinned to blockHash (see executeOracleState). Per vault it batches
// convertToAssets(10^ShareDecimals) on the vault and latestRoundData() on the
// underlying USD feed — both per-block state, so a reorg must not answer from
// the wrong fork. Both calls use AllowFailure so vaults fail independently; a
// vault whose vault call or underlying feed fails (or returns a non-positive
// value) is reported Success: false rather than producing a wrong price. If
// every vault fails it returns an error.
func FetchERC4626SharePrices(
	ctx context.Context,
	multicaller outbound.Multicaller,
	shareABI *abi.ABI,
	feedABI *abi.ABI,
	vaults []ERC4626VaultConfig,
	blockNum int64,
	blockHash common.Hash,
	logger *slog.Logger,
) ([]FeedPriceResult, error) {
	if len(vaults) == 0 {
		return nil, nil
	}

	calls, err := buildERC4626Calls(shareABI, feedABI, vaults)
	if err != nil {
		return nil, err
	}

	results, err := executeOracleState(ctx, multicaller, calls, blockNum, blockHash)
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

	// A total failure is never a legitimate no-op: the price is unknown, not
	// unchanged. Returning an error makes the hole loud: the SQS worker retries
	// the message, and the backfill service logs the block as failed and
	// continues (it does not abort the run). Partial success stays a soft skip.
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

// ValidateERC4626UnderlyingDecimals verifies on-chain that the underlying token's
// decimals() matches the configured UnderlyingDecimals for each vault. A mismatch
// would mis-scale the share ratio and produce wrong USD prices.
//
// Two sequential multicall batches per oracle: batch 1 calls asset() on each vault
// to resolve the underlying token address; batch 2 calls decimals() on those
// addresses (reusing the feed ABI's decimals selector, same ERC-20 standard). Any
// vault whose asset() reverts is a hard error rather than a soft skip, because
// a vault with no underlying is unrecoverable configuration corruption.
func ValidateERC4626UnderlyingDecimals(
	ctx context.Context,
	multicaller outbound.Multicaller,
	shareABI *abi.ABI,
	feedABI *abi.ABI,
	vaults []ERC4626VaultConfig,
	blockNum int64,
	logger *slog.Logger,
) error {
	if len(vaults) == 0 {
		return nil
	}

	assetCallData, err := shareABI.Pack("asset")
	if err != nil {
		return fmt.Errorf("packing asset(): %w", err)
	}

	assetCalls := make([]outbound.Call, len(vaults))
	for i, vault := range vaults {
		assetCalls[i] = outbound.Call{Target: vault.VaultAddress, AllowFailure: false, CallData: assetCallData}
	}

	block := new(big.Int).SetInt64(blockNum)
	assetResults, err := multicaller.Execute(ctx, assetCalls, block)
	if err != nil {
		return fmt.Errorf("executing asset() multicall at block %d: %w", blockNum, err)
	}
	if len(assetResults) != len(vaults) {
		return fmt.Errorf("asset() multicall: expected %d results, got %d", len(vaults), len(assetResults))
	}

	underlyingAddrs := make([]common.Address, len(vaults))
	for i, r := range assetResults {
		if !r.Success {
			return fmt.Errorf("asset() call failed for vault %s (tokenID %d) at block %d",
				vaults[i].VaultAddress.Hex(), vaults[i].TokenID, blockNum)
		}
		addr, err := unpackAsset(shareABI, r.ReturnData)
		if err != nil {
			return fmt.Errorf("unpacking asset() for tokenID %d: %w", vaults[i].TokenID, err)
		}
		underlyingAddrs[i] = addr
	}

	decimalsCallData, err := feedABI.Pack("decimals")
	if err != nil {
		return fmt.Errorf("packing decimals(): %w", err)
	}

	decimalsCalls := make([]outbound.Call, len(vaults))
	for i, addr := range underlyingAddrs {
		decimalsCalls[i] = outbound.Call{Target: addr, AllowFailure: true, CallData: decimalsCallData}
	}

	decimalsResults, err := multicaller.Execute(ctx, decimalsCalls, block)
	if err != nil {
		return fmt.Errorf("executing underlying decimals() multicall at block %d: %w", blockNum, err)
	}
	if len(decimalsResults) != len(vaults) {
		return fmt.Errorf("underlying decimals() multicall: expected %d results, got %d", len(vaults), len(decimalsResults))
	}

	var mismatches []string
	for i, r := range decimalsResults {
		if !r.Success {
			logger.Warn("underlying token decimals() reverted, skipping validation",
				"tokenID", vaults[i].TokenID,
				"underlying", underlyingAddrs[i].Hex(),
				"block", blockNum)
			continue
		}

		onChain, err := unpackDecimals(feedABI, r.ReturnData)
		if err != nil {
			logger.Warn("failed to unpack underlying decimals(), skipping validation",
				"tokenID", vaults[i].TokenID,
				"underlying", underlyingAddrs[i].Hex(),
				"block", blockNum,
				"error", err)
			continue
		}

		if int(onChain) != vaults[i].UnderlyingDecimals {
			mismatches = append(mismatches,
				fmt.Sprintf("underlying %s (tokenID %d): on-chain decimals=%d, configured=%d",
					underlyingAddrs[i].Hex(), vaults[i].TokenID, onChain, vaults[i].UnderlyingDecimals))
		}
	}

	if len(mismatches) > 0 {
		return fmt.Errorf("underlying decimals mismatch -- prices would be wrong:\n  %s", strings.Join(mismatches, "\n  "))
	}

	return nil
}

func unpackAsset(shareABI *abi.ABI, data []byte) (common.Address, error) {
	unpacked, err := shareABI.Unpack("asset", data)
	if err != nil {
		return common.Address{}, err
	}
	addr, ok := unpacked[0].(common.Address)
	if !ok {
		return common.Address{}, fmt.Errorf("asset: expected common.Address, got %T", unpacked[0])
	}
	return addr, nil
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
	if assets.Sign() <= 0 {
		logger.Warn("convertToAssets returned non-positive assets",
			"tokenID", vault.TokenID, "vault", vault.VaultAddress.Hex(), "block", blockNum)
		return 0, false, nil
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

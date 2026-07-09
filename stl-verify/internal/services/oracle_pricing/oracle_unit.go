// Package oracle_pricing provides shared types and logic for oracle price
// fetching, used by both the live price worker and historical backfill services.
package oracle_pricing

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sort"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// quoteCurrencyTokenAddr maps quote currencies to their well-known mainnet
// token addresses, used for reference feed identification.
var quoteCurrencyTokenAddr = map[string]common.Address{
	"ETH": common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"), // WETH
	"BTC": common.HexToAddress("0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"), // WBTC
}

// OracleUnit holds everything needed to fetch prices for one oracle.
type OracleUnit struct {
	Oracle     *entity.Oracle
	OracleID   int16
	OracleAddr common.Address
	TokenAddrs []common.Address // for aave_oracle: ordered list of token addresses
	// TokenIDs lists the priced tokens: parallel to TokenAddrs (aave), Feeds
	// (feed oracles), or ERC4626Vaults (erc4626_share). For curve_lp_ng it is
	// a single element, the LP token, deliberately NOT parallel to
	// CurveLPNGPool.CoinFeeds: all coin feeds price that one LP token.
	TokenIDs    []int64
	Feeds       []blockchain.FeedConfig // for chainlink_feed: per-feed config
	RefFeedIdx  map[string]int          // quote currency → index in Feeds[] for USD-denominated reference
	NonUSDFeeds map[int]string          // feed index → quote currency for non-USD feeds

	ERC4626Vaults []blockchain.ERC4626VaultConfig // for erc4626_share: per-vault config

	CurveLPNGPool *blockchain.CurveLPNGPoolConfig // for curve_lp_ng: the one pool this oracle prices
}

// LoadOracleUnits loads all enabled oracles from DB, deduplicates by oracle ID,
// and builds OracleUnit structs for each.
func LoadOracleUnits(ctx context.Context, repo outbound.OnchainPriceRepository, chainID int64, logger *slog.Logger) ([]*OracleUnit, error) {
	allOracles, err := repo.GetEnabledOraclesByChain(ctx, chainID)
	if err != nil {
		return nil, fmt.Errorf("getting enabled oracles: %w", err)
	}

	seen := make(map[int64]bool)
	var units []*OracleUnit

	for _, oracle := range allOracles {
		if seen[oracle.ID] {
			continue
		}
		seen[oracle.ID] = true

		unit, err := buildOracleUnit(ctx, repo, oracle)
		if err != nil {
			return nil, fmt.Errorf("building oracle unit %q: %w", oracle.Name, err)
		}
		if unit == nil {
			return nil, fmt.Errorf("oracle %q has no enabled assets", oracle.Name)
		}
		units = append(units, unit)
	}

	logger.Info("loaded oracle units",
		"loaded", len(units),
		"total", len(seen))

	return units, nil
}

// ConvertNonUSDPrices converts non-USD feed prices to USD using reference feeds.
// Returns a new slice with converted prices. Feeds without available reference
// prices are marked as failed in the returned slice.
func ConvertNonUSDPrices(results []blockchain.FeedPriceResult, unit *OracleUnit, logger *slog.Logger, blockNum int64) []blockchain.FeedPriceResult {
	out := make([]blockchain.FeedPriceResult, len(results))
	copy(out, results)

	for feedIdx, quoteCurrency := range unit.NonUSDFeeds {
		if !out[feedIdx].Success {
			continue
		}
		refIdx, hasRef := unit.RefFeedIdx[quoteCurrency]
		if !hasRef || !out[refIdx].Success {
			out[feedIdx].Success = false
			out[feedIdx].Price = 0
			logger.Warn("reference price unavailable, discarding non-USD feed price",
				"oracle", unit.Oracle.Name,
				"feed_token", unit.Feeds[feedIdx].TokenID,
				"feedAddress", unit.Feeds[feedIdx].FeedAddress.Hex(),
				"quote", quoteCurrency,
				"block", blockNum)
			continue
		}
		out[feedIdx].Price *= out[refIdx].Price
	}
	return out
}

func buildOracleUnit(ctx context.Context, repo outbound.OnchainPriceRepository, oracle *entity.Oracle) (*OracleUnit, error) {
	var (
		unit *OracleUnit
		err  error
	)
	switch {
	case oracle.OracleType.IsFeedOracle():
		unit, err = buildFeedUnit(ctx, repo, oracle)
	case oracle.OracleType.IsERC4626Oracle():
		unit, err = buildERC4626Unit(ctx, repo, oracle)
	case oracle.OracleType.IsCurveLPNGOracle():
		unit, err = buildCurveLPNGUnit(ctx, repo, oracle)
	default:
		unit, err = buildAaveUnit(ctx, repo, oracle)
	}
	if err != nil {
		return nil, err
	}
	if unit != nil {
		if oracle.ID < 1 || oracle.ID > math.MaxInt16 {
			return nil, fmt.Errorf("oracle ID %d out of int16 range [1, %d]", oracle.ID, math.MaxInt16)
		}
		unit.OracleID = int16(oracle.ID)
	}
	return unit, nil
}

func buildAaveUnit(ctx context.Context, repo outbound.OnchainPriceRepository, oracle *entity.Oracle) (*OracleUnit, error) {
	assets, err := repo.GetEnabledAssets(ctx, oracle.ID)
	if err != nil {
		return nil, fmt.Errorf("getting enabled assets: %w", err)
	}
	if len(assets) == 0 {
		return nil, nil
	}

	tokenInfos, err := repo.GetTokenInfos(ctx, oracle.ID)
	if err != nil {
		return nil, fmt.Errorf("getting token infos: %w", err)
	}

	tokenAddrs := make([]common.Address, len(assets))
	tokenIDs := make([]int64, len(assets))
	for i, asset := range assets {
		info, ok := tokenInfos[asset.TokenID]
		if !ok {
			return nil, fmt.Errorf("token address not found for token_id %d", asset.TokenID)
		}
		tokenAddrs[i] = common.BytesToAddress(info.Address)
		tokenIDs[i] = asset.TokenID
	}

	if oracle.Address == (common.Address{}) {
		return nil, fmt.Errorf("aave oracle %q: oracle address is required for aave_oracle type", oracle.Name)
	}

	return &OracleUnit{
		Oracle:     oracle,
		OracleAddr: oracle.Address,
		TokenAddrs: tokenAddrs,
		TokenIDs:   tokenIDs,
	}, nil
}

func buildFeedUnit(ctx context.Context, repo outbound.OnchainPriceRepository, oracle *entity.Oracle) (*OracleUnit, error) {
	assets, err := repo.GetEnabledAssets(ctx, oracle.ID)
	if err != nil {
		return nil, fmt.Errorf("getting enabled assets: %w", err)
	}
	if len(assets) == 0 {
		return nil, nil
	}

	tokenInfos, err := repo.GetTokenInfos(ctx, oracle.ID)
	if err != nil {
		return nil, fmt.Errorf("getting token infos: %w", err)
	}

	feeds := make([]blockchain.FeedConfig, len(assets))
	tokenIDs := make([]int64, len(assets))
	for i, asset := range assets {
		if asset.FeedAddress == (common.Address{}) {
			return nil, fmt.Errorf("feed address missing for token_id %d", asset.TokenID)
		}
		decimals := asset.FeedDecimals
		if decimals <= 0 {
			return nil, fmt.Errorf("invalid feed decimals for token_id %d: %d", asset.TokenID, decimals)
		}
		if asset.QuoteCurrency == "" {
			return nil, fmt.Errorf("quote currency missing for token_id %d", asset.TokenID)
		}
		feeds[i] = blockchain.FeedConfig{
			TokenID:       asset.TokenID,
			FeedAddress:   asset.FeedAddress,
			FeedDecimals:  decimals,
			QuoteCurrency: string(asset.QuoteCurrency),
		}
		tokenIDs[i] = asset.TokenID
	}

	tokenAddrs := tokenInfosToAddressMap(tokenInfos)
	refFeedIdx, nonUSDFeeds := buildRefFeedIdx(feeds, tokenAddrs)

	if err := validateRefFeeds(nonUSDFeeds, refFeedIdx, oracle.Name); err != nil {
		return nil, err
	}

	return &OracleUnit{
		Oracle:      oracle,
		TokenIDs:    tokenIDs,
		Feeds:       feeds,
		RefFeedIdx:  refFeedIdx,
		NonUSDFeeds: nonUSDFeeds,
	}, nil
}

// buildERC4626Unit builds a unit that prices ERC-4626 vault shares. Each enabled
// asset is a vault token whose USD price is convertToAssets(1 share) in underlying
// units times the underlying token's USD feed (carried on the asset's feed_address).
// Underlying decimals equal the vault's share decimals for the peg vaults this
// oracle type covers (e.g. fsUSDS 18 ⇄ USDS 18), so the single token-decimals value
// scales both the convertToAssets input and its output.
func buildERC4626Unit(ctx context.Context, repo outbound.OnchainPriceRepository, oracle *entity.Oracle) (*OracleUnit, error) {
	assets, err := repo.GetEnabledAssets(ctx, oracle.ID)
	if err != nil {
		return nil, fmt.Errorf("getting enabled assets: %w", err)
	}
	if len(assets) == 0 {
		return nil, nil
	}

	tokenInfos, err := repo.GetTokenInfos(ctx, oracle.ID)
	if err != nil {
		return nil, fmt.Errorf("getting token infos: %w", err)
	}

	vaults := make([]blockchain.ERC4626VaultConfig, len(assets))
	tokenIDs := make([]int64, len(assets))
	for i, asset := range assets {
		info, ok := tokenInfos[asset.TokenID]
		if !ok {
			return nil, fmt.Errorf("token address not found for token_id %d", asset.TokenID)
		}
		decimals := info.Decimals
		if decimals == 0 {
			return nil, fmt.Errorf("token decimals not found for token_id %d", asset.TokenID)
		}
		if asset.FeedAddress == (common.Address{}) {
			return nil, fmt.Errorf("underlying feed address missing for token_id %d", asset.TokenID)
		}
		if asset.FeedDecimals <= 0 {
			return nil, fmt.Errorf("invalid underlying feed decimals for token_id %d: %d", asset.TokenID, asset.FeedDecimals)
		}
		if asset.QuoteCurrency != entity.QuoteCurrencyUSD {
			return nil, fmt.Errorf("erc4626 underlying feed for token_id %d must be USD-denominated, got %q", asset.TokenID, asset.QuoteCurrency)
		}

		vaults[i] = blockchain.ERC4626VaultConfig{
			TokenID:            asset.TokenID,
			VaultAddress:       common.BytesToAddress(info.Address),
			ShareDecimals:      decimals,
			UnderlyingFeed:     asset.FeedAddress,
			UnderlyingDecimals: decimals,
			FeedDecimals:       asset.FeedDecimals,
		}
		tokenIDs[i] = asset.TokenID
	}

	return &OracleUnit{
		Oracle:        oracle,
		TokenIDs:      tokenIDs,
		ERC4626Vaults: vaults,
	}, nil
}

// buildCurveLPNGUnit builds a unit that prices a Curve StableSwap-NG pool's LP
// token. The registry shape is one oracle row per pool (oracle.Address is the
// pool, which is also the LP ERC-20) plus one oracle_asset row per pool coin,
// all carrying the same LP token_id and each coin's USD feed in
// feed_address/feed_decimals. Every shape violation is an error here rather
// than at fetch time, so a malformed registry fails the worker at startup
// instead of poison-stalling block processing.
func buildCurveLPNGUnit(ctx context.Context, repo outbound.OnchainPriceRepository, oracle *entity.Oracle) (*OracleUnit, error) {
	assets, err := repo.GetEnabledAssets(ctx, oracle.ID)
	if err != nil {
		return nil, fmt.Errorf("getting enabled assets: %w", err)
	}
	if len(assets) == 0 {
		return nil, nil
	}

	if oracle.Address == (common.Address{}) {
		return nil, fmt.Errorf("curve_lp_ng oracle %q: oracle address (the pool) is required", oracle.Name)
	}
	if len(assets) < 2 {
		return nil, fmt.Errorf("curve_lp_ng oracle %q: needs at least 2 coin feeds for the min() price guard, got %d", oracle.Name, len(assets))
	}

	lpTokenID := assets[0].TokenID
	feeds := make([]blockchain.FeedConfig, len(assets))
	for i, asset := range assets {
		if asset.TokenID != lpTokenID {
			return nil, fmt.Errorf("curve_lp_ng oracle %q: all assets must share the same LP token, got token_ids %d and %d", oracle.Name, lpTokenID, asset.TokenID)
		}
		if asset.FeedAddress == (common.Address{}) {
			return nil, fmt.Errorf("curve_lp_ng oracle %q: coin feed address missing on asset %d", oracle.Name, asset.ID)
		}
		if asset.FeedDecimals <= 0 {
			return nil, fmt.Errorf("curve_lp_ng oracle %q: invalid coin feed decimals on asset %d: %d", oracle.Name, asset.ID, asset.FeedDecimals)
		}
		if asset.QuoteCurrency != entity.QuoteCurrencyUSD {
			return nil, fmt.Errorf("curve_lp_ng oracle %q: coin feed on asset %d must be USD-denominated, got %q", oracle.Name, asset.ID, asset.QuoteCurrency)
		}
		feeds[i] = blockchain.FeedConfig{
			TokenID:       lpTokenID,
			FeedAddress:   asset.FeedAddress,
			FeedDecimals:  asset.FeedDecimals,
			QuoteCurrency: string(asset.QuoteCurrency),
		}
	}

	tokenInfos, err := repo.GetTokenInfos(ctx, oracle.ID)
	if err != nil {
		return nil, fmt.Errorf("getting token infos: %w", err)
	}
	info, ok := tokenInfos[lpTokenID]
	if !ok {
		return nil, fmt.Errorf("token address not found for token_id %d", lpTokenID)
	}
	if lpAddr := common.BytesToAddress(info.Address); lpAddr != oracle.Address {
		return nil, fmt.Errorf("curve_lp_ng oracle %q: LP token %d address %s must equal the pool address %s (NG pools are their own LP ERC-20)",
			oracle.Name, lpTokenID, lpAddr.Hex(), oracle.Address.Hex())
	}

	return &OracleUnit{
		Oracle:   oracle,
		TokenIDs: []int64{lpTokenID},
		CurveLPNGPool: &blockchain.CurveLPNGPoolConfig{
			TokenID:     lpTokenID,
			PoolAddress: oracle.Address,
			CoinFeeds:   feeds,
		},
	}, nil
}

// validateRefFeeds verifies that every non-USD quote currency used by feeds has a
// corresponding USD-denominated reference feed. Returns an error listing all
// missing reference currencies.
func validateRefFeeds(nonUSDFeeds map[int]string, refFeedIdx map[string]int, oracleName string) error {
	if len(nonUSDFeeds) == 0 {
		return nil
	}

	needed := make(map[string]bool)
	for _, currency := range nonUSDFeeds {
		needed[currency] = true
	}

	var missing []string
	for currency := range needed {
		if _, ok := refFeedIdx[currency]; !ok {
			missing = append(missing, currency)
		}
	}

	if len(missing) == 0 {
		return nil
	}

	sort.Strings(missing)
	return fmt.Errorf("oracle %s: missing USD reference feeds for currencies: %v", oracleName, missing)
}

// buildRefFeedIdx identifies which feeds serve as USD-denominated reference prices
// for non-USD quote currencies. It matches token addresses against well-known
// WETH/WBTC addresses to find feeds that provide ETH/USD and BTC/USD.
func buildRefFeedIdx(feeds []blockchain.FeedConfig, tokenAddrs map[int64]common.Address) (map[string]int, map[int]string) {
	refFeedIdx := make(map[string]int)
	nonUSDFeeds := make(map[int]string)

	for i, feed := range feeds {
		if feed.QuoteCurrency != "USD" {
			nonUSDFeeds[i] = feed.QuoteCurrency
			continue
		}
		addr, ok := tokenAddrs[feed.TokenID]
		if !ok {
			continue
		}
		for currency, refAddr := range quoteCurrencyTokenAddr {
			if addr == refAddr {
				refFeedIdx[currency] = i
			}
		}
	}

	return refFeedIdx, nonUSDFeeds
}

func tokenInfosToAddressMap(infos map[int64]outbound.TokenInfo) map[int64]common.Address {
	out := make(map[int64]common.Address, len(infos))
	for id, info := range infos {
		out[id] = common.BytesToAddress(info.Address)
	}
	return out
}

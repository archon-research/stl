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
	Oracle      *entity.Oracle
	OracleID    int16
	OracleAddr  common.Address
	TokenAddrs  []common.Address        // for aave_oracle: ordered list of token addresses
	TokenIDs    []int64                 // parallel to TokenAddrs (aave) or Feeds (chainlink_feed)
	Feeds       []blockchain.FeedConfig // for chainlink_feed: per-feed config
	RefFeedIdx  map[string]int          // quote currency → index in Feeds[] for USD-denominated reference
	NonUSDFeeds map[int]string          // feed index → quote currency for non-USD feeds
}

// LoadOracleUnits loads all enabled oracles from DB, deduplicates by oracle ID,
// and builds OracleUnit structs for each.
func LoadOracleUnits(ctx context.Context, repo outbound.OnchainPriceRepository, logger *slog.Logger) ([]*OracleUnit, error) {
	allOracles, err := repo.GetAllEnabledOracles(ctx)
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
	if oracle.OracleType.IsFeedOracle() {
		unit, err = buildFeedUnit(ctx, repo, oracle)
	} else {
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

	tokenAddrBytes, err := repo.GetTokenAddresses(ctx, oracle.ID)
	if err != nil {
		return nil, fmt.Errorf("getting token addresses: %w", err)
	}

	tokenAddrs := make([]common.Address, len(assets))
	tokenIDs := make([]int64, len(assets))
	for i, asset := range assets {
		addrBytes, ok := tokenAddrBytes[asset.TokenID]
		if !ok {
			return nil, fmt.Errorf("token address not found for token_id %d", asset.TokenID)
		}
		tokenAddrs[i] = common.BytesToAddress(addrBytes)
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

	tokenAddrBytes, err := repo.GetTokenAddresses(ctx, oracle.ID)
	if err != nil {
		return nil, fmt.Errorf("getting token addresses: %w", err)
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

	tokenAddrs := bytesToAddressMap(tokenAddrBytes)
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

func bytesToAddressMap(raw map[int64][]byte) map[int64]common.Address {
	out := make(map[int64]common.Address, len(raw))
	for id, b := range raw {
		out[id] = common.BytesToAddress(b)
	}
	return out
}

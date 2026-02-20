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
	OracleID    int16 // safe int16 conversion of Oracle.ID, validated at load time
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
	var skipped int

	for _, oracle := range allOracles {
		if seen[oracle.ID] {
			continue
		}
		seen[oracle.ID] = true

		unit, err := buildOracleUnit(ctx, repo, oracle)
		if err != nil {
			logger.Warn("skipping oracle", "name", oracle.Name, "error", err)
			skipped++
			continue
		}
		if unit != nil {
			units = append(units, unit)
		}
	}

	logger.Info("loaded oracle units",
		"loaded", len(units),
		"total", len(seen),
		"skipped", skipped)

	return units, nil
}

// ConvertNonUSDPrices converts non-USD feed prices to USD using reference feeds.
// Modifies results in-place. Feeds without available reference prices are marked as failed.
func ConvertNonUSDPrices(results []blockchain.FeedPriceResult, unit *OracleUnit, logger *slog.Logger, blockNum int64) {
	for feedIdx, quoteCurrency := range unit.NonUSDFeeds {
		if !results[feedIdx].Success {
			continue
		}
		refIdx, hasRef := unit.RefFeedIdx[quoteCurrency]
		if !hasRef || !results[refIdx].Success {
			results[feedIdx].Success = false
			results[feedIdx].Price = 0
			logger.Warn("reference price unavailable, discarding non-USD feed price",
				"oracle", unit.Oracle.Name,
				"feed_token", unit.Feeds[feedIdx].TokenID,
				"feedAddress", unit.Feeds[feedIdx].FeedAddress.Hex(),
				"quote", quoteCurrency,
				"block", blockNum)
			continue
		}
		results[feedIdx].Price *= results[refIdx].Price
	}
}

// LogFeedFailures logs individual feed failures and emits an error if all feeds failed.
func LogFeedFailures(results []blockchain.FeedPriceResult, unit *OracleUnit, logger *slog.Logger, blockNum int64) {
	var failCount int
	for i, r := range results {
		if !r.Success {
			failCount++
			logger.Warn("feed call failed",
				"oracle", unit.Oracle.Name,
				"tokenID", r.TokenID,
				"feedAddress", unit.Feeds[i].FeedAddress.Hex(),
				"block", blockNum)
		}
	}
	if failCount == len(results) && len(results) > 0 {
		logger.Error("all feeds failed for oracle, check configuration",
			"oracle", unit.Oracle.Name,
			"block", blockNum,
			"feedCount", len(results))
	}
}

// safeOracleID converts an int64 oracle ID to int16, returning an error if the
// value is out of the valid range [1, math.MaxInt16].
func safeOracleID(id int64) (int16, error) {
	if id < 1 || id > math.MaxInt16 {
		return 0, fmt.Errorf("oracle ID %d out of int16 range [1, %d]", id, math.MaxInt16)
	}
	return int16(id), nil
}

func buildOracleUnit(ctx context.Context, repo outbound.OnchainPriceRepository, oracle *entity.Oracle) (*OracleUnit, error) {
	oracleID, err := safeOracleID(oracle.ID)
	if err != nil {
		return nil, err
	}

	var unit *OracleUnit
	switch oracle.OracleType {
	case entity.OracleTypeChainlinkFeed, entity.OracleTypeChronicle, entity.OracleTypeRedstone:
		unit, err = buildFeedUnit(ctx, repo, oracle)
	default:
		unit, err = buildAaveUnit(ctx, repo, oracle)
	}
	if err != nil {
		return nil, err
	}
	if unit != nil {
		unit.OracleID = oracleID
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

	return &OracleUnit{
		Oracle:     oracle,
		OracleAddr: common.Address(oracle.Address),
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
		if len(asset.FeedAddress) == 0 {
			return nil, fmt.Errorf("feed address missing for token_id %d", asset.TokenID)
		}
		var decimals int
		if asset.FeedDecimals != nil {
			decimals = *asset.FeedDecimals
		}
		if decimals == 0 {
			decimals = oracle.PriceDecimals
		}
		quoteCurrency := asset.QuoteCurrency
		if quoteCurrency == "" {
			quoteCurrency = "USD"
		}
		feeds[i] = blockchain.FeedConfig{
			TokenID:       asset.TokenID,
			FeedAddress:   common.BytesToAddress(asset.FeedAddress),
			FeedDecimals:  decimals,
			QuoteCurrency: quoteCurrency,
		}
		tokenIDs[i] = asset.TokenID
	}

	refFeedIdx, nonUSDFeeds := buildRefFeedIdx(feeds, tokenAddrBytes)

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
func buildRefFeedIdx(feeds []blockchain.FeedConfig, tokenAddrBytes map[int64][]byte) (map[string]int, map[int]string) {
	tokenAddr := make(map[int64]common.Address, len(tokenAddrBytes))
	for tokenID, addrBytes := range tokenAddrBytes {
		tokenAddr[tokenID] = common.BytesToAddress(addrBytes)
	}

	refFeedIdx := make(map[string]int)
	nonUSDFeeds := make(map[int]string)

	for i, feed := range feeds {
		if feed.QuoteCurrency != "USD" {
			nonUSDFeeds[i] = feed.QuoteCurrency
			continue
		}
		addr, ok := tokenAddr[feed.TokenID]
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

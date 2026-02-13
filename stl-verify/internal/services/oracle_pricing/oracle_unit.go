// Package oracle_pricing provides shared types and logic for oracle price
// fetching, used by both the live price worker and historical backfill services.
package oracle_pricing

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// QuoteCurrencyTokenAddr maps quote currencies to their well-known mainnet
// token addresses, used for reference feed identification.
var QuoteCurrencyTokenAddr = map[string]common.Address{
	"ETH": common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"), // WETH
	"BTC": common.HexToAddress("0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"), // WBTC
}

// OracleUnit holds everything needed to fetch prices for one oracle.
type OracleUnit struct {
	Oracle      *entity.Oracle
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
			logger.Warn("skipping oracle", "name", oracle.Name, "error", err)
			continue
		}
		if unit != nil {
			units = append(units, unit)
		}
	}

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
			logger.Debug("reference price unavailable for conversion",
				"feed_token", unit.Feeds[feedIdx].TokenID,
				"quote", quoteCurrency,
				"block", blockNum)
			continue
		}
		results[feedIdx].Price *= results[refIdx].Price
	}
}

func buildOracleUnit(ctx context.Context, repo outbound.OnchainPriceRepository, oracle *entity.Oracle) (*OracleUnit, error) {
	switch oracle.OracleType {
	case "chainlink_feed", "chronicle":
		return buildFeedUnit(ctx, repo, oracle)
	default:
		return buildAaveUnit(ctx, repo, oracle)
	}
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

	refFeedIdx, nonUSDFeeds := BuildRefFeedIdx(feeds, tokenAddrBytes)

	return &OracleUnit{
		Oracle:      oracle,
		TokenIDs:    tokenIDs,
		Feeds:       feeds,
		RefFeedIdx:  refFeedIdx,
		NonUSDFeeds: nonUSDFeeds,
	}, nil
}

// BuildRefFeedIdx identifies which feeds serve as USD-denominated reference prices
// for non-USD quote currencies. It matches token addresses against well-known
// WETH/WBTC addresses to find feeds that provide ETH/USD and BTC/USD.
func BuildRefFeedIdx(feeds []blockchain.FeedConfig, tokenAddrBytes map[int64][]byte) (map[string]int, map[int]string) {
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
		for currency, refAddr := range QuoteCurrencyTokenAddr {
			if addr == refAddr {
				refFeedIdx[currency] = i
			}
		}
	}

	return refFeedIdx, nonUSDFeeds
}

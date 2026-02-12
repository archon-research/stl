// Package oracle_price_worker provides an SQS consumer that fetches onchain oracle
// prices for each new block and stores them in the database.
// All oracles are loaded from the DB — no hardcoded oracle configuration.
package oracle_price_worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Well-known mainnet token addresses for quote currency reference feed identification.
var quoteCurrencyTokenAddr = map[string]common.Address{
	"ETH": common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"), // WETH
	"BTC": common.HexToAddress("0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"), // WBTC
}

// Config holds configuration for the oracle price worker.
type Config struct {
	MaxMessages  int
	PollInterval time.Duration
	Logger       *slog.Logger
}

func configDefaults() Config {
	return Config{
		MaxMessages:  10,
		PollInterval: 100 * time.Millisecond,
		Logger:       slog.Default(),
	}
}

// oracleUnit holds everything needed to fetch prices for one oracle.
type oracleUnit struct {
	oracle      *entity.Oracle
	oracleAddr  common.Address
	tokenAddrs  []common.Address        // for aave_oracle: ordered list of token addresses
	tokenIDs    []int64                 // parallel to tokenAddrs (aave) or feeds (chainlink_feed)
	feeds       []blockchain.FeedConfig // for chainlink_feed: per-feed config
	refFeedIdx  map[string]int          // quote currency → index in feeds[] for USD-denominated reference
	nonUSDFeeds map[int]string          // feed index → quote currency for non-USD feeds
	priceCache  map[int64]float64       // tokenID → last stored price (for change detection)
}

// Service processes SQS block events and fetches oracle prices for each block.
type Service struct {
	config      Config
	consumer    outbound.SQSConsumer
	repo        outbound.OnchainPriceRepository
	multicaller outbound.Multicaller

	oracleABI *abi.ABI
	feedABI   *abi.ABI
	units     []*oracleUnit

	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger
}

// NewService creates a new oracle price worker service.
func NewService(
	config Config,
	consumer outbound.SQSConsumer,
	multicaller outbound.Multicaller,
	repo outbound.OnchainPriceRepository,
) (*Service, error) {
	if consumer == nil {
		return nil, fmt.Errorf("consumer cannot be nil")
	}
	if multicaller == nil {
		return nil, fmt.Errorf("multicaller cannot be nil")
	}
	if repo == nil {
		return nil, fmt.Errorf("repo cannot be nil")
	}

	defaults := configDefaults()
	if config.MaxMessages == 0 {
		config.MaxMessages = defaults.MaxMessages
	}
	if config.PollInterval == 0 {
		config.PollInterval = defaults.PollInterval
	}
	if config.Logger == nil {
		config.Logger = defaults.Logger
	}

	oracleABI, err := abis.GetAaveOracleABI()
	if err != nil {
		return nil, fmt.Errorf("loading Oracle ABI: %w", err)
	}

	feedABI, err := abis.GetAggregatorV3ABI()
	if err != nil {
		return nil, fmt.Errorf("loading AggregatorV3 ABI: %w", err)
	}

	return &Service{
		config:      config,
		consumer:    consumer,
		repo:        repo,
		multicaller: multicaller,
		oracleABI:   oracleABI,
		feedABI:     feedABI,
		logger:      config.Logger.With("component", "oracle-price-worker"),
	}, nil
}

// Start initializes the service and begins processing SQS messages.
func (s *Service) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	if err := s.initialize(s.ctx); err != nil {
		return fmt.Errorf("initializing: %w", err)
	}

	go s.processLoop()

	s.logger.Info("oracle price worker started",
		"oracles", len(s.units))
	return nil
}

// Stop stops the service.
func (s *Service) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.logger.Info("oracle price worker stopped")
	return nil
}

func (s *Service) initialize(ctx context.Context) error {
	allOracles, err := s.repo.GetAllEnabledOracles(ctx)
	if err != nil {
		return fmt.Errorf("getting enabled oracles: %w", err)
	}

	seen := make(map[int64]bool)
	for _, oracle := range allOracles {
		if seen[oracle.ID] {
			continue
		}
		seen[oracle.ID] = true

		unit, err := s.buildOracleUnit(ctx, oracle)
		if err != nil {
			s.logger.Warn("skipping oracle", "name", oracle.Name, "error", err)
			continue
		}
		if unit != nil {
			s.units = append(s.units, unit)
		}
	}

	if len(s.units) == 0 {
		return fmt.Errorf("no oracles with enabled assets found")
	}

	s.logger.Info("initialized", "oracles", len(s.units))
	return nil
}

func (s *Service) buildOracleUnit(ctx context.Context, oracle *entity.Oracle) (*oracleUnit, error) {
	switch oracle.OracleType {
	case "chainlink_feed":
		return s.buildFeedUnit(ctx, oracle)
	default:
		return s.buildAaveUnit(ctx, oracle)
	}
}

func (s *Service) buildAaveUnit(ctx context.Context, oracle *entity.Oracle) (*oracleUnit, error) {
	assets, err := s.repo.GetEnabledAssets(ctx, oracle.ID)
	if err != nil {
		return nil, fmt.Errorf("getting enabled assets: %w", err)
	}
	if len(assets) == 0 {
		return nil, nil
	}

	tokenAddrBytes, err := s.repo.GetTokenAddresses(ctx, oracle.ID)
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

	// Initialize price cache from DB
	cached, err := s.repo.GetLatestPrices(ctx, oracle.ID)
	if err != nil {
		return nil, fmt.Errorf("loading latest prices: %w", err)
	}

	tokenHexAddrs := make([]string, len(tokenAddrs))
	for i, addr := range tokenAddrs {
		tokenHexAddrs[i] = addr.Hex()
	}
	s.logger.Info("loaded aave oracle",
		"name", oracle.Name,
		"oracleAddr", common.Address(oracle.Address).Hex(),
		"assets", len(assets),
		"tokenAddrs", tokenHexAddrs,
		"cachedPrices", len(cached))

	return &oracleUnit{
		oracle:     oracle,
		oracleAddr: common.Address(oracle.Address),
		tokenAddrs: tokenAddrs,
		tokenIDs:   tokenIDs,
		priceCache: cached,
	}, nil
}

func (s *Service) buildFeedUnit(ctx context.Context, oracle *entity.Oracle) (*oracleUnit, error) {
	assets, err := s.repo.GetEnabledAssets(ctx, oracle.ID)
	if err != nil {
		return nil, fmt.Errorf("getting enabled assets: %w", err)
	}
	if len(assets) == 0 {
		return nil, nil
	}

	tokenAddrBytes, err := s.repo.GetTokenAddresses(ctx, oracle.ID)
	if err != nil {
		return nil, fmt.Errorf("getting token addresses: %w", err)
	}

	feeds := make([]blockchain.FeedConfig, len(assets))
	tokenIDs := make([]int64, len(assets))
	for i, asset := range assets {
		if len(asset.FeedAddress) == 0 {
			return nil, fmt.Errorf("feed address missing for token_id %d", asset.TokenID)
		}
		decimals := asset.FeedDecimals
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

	cached, err := s.repo.GetLatestPrices(ctx, oracle.ID)
	if err != nil {
		return nil, fmt.Errorf("loading latest prices: %w", err)
	}

	feedAddrs := make([]string, len(feeds))
	for i, f := range feeds {
		feedAddrs[i] = f.FeedAddress.Hex()
	}
	s.logger.Info("loaded feed oracle",
		"name", oracle.Name,
		"feeds", len(feeds),
		"feedAddrs", feedAddrs,
		"nonUSDFeeds", len(nonUSDFeeds),
		"cachedPrices", len(cached))

	return &oracleUnit{
		oracle:      oracle,
		tokenIDs:    tokenIDs,
		feeds:       feeds,
		refFeedIdx:  refFeedIdx,
		nonUSDFeeds: nonUSDFeeds,
		priceCache:  cached,
	}, nil
}

// buildRefFeedIdx identifies which feeds serve as USD-denominated reference prices
// for non-USD quote currencies. It matches token addresses against well-known
// WETH/WBTC addresses to find feeds that provide ETH/USD and BTC/USD.
func buildRefFeedIdx(feeds []blockchain.FeedConfig, tokenAddrBytes map[int64][]byte) (map[string]int, map[int]string) {
	// Map token ID → token address for lookup.
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
		// Check if this USD-denominated feed's token matches a well-known reference address.
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

func (s *Service) processLoop() {
	ticker := time.NewTicker(s.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if err := s.processMessages(s.ctx); err != nil {
				s.logger.Error("error processing messages", "error", err)
			}
		}
	}
}

func (s *Service) processMessages(ctx context.Context) error {
	messages, err := s.consumer.ReceiveMessages(ctx, s.config.MaxMessages)
	if err != nil {
		return fmt.Errorf("receiving messages: %w", err)
	}

	if len(messages) == 0 {
		return nil
	}

	s.logger.Info("received messages", "count", len(messages))

	var errs []error
	for _, msg := range messages {
		if err := s.processMessage(ctx, msg); err != nil {
			s.logger.Error("failed to process message", "error", err)
			errs = append(errs, err)
			continue
		}

		if deleteErr := s.consumer.DeleteMessage(ctx, msg.ReceiptHandle); deleteErr != nil {
			s.logger.Error("failed to delete message", "error", deleteErr)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// blockEvent is the SQS message payload for block events.
type blockEvent struct {
	ChainID        int64  `json:"chainId"`
	BlockNumber    int64  `json:"blockNumber"`
	Version        int    `json:"version"`
	BlockHash      string `json:"blockHash"`
	BlockTimestamp int64  `json:"blockTimestamp"`
	IsReorg        bool   `json:"isReorg,omitempty"`
}

func (s *Service) processMessage(ctx context.Context, msg outbound.SQSMessage) error {
	var event blockEvent
	if err := json.Unmarshal([]byte(msg.Body), &event); err != nil {
		return fmt.Errorf("parsing block event: %w", err)
	}

	return s.processBlock(ctx, event)
}

func (s *Service) processBlock(ctx context.Context, event blockEvent) error {
	var errs []error
	for _, unit := range s.units {
		if err := s.processBlockForOracle(ctx, event, unit); err != nil {
			s.logger.Error("failed to process oracle",
				"oracle", unit.oracle.Name,
				"block", event.BlockNumber,
				"error", err)
			errs = append(errs, fmt.Errorf("oracle %s: %w", unit.oracle.Name, err))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (s *Service) processBlockForOracle(ctx context.Context, event blockEvent, unit *oracleUnit) error {
	switch unit.oracle.OracleType {
	case "chainlink_feed":
		return s.processBlockForFeedOracle(ctx, event, unit)
	default:
		return s.processBlockForAaveOracle(ctx, event, unit)
	}
}

func (s *Service) processBlockForAaveOracle(ctx context.Context, event blockEvent, unit *oracleUnit) error {
	prices, err := blockchain.FetchOraclePrices(
		ctx,
		s.multicaller,
		s.oracleABI,
		unit.oracleAddr,
		unit.tokenAddrs,
		event.BlockNumber,
	)
	if err != nil {
		return fmt.Errorf("fetching oracle prices at block %d: %w", event.BlockNumber, err)
	}

	if len(prices) != len(unit.tokenIDs) {
		return fmt.Errorf("price count mismatch: expected %d, got %d", len(unit.tokenIDs), len(prices))
	}

	changed := s.detectChanges(prices, event, unit)

	if len(changed) == 0 {
		s.logger.Debug("no price changes", "oracle", unit.oracle.Name, "block", event.BlockNumber)
		return nil
	}

	if err := s.repo.UpsertPrices(ctx, changed); err != nil {
		return fmt.Errorf("storing prices at block %d: %w", event.BlockNumber, err)
	}

	s.logger.Info("stored oracle prices",
		"oracle", unit.oracle.Name,
		"block", event.BlockNumber,
		"changed", len(changed),
		"total", len(unit.tokenIDs))

	return nil
}

func (s *Service) processBlockForFeedOracle(ctx context.Context, event blockEvent, unit *oracleUnit) error {
	results, err := blockchain.FetchFeedPrices(
		ctx,
		s.multicaller,
		s.feedABI,
		unit.feeds,
		event.BlockNumber,
	)
	if err != nil {
		return fmt.Errorf("fetching feed prices at block %d: %w", event.BlockNumber, err)
	}

	s.convertNonUSDPrices(results, unit, event.BlockNumber)

	changed := s.detectFeedChanges(results, event, unit)

	if len(changed) == 0 {
		s.logger.Debug("no price changes", "oracle", unit.oracle.Name, "block", event.BlockNumber)
		return nil
	}

	if err := s.repo.UpsertPrices(ctx, changed); err != nil {
		return fmt.Errorf("storing prices at block %d: %w", event.BlockNumber, err)
	}

	s.logger.Info("stored feed prices",
		"oracle", unit.oracle.Name,
		"block", event.BlockNumber,
		"changed", len(changed),
		"total", len(unit.feeds))

	return nil
}

// convertNonUSDPrices converts non-USD feed prices to USD using reference feeds.
func (s *Service) convertNonUSDPrices(results []blockchain.FeedPriceResult, unit *oracleUnit, blockNum int64) {
	for feedIdx, quoteCurrency := range unit.nonUSDFeeds {
		if !results[feedIdx].Success {
			continue
		}
		refIdx, hasRef := unit.refFeedIdx[quoteCurrency]
		if !hasRef || !results[refIdx].Success {
			results[feedIdx].Success = false
			s.logger.Warn("reference price unavailable for conversion",
				"oracle", unit.oracle.Name,
				"feed_token", unit.feeds[feedIdx].TokenID,
				"quote", quoteCurrency,
				"block", blockNum)
			continue
		}
		results[feedIdx].Price *= results[refIdx].Price
	}
}

func (s *Service) detectChanges(rawPrices []*big.Int, event blockEvent, unit *oracleUnit) []*entity.OnchainTokenPrice {
	blockTimestamp := time.Unix(event.BlockTimestamp, 0).UTC()
	oracleID := int16(unit.oracle.ID)
	priceDecimals := unit.oracle.PriceDecimals
	if priceDecimals == 0 {
		priceDecimals = 8
	}

	var changed []*entity.OnchainTokenPrice
	for i, rawPrice := range rawPrices {
		priceUSD := blockchain.ConvertOraclePriceToUSD(rawPrice, priceDecimals)
		tokenID := unit.tokenIDs[i]

		if cachedPrice, ok := unit.priceCache[tokenID]; ok && cachedPrice == priceUSD {
			continue
		}

		price, err := entity.NewOnchainTokenPrice(
			tokenID,
			oracleID,
			event.BlockNumber,
			int16(event.Version),
			blockTimestamp,
			priceUSD,
		)
		if err != nil {
			s.logger.Error("invalid price entity", "tokenID", tokenID, "error", err)
			continue
		}
		changed = append(changed, price)
		unit.priceCache[tokenID] = priceUSD
	}

	return changed
}

func (s *Service) detectFeedChanges(results []blockchain.FeedPriceResult, event blockEvent, unit *oracleUnit) []*entity.OnchainTokenPrice {
	blockTimestamp := time.Unix(event.BlockTimestamp, 0).UTC()
	oracleID := int16(unit.oracle.ID)

	var changed []*entity.OnchainTokenPrice
	for _, result := range results {
		if !result.Success {
			continue
		}

		if cachedPrice, ok := unit.priceCache[result.TokenID]; ok && cachedPrice == result.Price {
			continue
		}

		price, err := entity.NewOnchainTokenPrice(
			result.TokenID,
			oracleID,
			event.BlockNumber,
			int16(event.Version),
			blockTimestamp,
			result.Price,
		)
		if err != nil {
			s.logger.Error("invalid price entity", "tokenID", result.TokenID, "error", err)
			continue
		}
		changed = append(changed, price)
		unit.priceCache[result.TokenID] = result.Price
	}

	return changed
}

// Package oracle_price_worker provides an SQS consumer that fetches onchain oracle
// prices for each new block and stores them in the database.
// All oracles are loaded from the DB — no hardcoded oracle configuration.
package oracle_price_worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"

	"github.com/archon-research/stl/stl-verify/internal/common/sqsutil"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/oracle_pricing"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// MulticallerFactory creates a new Multicaller for the given oracle type.
// Returns the appropriate implementation (e.g. Multicall3 for Chainlink/Aave,
// DirectCaller for Chronicle where msg.sender must be address(0)).
type MulticallerFactory func(entity.OracleType) (outbound.Multicaller, error)

// oracleUnit wraps a shared OracleUnit with a per-oracle price cache
// for persistent change detection across blocks.
type oracleUnit struct {
	*oracle_pricing.OracleUnit
	priceCache  map[int64]float64    // tokenID → last stored price
	multicaller outbound.Multicaller // per-unit multicaller (DirectCaller for chronicle, Multicall3 for others)
}

// Service processes SQS block events and fetches oracle prices for each block.
type Service struct {
	config         shared.SQSConsumerConfig
	consumer       outbound.SQSConsumer
	repo           outbound.OnchainPriceRepository
	newMulticaller MulticallerFactory

	oracleABI *abi.ABI
	feedABI   *abi.ABI
	units     []*oracleUnit

	decimalsValidated bool // set after first successful feed decimals check

	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger
}

// NewService creates a new oracle price worker service.
func NewService(
	config shared.SQSConsumerConfig,
	consumer outbound.SQSConsumer,
	repo outbound.OnchainPriceRepository,
	newMulticaller MulticallerFactory,
) (*Service, error) {
	if consumer == nil {
		return nil, fmt.Errorf("consumer cannot be nil")
	}
	if repo == nil {
		return nil, fmt.Errorf("repo cannot be nil")
	}
	if newMulticaller == nil {
		return nil, fmt.Errorf("newMulticaller cannot be nil")
	}

	config.ApplyDefaults()

	oracleABI, err := abis.GetAaveOracleABI()
	if err != nil {
		return nil, fmt.Errorf("loading Oracle ABI: %w", err)
	}

	feedABI, err := abis.GetAggregatorV3ABI()
	if err != nil {
		return nil, fmt.Errorf("loading AggregatorV3 ABI: %w", err)
	}

	return &Service{
		config:         config,
		consumer:       consumer,
		repo:           repo,
		newMulticaller: newMulticaller,
		oracleABI:      oracleABI,
		feedABI:        feedABI,
		logger:         config.Logger.With("component", "oracle-price-worker"),
	}, nil
}

// Start initializes the service and begins processing SQS messages.
func (s *Service) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	if err := s.initialize(s.ctx); err != nil {
		return fmt.Errorf("initializing: %w", err)
	}

	go sqsutil.RunLoop(s.ctx, sqsutil.Config{
		Consumer:     s.consumer,
		MaxMessages:  s.config.MaxMessages,
		PollInterval: s.config.PollInterval,
		Logger:       s.logger,
	}, s.processBlock)

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
	shared, err := oracle_pricing.LoadOracleUnits(ctx, s.repo, s.logger)
	if err != nil {
		return err
	}

	for _, su := range shared {
		cached, err := s.repo.GetLatestPrices(ctx, su.Oracle.ID)
		if err != nil {
			s.logger.Warn("skipping oracle", "name", su.Oracle.Name, "error", fmt.Errorf("loading latest prices: %w", err))
			continue
		}

		mc, err := s.newMulticaller(su.Oracle.OracleType)
		if err != nil {
			s.logger.Warn("skipping oracle", "name", su.Oracle.Name, "error", fmt.Errorf("creating multicaller: %w", err))
			continue
		}

		s.logOracleUnit(su, cached)

		s.units = append(s.units, &oracleUnit{
			OracleUnit:  su,
			priceCache:  cached,
			multicaller: mc,
		})
	}

	if len(s.units) == 0 {
		return fmt.Errorf("no oracles with enabled assets found")
	}

	s.logger.Info("initialized", "oracles", len(s.units))
	return nil
}

func (s *Service) logOracleUnit(su *oracle_pricing.OracleUnit, cached map[int64]float64) {
	if su.Oracle.OracleType.IsFeedOracle() {
		feedAddrs := make([]string, len(su.Feeds))
		for i, f := range su.Feeds {
			feedAddrs[i] = f.FeedAddress.Hex()
		}
		s.logger.Info("loaded feed oracle",
			"name", su.Oracle.Name,
			"type", su.Oracle.OracleType,
			"feeds", len(su.Feeds),
			"feedAddrs", feedAddrs,
			"nonUSDFeeds", len(su.NonUSDFeeds),
			"cachedPrices", len(cached))
	} else {
		tokenHexAddrs := make([]string, len(su.TokenAddrs))
		for i, addr := range su.TokenAddrs {
			tokenHexAddrs[i] = addr.Hex()
		}
		s.logger.Info("loaded oracle",
			"name", su.Oracle.Name,
			"type", su.Oracle.OracleType,
			"oracleAddr", su.OracleAddr.Hex(),
			"assets", len(su.TokenAddrs),
			"tokenAddrs", tokenHexAddrs,
			"cachedPrices", len(cached))
	}
}

func (s *Service) validateFeedDecimals(ctx context.Context, blockNum int64) error {
	for _, unit := range s.units {
		if !unit.Oracle.OracleType.IsFeedOracle() {
			continue
		}
		if err := blockchain.ValidateFeedDecimals(
			ctx, unit.multicaller, s.feedABI,
			unit.Feeds, blockNum, s.logger,
		); err != nil {
			return fmt.Errorf("oracle %s: %w", unit.Oracle.Name, err)
		}
	}
	return nil
}

func (s *Service) processBlock(ctx context.Context, event outbound.BlockEvent) error {
	if !s.decimalsValidated {
		if err := s.validateFeedDecimals(ctx, event.BlockNumber); err != nil {
			return fmt.Errorf("feed decimals validation: %w", err)
		}
		s.decimalsValidated = true
	}

	var errs []error
	for _, unit := range s.units {
		if err := s.processBlockForOracle(ctx, event, unit); err != nil {
			s.logger.Error("failed to process oracle",
				"oracle", unit.Oracle.Name,
				"block", event.BlockNumber,
				"error", err)
			errs = append(errs, fmt.Errorf("oracle %s: %w", unit.Oracle.Name, err))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (s *Service) processBlockForOracle(ctx context.Context, event outbound.BlockEvent, unit *oracleUnit) error {
	switch unit.Oracle.OracleType {
	case entity.OracleTypeChainlinkFeed, entity.OracleTypeChronicle, entity.OracleTypeRedstone:
		return s.processBlockForFeedOracle(ctx, event, unit)
	case entity.OracleTypeAave:
		return s.processBlockForAaveOracle(ctx, event, unit)
	default:
		return fmt.Errorf("unsupported oracle type: %s", unit.Oracle.OracleType)
	}
}

func (s *Service) processBlockForAaveOracle(ctx context.Context, event outbound.BlockEvent, unit *oracleUnit) error {
	prices, err := blockchain.FetchOraclePrices(
		ctx,
		unit.multicaller,
		s.oracleABI,
		unit.OracleAddr,
		unit.TokenAddrs,
		event.BlockNumber,
	)
	if err != nil {
		return fmt.Errorf("fetching oracle prices at block %d: %w", event.BlockNumber, err)
	}

	if len(prices) != len(unit.TokenIDs) {
		return fmt.Errorf("price count mismatch: expected %d, got %d", len(unit.TokenIDs), len(prices))
	}

	changed, err := s.detectChanges(prices, event, unit)
	if err != nil {
		return fmt.Errorf("detecting changes at block %d: %w", event.BlockNumber, err)
	}

	if len(changed) == 0 {
		s.logger.Debug("no price changes", "oracle", unit.Oracle.Name, "block", event.BlockNumber)
		return nil
	}

	if err := s.repo.UpsertPrices(ctx, changed); err != nil {
		return fmt.Errorf("storing prices at block %d: %w", event.BlockNumber, err)
	}

	s.logger.Info("stored oracle prices",
		"oracle", unit.Oracle.Name,
		"block", event.BlockNumber,
		"changed", len(changed),
		"total", len(unit.TokenIDs))

	return nil
}

func (s *Service) processBlockForFeedOracle(ctx context.Context, event outbound.BlockEvent, unit *oracleUnit) error {
	results, err := blockchain.FetchFeedPrices(
		ctx,
		unit.multicaller,
		s.feedABI,
		unit.Feeds,
		event.BlockNumber,
		s.logger,
	)
	if err != nil {
		return fmt.Errorf("fetching feed prices at block %d: %w", event.BlockNumber, err)
	}

	results = oracle_pricing.ConvertNonUSDPrices(results, unit.OracleUnit, s.logger, event.BlockNumber)

	changed, err := s.detectFeedChanges(results, event, unit)
	if err != nil {
		return fmt.Errorf("detecting feed changes at block %d: %w", event.BlockNumber, err)
	}

	if len(changed) == 0 {
		s.logger.Debug("no price changes", "oracle", unit.Oracle.Name, "block", event.BlockNumber)
		return nil
	}

	if err := s.repo.UpsertPrices(ctx, changed); err != nil {
		return fmt.Errorf("storing prices at block %d: %w", event.BlockNumber, err)
	}

	s.logger.Info("stored feed prices",
		"oracle", unit.Oracle.Name,
		"block", event.BlockNumber,
		"changed", len(changed),
		"total", len(unit.Feeds))

	return nil
}

func (s *Service) detectChanges(rawPrices []*big.Int, event outbound.BlockEvent, unit *oracleUnit) ([]*entity.OnchainTokenPrice, error) {
	blockTimestamp := time.Unix(event.BlockTimestamp, 0).UTC()
	oracleID := unit.OracleID
	priceDecimals := unit.Oracle.PriceDecimals
	if priceDecimals == 0 {
		priceDecimals = 8
	}

	var changed []*entity.OnchainTokenPrice
	for i, rawPrice := range rawPrices {
		priceUSD := blockchain.ScaleByDecimals(rawPrice, priceDecimals)
		tokenID := unit.TokenIDs[i]

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
			return nil, fmt.Errorf("invalid price entity for tokenID %d: %w", tokenID, err)
		}
		changed = append(changed, price)
		unit.priceCache[tokenID] = priceUSD
	}

	return changed, nil
}

func (s *Service) detectFeedChanges(results []blockchain.FeedPriceResult, event outbound.BlockEvent, unit *oracleUnit) ([]*entity.OnchainTokenPrice, error) {
	blockTimestamp := time.Unix(event.BlockTimestamp, 0).UTC()
	oracleID := unit.OracleID

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
			return nil, fmt.Errorf("invalid price entity for tokenID %d: %w", result.TokenID, err)
		}
		changed = append(changed, price)
		unit.priceCache[result.TokenID] = result.Price
	}

	return changed, nil
}

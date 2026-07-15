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
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/archon-research/stl/stl-verify/internal/common/sqsutil"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/archiving"
	"github.com/archon-research/stl/stl-verify/internal/pkg/hexutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/rpcutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/oracle_pricing"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// MulticallerFactory creates a new Multicaller for the given oracle type.
// Types where entity.RequiresDirectCall is true (chronicle, erc4626_share)
// get a DirectCaller because their feeds revert when read through Multicall3;
// everything else gets the Multicall3 client.
type MulticallerFactory func(entity.OracleType) (outbound.Multicaller, error)

// oracleUnit wraps a shared OracleUnit with a per-oracle price cache
// for persistent change detection across blocks.
type oracleUnit struct {
	*oracle_pricing.OracleUnit
	priceCache  map[int64]float64    // tokenID → last stored price
	multicaller outbound.Multicaller // per-unit multicaller; type routing is documented on MulticallerFactory
}

// commitPriceCache records the upserted prices in the change-detection cache.
// Call it only after UpsertPrices succeeds: caching earlier would make the SQS
// redelivery of a failed block detect "no change", ack, and silently drop the
// rows forever.
func (u *oracleUnit) commitPriceCache(changed []*entity.OnchainTokenPrice) {
	for _, p := range changed {
		u.priceCache[p.TokenID] = p.PriceUSD
	}
}

// suppressAsUnchanged reports whether the price for tokenID can be skipped as
// unchanged from the cached value. A reorg republish (event.Version > 0) is
// never suppressed: readers order by block_version DESC, so the reorged block
// needs a row at its new version even when the value happens to match the
// cache; the upsert is idempotent, so re-emitting is safe.
func (u *oracleUnit) suppressAsUnchanged(event outbound.BlockEvent, tokenID int64, priceUSD float64) bool {
	if event.Version > 0 {
		return false
	}
	cached, ok := u.priceCache[tokenID]
	return ok && cached == priceUSD
}

// Service processes SQS block events and fetches oracle prices for each block.
type Service struct {
	config         shared.SQSConsumerConfig
	consumer       outbound.SQSConsumer
	cacheReader    outbound.BlockCacheReader
	repo           outbound.OnchainPriceRepository
	newMulticaller MulticallerFactory

	oracleABI    *abi.ABI
	feedABI      *abi.ABI
	shareABI     *abi.ABI
	curvePoolABI *abi.ABI
	units        []*oracleUnit

	decimalsValidated bool // set after first successful feed decimals check

	telemetry *Telemetry

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup // tracks the SQS run loop so Stop can drain it
	logger *slog.Logger
}

// WithTelemetry sets the telemetry instance for the service. Call before Start.
func (s *Service) WithTelemetry(t *Telemetry) {
	s.telemetry = t
}

// NewService creates a new oracle price worker service.
// cacheReader supplies raw block JSON; the worker derives each block's
// timestamp from it (cache miss falls back to S3 inside the reader).
func NewService(
	config shared.SQSConsumerConfig,
	consumer outbound.SQSConsumer,
	cacheReader outbound.BlockCacheReader,
	repo outbound.OnchainPriceRepository,
	newMulticaller MulticallerFactory,
) (*Service, error) {
	if consumer == nil {
		return nil, fmt.Errorf("consumer cannot be nil")
	}
	if cacheReader == nil {
		return nil, fmt.Errorf("cacheReader cannot be nil")
	}
	if repo == nil {
		return nil, fmt.Errorf("repo cannot be nil")
	}
	if newMulticaller == nil {
		return nil, fmt.Errorf("newMulticaller cannot be nil")
	}

	config.ApplyDefaults()
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}

	oracleABI, err := abis.GetAaveOracleABI()
	if err != nil {
		return nil, fmt.Errorf("loading Oracle ABI: %w", err)
	}

	feedABI, err := abis.GetAggregatorV3ABI()
	if err != nil {
		return nil, fmt.Errorf("loading AggregatorV3 ABI: %w", err)
	}

	shareABI, err := abis.GetERC4626ABI()
	if err != nil {
		return nil, fmt.Errorf("loading ERC4626 ABI: %w", err)
	}

	curvePoolABI, err := abis.GetCurveNGPoolABI()
	if err != nil {
		return nil, fmt.Errorf("loading Curve NG pool ABI: %w", err)
	}

	return &Service{
		config:         config,
		consumer:       consumer,
		cacheReader:    cacheReader,
		repo:           repo,
		newMulticaller: newMulticaller,
		oracleABI:      oracleABI,
		feedABI:        feedABI,
		shareABI:       shareABI,
		curvePoolABI:   curvePoolABI,
		logger:         config.Logger.With("component", "oracle-price-worker"),
	}, nil
}

// Start initializes the service and begins processing SQS messages.
func (s *Service) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	if err := s.initialize(s.ctx); err != nil {
		return fmt.Errorf("initializing: %w", err)
	}

	s.wg.Go(func() {
		sqsutil.RunLoop(s.ctx, sqsutil.Config{
			Consumer:     s.consumer,
			MaxMessages:  s.config.MaxMessages,
			PollInterval: s.config.PollInterval,
			Logger:       s.logger,
			ChainID:      s.config.ChainID,
		}, s.processBlock)
	})

	s.logger.Info("oracle price worker started",
		"oracles", len(s.units))
	return nil
}

// Stop cancels the SQS processing loop and waits for the goroutine to exit, so
// no in-flight handler outlives shutdown (and no archive write is scheduled
// after the archiving drain begins).
func (s *Service) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	s.logger.Info("oracle price worker stopped")
	return nil
}

func (s *Service) initialize(ctx context.Context) error {
	shared, err := oracle_pricing.LoadOracleUnits(ctx, s.repo, s.config.ChainID, s.logger)
	if err != nil {
		return err
	}

	// A failed unit fails startup so the orchestrator restarts the worker;
	// warn-and-skip would leave the oracle silently unpriced for the whole
	// process lifetime.
	for _, su := range shared {
		cached, err := s.repo.GetLatestPrices(ctx, su.Oracle.ID)
		if err != nil {
			return fmt.Errorf("oracle %s: loading latest prices: %w", su.Oracle.Name, err)
		}

		mc, err := s.newMulticaller(su.Oracle.OracleType)
		if err != nil {
			return fmt.Errorf("oracle %s: creating multicaller: %w", su.Oracle.Name, err)
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

	// Baseline every loaded unit's freshness gauge; rationale on
	// Telemetry.RecordUnitLoaded.
	for _, unit := range s.units {
		s.telemetry.RecordUnitLoaded(ctx, unit.Oracle.Name)
	}

	s.logger.Info("initialized", "oracles", len(s.units))
	return nil
}

func (s *Service) logOracleUnit(su *oracle_pricing.OracleUnit, cached map[int64]float64) {
	switch {
	case su.Oracle.OracleType.IsERC4626Oracle():
		vaultAddrs := make([]string, len(su.ERC4626Vaults))
		for i, v := range su.ERC4626Vaults {
			vaultAddrs[i] = v.VaultAddress.Hex()
		}
		s.logger.Info("loaded erc4626 oracle",
			"name", su.Oracle.Name,
			"type", su.Oracle.OracleType,
			"vaults", len(su.ERC4626Vaults),
			"vaultAddrs", vaultAddrs,
			"cachedPrices", len(cached))
	case su.Oracle.OracleType.IsCurveLPNGOracle():
		coinFeedAddrs := make([]string, len(su.CurveLPNGPool.CoinFeeds))
		for i, f := range su.CurveLPNGPool.CoinFeeds {
			coinFeedAddrs[i] = f.FeedAddress.Hex()
		}
		s.logger.Info("loaded curve lp oracle",
			"name", su.Oracle.Name,
			"type", su.Oracle.OracleType,
			"pool", su.CurveLPNGPool.PoolAddress.Hex(),
			"coinFeeds", coinFeedAddrs,
			"cachedPrices", len(cached))
	case su.Oracle.OracleType.IsFeedOracle():
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
	default:
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
		feeds, ok := oracle_pricing.ValidationFeeds(unit.OracleUnit)
		if !ok {
			continue
		}
		if err := blockchain.ValidateFeedDecimals(
			ctx, unit.multicaller, s.feedABI,
			feeds, blockNum, s.logger,
		); err != nil {
			return fmt.Errorf("oracle %s: %w", unit.Oracle.Name, err)
		}
		if unit.Oracle.OracleType.IsERC4626Oracle() {
			if err := blockchain.ValidateERC4626UnderlyingDecimals(
				ctx, unit.multicaller, s.shareABI, s.feedABI,
				unit.ERC4626Vaults, blockNum, s.logger,
			); err != nil {
				return fmt.Errorf("oracle %s: %w", unit.Oracle.Name, err)
			}
		}
	}
	return nil
}

func (s *Service) processBlock(ctx context.Context, event outbound.BlockEvent) (retErr error) {
	ctx = archiving.WithBlockVersion(ctx, event.Version)
	ctx = archiving.WithBlockNumber(ctx, event.BlockNumber)
	ctx, span := s.telemetry.StartBlockSpan(ctx, event.BlockNumber)
	defer span.End()

	start := time.Now()
	defer func() {
		duration := time.Since(start)
		s.telemetry.RecordBlockProcessed(ctx, duration, retErr)
		if retErr != nil {
			telemetry.SetSpanError(span, retErr, "block processing failed")
			s.telemetry.RecordError(ctx, "processBlock", retErr)
		}
	}()

	if !s.decimalsValidated {
		if err := s.validateFeedDecimals(ctx, event.BlockNumber); err != nil {
			return fmt.Errorf("feed decimals validation: %w", err)
		}
		s.decimalsValidated = true
	}

	blockTimestamp, err := s.resolveBlockTimestamp(ctx, event)
	if err != nil {
		return fmt.Errorf("resolving block timestamp: %w", err)
	}

	var errs []error
	for _, unit := range s.units {
		if err := s.processBlockForOracle(ctx, event, blockTimestamp, unit); err != nil {
			s.logger.Error("failed to process oracle",
				"oracle", unit.Oracle.Name,
				"block", event.BlockNumber,
				"error", err)
			errs = append(errs, fmt.Errorf("oracle %s: %w", unit.Oracle.Name, err))
			continue
		}
		// Freshness advances on every successful pass, written rows or not;
		// rationale on Telemetry.RecordUnitSuccess.
		s.telemetry.RecordUnitSuccess(ctx, unit.Oracle.Name)
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// resolveBlockTimestamp fetches the block JSON from the cache (Redis →
// S3 fallback) and returns the timestamp. A miss in both stores is a hard
// error so SQS retries pick it up once the watcher has finished backing the
// block up.
func (s *Service) resolveBlockTimestamp(ctx context.Context, event outbound.BlockEvent) (time.Time, error) {
	data, err := s.cacheReader.GetBlock(ctx, event.ChainID, event.BlockNumber, event.Version)
	if err != nil {
		return time.Time{}, fmt.Errorf("getting block %d (version %d) from cache: %w", event.BlockNumber, event.Version, err)
	}
	// `data == nil` covers a plain miss. `IsNullOrEmpty` also catches a
	// poisoned cache row written by a pre-VEC-242 watcher (literal []byte("null")):
	// unmarshalling that into the timestamp struct silently succeeds with an
	// empty hex string, which then misattributes the failure as "parsing block N
	// timestamp \"\"" instead of the truthful "cache returned null payload".
	// We surface a clear error and let SQS retry — the watcher's fix will have
	// overwritten the row with valid data by the time the retry fires.
	if rpcutil.IsNullOrEmpty(data) {
		return time.Time{}, fmt.Errorf("block %d (version %d, chain %d) not found in cache or s3 (or cached value is null)", event.BlockNumber, event.Version, event.ChainID)
	}

	var block struct {
		Timestamp string `json:"timestamp"`
	}
	if err := json.Unmarshal(data, &block); err != nil {
		return time.Time{}, fmt.Errorf("unmarshalling block %d: %w", event.BlockNumber, err)
	}

	ts, err := hexutil.ParseInt64(block.Timestamp)
	if err != nil {
		return time.Time{}, fmt.Errorf("parsing block %d timestamp %q: %w", event.BlockNumber, block.Timestamp, err)
	}

	return time.Unix(ts, 0).UTC(), nil
}

func (s *Service) processBlockForOracle(ctx context.Context, event outbound.BlockEvent, blockTimestamp time.Time, unit *oracleUnit) (retErr error) {
	ctx, span := s.telemetry.StartSpan(ctx, "oracle.processBlockForOracle",
		attribute.String("oracle.name", unit.Oracle.Name),
		attribute.String("oracle.type", string(unit.Oracle.OracleType)))
	defer func() {
		if retErr != nil {
			telemetry.SetSpanError(span, retErr, "oracle processing failed")
		}
		span.End()
	}()

	switch unit.Oracle.OracleType {
	case entity.OracleTypeChainlinkFeed, entity.OracleTypeChronicle, entity.OracleTypeRedstone:
		return s.processBlockForFeedOracle(ctx, event, blockTimestamp, unit)
	case entity.OracleTypeAave:
		return s.processBlockForAaveOracle(ctx, event, blockTimestamp, unit)
	case entity.OracleTypeERC4626Share:
		return s.processBlockForERC4626Oracle(ctx, event, blockTimestamp, unit)
	case entity.OracleTypeCurveLPNG:
		return s.processBlockForCurveLPNGOracle(ctx, event, blockTimestamp, unit)
	default:
		return fmt.Errorf("unsupported oracle type: %s", unit.Oracle.OracleType)
	}
}

func (s *Service) processBlockForAaveOracle(ctx context.Context, event outbound.BlockEvent, blockTimestamp time.Time, unit *oracleUnit) error {
	blockHash, err := event.ParsedBlockHash()
	if err != nil {
		return fmt.Errorf("parse block hash: %w", err)
	}

	// Fetch prices (RPC span)
	ctx, fetchSpan := s.telemetry.StartSpan(ctx, "oracle.fetchPrices",
		attribute.String("rpc.method", "getAssetsPrices"))
	rpcStart := time.Now()
	prices, err := blockchain.FetchOraclePrices(ctx, unit.multicaller, s.oracleABI, unit.OracleAddr, unit.TokenAddrs, event.BlockNumber, blockHash)
	rpcDuration := time.Since(rpcStart)
	s.telemetry.RecordRPCCall(ctx, "getAssetsPrices", rpcDuration, err)
	if err != nil {
		telemetry.SetSpanError(fetchSpan, err, "fetch oracle prices failed")
	}
	fetchSpan.End()
	if err != nil {
		return fmt.Errorf("fetching oracle prices at block %d: %w", event.BlockNumber, err)
	}

	if len(prices) != len(unit.TokenIDs) {
		return fmt.Errorf("price count mismatch: expected %d, got %d", len(unit.TokenIDs), len(prices))
	}

	s.telemetry.RecordUnitReads(ctx, unit.Oracle.Name, countNonZeroPrices(prices), len(prices))

	// Detect changes
	ctx, detectSpan := s.telemetry.StartSpan(ctx, "oracle.detectChanges",
		attribute.Int("prices.total", len(prices)))
	changed, err := s.detectChanges(prices, event, blockTimestamp, unit)
	if err != nil {
		telemetry.SetSpanError(detectSpan, err, "detect changes failed")
		detectSpan.End()
		return fmt.Errorf("detecting changes at block %d: %w", event.BlockNumber, err)
	}
	detectSpan.SetAttributes(attribute.Int("prices.changed", len(changed)))
	recordPriceChangeEvents(detectSpan, unit, changed)
	detectSpan.End()

	if len(changed) == 0 {
		s.logger.Debug("no price changes", "oracle", unit.Oracle.Name, "block", event.BlockNumber)
		return nil
	}

	// Upsert prices (DB span)
	ctx, upsertSpan := s.telemetry.StartSpan(ctx, "oracle.upsertPrices",
		attribute.Int("prices.changed", len(changed)))
	err = s.repo.UpsertPrices(ctx, changed)
	if err != nil {
		telemetry.SetSpanError(upsertSpan, err, "upsert prices failed")
	}
	upsertSpan.End()
	if err != nil {
		return fmt.Errorf("storing prices at block %d: %w", event.BlockNumber, err)
	}
	unit.commitPriceCache(changed)

	s.telemetry.RecordPricesChanged(ctx, unit.Oracle.Name, len(changed))

	s.logger.Info("stored oracle prices",
		"oracle", unit.Oracle.Name,
		"block", event.BlockNumber,
		"changed", len(changed),
		"total", len(unit.TokenIDs))

	return nil
}

func (s *Service) processBlockForFeedOracle(ctx context.Context, event outbound.BlockEvent, blockTimestamp time.Time, unit *oracleUnit) error {
	blockHash, err := event.ParsedBlockHash()
	if err != nil {
		return fmt.Errorf("parse block hash: %w", err)
	}

	// Fetch prices (RPC span)
	ctx, fetchSpan := s.telemetry.StartSpan(ctx, "oracle.fetchPrices",
		attribute.String("rpc.method", "latestRoundData"))
	rpcStart := time.Now()
	results, err := blockchain.FetchFeedPrices(ctx, unit.multicaller, s.feedABI, unit.Feeds, event.BlockNumber, blockHash, s.logger)
	rpcDuration := time.Since(rpcStart)
	s.telemetry.RecordRPCCall(ctx, "latestRoundData", rpcDuration, err)
	if err != nil {
		telemetry.SetSpanError(fetchSpan, err, "fetch feed prices failed")
	}
	fetchSpan.End()
	if err != nil {
		return fmt.Errorf("fetching feed prices at block %d: %w", event.BlockNumber, err)
	}

	results = oracle_pricing.ConvertNonUSDPrices(results, unit.OracleUnit, s.logger, event.BlockNumber)

	return s.storeFeedResults(ctx, event, blockTimestamp, unit, results, "feed changes", "stored feed prices", len(unit.Feeds))
}

func (s *Service) processBlockForERC4626Oracle(ctx context.Context, event outbound.BlockEvent, blockTimestamp time.Time, unit *oracleUnit) error {
	blockHash, err := event.ParsedBlockHash()
	if err != nil {
		return fmt.Errorf("parse block hash: %w", err)
	}
	ctx, fetchSpan := s.telemetry.StartSpan(ctx, "oracle.fetchPrices",
		attribute.String("rpc.method", "convertToAssets"))
	rpcStart := time.Now()
	results, err := blockchain.FetchERC4626SharePrices(ctx, unit.multicaller, s.shareABI, s.feedABI, unit.ERC4626Vaults, event.BlockNumber, blockHash, s.logger)
	rpcDuration := time.Since(rpcStart)
	s.telemetry.RecordRPCCall(ctx, "convertToAssets", rpcDuration, err)
	if err != nil {
		telemetry.SetSpanError(fetchSpan, err, "fetch erc4626 share prices failed")
	}
	fetchSpan.End()
	if err != nil {
		return fmt.Errorf("fetching erc4626 share prices at block %d: %w", event.BlockNumber, err)
	}

	return s.storeFeedResults(ctx, event, blockTimestamp, unit, results, "erc4626 changes", "stored erc4626 share prices", len(unit.ERC4626Vaults))
}

func (s *Service) processBlockForCurveLPNGOracle(ctx context.Context, event outbound.BlockEvent, blockTimestamp time.Time, unit *oracleUnit) error {
	blockHash, err := event.ParsedBlockHash()
	if err != nil {
		return fmt.Errorf("parse block hash: %w", err)
	}

	ctx, fetchSpan := s.telemetry.StartSpan(ctx, "oracle.fetchPrices",
		attribute.String("rpc.method", "get_virtual_price"))
	rpcStart := time.Now()
	results, err := blockchain.FetchCurveLPNGPrices(ctx, unit.multicaller, s.curvePoolABI, s.feedABI, *unit.CurveLPNGPool, event.BlockNumber, blockHash)
	rpcDuration := time.Since(rpcStart)
	s.telemetry.RecordRPCCall(ctx, "get_virtual_price", rpcDuration, err)
	if err != nil {
		telemetry.SetSpanError(fetchSpan, err, "fetch curve lp prices failed")
	}
	fetchSpan.End()
	if err != nil {
		return fmt.Errorf("fetching curve lp prices at block %d: %w", event.BlockNumber, err)
	}

	return s.storeFeedResults(ctx, event, blockTimestamp, unit, results, "curve lp changes", "stored curve lp prices", len(unit.CurveLPNGPool.CoinFeeds))
}

// storeFeedResults runs the detect-changes, upsert, and cache-commit tail
// shared by every oracle path that produces FeedPriceResult slices. kind and
// logMsg carry the per-path strings; total is the source-collection size for
// the log line.
func (s *Service) storeFeedResults(ctx context.Context, event outbound.BlockEvent, blockTimestamp time.Time, unit *oracleUnit, results []blockchain.FeedPriceResult, kind, logMsg string, total int) error {
	// The reads metric's expected count is len(results), NOT total: every
	// fetcher returns exactly one outcome per read it performs (and errors on
	// a count mismatch), whereas total is the source-collection size, which
	// the curve path folds into a single LP-price result; counting its coin
	// feeds would record phantom failed reads on every healthy pass. Deriving
	// from the result set keeps future paths correct by construction.
	s.telemetry.RecordUnitReads(ctx, unit.Oracle.Name, countSuccessfulResults(results), len(results))

	ctx, detectSpan := s.telemetry.StartSpan(ctx, "oracle.detectChanges",
		attribute.Int("prices.total", len(results)))
	changed, err := s.detectFeedChanges(results, event, blockTimestamp, unit)
	if err != nil {
		telemetry.SetSpanError(detectSpan, err, "detect "+kind+" failed")
		detectSpan.End()
		return fmt.Errorf("detecting %s at block %d: %w", kind, event.BlockNumber, err)
	}
	detectSpan.SetAttributes(attribute.Int("prices.changed", len(changed)))
	recordPriceChangeEvents(detectSpan, unit, changed)
	detectSpan.End()

	if len(changed) == 0 {
		s.logger.Debug("no price changes", "oracle", unit.Oracle.Name, "block", event.BlockNumber)
		return nil
	}

	ctx, upsertSpan := s.telemetry.StartSpan(ctx, "oracle.upsertPrices",
		attribute.Int("prices.changed", len(changed)))
	err = s.repo.UpsertPrices(ctx, changed)
	if err != nil {
		telemetry.SetSpanError(upsertSpan, err, "upsert prices failed")
	}
	upsertSpan.End()
	if err != nil {
		return fmt.Errorf("storing prices at block %d: %w", event.BlockNumber, err)
	}
	unit.commitPriceCache(changed)

	s.telemetry.RecordPricesChanged(ctx, unit.Oracle.Name, len(changed))

	s.logger.Info(logMsg,
		"oracle", unit.Oracle.Name,
		"block", event.BlockNumber,
		"changed", len(changed),
		"total", total)

	return nil
}

func (s *Service) detectChanges(rawPrices []*big.Int, event outbound.BlockEvent, blockTimestamp time.Time, unit *oracleUnit) ([]*entity.OnchainTokenPrice, error) {
	oracleID := unit.OracleID
	priceDecimals := unit.Oracle.PriceDecimals
	if priceDecimals == 0 {
		priceDecimals = 8
	}

	var changed []*entity.OnchainTokenPrice
	for i, rawPrice := range rawPrices {
		tokenID := unit.TokenIDs[i]

		if rawPrice == nil || rawPrice.Sign() == 0 {
			// A 0 (or absent) price is never a real USD quote — treat it as
			// unpriceable and skip so amount_usd degrades to null downstream rather
			// than persisting a bogus $0 that reads as a real price. Mirrors
			// detectFeedChanges' !Success skip. (The AaveOracle reverts rather than
			// returning 0 for a source-less asset with a zero fallback, so an
			// unpriceable asset must be kept out of the getAssetsPrices batch in the
			// first place — see 20260702_120000_maple_syrup_allocation_exposure.sql;
			// this guard is the general safety net for any 0 that does slip through.)
			s.logger.Warn("skipping unpriceable asset (oracle returned zero price)",
				"oracle", unit.Oracle.Name, "tokenID", tokenID, "block", event.BlockNumber)
			continue
		}

		priceUSD := blockchain.ScaleByDecimals(rawPrice, priceDecimals)

		if unit.suppressAsUnchanged(event, tokenID, priceUSD) {
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
	}

	return changed, nil
}

func (s *Service) detectFeedChanges(results []blockchain.FeedPriceResult, event outbound.BlockEvent, blockTimestamp time.Time, unit *oracleUnit) ([]*entity.OnchainTokenPrice, error) {
	oracleID := unit.OracleID

	var changed []*entity.OnchainTokenPrice
	for _, result := range results {
		if !result.Success {
			continue
		}

		if unit.suppressAsUnchanged(event, result.TokenID, result.Price) {
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
	}

	return changed, nil
}

// countNonZeroPrices counts the quotes the fetched-prices metric treats as
// usable, mirroring detectChanges' zero-price guard: nil or zero is
// unpriceable, not a real quote.
func countNonZeroPrices(prices []*big.Int) int {
	n := 0
	for _, p := range prices {
		if p != nil && p.Sign() != 0 {
			n++
		}
	}
	return n
}

// countSuccessfulResults counts the feed reads the fetched-prices metric
// treats as usable, mirroring detectFeedChanges' !Success skip: a reverting
// feed is guard-skipped without erroring, so it must not count as fetched.
func countSuccessfulResults(results []blockchain.FeedPriceResult) int {
	n := 0
	for _, r := range results {
		if r.Success {
			n++
		}
	}
	return n
}

// recordPriceChangeEvents adds a span event for each changed price so Jaeger
// shows which tokens changed, from which oracle/feed, and to what value.
func recordPriceChangeEvents(span trace.Span, unit *oracleUnit, changed []*entity.OnchainTokenPrice) {
	// Build tokenID → feed address lookup for feed oracles.
	feedAddr := make(map[int64]string, len(unit.Feeds))
	for _, f := range unit.Feeds {
		feedAddr[f.TokenID] = f.FeedAddress.Hex()
	}

	for _, p := range changed {
		attrs := []attribute.KeyValue{
			attribute.String("oracle.name", unit.Oracle.Name),
			attribute.Int64("token.id", p.TokenID),
			attribute.Float64("price.usd", p.PriceUSD),
		}
		if addr, ok := feedAddr[p.TokenID]; ok {
			attrs = append(attrs, attribute.String("feed.address", addr))
		}
		span.AddEvent("price.changed", trace.WithAttributes(attrs...))
	}
}

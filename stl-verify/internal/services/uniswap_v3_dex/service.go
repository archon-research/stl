package uniswap_v3_dex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/common/sqsutil"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/dextelemetry"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// UniswapV3ProtocolAddress is the canonical mainnet UniswapV3Factory address
// — the registry's `protocol` row uses this as the contract_address. Matches
// the seed in db/migrations/20260521_100000_create_dex_prereqs.sql.
var UniswapV3ProtocolAddress = common.HexToAddress("0x1F98431c8aD98523631AE4a59f267346ea31F984")

// DefaultNFPMAddress is the canonical mainnet NonfungiblePositionManager,
// used directly as the NFPM address for chain_id 1.
var DefaultNFPMAddress = common.HexToAddress("0xC36442b4a4522E871399CD717aBDD847Ab11FE88")

// uniswapV3ProtocolDeployBlock is the block at which the UniswapV3 factory
// landed on mainnet. Reused as created_at_block when GetOrCreateProtocol has
// to insert the protocol row (it should already exist via the prereq
// migration).
const uniswapV3ProtocolDeployBlock = int64(12369621)

// Config holds service configuration.
type Config struct {
	shared.SQSConsumerConfig
	// NFPMAddress is the NonfungiblePositionManager whose events drive
	// per-position state. Defaults to the mainnet address; configurable so
	// other chains can plug in a different NFPM deployment.
	NFPMAddress common.Address

	// Telemetry, when non-nil, is invoked once per processBlockEvent with the
	// terminal duration and error. Wired by the worker main.go to emit
	// uniswap_v3_blocks_processed_total / uniswap_v3_errors_total. Nil
	// disables.
	Telemetry *dextelemetry.Telemetry
}

// ConfigDefaults returns default configuration values.
func ConfigDefaults() Config {
	return Config{
		SQSConsumerConfig: shared.SQSConsumerConfigDefaults(),
		NFPMAddress:       DefaultNFPMAddress,
	}
}

// Service is the Uniswap V3 DEX SQS consumer.
type Service struct {
	config Config

	consumer     outbound.SQSConsumer
	cache        outbound.BlockCacheReader
	txManager    outbound.TxManager
	uniswapRepo  outbound.UniswapV3PoolRepository
	tokenRepo    outbound.TokenRepository
	protocolRepo outbound.ProtocolRepository
	eventRepo    outbound.EventRepository

	blockchain *blockchainService
	extractor  *eventExtractor
	telemetry  *dextelemetry.Telemetry

	registry *poolRegistry
	posCache *positionCache

	// uniswapV3ProtocolID is the cached protocol_event.protocol_id for the
	// Uniswap V3 protocol row, populated via a check-lock-check on first
	// call. Reads on the hot path are unsynchronised; the mutex is only
	// taken to serialise the first GetOrCreateProtocol I/O. Mirrors the
	// Curve and Balancer adapters' resolveProtocolID pattern.
	protocolIDMu        sync.Mutex
	uniswapV3ProtocolID atomic.Int64

	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger
}

// NewService constructs a Uniswap V3 DEX service. Dependencies follow
// hexagonal ports — no postgres/sqs/alchemy adapter packages are imported.
func NewService(
	config Config,
	consumer outbound.SQSConsumer,
	cache outbound.BlockCacheReader,
	multicaller outbound.Multicaller,
	txManager outbound.TxManager,
	uniswapRepo outbound.UniswapV3PoolRepository,
	tokenRepo outbound.TokenRepository,
	protocolRepo outbound.ProtocolRepository,
	eventRepo outbound.EventRepository,
) (*Service, error) {
	if err := validateDependencies(consumer, cache, multicaller, txManager, uniswapRepo, tokenRepo, protocolRepo, eventRepo); err != nil {
		return nil, fmt.Errorf("validating dependencies: %w", err)
	}
	config.SQSConsumerConfig.ApplyDefaults()
	if err := config.SQSConsumerConfig.Validate(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}
	if (config.NFPMAddress == common.Address{}) {
		config.NFPMAddress = DefaultNFPMAddress
	}

	extractor, err := newEventExtractor()
	if err != nil {
		return nil, fmt.Errorf("creating event extractor: %w", err)
	}
	bc, err := newBlockchainService(multicaller)
	if err != nil {
		return nil, fmt.Errorf("creating blockchain service: %w", err)
	}

	return &Service{
		config:       config,
		consumer:     consumer,
		cache:        cache,
		txManager:    txManager,
		uniswapRepo:  uniswapRepo,
		tokenRepo:    tokenRepo,
		protocolRepo: protocolRepo,
		eventRepo:    eventRepo,
		blockchain:   bc,
		extractor:    extractor,
		telemetry:    config.Telemetry,
		registry:     newPoolRegistry(),
		posCache:     newPositionCache(),
		logger:       config.Logger.With("component", "uniswap-v3-dex-worker"),
	}, nil
}

// Start primes the in-process pool registry from the database and begins
// the SQS poll loop.
func (s *Service) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	if err := s.loadRegistry(ctx); err != nil {
		return fmt.Errorf("loading uniswap v3 pool registry: %w", err)
	}

	go sqsutil.RunLoop(s.ctx, sqsutil.Config{
		Consumer:     s.consumer,
		MaxMessages:  s.config.MaxMessages,
		PollInterval: s.config.PollInterval,
		Logger:       s.logger,
		ChainID:      s.config.ChainID,
	}, s.processBlockEvent)

	s.logger.Info("uniswap v3 dex worker started",
		"chainID", s.config.ChainID,
		"pools", s.registry.poolCount(),
		"nfpm", s.config.NFPMAddress.Hex())
	return nil
}

// Stop cancels the background loop.
func (s *Service) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.logger.Info("uniswap v3 dex worker stopped")
	return nil
}

func (s *Service) loadRegistry(ctx context.Context) error {
	pools, err := s.uniswapRepo.ListEnabledUniswapV3Pools(ctx, s.config.ChainID)
	if err != nil {
		return fmt.Errorf("listing uniswap v3 pools: %w", err)
	}
	for _, p := range pools {
		s.registry.addPool(p)
	}
	return nil
}

// processBlockEvent is the per-message handler. Each receipt's logs are
// filtered to the Uniswap V3 scope (known pool addresses + NFPM address) and
// dispatched per event category.
func (s *Service) processBlockEvent(ctx context.Context, event outbound.BlockEvent) (retErr error) {
	start := time.Now()
	defer func() {
		s.telemetry.RecordBlockProcessed(ctx, time.Since(start), retErr)
		if retErr != nil {
			s.telemetry.RecordError(ctx, "processBlockEvent", retErr)
		}
	}()
	receiptsJSON, err := s.cache.GetReceipts(ctx, event.ChainID, event.BlockNumber, event.Version)
	if err != nil {
		return fmt.Errorf("fetching receipts from cache: %w", err)
	}
	if receiptsJSON == nil {
		return fmt.Errorf("receipts not found in cache for block %d (chain=%d, version=%d)", event.BlockNumber, event.ChainID, event.Version)
	}

	var receipts []shared.TransactionReceipt
	if err := json.Unmarshal(receiptsJSON, &receipts); err != nil {
		return fmt.Errorf("unmarshalling receipts: %w", err)
	}

	blockTimestamp := time.Unix(event.BlockTimestamp, 0).UTC()

	var errs []error
	for _, r := range receipts {
		if err := s.processReceipt(ctx, r, event.ChainID, event.BlockNumber, event.Version, blockTimestamp); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (s *Service) processReceipt(ctx context.Context, receipt shared.TransactionReceipt, chainID, blockNumber int64, blockVersion int, blockTimestamp time.Time) error {
	// Per-receipt dedup: only one state-row per pool/position per receipt no
	// matter how many events land on it.
	poolStatesWritten := make(map[int64]bool)
	positionStatesWritten := make(map[int64]bool)

	var errs []error
	for _, log := range receipt.Logs {
		addr := common.HexToAddress(log.Address)

		// Branch 1: log emitted by a known V3 pool.
		if pool := s.registry.poolByAddress(addr); pool != nil {
			if _, ok := s.extractor.poolTopic(log); ok {
				if err := s.handlePoolLog(ctx, log, pool, chainID, blockNumber, blockVersion, blockTimestamp, poolStatesWritten); err != nil {
					errs = append(errs, err)
				}
				continue
			}
		}

		// Branch 2: log emitted by the NFPM contract.
		if addr == s.config.NFPMAddress {
			if _, ok := s.extractor.nfpmTopic(log); ok {
				if err := s.handleNFPMLog(ctx, log, chainID, blockNumber, blockVersion, blockTimestamp, poolStatesWritten, positionStatesWritten); err != nil {
					errs = append(errs, err)
				}
				continue
			}
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// handlePoolLog processes a single pool-emitted event: persists the audit-log
// row, the typed projection, and the event-triggered pool state snapshot
// (deduped per receipt). All writes share one transaction.
func (s *Service) handlePoolLog(ctx context.Context, log shared.Log, pool *entity.UniswapV3Pool, chainID, blockNumber int64, blockVersion int, blockTimestamp time.Time, poolStatesWritten map[int64]bool) error {
	decoded, err := s.extractor.extractPoolEvent(log)
	if err != nil {
		return fmt.Errorf("decoding pool event for %s: %w", pool.Label, err)
	}
	if decoded == nil {
		// Flash, or topic we don't decode — silently drop.
		return nil
	}

	state, err := s.readPoolStateForEvent(ctx, pool, blockNumber)
	if err != nil {
		return fmt.Errorf("reading pool state for %s at block %d: %w", pool.Label, blockNumber, err)
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		if err := s.saveProtocolEvent(ctx, tx, decoded, pool.Address, chainID, blockNumber, blockVersion, blockTimestamp); err != nil {
			return err
		}
		if err := s.savePoolEventProjection(ctx, tx, decoded, pool, blockNumber, blockVersion, blockTimestamp); err != nil {
			return err
		}
		if !poolStatesWritten[pool.ID] {
			if err := s.savePoolState(ctx, tx, pool, state, blockNumber, blockVersion, blockTimestamp); err != nil {
				return err
			}
			poolStatesWritten[pool.ID] = true
		}
		return nil
	})
}

// handleNFPMLog handles NFPM-emitted events. The IncreaseLiquidity /
// DecreaseLiquidity / Collect / Transfer flow:
//  1. decode event → tokenID
//  2. look up the position in cache → if missing, NFPM.positions(tokenId)
//     resolves the pool. If pool isn't tracked, mark the tokenId as out-of-scope
//     and drop.
//  3. for liquidity-changing events, also issue a pool multicall for the
//     position's pool to refresh uniswap_v3_pool_state.
func (s *Service) handleNFPMLog(ctx context.Context, log shared.Log, chainID, blockNumber int64, blockVersion int, blockTimestamp time.Time, poolStatesWritten, positionStatesWritten map[int64]bool) error {
	decoded, err := s.extractor.extractNFPMEvent(log)
	if err != nil {
		return fmt.Errorf("decoding NFPM event: %w", err)
	}
	if decoded == nil {
		return nil
	}

	tokenID := nfpmTokenID(decoded)
	if tokenID == nil {
		return fmt.Errorf("NFPM event %s missing tokenId", decoded.Name)
	}

	position, posState, err := s.resolvePosition(ctx, decoded, tokenID, chainID, blockNumber, blockVersion, blockTimestamp)
	if err != nil {
		return err
	}
	if position == nil {
		// Out-of-scope tokenId — only the audit row is persisted for the
		// Transfer/Collect/etc, no state changes.
		return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
			return s.saveProtocolEvent(ctx, tx, decoded, s.config.NFPMAddress, chainID, blockNumber, blockVersion, blockTimestamp)
		})
	}

	pool := s.registry.poolByID(position.UniswapV3PoolID)
	if pool == nil {
		// Pool was unregistered between cache load and now — drop silently.
		return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
			return s.saveProtocolEvent(ctx, tx, decoded, s.config.NFPMAddress, chainID, blockNumber, blockVersion, blockTimestamp)
		})
	}

	// For liquidity-changing events, also refresh pool state (deduped per receipt).
	var poolState *poolMulticallResult
	if isLiquidityChanging(decoded) {
		ps, err := s.readPoolStateForEvent(ctx, pool, blockNumber)
		if err != nil {
			return fmt.Errorf("reading pool state for %s at block %d: %w", pool.Label, blockNumber, err)
		}
		poolState = ps
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		if err := s.saveProtocolEvent(ctx, tx, decoded, s.config.NFPMAddress, chainID, blockNumber, blockVersion, blockTimestamp); err != nil {
			return err
		}
		if err := s.handleNFPMSideEffects(ctx, tx, decoded, position); err != nil {
			return err
		}
		if posState != nil && !positionStatesWritten[position.ID] {
			if err := s.savePositionState(ctx, tx, position.ID, posState, blockNumber, blockVersion, blockTimestamp); err != nil {
				return err
			}
			positionStatesWritten[position.ID] = true
		}
		if poolState != nil && !poolStatesWritten[pool.ID] {
			if err := s.savePoolState(ctx, tx, pool, poolState, blockNumber, blockVersion, blockTimestamp); err != nil {
				return err
			}
			poolStatesWritten[pool.ID] = true
		}
		return nil
	})
}

// handleNFPMSideEffects applies side effects keyed on event type — owner
// change (Transfer) — plus burned-flag persistence for any event whose
// resolvePosition pass flagged the position as burned (DecreaseLiquidity to
// liquidity=0, or any event on a token whose positions() read reverted
// because the NFT was burned in the same block).
func (s *Service) handleNFPMSideEffects(ctx context.Context, tx pgx.Tx, decoded *decodedEvent, position *entity.UniswapV3Position) error {
	if decoded.Name == eventNFPMTransfer {
		t := decoded.NFPMTransfer
		zero := common.Address{}
		if t.From != zero && t.To != zero {
			if err := s.uniswapRepo.SetUniswapV3PositionOwner(ctx, tx, position.ID, t.To); err != nil {
				return fmt.Errorf("updating owner for tokenId %s: %w", t.TokenID, err)
			}
			position.Owner = t.To
		}
	}
	// resolvePosition (called above) invoked markBurned via the cache write
	// lock if the post-event positions.liquidity dropped to zero OR the
	// positions() read reverted (NFT burned). Read the cached `Burned`
	// directly — `position` is the same pointer the cache holds.
	if position.Burned {
		if err := s.uniswapRepo.SetUniswapV3PositionBurned(ctx, tx, position.ID); err != nil {
			return fmt.Errorf("burning position id %d: %w", position.ID, err)
		}
	}
	return nil
}

// resolvePosition loads the position registry row for the event's tokenId.
// Cold path (Increase / first sighting): NFPM.positions(tokenId) yields the
// pool token0/token1/fee, which we look up against our registry; missing-pool
// tokenIds are cached as out-of-scope.
//
// First-sighting cost is one NFPM.positions() round-trip even if the tokenId
// turns out to be out-of-scope — there is no way to know in advance which
// pool a tokenId targets without asking the NFPM. The missing-marker cache
// guarantees this cost is paid AT MOST ONCE per tokenId across the worker's
// lifetime; every subsequent Transfer/Increase/Decrease/Collect on an out-of-
// scope tokenId short-circuits at the `isMissingMarker(cached)` check above.
//
// Returns (position, positionsCallResult, error). When the position is out-of-
// scope the returned position is nil and posState is nil.
func (s *Service) resolvePosition(ctx context.Context, decoded *decodedEvent, tokenID *big.Int, chainID, blockNumber int64, blockVersion int, blockTimestamp time.Time) (*entity.UniswapV3Position, *nfpmPositionResult, error) {
	cached := s.posCache.get(tokenID)
	if isMissingMarker(cached) {
		return nil, nil, nil
	}

	// Always read positions() so we get the latest fee growth / liquidity to
	// write to position_state. Cold or warm — same call.
	pos, err := s.blockchain.readNFPMPosition(ctx, s.config.NFPMAddress, tokenID, blockNumber)
	if err != nil {
		return nil, nil, fmt.Errorf("reading positions(%s): %w", tokenID, err)
	}
	if pos == nil {
		// positions(tokenId) reverted — the NFT was burned at (or before) this
		// block. Deterministic, so never retry.
		if cached == nil {
			// Cold path: the poolKey is unrecoverable, so scope is unknowable.
			// Treat as out-of-scope; the raw protocol_event row is still the
			// audit trail for the triggering event.
			s.logger.Warn("positions() reverted for unknown tokenId (burned); marking out-of-scope",
				"tokenId", tokenID.String(), "block", blockNumber)
			s.posCache.putMissing(tokenID)
			return nil, nil, nil
		}
		// Warm path: a tracked position was burned this block. Flag it; the
		// caller persists Burned via handleNFPMSideEffects and skips the state
		// row (there is no position state left to read).
		s.posCache.markBurned(cached.TokenID)
		return cached, nil, nil
	}

	if cached == nil {
		registered, err := s.registerPosition(ctx, decoded, tokenID, pos, chainID, blockNumber, blockTimestamp)
		if err != nil {
			return nil, nil, err
		}
		if registered == nil {
			s.posCache.putMissing(tokenID)
			return nil, nil, nil
		}
		cached = registered
	}

	// Update burned flag on the cached entity if NFPM reports liquidity == 0
	// post-Decrease. Don't flip back to false otherwise. markBurned writes
	// under the cache mutex so the field write doesn't race a concurrent
	// put/putMissing on the same entry.
	if pos.Liquidity != nil && pos.Liquidity.Sign() == 0 && decoded.Name == eventNFPMDecreaseLiquidity {
		s.posCache.markBurned(cached.TokenID)
	}

	_ = blockVersion
	_ = blockTimestamp
	return cached, pos, nil
}

// registerPosition inserts a uniswap_v3_position row if NFPM.positions() points
// at one of our tracked pools. Returns nil, nil when out-of-scope (caller marks
// the tokenId as missing).
func (s *Service) registerPosition(ctx context.Context, decoded *decodedEvent, tokenID *big.Int, p *nfpmPositionResult, chainID, blockNumber int64, _ time.Time) (*entity.UniswapV3Position, error) {
	pool := s.findPoolForPosition(p)
	if pool == nil {
		return nil, nil
	}
	// Resolve owner. For IncreaseLiquidity / DecreaseLiquidity / Collect the
	// NFT owner is not in the event payload; we leave it zero on first sight
	// and let a later ERC-721 Transfer carry the real owner.
	owner := common.Address{}
	if decoded != nil && decoded.NFPMTransfer != nil {
		owner = decoded.NFPMTransfer.To
	}

	entry := &entity.UniswapV3Position{
		ChainID:         chainID,
		NFPMAddress:     s.config.NFPMAddress,
		TokenID:         new(big.Int).Set(tokenID),
		UniswapV3PoolID: pool.ID,
		Owner:           owner,
		TickLower:       p.TickLower,
		TickUpper:       p.TickUpper,
		Fee:             p.Fee,
		CreatedAtBlock:  blockNumber,
	}
	err := s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		id, err := s.uniswapRepo.UpsertUniswapV3Position(ctx, tx, entry)
		if err != nil {
			return fmt.Errorf("upserting position tokenId=%s: %w", tokenID, err)
		}
		entry.ID = id
		return nil
	})
	if err != nil {
		return nil, err
	}
	s.posCache.put(entry)
	return entry, nil
}

// findPoolForPosition matches NFPM.positions() output against the in-memory
// pool registry by (token0, token1, fee). Returns nil if no match.
//
// sync.RWMutex is non-reentrant — a writer queued between two RLocks
// deadlocks the inner one. So this method holds the registry lock exactly
// once and reads tokens through the lock-less helper.
func (s *Service) findPoolForPosition(p *nfpmPositionResult) *entity.UniswapV3Pool {
	s.registry.mu.RLock()
	defer s.registry.mu.RUnlock()
	for _, pool := range s.registry.poolsByID {
		t0, ok0 := s.registry.token0[pool.ID]
		t1, ok1 := s.registry.token1[pool.ID]
		if !ok0 || !ok1 {
			continue
		}
		if t0 == p.Token0 && t1 == p.Token1 && pool.FeeTier == p.Fee {
			return pool
		}
	}
	return nil
}

// readPoolStateForEvent resolves the pool's token0/token1 addresses (lazy via
// a static read on first event) and issues the event-triggered multicall.
func (s *Service) readPoolStateForEvent(ctx context.Context, pool *entity.UniswapV3Pool, blockNumber int64) (*poolMulticallResult, error) {
	t0, t1, ok := s.registry.tokenAddresses(pool.ID)
	if !ok {
		gotT0, gotT1, _, err := s.blockchain.readPoolStatic(ctx, pool.Address, blockNumber)
		if err != nil {
			return nil, fmt.Errorf("reading pool static for %s: %w", pool.Label, err)
		}
		s.registry.setTokenAddresses(pool.ID, gotT0, gotT1)
		t0, t1 = gotT0, gotT1
	}
	return s.blockchain.readPoolState(ctx, pool, t0, t1, blockNumber)
}

// resolveProtocolID returns the cached UniswapV3 protocol_id, populating it
// via GetOrCreateProtocol on first call. Check-lock-check: a fast unlocked
// load short-circuits the steady-state hot path; the mutex is only taken on
// the first (or post-error) call to serialise the I/O. The atomic store
// publishes to subsequent unsynchronised reads.
func (s *Service) resolveProtocolID(ctx context.Context, tx pgx.Tx, chainID int64) (int64, error) {
	if id := s.uniswapV3ProtocolID.Load(); id != 0 {
		return id, nil
	}
	s.protocolIDMu.Lock()
	defer s.protocolIDMu.Unlock()
	if id := s.uniswapV3ProtocolID.Load(); id != 0 {
		return id, nil
	}
	id, err := s.protocolRepo.GetOrCreateProtocol(ctx, tx, chainID, UniswapV3ProtocolAddress, "UniswapV3", "dex", uniswapV3ProtocolDeployBlock)
	if err != nil {
		return 0, fmt.Errorf("getting UniswapV3 protocol_id: %w", err)
	}
	s.uniswapV3ProtocolID.Store(id)
	return id, nil
}

// saveProtocolEvent writes the raw audit row for any decoded event.
func (s *Service) saveProtocolEvent(ctx context.Context, tx pgx.Tx, decoded *decodedEvent, contractAddress common.Address, chainID, blockNumber int64, blockVersion int, blockTimestamp time.Time) error {
	protocolID, err := s.resolveProtocolID(ctx, tx, chainID)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(decoded)
	if err != nil {
		return fmt.Errorf("marshalling decoded event: %w", err)
	}
	evt, err := entity.NewProtocolEvent(
		int(chainID),
		protocolID,
		blockNumber,
		blockVersion,
		decoded.TxHash.Bytes(),
		int(decoded.LogIdx),
		contractAddress.Bytes(),
		string(decoded.Name),
		payload,
		blockTimestamp,
	)
	if err != nil {
		return fmt.Errorf("building protocol_event: %w", err)
	}
	return s.eventRepo.SaveEvent(ctx, tx, evt)
}

func validateDependencies(
	consumer outbound.SQSConsumer,
	cache outbound.BlockCacheReader,
	mc outbound.Multicaller,
	txm outbound.TxManager,
	uniswap outbound.UniswapV3PoolRepository,
	tok outbound.TokenRepository,
	proto outbound.ProtocolRepository,
	evt outbound.EventRepository,
) error {
	if consumer == nil {
		return fmt.Errorf("consumer is required")
	}
	if cache == nil {
		return fmt.Errorf("cache is required")
	}
	if mc == nil {
		return fmt.Errorf("multicaller is required")
	}
	if txm == nil {
		return fmt.Errorf("txManager is required")
	}
	if uniswap == nil {
		return fmt.Errorf("uniswapRepo is required")
	}
	if tok == nil {
		return fmt.Errorf("tokenRepo is required")
	}
	if proto == nil {
		return fmt.Errorf("protocolRepo is required")
	}
	if evt == nil {
		return fmt.Errorf("eventRepo is required")
	}
	return nil
}

// -----------------------------------------------------------------------------
// helpers
// -----------------------------------------------------------------------------

func nfpmTokenID(d *decodedEvent) *big.Int {
	switch {
	case d.NFPMIncrease != nil:
		return d.NFPMIncrease.TokenID
	case d.NFPMDecrease != nil:
		return d.NFPMDecrease.TokenID
	case d.NFPMCollect != nil:
		return d.NFPMCollect.TokenID
	case d.NFPMTransfer != nil:
		return d.NFPMTransfer.TokenID
	}
	return nil
}

// isLiquidityChanging returns true for NFPM events that move pool tick liquidity.
// Transfer (ERC-721) and Collect (fee withdrawal) don't change pool ticks; the
// pool emits its own Mint/Burn/Collect for the actual tick math, which we handle
// via the pool branch.
func isLiquidityChanging(d *decodedEvent) bool {
	return d.NFPMIncrease != nil || d.NFPMDecrease != nil
}

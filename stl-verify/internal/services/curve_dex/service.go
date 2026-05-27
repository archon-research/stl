package curve_dex

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
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// CurveProtocolAddress is the address recorded for the Curve protocol row in
// the `protocol` table. Matches the seed in
// db/migrations/20260521_100000_create_dex_prereqs.sql (the Stableswap-NG
// factory at 0x6A8cbed756804B16E05E741eDaBd5cB544AE21bf). Used as
// protocol_event.contract_address for events emitted by pools/LP-tokens, and
// as the GetOrCreateProtocol key.
var CurveProtocolAddress = common.HexToAddress("0x6A8cbed756804B16E05E741eDaBd5cB544AE21bf")

// CurveGaugeControllerAddress is the canonical mainnet GaugeController.
// Curve worker subscribes to NewGauge / KillGauge / Killed events here for
// runtime gauge discovery.
var CurveGaugeControllerAddress = common.HexToAddress("0x2F50D538606Fa9EDD2B11E2446BEb18C9D5846bB")

// CurveMetaRegistryAddress is the canonical mainnet MetaRegistry. Used by the
// one-time startup bootstrap to backfill curve_gauge rows from history before
// the event-driven GaugeController subscription takes over.
var CurveMetaRegistryAddress = common.HexToAddress("0xF98B45FA17DE75FB1aD0e7aFD971b0ca00e379fC")

// curveProtocolDeployBlock is the block at which the Curve Stableswap-NG
// factory landed on mainnet. Reused as created_at_block when GetOrCreateProtocol
// has to insert the protocol row (it should already exist via the prereq
// migration).
const curveProtocolDeployBlock = int64(19421686)

// Config holds service configuration.
type Config struct {
	shared.SQSConsumerConfig
}

// ConfigDefaults returns default configuration values.
func ConfigDefaults() Config {
	return Config{
		SQSConsumerConfig: shared.SQSConsumerConfigDefaults(),
	}
}

// Service is the Curve DEX SQS consumer.
type Service struct {
	config Config

	consumer     outbound.SQSConsumer
	cache        outbound.BlockCacheReader
	txManager    outbound.TxManager
	curveRepo    outbound.CurvePoolRepository
	tokenRepo    outbound.TokenRepository
	protocolRepo outbound.ProtocolRepository
	eventRepo    outbound.EventRepository

	blockchain *blockchainService
	extractor  *eventExtractor

	registry *poolRegistry

	// registeredTokens caches token addresses we've already registered this
	// session so we don't re-read symbol()/decimals() on every gauge event.
	registeredTokensMu sync.Mutex
	registeredTokens   map[common.Address]struct{}

	// curveProtocolID is the cached protocol_event.protocol_id for the Curve
	// protocol row, populated via a check-lock-check on first call. Reads on
	// the hot path are unsynchronised (a stale read just falls into the
	// locked branch and re-checks). This avoids holding protocolIDMu across
	// the GetOrCreateProtocol I/O call — a transient DB stall would
	// otherwise block every subsequent Curve event.
	protocolIDMu    sync.Mutex
	curveProtocolID atomic.Int64

	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger
}

// NewService constructs a Curve DEX service. Dependencies follow hexagonal
// ports — no postgres/sqs adapter packages are imported here.
func NewService(
	config Config,
	consumer outbound.SQSConsumer,
	cache outbound.BlockCacheReader,
	multicaller outbound.Multicaller,
	txManager outbound.TxManager,
	curveRepo outbound.CurvePoolRepository,
	tokenRepo outbound.TokenRepository,
	protocolRepo outbound.ProtocolRepository,
	eventRepo outbound.EventRepository,
) (*Service, error) {
	if err := validateDependencies(consumer, cache, multicaller, txManager, curveRepo, tokenRepo, protocolRepo, eventRepo); err != nil {
		return nil, fmt.Errorf("validating dependencies: %w", err)
	}
	config.SQSConsumerConfig.ApplyDefaults()
	if err := config.SQSConsumerConfig.Validate(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}

	extractor, err := newEventExtractor()
	if err != nil {
		return nil, fmt.Errorf("creating event extractor: %w", err)
	}
	bc, err := newBlockchainService(multicaller, config.Logger.With("component", "curve-dex-worker", "subsystem", "multicall"))
	if err != nil {
		return nil, fmt.Errorf("creating blockchain service: %w", err)
	}

	return &Service{
		config:           config,
		consumer:         consumer,
		cache:            cache,
		txManager:        txManager,
		curveRepo:        curveRepo,
		tokenRepo:        tokenRepo,
		protocolRepo:     protocolRepo,
		eventRepo:        eventRepo,
		blockchain:       bc,
		extractor:        extractor,
		registry:         newPoolRegistry(),
		registeredTokens: make(map[common.Address]struct{}),
		logger:           config.Logger.With("component", "curve-dex-worker"),
	}, nil
}

// Start primes the in-process pool/gauge registry from the database and begins
// the SQS poll loop.
func (s *Service) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	if err := s.loadRegistry(ctx); err != nil {
		return fmt.Errorf("loading curve pool registry: %w", err)
	}

	go sqsutil.RunLoop(s.ctx, sqsutil.Config{
		Consumer:     s.consumer,
		MaxMessages:  s.config.MaxMessages,
		PollInterval: s.config.PollInterval,
		Logger:       s.logger,
		ChainID:      s.config.ChainID,
	}, s.processBlockEvent)

	s.logger.Info("curve dex worker started",
		"chainID", s.config.ChainID,
		"pools", s.registry.poolCount(),
		"gauges", s.registry.gaugeCount())
	return nil
}

// Stop cancels the background loop.
func (s *Service) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.logger.Info("curve dex worker stopped")
	return nil
}

func (s *Service) loadRegistry(ctx context.Context) error {
	pools, err := s.curveRepo.ListEnabledCurvePools(ctx, s.config.ChainID)
	if err != nil {
		return fmt.Errorf("listing curve pools: %w", err)
	}
	for _, p := range pools {
		s.registry.addPool(p)
		gauge, err := s.curveRepo.GetCurveGauge(ctx, p.ID)
		if err != nil {
			return fmt.Errorf("loading gauge for pool %d: %w", p.ID, err)
		}
		if gauge != nil {
			s.registry.addGauge(gauge)
			continue
		}
		// No DB row yet — try the MetaRegistry one-time bootstrap so we pick
		// up gauges that were deployed before our GaugeController event
		// subscription window. Pinned to the head block (0) since gauge
		// addresses are immutable after deploy. A zero result means the pool
		// has no gauge today; the runtime NewGauge listener will catch any
		// future addition.
		if err := s.bootstrapGaugeFromMetaRegistry(ctx, p); err != nil {
			return fmt.Errorf("bootstrapping gauge for pool %d: %w", p.ID, err)
		}
	}
	return nil
}

// bootstrapGaugeFromMetaRegistry calls MetaRegistry.get_gauge(pool) and, if a
// non-zero gauge is returned, upserts a curve_gauge row and registers it
// in-process. blockNumber=nil ("latest") because gauge addresses are immutable
// once deployed — there is no event block to pin against at startup.
func (s *Service) bootstrapGaugeFromMetaRegistry(ctx context.Context, pool *entity.CurvePool) error {
	gaugeAddr, err := s.blockchain.readMetaRegistryGauge(ctx, CurveMetaRegistryAddress, pool.Address, nil)
	if err != nil {
		return fmt.Errorf("MetaRegistry.get_gauge for pool %s: %w", pool.Address.Hex(), err)
	}
	if (gaugeAddr == common.Address{}) {
		// Expected: pool has no gauge registered. Skip without writing a row.
		return nil
	}
	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		gauge := &entity.CurveGauge{
			CurvePoolID: pool.ID,
			ChainID:     pool.ChainID,
			Address:     gaugeAddr,
			Enabled:     true,
		}
		id, err := s.curveRepo.UpsertCurveGauge(ctx, tx, gauge)
		if err != nil {
			return fmt.Errorf("upserting bootstrap gauge %s: %w", gaugeAddr.Hex(), err)
		}
		gauge.ID = id
		s.registry.addGauge(gauge)
		return nil
	})
}

// processBlockEvent is the per-message handler. Each receipt's logs are filtered
// to the Curve scope and then dispatched per event category.
func (s *Service) processBlockEvent(ctx context.Context, event outbound.BlockEvent) error {
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
	// Cache of pools whose state we've snapshotted in this receipt — every
	// matched pool event triggers the same multicall, so dedupe per receipt.
	statesWritten := make(map[int64]bool)
	gaugesWritten := make(map[int64]bool)

	var errs []error
	for _, log := range receipt.Logs {
		addr := common.HexToAddress(log.Address)

		// Branch 1: log is from a Curve GaugeController.
		if addr == CurveGaugeControllerAddress {
			if entry := s.extractor.gaugeCtrlSig(log); entry != nil {
				if err := s.handleGaugeControllerLog(ctx, log, chainID, blockNumber, blockVersion, blockTimestamp); err != nil {
					errs = append(errs, err)
				}
				continue
			}
		}

		// Branch 2: log is from a known Curve pool.
		if pool := s.registry.poolByAddress(addr); pool != nil {
			if name, ok := s.extractor.poolTopic(log); ok {
				if err := s.handlePoolLog(ctx, log, pool, name, chainID, blockNumber, blockVersion, blockTimestamp, statesWritten, gaugesWritten); err != nil {
					errs = append(errs, err)
				}
				continue
			}
		}

		// Branch 3: LP token Transfer. Matches either a separate LP token
		// contract address (V1) or the pool address itself (NG, where the
		// pool is its own ERC-20). We also accept Transfer events emitted
		// by a tracked pool (already known to be the LP token).
		if s.extractor.isLPTokenTransfer(log) {
			if pool := s.registry.poolByLPToken(addr); pool != nil {
				if err := s.handleLPTokenTransfer(ctx, log, pool, chainID, blockNumber, blockVersion, blockTimestamp); err != nil {
					errs = append(errs, err)
				}
				continue
			}
		}

		// Branch 4: gauge event from a known gauge.
		if gauge := s.registry.gaugeByAddress(addr); gauge != nil {
			if s.extractor.gaugeSig(log) != nil {
				if err := s.handleGaugeLog(ctx, log, gauge, chainID, blockNumber, blockVersion, blockTimestamp, gaugesWritten); err != nil {
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
// row, the typed projection, the event-triggered pool state snapshot + exchange
// rate rows, and the gauge snapshot if a gauge is attached. All writes share
// one transaction.
func (s *Service) handlePoolLog(ctx context.Context, log shared.Log, pool *entity.CurvePool, name CurveEventName, chainID, blockNumber int64, blockVersion int, blockTimestamp time.Time, statesWritten, gaugesWritten map[int64]bool) error {
	decoded, err := s.extractor.extractPoolEvent(log, pool.PoolKind)
	if err != nil {
		return fmt.Errorf("decoding %s for pool %s: %w", name, pool.Label, err)
	}
	if decoded == nil {
		return nil
	}

	// Multicall runs BEFORE opening the DB tx: an RPC revert aborts here
	// without leaving partial state. SQS retry semantics are clean because
	// every typed-table INSERT in the tx below uses ON CONFLICT DO NOTHING
	// (ADR-0002); a same-build redelivery silently no-ops, a cross-build
	// reprocess lands at a fresh processing_version. The audit row + typed
	// projection therefore share one transaction (matching the other six
	// Curve handlers and both other DEX workers).
	state, err := s.blockchain.readPoolState(ctx, pool, blockNumber)
	if err != nil {
		return fmt.Errorf("reading pool state for %s at block %d: %w", pool.Label, blockNumber, err)
	}

	var gaugeState *gaugeMulticallResult
	gauge := s.registry.gaugeByPoolID(pool.ID)
	if gauge != nil {
		gs, err := s.blockchain.readGaugeState(ctx, gauge.Address, blockNumber)
		if err != nil {
			return fmt.Errorf("reading gauge state for %s at block %d: %w", pool.Label, blockNumber, err)
		}
		if gs.RewardCount != nil && *gs.RewardCount > 0 {
			tokens, rates, finishes, err := s.blockchain.readGaugeRewards(ctx, gauge.Address, *gs.RewardCount, blockNumber)
			if err != nil {
				return fmt.Errorf("reading gauge rewards for %s: %w", pool.Label, err)
			}
			gs.RewardTokens = tokens
			gs.RewardRates = rates
			gs.RewardPeriodFinish = finishes
		}
		gaugeState = gs
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		if err := s.saveProtocolEvent(ctx, tx, decoded, pool.Address, chainID, blockNumber, blockVersion, blockTimestamp); err != nil {
			return err
		}
		if err := s.savePoolEventProjection(ctx, tx, decoded, pool, chainID, blockNumber, blockVersion, blockTimestamp); err != nil {
			return err
		}
		if !statesWritten[pool.ID] {
			if err := s.savePoolState(ctx, tx, pool, state, blockNumber, blockVersion, blockTimestamp); err != nil {
				return err
			}
			if err := s.saveExchangeRates(ctx, tx, pool, state, blockNumber, blockVersion, blockTimestamp); err != nil {
				return err
			}
			statesWritten[pool.ID] = true
		}
		if gauge != nil && gaugeState != nil && !gaugesWritten[gauge.ID] {
			if err := s.saveGaugeState(ctx, tx, gauge, gaugeState, chainID, blockNumber, blockVersion, blockTimestamp); err != nil {
				return err
			}
			gaugesWritten[gauge.ID] = true
		}
		return nil
	})
}

// handleLPTokenTransfer decodes an ERC-20 Transfer on the LP token, writes the
// protocol_event audit row, and saves one curve_user_lp_position row per
// non-zero side (from + to). LP balances for each side are read via a single
// balanceOf multicall pinned to the event block; on per-user revert the
// balance lands as NULL and the consumer must fall back to SUM(delta).
func (s *Service) handleLPTokenTransfer(ctx context.Context, log shared.Log, pool *entity.CurvePool, chainID, blockNumber int64, blockVersion int, blockTimestamp time.Time) error {
	decoded, err := s.extractor.extractLPTokenTransfer(log)
	if err != nil {
		return fmt.Errorf("decoding Transfer for pool %s: %w", pool.Label, err)
	}
	t := decoded.Transfer
	if t == nil {
		return nil
	}

	zero := common.Address{}
	users := make([]common.Address, 0, 2)
	if t.From != zero {
		users = append(users, t.From)
	}
	if t.To != zero {
		users = append(users, t.To)
	}
	balances, err := s.blockchain.readLPBalances(ctx, effectiveLPToken(pool), users, blockNumber)
	if err != nil {
		return fmt.Errorf("reading LP balances for pool %s: %w", pool.Label, err)
	}
	balanceFor := func(addr common.Address) *big.Int {
		for i, u := range users {
			if u == addr {
				return balances[i]
			}
		}
		return nil
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		if err := s.saveProtocolEvent(ctx, tx, decoded, decoded.Address, chainID, blockNumber, blockVersion, blockTimestamp); err != nil {
			return err
		}
		if t.From != zero {
			pos := &entity.CurveUserLpPosition{
				CurvePoolID:  pool.ID,
				UserAddress:  t.From,
				BlockNumber:  blockNumber,
				BlockVersion: int32(blockVersion),
				Timestamp:    blockTimestamp,
				TxHash:       decoded.TxHash,
				LogIndex:     decoded.LogIdx,
				LpBalance:    balanceFor(t.From),
				Delta:        negate(t.Value),
			}
			if err := s.curveRepo.SaveCurveUserLpPosition(ctx, tx, pos); err != nil {
				return fmt.Errorf("saving lp position (from) for pool %s: %w", pool.Label, err)
			}
		}
		if t.To != zero {
			pos := &entity.CurveUserLpPosition{
				CurvePoolID:  pool.ID,
				UserAddress:  t.To,
				BlockNumber:  blockNumber,
				BlockVersion: int32(blockVersion),
				Timestamp:    blockTimestamp,
				TxHash:       decoded.TxHash,
				LogIndex:     decoded.LogIdx,
				LpBalance:    balanceFor(t.To),
				Delta:        bigIntCopy(t.Value),
			}
			if err := s.curveRepo.SaveCurveUserLpPosition(ctx, tx, pos); err != nil {
				return fmt.Errorf("saving lp position (to) for pool %s: %w", pool.Label, err)
			}
		}
		return nil
	})
}

// effectiveLPToken returns the contract address where the LP-token ERC-20
// state lives: pool.LPTokenAddress if a separate LP contract exists
// (pre-NG pools like steCRV / 3CRV), otherwise pool.Address itself
// (NG factory convention where the pool IS the LP token).
func effectiveLPToken(pool *entity.CurvePool) common.Address {
	if pool.LPTokenAddress != nil {
		return *pool.LPTokenAddress
	}
	return pool.Address
}

// handleGaugeLog processes a gauge-emitted Deposit / Withdraw /
// UpdateLiquidityLimit event. Saves the audit row and re-reads gauge view
// methods to produce a curve_gauge_state row.
func (s *Service) handleGaugeLog(ctx context.Context, log shared.Log, gauge *entity.CurveGauge, chainID, blockNumber int64, blockVersion int, blockTimestamp time.Time, gaugesWritten map[int64]bool) error {
	decoded, err := s.extractor.extractGaugeEvent(log)
	if err != nil {
		return fmt.Errorf("decoding gauge event at %s: %w", gauge.Address.Hex(), err)
	}
	gs, err := s.blockchain.readGaugeState(ctx, gauge.Address, blockNumber)
	if err != nil {
		return fmt.Errorf("reading gauge state for %s: %w", gauge.Address.Hex(), err)
	}
	if gs.RewardCount != nil && *gs.RewardCount > 0 {
		tokens, rates, finishes, err := s.blockchain.readGaugeRewards(ctx, gauge.Address, *gs.RewardCount, blockNumber)
		if err != nil {
			return fmt.Errorf("reading gauge rewards for %s: %w", gauge.Address.Hex(), err)
		}
		gs.RewardTokens = tokens
		gs.RewardRates = rates
		gs.RewardPeriodFinish = finishes
	}
	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		if err := s.saveProtocolEvent(ctx, tx, decoded, gauge.Address, chainID, blockNumber, blockVersion, blockTimestamp); err != nil {
			return err
		}
		if !gaugesWritten[gauge.ID] {
			if err := s.saveGaugeState(ctx, tx, gauge, gs, chainID, blockNumber, blockVersion, blockTimestamp); err != nil {
				return err
			}
			gaugesWritten[gauge.ID] = true
		}
		return nil
	})
}

// handleGaugeControllerLog handles NewGauge / KillGauge / Killed. NewGauge
// triggers gauge discovery: look up lp_token() and only persist if the LP
// token matches a tracked pool. Kill events flip is_killed on a known gauge.
func (s *Service) handleGaugeControllerLog(ctx context.Context, log shared.Log, chainID, blockNumber int64, blockVersion int, blockTimestamp time.Time) error {
	decoded, err := s.extractor.extractGaugeControllerEvent(log)
	if err != nil {
		return fmt.Errorf("decoding GaugeController event: %w", err)
	}
	switch decoded.Name {
	case EventNewGauge:
		return s.discoverNewGauge(ctx, decoded, chainID, blockNumber, blockVersion, blockTimestamp)
	case EventKillGauge, EventKilled, EventUnkillGauge, EventUnkilled:
		// Kill / Unkill toggles is_killed. Both controller-emitted variants
		// (KillGauge / UnkillGauge) and gauge-emitted variants (Killed /
		// Unkilled) flow through this branch.
		isKilled := decoded.Name == EventKillGauge || decoded.Name == EventKilled
		return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
			if err := s.saveProtocolEvent(ctx, tx, decoded, CurveGaugeControllerAddress, chainID, blockNumber, blockVersion, blockTimestamp); err != nil {
				return err
			}
			if err := s.curveRepo.SetCurveGaugeKilled(ctx, tx, chainID, decoded.GaugeController.GaugeAddress, isKilled); err != nil {
				return fmt.Errorf("flipping is_killed on %s: %w", decoded.GaugeController.GaugeAddress.Hex(), err)
			}
			return nil
		})
	default:
		return nil
	}
}

func (s *Service) discoverNewGauge(ctx context.Context, decoded *DecodedEvent, chainID, blockNumber int64, blockVersion int, blockTimestamp time.Time) error {
	gaugeAddr := decoded.GaugeController.GaugeAddress

	// Step 1: probe gauge_types to filter out non-stableswap gauges before
	// the lp_token() probe. The only type we index is 0 (Liquidity /
	// stableswap); 1+ are crypto/tricrypto/lending/etc. Skipping early
	// avoids hitting a revert-prone lp_token() on contracts that don't
	// implement it.
	gtype, err := s.blockchain.readGaugeType(ctx, CurveGaugeControllerAddress, gaugeAddr, blockNumber)
	if err != nil {
		return fmt.Errorf("reading gauge_types for %s: %w", gaugeAddr.Hex(), err)
	}
	if gtype.Sign() != 0 {
		// Expected: non-stableswap gauge. Log + skip without writing a row.
		s.logger.Info("ignoring NewGauge (non-stableswap type)", "gauge", gaugeAddr.Hex(), "type", gtype.String())
		return nil
	}

	lpToken, err := s.blockchain.readGaugeLPToken(ctx, gaugeAddr, blockNumber)
	if err != nil {
		// gauge_types said this IS a stableswap gauge — lp_token must succeed.
		// A revert here is now UNEXPECTED, so propagate so SQS retries.
		return fmt.Errorf("reading lp_token for stableswap gauge %s: %w", gaugeAddr.Hex(), err)
	}
	pool := s.registry.poolByLPToken(lpToken)
	if pool == nil {
		// Gauge belongs to a pool we don't track; only persist the audit row.
		return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
			return s.saveProtocolEvent(ctx, tx, decoded, CurveGaugeControllerAddress, chainID, blockNumber, blockVersion, blockTimestamp)
		})
	}
	gauge := &entity.CurveGauge{
		CurvePoolID:     pool.ID,
		ChainID:         chainID,
		Address:         gaugeAddr,
		DeploymentBlock: &blockNumber,
		IsKilled:        false,
		HasNoCRV:        false,
		Enabled:         true,
	}
	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		if err := s.saveProtocolEvent(ctx, tx, decoded, CurveGaugeControllerAddress, chainID, blockNumber, blockVersion, blockTimestamp); err != nil {
			return err
		}
		id, err := s.curveRepo.UpsertCurveGauge(ctx, tx, gauge)
		if err != nil {
			return fmt.Errorf("upserting gauge for pool %s: %w", pool.Label, err)
		}
		gauge.ID = id
		s.registry.addGauge(gauge)
		return nil
	})
}

// resolveProtocolID returns the cached Curve protocol_id, populating it via
// GetOrCreateProtocol on first call. Check-lock-check: a fast unlocked load
// short-circuits the steady-state hot path; the mutex is only taken on the
// first (or post-error) call to serialise the I/O, and is released before
// any subsequent read. The atomic store inside the lock publishes the value
// to readers without an explicit unlock-after-store fence.
func (s *Service) resolveProtocolID(ctx context.Context, tx pgx.Tx, chainID int64) (int64, error) {
	if id := s.curveProtocolID.Load(); id != 0 {
		return id, nil
	}
	s.protocolIDMu.Lock()
	defer s.protocolIDMu.Unlock()
	if id := s.curveProtocolID.Load(); id != 0 {
		return id, nil
	}
	id, err := s.protocolRepo.GetOrCreateProtocol(ctx, tx, chainID, CurveProtocolAddress, "Curve", "dex", curveProtocolDeployBlock)
	if err != nil {
		return 0, fmt.Errorf("getting Curve protocol_id: %w", err)
	}
	s.curveProtocolID.Store(id)
	return id, nil
}

// saveProtocolEvent writes the raw audit row for any decoded event.
func (s *Service) saveProtocolEvent(ctx context.Context, tx pgx.Tx, decoded *DecodedEvent, contractAddress common.Address, chainID, blockNumber int64, blockVersion int, blockTimestamp time.Time) error {
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
	curve outbound.CurvePoolRepository,
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
	if curve == nil {
		return fmt.Errorf("curveRepo is required")
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
// poolRegistry — in-memory snapshot of curve_pool / curve_gauge.
// -----------------------------------------------------------------------------

type poolRegistry struct {
	mu sync.RWMutex

	poolsByAddress map[common.Address]*entity.CurvePool
	poolsByLP      map[common.Address]*entity.CurvePool

	gaugesByAddress map[common.Address]*entity.CurveGauge
	gaugesByPool    map[int64]*entity.CurveGauge
}

func newPoolRegistry() *poolRegistry {
	return &poolRegistry{
		poolsByAddress:  make(map[common.Address]*entity.CurvePool),
		poolsByLP:       make(map[common.Address]*entity.CurvePool),
		gaugesByAddress: make(map[common.Address]*entity.CurveGauge),
		gaugesByPool:    make(map[int64]*entity.CurveGauge),
	}
}

func (r *poolRegistry) addPool(p *entity.CurvePool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.poolsByAddress[p.Address] = p
	// NG factory pools are their own LP token — the pool address is what
	// emits Transfer. V1 has a separate LP token contract; we register both
	// so address-lookup hits whichever the log came from.
	if p.LPTokenAddress != nil {
		r.poolsByLP[*p.LPTokenAddress] = p
	} else {
		r.poolsByLP[p.Address] = p
	}
}

func (r *poolRegistry) addGauge(g *entity.CurveGauge) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gaugesByAddress[g.Address] = g
	r.gaugesByPool[g.CurvePoolID] = g
}

func (r *poolRegistry) poolByAddress(a common.Address) *entity.CurvePool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.poolsByAddress[a]
}

func (r *poolRegistry) poolByLPToken(a common.Address) *entity.CurvePool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.poolsByLP[a]
}

func (r *poolRegistry) gaugeByAddress(a common.Address) *entity.CurveGauge {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.gaugesByAddress[a]
}

func (r *poolRegistry) gaugeByPoolID(id int64) *entity.CurveGauge {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.gaugesByPool[id]
}

func (r *poolRegistry) poolCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.poolsByAddress)
}

func (r *poolRegistry) gaugeCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.gaugesByAddress)
}

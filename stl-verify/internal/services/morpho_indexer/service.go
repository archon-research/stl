package morpho_indexer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"

	"github.com/archon-research/stl/stl-verify/internal/common/sqsutil"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/archiving"
	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// morphoBlueDeployBlocks maps chain IDs to the block at which Morpho Blue
// was deployed on that chain. Morpho Blue is deployed via CREATE2 at the
// same address on all chains, but each deployment occurred at a different block.
var morphoBlueDeployBlocks = map[int64]int64{
	1:     18883124,  // Ethereum mainnet
	8453:  18925795,  // Base
	42161: 226833208, // Arbitrum
}

// MorphoBlueDeployBlock returns the deploy block for the given chain ID.
func MorphoBlueDeployBlock(chainID int64) (int64, error) {
	block, ok := morphoBlueDeployBlocks[chainID]
	if !ok {
		return 0, fmt.Errorf("unsupported chain ID %d for Morpho Blue: no known deploy block", chainID)
	}
	return block, nil
}

// vaultV2FactoryDeployBlocks maps chain IDs to the block at which the Morpho
// VaultV2 factory (0xA1D94F746dEfa1928926b84fB2596c06926C0405) was deployed.
// Verified on-chain: the factory has no code at 23375072 and code at 23375073.
// Used by the morpho-vault-backfill's fromV2Deploy parameter to default `from`
// to the earliest block any VaultV2 could exist. That bounds the whole backfill
// pipeline — phase-1 discovery included — not just the V2 replay, so a V1/V1.1
// vault whose only activity predates the factory is not discovered.
var vaultV2FactoryDeployBlocks = map[int64]int64{
	1: 23_375_073, // Ethereum mainnet
}

// VaultV2FactoryDeployBlock returns the VaultV2 factory deploy block for the
// given chain ID.
func VaultV2FactoryDeployBlock(chainID int64) (int64, error) {
	block, ok := vaultV2FactoryDeployBlocks[chainID]
	if !ok {
		return 0, fmt.Errorf("unsupported chain ID %d for Morpho VaultV2: no known factory deploy block", chainID)
	}
	return block, nil
}

// Config holds service configuration.
type Config struct {
	shared.SQSConsumerConfig
	Telemetry *Telemetry // optional, nil-safe
}

// ConfigDefaults returns default configuration values.
func ConfigDefaults() Config {
	return Config{
		SQSConsumerConfig: shared.SQSConsumerConfigDefaults(),
	}
}

// NewReplayConfig builds the Config every replay composition root
// (morpho-vault-backfill, morpho-v2-bootstrap) hands NewReplayService, telemetry
// included. It exists so the wiring lives once: Config.Telemetry is nil-safe, so a
// root that forgets it silently mutes every event and snapshot a run replays.
//
// Not among those signals, ordinarily, is
// morpho_v2_adapter_registrations_total{observed_via="bootstrap_seed"}: the seed
// only asserts what the replay already recorded, so a healthy run appends nothing
// under that label and its absence is the expected reading.
//
// Nothing here dials: the chain name is a table lookup and the instruments come
// from the global meter provider, which no-ops when no exporter is configured.
func NewReplayConfig(chainID int64, logger *slog.Logger) (Config, error) {
	chainName, err := entity.ChainName(chainID)
	if err != nil {
		return Config{}, fmt.Errorf("resolving the chain name for telemetry: %w", err)
	}
	replayTelemetry, err := NewTelemetry(chainName)
	if err != nil {
		return Config{}, fmt.Errorf("creating morpho telemetry: %w", err)
	}
	config := ConfigDefaults()
	config.ChainID = chainID
	config.Logger = logger
	config.Telemetry = replayTelemetry
	return config, nil
}

// Service is the Morpho indexer SQS consumer service.
type Service struct {
	config           Config
	deployBlock      int64 // resolved Morpho Blue deploy block for the configured chain
	consumer         outbound.SQSConsumer
	cache            outbound.BlockCacheReader
	txManager        outbound.TxManager
	userRepo         outbound.UserRepository
	protocolRepo     outbound.ProtocolRepository
	tokenRepo        outbound.TokenRepository
	morphoRepo       outbound.MorphoRepository
	eventRepo        outbound.EventRepository
	receiptTokenRepo outbound.ReceiptTokenRepository

	blockchainSvc  *blockchainService
	eventExtractor *EventExtractor
	vaultRegistry  *VaultRegistry
	telemetry      *Telemetry

	// v2StructuredTopics gates ReplayMetaMorphoLog: the replay constructor nils
	// the user/token/cache/consumer/receipt-token ports, so only the VaultV2
	// structured governance/allocation/cap/fee events (which never touch them)
	// are safe to replay. Any other MetaMorpho topic (e.g. a V1 Deposit) is
	// rejected before it can nil-deref the share-accounting path.
	v2StructuredTopics map[common.Hash]struct{}

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup // tracks the SQS run loop so Stop can drain it
	logger *slog.Logger
}

// NewService creates a new Morpho indexer service.
func NewService(
	config Config,
	consumer outbound.SQSConsumer,
	cache outbound.BlockCacheReader,
	multicallClient outbound.Multicaller,
	txManager outbound.TxManager,
	userRepo outbound.UserRepository,
	protocolRepo outbound.ProtocolRepository,
	tokenRepo outbound.TokenRepository,
	morphoRepo outbound.MorphoRepository,
	eventRepo outbound.EventRepository,
	receiptTokenRepo outbound.ReceiptTokenRepository,
) (*Service, error) {
	if err := validateDependencies(consumer, cache, multicallClient, txManager, userRepo, protocolRepo, tokenRepo, morphoRepo, eventRepo, receiptTokenRepo); err != nil {
		return nil, fmt.Errorf("validating dependencies: %w", err)
	}
	return newService(config, consumer, cache, multicallClient, txManager, userRepo, protocolRepo, tokenRepo, morphoRepo, eventRepo, receiptTokenRepo)
}

// NewReplayService builds a Service wired only for offline replay of
// already-persisted VaultV2 vaults' structured events — the morpho-vault-backfill
// backfiller's V2 replay phase. It shares NewService's internals but omits the
// SQS consumer and block cache: replay reads receipts from S3 and drives logs
// through ReplayMetaMorphoLog directly, never through the live SQS loop, so Start
// must not be called on the result. userRepo / tokenRepo / receiptTokenRepo are
// likewise absent because the replayed adapter / cap / fee events never touch
// them (share-accounting Deposit/Withdraw/Transfer writes are out of the replay's
// scope).
func NewReplayService(
	config Config,
	multicallClient outbound.Multicaller,
	txManager outbound.TxManager,
	protocolRepo outbound.ProtocolRepository,
	morphoRepo outbound.MorphoRepository,
	eventRepo outbound.EventRepository,
) (*Service, error) {
	if err := validateReplayDependencies(multicallClient, txManager, protocolRepo, morphoRepo, eventRepo); err != nil {
		return nil, fmt.Errorf("validating replay dependencies: %w", err)
	}
	return newService(config, nil, nil, multicallClient, txManager, nil, protocolRepo, nil, morphoRepo, eventRepo, nil)
}

// newService assembles the Service from dependencies already validated by the
// live (NewService) or replay (NewReplayService) constructor. Ports the replay
// path doesn't use may be nil; the replay entry point never dereferences them.
func newService(
	config Config,
	consumer outbound.SQSConsumer,
	cache outbound.BlockCacheReader,
	multicallClient outbound.Multicaller,
	txManager outbound.TxManager,
	userRepo outbound.UserRepository,
	protocolRepo outbound.ProtocolRepository,
	tokenRepo outbound.TokenRepository,
	morphoRepo outbound.MorphoRepository,
	eventRepo outbound.EventRepository,
	receiptTokenRepo outbound.ReceiptTokenRepository,
) (*Service, error) {
	config.SQSConsumerConfig.ApplyDefaults()
	if err := config.SQSConsumerConfig.Validate(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}

	deployBlock, err := MorphoBlueDeployBlock(config.ChainID)
	if err != nil {
		return nil, fmt.Errorf("getting deploy block: %w", err)
	}

	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		return nil, fmt.Errorf("failed to load ERC20 ABI: %w", err)
	}

	eventExtractor, err := NewEventExtractor()
	if err != nil {
		return nil, fmt.Errorf("failed to create event extractor: %w", err)
	}

	blockchainSvc, err := newBlockchainService(multicallClient, erc20ABI, config.Logger, config.Telemetry)
	if err != nil {
		return nil, fmt.Errorf("failed to create blockchain service: %w", err)
	}

	v2StructuredTopics, err := VaultV2StructuredEventTopics()
	if err != nil {
		return nil, fmt.Errorf("deriving VaultV2 structured event topics: %w", err)
	}

	return &Service{
		config:             config,
		deployBlock:        deployBlock,
		consumer:           consumer,
		cache:              cache,
		txManager:          txManager,
		userRepo:           userRepo,
		protocolRepo:       protocolRepo,
		tokenRepo:          tokenRepo,
		morphoRepo:         morphoRepo,
		eventRepo:          eventRepo,
		receiptTokenRepo:   receiptTokenRepo,
		blockchainSvc:      blockchainSvc,
		eventExtractor:     eventExtractor,
		vaultRegistry:      NewVaultRegistry(config.Logger),
		telemetry:          config.Telemetry,
		v2StructuredTopics: v2StructuredTopics,
		logger:             config.Logger.With("component", "morpho-indexer"),
	}, nil
}

// The visibility-timeout guard is fatal, so it runs before any startup I/O: a
// misconfigured pod would otherwise re-run the whole sweep on every
// CrashLoopBackOff cycle before refusing.
func (s *Service) consumeLoop() sqsutil.Config {
	return sqsutil.Config{
		Consumer:     s.consumer,
		MaxMessages:  s.config.MaxMessages,
		PollInterval: s.config.PollInterval,
		Logger:       s.logger,
		ChainID:      s.config.ChainID,
	}
}

// Start begins the SQS message processing loop.
func (s *Service) Start(ctx context.Context) error {
	loop := s.consumeLoop()
	if err := loop.Validate(); err != nil {
		return err
	}

	s.ctx, s.cancel = context.WithCancel(ctx)

	if err := s.LoadVaultRegistry(ctx); err != nil {
		return err
	}

	s.wg.Go(func() {
		sqsutil.RunLoop(s.ctx, loop, s.processBlockEvent)
	})

	s.logger.Info("morpho indexer started",
		"maxMessages", s.config.MaxMessages,
		"vaults", s.vaultRegistry.Count())
	return nil
}

// Stop cancels the SQS processing loop and waits for the loop goroutine to
// exit. A handler the drain abandoned can outlive it; archiving's drain gate is
// what refuses that handler's late archive write.
func (s *Service) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	s.logger.Info("morpho indexer stopped")
	return nil
}

func (s *Service) processBlockEvent(ctx context.Context, event outbound.BlockEvent) error {
	// Stamp the reorg-aware block version once, here, so both receipt processing
	// and the symbol-reconciliation sweep below archive raw SC calls under the
	// block's actual version. Setting it only inside fetchAndProcessReceipts would
	// leave reconcilePendingSymbols' multicalls keyed as version 0.
	ctx = archiving.WithBlockVersion(ctx, event.Version)
	ctx = archiving.WithBlockNumber(ctx, event.BlockNumber)
	if err := s.fetchAndProcessReceipts(ctx, event); err != nil {
		return err
	}
	// Best-effort symbol reconciliation. Never fails the block: the block is
	// already fully indexed, and a sweep error just leaves tokens pending for the
	// next sweep. Reads only at the block just processed.
	s.reconcilePendingSymbols(ctx, event.ChainID, event.BlockNumber)
	return nil
}

// Sweep cadence and batch bound for symbol reconciliation. Hardcoded: the sweep
// is one bounded multicall every symbolSweepIntervalBlocks blocks, so there is
// nothing worth tuning per environment.
const (
	symbolSweepIntervalBlocks = 10
	symbolSweepBatchSize      = 500
)

// reconcilePendingSymbols runs, every symbolSweepIntervalBlocks processed blocks,
// a best-effort pass that re-reads symbol() for tokens still missing one, at the
// block just processed. An empty symbol column is itself the "pending" marker, so
// there is no extra bookkeeping state; tokens whose symbol() never becomes
// readable are simply retried each sweep (one bounded multicall). All errors are
// logged and swallowed: the block is already indexed and must never be failed by
// this pass.
func (s *Service) reconcilePendingSymbols(ctx context.Context, chainID, blockNumber int64) {
	if blockNumber%symbolSweepIntervalBlocks != 0 {
		return
	}
	missing, err := s.tokenRepo.ListTokensMissingSymbol(ctx, chainID, symbolSweepBatchSize)
	if err != nil {
		s.logger.Warn("symbol reconciliation: listing tokens missing symbol failed", "error", err, "block", blockNumber)
		s.telemetry.RecordError(ctx, "reconcilePendingSymbols", err)
		return
	}
	// Surface the backlog size on every sweep (capped at the batch size): with no
	// backstop, growth toward the batch limit is the signal that tokens are
	// accumulating that never resolve, and oldest-first ordering would starve
	// newer ones once the limit is hit.
	s.telemetry.RecordSymbolsMissing(ctx, int64(len(missing)))
	if len(missing) == 0 {
		return
	}
	if len(missing) == symbolSweepBatchSize {
		s.logger.Warn("symbol reconciliation: batch full; remaining tokens are picked up on later sweeps",
			"batch", symbolSweepBatchSize, "block", blockNumber)
	}
	resolved, err := s.blockchainSvc.resolveSymbolsAt(ctx, missing, blockNumber)
	if err != nil {
		s.logger.Warn("symbol reconciliation: resolving symbols failed", "error", err, "block", blockNumber)
		s.telemetry.RecordError(ctx, "reconcilePendingSymbols", err)
		return
	}
	for addr, sym := range resolved {
		if err := s.tokenRepo.ResolveTokenSymbol(ctx, chainID, addr, sym); err != nil {
			s.logger.Warn("symbol reconciliation: persisting resolved symbol failed", "error", err, "address", addr.Hex())
			s.telemetry.RecordError(ctx, "reconcilePendingSymbols", err)
			continue
		}
		s.logger.Info("symbol reconciliation: resolved token symbol", "address", addr.Hex(), "symbol", sym, "block", blockNumber)
	}
}

func (s *Service) fetchAndProcessReceipts(ctx context.Context, event outbound.BlockEvent) (retErr error) {
	// Block version is stamped by the caller (processBlockEvent) so the symbol
	// sweep shares it; see the comment there.
	ctx, span := s.telemetry.StartBlockSpan(ctx, event.BlockNumber)
	defer span.End()

	start := time.Now()
	defer func() {
		duration := time.Since(start)
		s.telemetry.RecordBlockProcessed(ctx, duration, retErr)
		if retErr != nil {
			telemetry.SetSpanError(span, retErr, "block processing failed")
			s.telemetry.RecordError(ctx, "fetchAndProcessReceipts", retErr)
		}
		s.logger.Debug("fetchAndProcessReceipts completed",
			"block", event.BlockNumber,
			"duration", duration)
	}()

	receiptsJSON, err := s.cache.GetReceipts(ctx, event.ChainID, event.BlockNumber, event.Version)
	if err != nil {
		return fmt.Errorf("fetching receipts from cache: %w", err)
	}
	if receiptsJSON == nil {
		return fmt.Errorf("receipts not found in cache for block %d (chain=%d, version=%d)", event.BlockNumber, event.ChainID, event.Version)
	}

	blockHash, err := event.ParsedBlockHash()
	if err != nil {
		return fmt.Errorf("parse block hash: %w", err)
	}

	var receipts []shared.TransactionReceipt
	if err := json.Unmarshal(receiptsJSON, &receipts); err != nil {
		return fmt.Errorf("unmarshalling receipts: %w", err)
	}

	span.SetAttributes(attribute.Int("receipts.count", len(receipts)))

	totalLogs := 0
	for _, r := range receipts {
		totalLogs += len(r.Logs)
	}
	s.logger.Debug("processing block",
		"block", event.BlockNumber,
		"version", event.Version,
		"receipts", len(receipts),
		"logs", totalLogs,
		"knownVaults", s.vaultRegistry.Count())

	blockTimestamp := time.Unix(event.BlockTimestamp, 0).UTC()

	var errs []error
	for _, receipt := range receipts {
		if err := s.processReceipt(ctx, receipt, event.ChainID, event.BlockNumber, blockHash, event.Version, blockTimestamp); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// hasRelevantEvents returns true if the receipt contains at least one
// Morpho Blue or MetaMorpho event that will actually be processed.
// This checks both topic signatures and addresses to avoid creating
// empty spans for common events (e.g. Transfer) from non-vault contracts.
func (s *Service) hasRelevantEvents(receipt shared.TransactionReceipt) bool {
	morphoBlueAddr := MorphoBlueAddress
	for _, log := range receipt.Logs {
		logAddress := common.HexToAddress(log.Address)
		isMorphoBlue := s.eventExtractor.IsMorphoBlueEvent(log)
		isMetaMorpho := s.eventExtractor.IsMetaMorphoEvent(log)

		if !isMorphoBlue && !isMetaMorpho {
			continue
		}
		// MorphoBlue events from the MorphoBlue contract are always relevant.
		if logAddress == morphoBlueAddr && isMorphoBlue {
			return true
		}
		// Skip known non-vaults (except MorphoBlue address, handled above).
		if logAddress != morphoBlueAddr && s.vaultRegistry.IsKnownNotVault(logAddress) {
			continue
		}
		// MetaMorpho event from an address that isn't the MorphoBlue contract and
		// isn't a known non-vault. This covers two cases:
		//  1. Known vault emitting a MetaMorpho event (Deposit, Withdraw, Transfer, AccrueInterest).
		//     Always relevant — we already know it's a vault.
		//  2. Unknown address. Only worth tracing if the event is vault-activity
		//     (Deposit / Withdraw / AccrueInterest). Plain ERC20 Transfer from
		//     an unknown address is not a discovery trigger; processReceipt
		//     would otherwise skip it after gating, leaving an empty span.
		//     Mirrors the gate in processReceipt's default branch.
		if isMetaMorpho && logAddress != morphoBlueAddr {
			if s.vaultRegistry.IsKnownVault(logAddress) || s.eventExtractor.IsVaultActivityEvent(log) {
				return true
			}
		}
	}
	return false
}

// processReceipt processes all Morpho-related logs in a single transaction receipt.
//
// Note: eth_call reads return end-of-block state. Multiple events for the same
// entity within one block produce identical on-chain snapshots. The ON CONFLICT
// clause means only the last-written event_type/tx_hash is retained, but the
// on-chain state (shares, assets, collateral) is always correct.
func (s *Service) processReceipt(ctx context.Context, receipt shared.TransactionReceipt, chainID, blockNumber int64, blockHash common.Hash, blockVersion int, blockTimestamp time.Time) (retErr error) {
	if !s.hasRelevantEvents(receipt) {
		return nil
	}

	ctx, span := s.telemetry.StartSpan(ctx, "morpho.processReceipt",
		attribute.String("tx.hash", receipt.TransactionHash))
	defer func() {
		if retErr != nil {
			telemetry.SetSpanError(span, retErr, "receipt processing failed")
		}
		span.End()
	}()

	// Pre-walk: probe Morpho Blue events' caller / onBehalf (or borrower
	// for Liquidate) for V1/V1.1 vault discovery BEFORE the main loop
	// processes any log. This mirrors the morpho-vault-backfill's
	// V1/V1.1 path; it has to live in the live indexer because the
	// backfiller is recovery-only and IsVaultActivityEvent is narrowed to
	// the V2 4-field AccrueInterest topic, so V1/V1.1 vaults emitting their
	// own Deposit/Withdraw/V1 AccrueInterest logs would otherwise be
	// invisible to live indexing.
	//
	// Pre-walk-then-main-walk (rather than discover-during-main-walk +
	// SQS redelivery) means by the time the main loop reaches a vault's
	// Deposit log, the registry already has the vault. No reprocessing of
	// the receipt or block is needed; transient probe failures still
	// propagate naturally and SQS still redelivers them, but ordinary
	// first-activity-for-a-brand-new-vault is a single-pass success path.
	//
	// The V2 IsVaultActivityEvent path stays inline in the default case
	// below — V2 emits its 4-field AccrueInterest first in every
	// state-changing transaction, so single-pass discovery there is correct.
	if err := s.discoverV1V11VaultsInReceipt(ctx, receipt, chainID, blockNumber, blockHash, blockVersion, blockTimestamp); err != nil {
		return err
	}

	var errs []error
	// Track the FIRST transient vault discovery failure per address.
	// VEC-188: a later success for the same address must NOT wipe an earlier
	// failure — the earlier log's event was never persisted, so we must
	// surface the error and let SQS redeliver so both logs are retried.
	discoveryErrs := make(map[common.Address]error)
	morphoBlueAddr := MorphoBlueAddress

	for _, log := range receipt.Logs {
		logAddress := common.HexToAddress(log.Address)
		isMorphoBlue := s.eventExtractor.IsMorphoBlueEvent(log)
		isMetaMorpho := s.eventExtractor.IsMetaMorphoEvent(log)

		if !isMorphoBlue && !isMetaMorpho {
			continue
		}
		if logAddress != morphoBlueAddr && s.vaultRegistry.IsKnownNotVault(logAddress) {
			continue
		}

		switch {
		case logAddress == morphoBlueAddr && isMorphoBlue:
			s.logger.Debug("processing Morpho Blue event", "tx", receipt.TransactionHash, "topic", log.Topics[0])
			if err := s.processMorphoBlueLog(ctx, log, chainID, blockNumber, blockHash, blockVersion, blockTimestamp); err != nil {
				s.logger.Error("failed to process Morpho Blue event", "error", err, "tx", receipt.TransactionHash)
				errs = append(errs, err)
			}

		case logAddress == morphoBlueAddr:
			s.logger.Debug("skipping morpho blue address event", "logAddress", logAddress.Hex(), "tx", receipt.TransactionHash, "topic", log.Topics[0])

		case s.vaultRegistry.IsKnownVault(logAddress) && isMetaMorpho:
			s.logger.Debug("processing MetaMorpho event", "tx", receipt.TransactionHash, "vault", logAddress.Hex(), "topic", log.Topics[0])
			if err := s.processMetaMorphoLog(ctx, log, logAddress, chainID, blockNumber, blockHash, blockVersion, blockTimestamp); err != nil {
				s.logger.Error("failed to process MetaMorpho event", "error", err, "tx", receipt.TransactionHash)
				errs = append(errs, err)
			}

		default:
			// Discovery gate: only a VaultV2 4-field AccrueInterest event from
			// an unknown address triggers a probe. V1/V1.1 vaults are
			// discovered via the Morpho Blue path (caller/onBehalf), so this
			// path is V2-only. See IsVaultActivityEvent docstring for the
			// full rationale.
			//
			// The narrow gate also keeps the probe well clear of legacy
			// ERC20s (BAT, STORJ, deployed pre-Solidity-0.4.10) that
			// terminate unrecognised selector calls with `INVALID` (0xfe)
			// instead of `REVERT`. `INVALID` consumes all available gas,
			// and Multicall3's `aggregate3` doesn't bound per-sub-call gas,
			// so a 4-call probe (VEC-198) against such contracts blows past
			// Alchemy's 550M `eth_call` cap and surfaces as a transient
			// transport error — never reaches `ErrNotVault`, never enters
			// the negative cache, retries forever.
			//
			// Same predicate is used by the morpho-vault-backfill
			// (see cmd/backfillers/morpho-vault-backfill/discovery.go), so the
			// live and offline discovery contracts stay aligned.
			if !s.eventExtractor.IsVaultActivityEvent(log) {
				continue
			}
			s.logger.Debug("attempting vault discovery", "address", logAddress.Hex(), "tx", receipt.TransactionHash)
			if err := s.tryDiscoverVault(ctx, log, logAddress, chainID, blockNumber, blockHash, blockVersion, blockTimestamp); err != nil {
				var nv *ErrNotVault
				if errors.As(err, &nv) {
					s.vaultRegistry.MarkNotVault(logAddress)
					if nv.VaultShaped {
						// Address exposes at least one of MORPHO/curator/liquidityAdapter
						// but didn't match a known vault flavour. Surface at WARN —
						// pre-VEC-198 this case (Morpho VaultV2) sat invisible for ~225
						// days; if Morpho ships a V3 we want a signal in logs/dashboards.
						s.logger.Warn("vault-shaped address rejected by probe — possible new vault flavour",
							"address", logAddress.Hex(), "reason", err)
					} else {
						s.logger.Debug("not a Morpho-family vault", "address", logAddress.Hex(), "reason", err)
					}
				} else {
					s.logger.Warn("vault discovery failed (will retry)", "address", logAddress.Hex(), "error", err)
					// VEC-188: keep the first failure. A later success for the
					// same vault address does NOT retroactively process the
					// earlier log — that log's event was never saved. Surfacing
					// the error forces SQS to redeliver so BOTH logs are retried.
					if _, seen := discoveryErrs[logAddress]; !seen {
						discoveryErrs[logAddress] = fmt.Errorf("vault discovery for %s in tx %s: %w", logAddress.Hex(), receipt.TransactionHash, err)
					}
				}
			}
			// Intentionally no delete(discoveryErrs, logAddress) on success:
			// a later success doesn't undo the earlier log's loss.
		}
	}

	for _, err := range discoveryErrs {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// processMorphoBlueLog handles a Morpho Blue event log.
func (s *Service) processMorphoBlueLog(ctx context.Context, log shared.Log, chainID, blockNumber int64, blockHash common.Hash, blockVersion int, blockTimestamp time.Time) error {
	event, err := s.eventExtractor.ExtractMorphoBlueEvent(log)
	if err != nil {
		return fmt.Errorf("extracting Morpho Blue event: %w", err)
	}

	marketID := event.MarketID()
	ctx, span := s.telemetry.StartSpan(ctx, "morpho.processMorphoBlueEvent",
		attribute.String("event.type", string(event.Type())),
		attribute.String("market.id", fmt.Sprintf("%x", marketID[:8])))
	defer span.End()
	s.telemetry.RecordEventProcessed(ctx, string(event.Type()))

	s.logger.Info("Morpho Blue event detected",
		"event", event.Type(),
		"market", fmt.Sprintf("%x", marketID[:8]),
		"tx", event.TxHash(),
		"block", blockNumber)

	// Save raw protocol event
	logIndex, err := parseLogIndex(log)
	if err != nil {
		return err
	}
	if err := s.saveProtocolEvent(ctx, event, chainID, blockNumber, blockVersion, int(logIndex), blockTimestamp); err != nil {
		return fmt.Errorf("saving protocol event: %w", err)
	}

	switch e := event.(type) {
	case *CreateMarketEvent:
		return s.handleCreateMarket(ctx, e, chainID, blockNumber, blockHash, blockVersion, blockTimestamp)
	case *SupplyEvent:
		return s.handlePositionEvent(ctx, e.MarketID(), e.OnBehalf, e.Type(), e.TxHash(), chainID, blockNumber, blockHash, blockVersion, blockTimestamp)
	case *WithdrawEvent:
		return s.handlePositionEvent(ctx, e.MarketID(), e.OnBehalf, e.Type(), e.TxHash(), chainID, blockNumber, blockHash, blockVersion, blockTimestamp)
	case *BorrowEvent:
		return s.handlePositionEvent(ctx, e.MarketID(), e.OnBehalf, e.Type(), e.TxHash(), chainID, blockNumber, blockHash, blockVersion, blockTimestamp)
	case *RepayEvent:
		return s.handlePositionEvent(ctx, e.MarketID(), e.OnBehalf, e.Type(), e.TxHash(), chainID, blockNumber, blockHash, blockVersion, blockTimestamp)
	case *SupplyCollateralEvent:
		return s.handlePositionEvent(ctx, e.MarketID(), e.OnBehalf, e.Type(), e.TxHash(), chainID, blockNumber, blockHash, blockVersion, blockTimestamp)
	case *WithdrawCollateralEvent:
		return s.handlePositionEvent(ctx, e.MarketID(), e.OnBehalf, e.Type(), e.TxHash(), chainID, blockNumber, blockHash, blockVersion, blockTimestamp)
	case *LiquidateEvent:
		return s.handleLiquidateEvent(ctx, e, chainID, blockNumber, blockHash, blockVersion, blockTimestamp)
	case *AccrueInterestEvent:
		return s.handleAccrueInterest(ctx, e, chainID, blockNumber, blockHash, blockVersion, blockTimestamp)
	case *SetFeeEvent:
		// Already saved as protocol_event above
		return nil
	default:
		return nil
	}
}

// processMetaMorphoLog handles a MetaMorpho vault event log.
//
// Every recognised vault event lands in protocol_event as an audit-log row,
// keyed by (tx_hash, log_index). Events with state-affecting typed handlers
// additionally produce structured rows via the dispatch below:
//   - Deposit / Withdraw / Transfer / AccrueInterest → vault state + position.
//   - AddAdapter / RemoveAdapter → the adapter registry.
//   - Allocate / Deallocate → an adapter realAssets() state snapshot.
//   - Increase/DecreaseAbsoluteCap, Increase/DecreaseRelativeCap → vault caps.
//   - SetPerformanceFee / SetManagementFee (+ their recipients) → a full
//     fee-config snapshot (morpho_vault_fee).
//   - ForceDeallocate → a WARN only (its companion Deallocate log snapshots).
//
// The remaining registered V2 surface (SetCurator, Submit, timelock / gate /
// metadata setters, …) has no typed handler: it lands in the audit log only.
func (s *Service) processMetaMorphoLog(ctx context.Context, log shared.Log, vaultAddress common.Address, chainID, blockNumber int64, blockHash common.Hash, blockVersion int, blockTimestamp time.Time) error {
	eventName, ok := s.eventExtractor.MetaMorphoEventName(log)
	if !ok {
		// Caller already filtered via IsMetaMorphoEvent; this shouldn't
		// happen unless the topic registration drifted.
		return fmt.Errorf("MetaMorpho event has unrecognised topic: %v", log.Topics)
	}

	// Parsed once here and passed down: the VaultV2 handlers record the position of
	// the observation WITHIN its block, which is what lets an add, a remove and a
	// re-add in one block be three distinct observations of the adapter set rather
	// than one collapsed row.
	logIndex, err := parseLogIndex(log)
	if err != nil {
		return err
	}

	ctx, span := s.telemetry.StartSpan(ctx, "morpho.processMetaMorphoEvent",
		attribute.String("event.type", eventName),
		attribute.String("vault.address", vaultAddress.Hex()))
	defer span.End()
	s.telemetry.RecordEventProcessed(ctx, eventName)

	s.logger.Info("MetaMorpho event detected",
		"event", eventName,
		"vault", vaultAddress.Hex(),
		"tx", log.TransactionHash,
		"block", blockNumber)

	if err := s.saveMetaMorphoProtocolEvent(ctx, log, vaultAddress, eventName, chainID, blockNumber, blockVersion, blockTimestamp, logIndex); err != nil {
		return fmt.Errorf("saving MetaMorpho protocol_event: %w", err)
	}

	event, err := s.eventExtractor.ExtractMetaMorphoEvent(log)
	if err != nil {
		return fmt.Errorf("extracting MetaMorpho event: %w", err)
	}
	if event == nil {
		// Registered topic without a typed handler — audit-log save above
		// is the only side effect.
		return nil
	}

	switch e := event.(type) {
	case *VaultDepositEvent:
		return s.saveVaultEventSnapshot(ctx, e.Owner, vaultAddress, chainID, blockNumber, blockHash, blockVersion, blockTimestamp, e.Type(), e.TxHash())
	case *VaultWithdrawEvent:
		return s.saveVaultEventSnapshot(ctx, e.Owner, vaultAddress, chainID, blockNumber, blockHash, blockVersion, blockTimestamp, e.Type(), e.TxHash())
	case *VaultTransferEvent:
		return s.handleVaultTransfer(ctx, e, vaultAddress, chainID, blockNumber, blockHash, blockVersion, blockTimestamp)
	case *VaultAccrueInterestEvent:
		return s.handleVaultAccrueInterest(ctx, e, vaultAddress, chainID, blockNumber, blockHash, blockVersion, blockTimestamp)
	case *AddAdapterEvent:
		return s.handleAddAdapter(ctx, e, vaultAddress, blockNumber, blockHash, blockVersion, blockTimestamp, logIndex)
	case *RemoveAdapterEvent:
		return s.handleRemoveAdapter(ctx, e, vaultAddress, blockNumber, blockVersion, blockTimestamp, logIndex)
	case *AllocateEvent:
		return s.handleAllocation(ctx, e.Adapter, vaultAddress, blockNumber, blockHash, blockVersion, blockTimestamp, logIndex)
	case *DeallocateEvent:
		return s.handleAllocation(ctx, e.Adapter, vaultAddress, blockNumber, blockHash, blockVersion, blockTimestamp, logIndex)
	case *ForceDeallocateEvent:
		return s.handleForceDeallocate(ctx, e, vaultAddress, blockNumber)
	case *IncreaseAbsoluteCapEvent:
		return s.handleCapChange(ctx, vaultAddress, e.ID, e.IDData, blockNumber, blockHash, blockVersion, blockTimestamp)
	case *DecreaseAbsoluteCapEvent:
		return s.handleCapChange(ctx, vaultAddress, e.ID, e.IDData, blockNumber, blockHash, blockVersion, blockTimestamp)
	case *IncreaseRelativeCapEvent:
		return s.handleCapChange(ctx, vaultAddress, e.ID, e.IDData, blockNumber, blockHash, blockVersion, blockTimestamp)
	case *DecreaseRelativeCapEvent:
		return s.handleCapChange(ctx, vaultAddress, e.ID, e.IDData, blockNumber, blockHash, blockVersion, blockTimestamp)
	case *SetPerformanceFeeEvent, *SetManagementFeeEvent,
		*SetPerformanceFeeRecipientEvent, *SetManagementFeeRecipientEvent:
		// Every Set* fee event snapshots the vault's FULL on-chain fee config; the
		// specific field the event changed is irrelevant to what is persisted (the
		// authoritative full state is the hash-pinned read), mirroring the cap events.
		return s.handleFeeChange(ctx, vaultAddress, blockNumber, blockHash, blockVersion, blockTimestamp)
	default:
		return nil
	}
}

// saveMetaMorphoProtocolEvent writes a protocol_event audit-log row for any
// MetaMorpho event emitted by a known vault. Used both by the typed Deposit /
// Withdraw / Transfer / AccrueInterest paths and by the V2 governance /
// allocation / cap / fee / role / timelock surface that doesn't yet have
// structured tables.
//
// EventData is a JSON snapshot of the raw log: { eventType, vault, topics,
// data }. ABI decoding of args is intentionally skipped — operators can decode
// downstream from the canonical signatures in
// stl-verify/internal/pkg/blockchain/abis/vault_v2_events_abi.go if needed.
// This keeps the writer cheap and avoids encoding-bug failure modes for
// event shapes the indexer doesn't yet structurally consume.
// parseLogIndex reads a log's position within its block. The wire format is hex- or
// decimal-encoded (hence strconv base 0), and the result is int32 because that is the
// width both protocol_event.log_index and morpho_adapter_membership.log_index store.
func parseLogIndex(log shared.Log) (int32, error) {
	logIndex, err := strconv.ParseInt(log.LogIndex, 0, 32)
	if err != nil {
		return 0, fmt.Errorf("parsing log index %q: %w", log.LogIndex, err)
	}
	return int32(logIndex), nil
}

func (s *Service) saveMetaMorphoProtocolEvent(ctx context.Context, log shared.Log, vaultAddress common.Address, eventName string, chainID, blockNumber int64, blockVersion int, blockTimestamp time.Time, logIndex int32) error {
	payload, err := json.Marshal(map[string]any{
		"eventType": eventName,
		"vault":     vaultAddress.Hex(),
		"tx":        log.TransactionHash,
		"topics":    log.Topics,
		"data":      log.Data,
	})
	if err != nil {
		return fmt.Errorf("marshalling MetaMorpho event payload: %w", err)
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		protocolID, err := s.protocolRepo.GetOrCreateProtocol(ctx, tx, chainID, MorphoBlueAddress, "Morpho Blue", "lending", s.deployBlock)
		if err != nil {
			return fmt.Errorf("getting protocol: %w", err)
		}
		protocolEvent, err := entity.NewProtocolEvent(
			int(chainID),
			protocolID,
			blockNumber,
			blockVersion,
			common.FromHex(log.TransactionHash),
			int(logIndex),
			vaultAddress.Bytes(),
			eventName,
			payload,
			blockTimestamp,
		)
		if err != nil {
			return fmt.Errorf("creating MetaMorpho protocol event entity: %w", err)
		}
		return s.eventRepo.SaveEvent(ctx, tx, protocolEvent)
	})
}

func (s *Service) saveProtocolEvent(ctx context.Context, event MorphoBlueEvent, chainID, blockNumber int64, blockVersion, logIndex int, blockTimestamp time.Time) error {
	eventJSON, err := event.ToJSON()
	if err != nil {
		return fmt.Errorf("serializing event data: %w", err)
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		protocolID, err := s.protocolRepo.GetOrCreateProtocol(ctx, tx, chainID, MorphoBlueAddress, "Morpho Blue", "lending", s.deployBlock)
		if err != nil {
			return fmt.Errorf("getting protocol: %w", err)
		}

		protocolEvent, err := entity.NewProtocolEvent(
			int(chainID),
			protocolID,
			blockNumber,
			blockVersion,
			common.FromHex(event.TxHash()),
			logIndex,
			MorphoBlueAddress.Bytes(),
			string(event.Type()),
			eventJSON,
			blockTimestamp,
		)
		if err != nil {
			return fmt.Errorf("creating protocol event entity: %w", err)
		}

		return s.eventRepo.SaveEvent(ctx, tx, protocolEvent)
	})
}

func validateDependencies(
	consumer outbound.SQSConsumer,
	cache outbound.BlockCacheReader,
	multicallClient outbound.Multicaller,
	txManager outbound.TxManager,
	userRepo outbound.UserRepository,
	protocolRepo outbound.ProtocolRepository,
	tokenRepo outbound.TokenRepository,
	morphoRepo outbound.MorphoRepository,
	eventRepo outbound.EventRepository,
	receiptTokenRepo outbound.ReceiptTokenRepository,
) error {
	if consumer == nil {
		return fmt.Errorf("consumer is required")
	}
	if cache == nil {
		return fmt.Errorf("cache is required")
	}
	if multicallClient == nil {
		return fmt.Errorf("multicallClient is required")
	}
	if txManager == nil {
		return fmt.Errorf("txManager is required")
	}
	if userRepo == nil {
		return fmt.Errorf("userRepo is required")
	}
	if protocolRepo == nil {
		return fmt.Errorf("protocolRepo is required")
	}
	if tokenRepo == nil {
		return fmt.Errorf("tokenRepo is required")
	}
	if morphoRepo == nil {
		return fmt.Errorf("morphoRepo is required")
	}
	if eventRepo == nil {
		return fmt.Errorf("eventRepo is required")
	}
	if receiptTokenRepo == nil {
		return fmt.Errorf("receiptTokenRepo is required")
	}
	return nil
}

func validateReplayDependencies(
	multicallClient outbound.Multicaller,
	txManager outbound.TxManager,
	protocolRepo outbound.ProtocolRepository,
	morphoRepo outbound.MorphoRepository,
	eventRepo outbound.EventRepository,
) error {
	if multicallClient == nil {
		return fmt.Errorf("multicallClient is required")
	}
	if txManager == nil {
		return fmt.Errorf("txManager is required")
	}
	if protocolRepo == nil {
		return fmt.Errorf("protocolRepo is required")
	}
	if morphoRepo == nil {
		return fmt.Errorf("morphoRepo is required")
	}
	if eventRepo == nil {
		return fmt.Errorf("eventRepo is required")
	}
	return nil
}

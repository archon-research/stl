package morpho_indexer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"slices"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"

	"github.com/archon-research/stl/stl-verify/internal/common/sqsutil"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
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

	ctx    context.Context
	cancel context.CancelFunc
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

	return &Service{
		config:           config,
		deployBlock:      deployBlock,
		consumer:         consumer,
		cache:            cache,
		txManager:        txManager,
		userRepo:         userRepo,
		protocolRepo:     protocolRepo,
		tokenRepo:        tokenRepo,
		morphoRepo:       morphoRepo,
		eventRepo:        eventRepo,
		receiptTokenRepo: receiptTokenRepo,
		blockchainSvc:    blockchainSvc,
		eventExtractor:   eventExtractor,
		vaultRegistry:    NewVaultRegistry(config.Logger),
		telemetry:        config.Telemetry,
		logger:           config.Logger.With("component", "morpho-indexer"),
	}, nil
}

// Start begins the SQS message processing loop.
func (s *Service) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	// Load known vaults from database
	if err := s.vaultRegistry.LoadFromDB(ctx, s.morphoRepo, s.config.ChainID); err != nil {
		return fmt.Errorf("loading vault registry: %w", err)
	}

	go sqsutil.RunLoop(s.ctx, sqsutil.Config{
		Consumer:     s.consumer,
		MaxMessages:  s.config.MaxMessages,
		PollInterval: s.config.PollInterval,
		Logger:       s.logger,
		ChainID:      s.config.ChainID,
	}, s.processBlockEvent)

	s.logger.Info("morpho indexer started",
		"maxMessages", s.config.MaxMessages,
		"vaults", s.vaultRegistry.Count())
	return nil
}

// Stop stops the service.
func (s *Service) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.logger.Info("morpho indexer stopped")
	return nil
}

func (s *Service) processBlockEvent(ctx context.Context, event outbound.BlockEvent) error {
	return s.fetchAndProcessReceipts(ctx, event)
}

func (s *Service) fetchAndProcessReceipts(ctx context.Context, event outbound.BlockEvent) (retErr error) {
	ctx, span := s.telemetry.StartBlockSpan(ctx, event.BlockNumber)
	defer span.End()

	start := time.Now()
	defer func() {
		duration := time.Since(start)
		s.telemetry.RecordBlockProcessed(ctx, duration, retErr)
		if retErr != nil {
			SetSpanError(span, retErr, "block processing failed")
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
		if err := s.processReceipt(ctx, receipt, event.ChainID, event.BlockNumber, event.Version, blockTimestamp); err != nil {
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
func (s *Service) processReceipt(ctx context.Context, receipt shared.TransactionReceipt, chainID, blockNumber int64, blockVersion int, blockTimestamp time.Time) (retErr error) {
	if !s.hasRelevantEvents(receipt) {
		return nil
	}

	ctx, span := s.telemetry.StartSpan(ctx, "morpho.processReceipt",
		attribute.String("tx.hash", receipt.TransactionHash))
	defer func() {
		if retErr != nil {
			SetSpanError(span, retErr, "receipt processing failed")
		}
		span.End()
	}()

	// Pre-walk: probe Morpho Blue events' caller / onBehalf (or borrower
	// for Liquidate) for V1/V1.1 vault discovery BEFORE the main loop
	// processes any log. This mirrors the morpho-vault-indexer backfiller's
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
	if err := s.discoverV1V11VaultsInReceipt(ctx, receipt, chainID, blockNumber); err != nil {
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
			if err := s.processMorphoBlueLog(ctx, log, chainID, blockNumber, blockVersion, blockTimestamp); err != nil {
				s.logger.Error("failed to process Morpho Blue event", "error", err, "tx", receipt.TransactionHash)
				errs = append(errs, err)
			}

		case logAddress == morphoBlueAddr:
			s.logger.Debug("skipping morpho blue address event", "logAddress", logAddress.Hex(), "tx", receipt.TransactionHash, "topic", log.Topics[0])

		case s.vaultRegistry.IsKnownVault(logAddress) && isMetaMorpho:
			s.logger.Debug("processing MetaMorpho event", "tx", receipt.TransactionHash, "vault", logAddress.Hex(), "topic", log.Topics[0])
			if err := s.processMetaMorphoLog(ctx, log, logAddress, chainID, blockNumber, blockVersion, blockTimestamp); err != nil {
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
			// Same predicate is used by the morpho-vault-indexer backfiller
			// (see cmd/backfillers/morpho-vault-indexer/main.go), so the
			// live and offline discovery contracts stay aligned.
			if !s.eventExtractor.IsVaultActivityEvent(log) {
				continue
			}
			s.logger.Debug("attempting vault discovery", "address", logAddress.Hex(), "tx", receipt.TransactionHash)
			if err := s.tryDiscoverVault(ctx, log, logAddress, chainID, blockNumber, blockVersion, blockTimestamp); err != nil {
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
func (s *Service) processMorphoBlueLog(ctx context.Context, log shared.Log, chainID, blockNumber int64, blockVersion int, blockTimestamp time.Time) error {
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
	logIndex, err := strconv.ParseInt(log.LogIndex, 0, 64)
	if err != nil {
		return fmt.Errorf("parsing log index %q: %w", log.LogIndex, err)
	}
	if err := s.saveProtocolEvent(ctx, event, chainID, blockNumber, blockVersion, int(logIndex), blockTimestamp); err != nil {
		return fmt.Errorf("saving protocol event: %w", err)
	}

	switch e := event.(type) {
	case *CreateMarketEvent:
		return s.handleCreateMarket(ctx, e, chainID, blockNumber, blockVersion, blockTimestamp)
	case *SupplyEvent:
		return s.handlePositionEvent(ctx, e.MarketID(), e.OnBehalf, e.Type(), e.TxHash(), chainID, blockNumber, blockVersion, blockTimestamp)
	case *WithdrawEvent:
		return s.handlePositionEvent(ctx, e.MarketID(), e.OnBehalf, e.Type(), e.TxHash(), chainID, blockNumber, blockVersion, blockTimestamp)
	case *BorrowEvent:
		return s.handlePositionEvent(ctx, e.MarketID(), e.OnBehalf, e.Type(), e.TxHash(), chainID, blockNumber, blockVersion, blockTimestamp)
	case *RepayEvent:
		return s.handlePositionEvent(ctx, e.MarketID(), e.OnBehalf, e.Type(), e.TxHash(), chainID, blockNumber, blockVersion, blockTimestamp)
	case *SupplyCollateralEvent:
		return s.handlePositionEvent(ctx, e.MarketID(), e.OnBehalf, e.Type(), e.TxHash(), chainID, blockNumber, blockVersion, blockTimestamp)
	case *WithdrawCollateralEvent:
		return s.handlePositionEvent(ctx, e.MarketID(), e.OnBehalf, e.Type(), e.TxHash(), chainID, blockNumber, blockVersion, blockTimestamp)
	case *LiquidateEvent:
		return s.handleLiquidateEvent(ctx, e, chainID, blockNumber, blockVersion, blockTimestamp)
	case *AccrueInterestEvent:
		return s.handleAccrueInterest(ctx, e, chainID, blockNumber, blockVersion, blockTimestamp)
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
// keyed by (tx_hash, log_index). The four events with state-affecting typed
// handlers — Deposit, Withdraw, Transfer, AccrueInterest — also produce
// structured snapshot rows in morpho_vault_state / morpho_vault_position via
// the dispatch below. The full V2 governance / allocation / cap / fee / role
// / timelock surface (Allocate, Deallocate, AddAdapter, IncreaseAbsoluteCap,
// SetPerformanceFee, SetCurator, Submit, …) is registered in the event
// extractor so it lands in the audit log; structured tables for those events
// are deferred per docs/vec-198-morpho-v2-followup-plan.md.
func (s *Service) processMetaMorphoLog(ctx context.Context, log shared.Log, vaultAddress common.Address, chainID, blockNumber int64, blockVersion int, blockTimestamp time.Time) error {
	eventName, ok := s.eventExtractor.MetaMorphoEventName(log)
	if !ok {
		// Caller already filtered via IsMetaMorphoEvent; this shouldn't
		// happen unless the topic registration drifted.
		return fmt.Errorf("MetaMorpho event has unrecognised topic: %v", log.Topics)
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

	if err := s.saveMetaMorphoProtocolEvent(ctx, log, vaultAddress, eventName, chainID, blockNumber, blockVersion, blockTimestamp); err != nil {
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
		return s.saveVaultEventSnapshot(ctx, e.Owner, vaultAddress, chainID, blockNumber, blockVersion, blockTimestamp, e.Type(), e.TxHash())
	case *VaultWithdrawEvent:
		return s.saveVaultEventSnapshot(ctx, e.Owner, vaultAddress, chainID, blockNumber, blockVersion, blockTimestamp, e.Type(), e.TxHash())
	case *VaultTransferEvent:
		return s.handleVaultTransfer(ctx, e, vaultAddress, chainID, blockNumber, blockVersion, blockTimestamp)
	case *VaultAccrueInterestEvent:
		return s.handleVaultAccrueInterest(ctx, e, vaultAddress, chainID, blockNumber, blockVersion, blockTimestamp)
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
func (s *Service) saveMetaMorphoProtocolEvent(ctx context.Context, log shared.Log, vaultAddress common.Address, eventName string, chainID, blockNumber int64, blockVersion int, blockTimestamp time.Time) error {
	logIndex, err := strconv.ParseInt(log.LogIndex, 0, 64)
	if err != nil {
		return fmt.Errorf("parsing log index %q: %w", log.LogIndex, err)
	}

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

// MorphoBlueVaultCandidates returns the addresses from a Morpho Blue event
// that could be MetaMorpho V1/V1.1 vaults — caller and onBehalf for the
// position-changing events, caller and borrower for Liquidate. Used by both
// the live indexer (this package) and the morpho-vault-indexer backfiller
// for V1/V1.1 vault discovery, since those vaults are characterised by
// their interaction with Morpho Blue rather than by emitting a uniquely
// shaped event of their own.
//
// V2 vaults can also appear here when they use a Morpho Blue market adapter,
// but the V2 4-field AccrueInterest topic catches them earlier via
// IsVaultActivityEvent — most candidates surfaced through this function are
// V1/V1.1.
//
// Borrower is included for Liquidate symmetry with the backfiller. In
// practice MetaMorpho V1/V1.1 vaults don't borrow on Morpho Blue, so the
// borrower slot almost always resolves to known-not-vault on first probe;
// it's retained so the same selector contract holds for both code paths
// even if a future vault flavour borrows.
//
// Caller-side filtering (zero address, MorphoBlueAddress, already-known)
// happens at the call site.
func MorphoBlueVaultCandidates(event MorphoBlueEvent) []common.Address {
	switch e := event.(type) {
	case *SupplyEvent:
		return []common.Address{e.Caller, e.OnBehalf}
	case *WithdrawEvent:
		return []common.Address{e.Caller, e.OnBehalf}
	case *BorrowEvent:
		return []common.Address{e.Caller, e.OnBehalf}
	case *RepayEvent:
		return []common.Address{e.Caller, e.OnBehalf}
	case *SupplyCollateralEvent:
		return []common.Address{e.Caller, e.OnBehalf}
	case *WithdrawCollateralEvent:
		return []common.Address{e.Caller, e.OnBehalf}
	case *LiquidateEvent:
		return []common.Address{e.Caller, e.Borrower}
	default:
		return nil
	}
}

// discoverAndRegisterVault probes vaultAddress on-chain, persists the vault
// and its asset token, and registers the vault in the in-memory registry.
// Returns *ErrNotVault if the address is definitively not a Morpho-family
// vault; transient errors (RPC, DB) propagate as plain errors so the caller
// can decide whether to retry.
//
// Used by both the IsVaultActivityEvent path (V2) via tryDiscoverVault and
// the Morpho Blue caller/onBehalf path (V1/V1.1) via
// discoverV1V11VaultsInReceipt.
func (s *Service) discoverAndRegisterVault(ctx context.Context, vaultAddress common.Address, chainID, blockNumber int64) error {
	metadata, err := s.blockchainSvc.getVaultMetadata(ctx, vaultAddress, blockNumber)
	if err != nil {
		return fmt.Errorf("fetching vault metadata: %w", err)
	}

	var vault *entity.MorphoVault
	if err := s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		assetMetadata, err := s.blockchainSvc.getTokenMetadata(ctx, metadata.Asset, blockNumber)
		if err != nil {
			return fmt.Errorf("fetching asset token metadata: %w", err)
		}

		tokenID, err := s.tokenRepo.GetOrCreateToken(ctx, tx, chainID, metadata.Asset, assetMetadata.Symbol, assetMetadata.Decimals, blockNumber)
		if err != nil {
			return fmt.Errorf("getting asset token: %w", err)
		}

		protocolID, err := s.protocolRepo.GetOrCreateProtocol(ctx, tx, chainID, MorphoBlueAddress, "Morpho Blue", "lending", s.deployBlock)
		if err != nil {
			return fmt.Errorf("getting protocol: %w", err)
		}

		vault, err = entity.NewMorphoVault(chainID, protocolID, vaultAddress.Bytes(), metadata.Name, metadata.Symbol, tokenID, metadata.Version, blockNumber)
		if err != nil {
			return fmt.Errorf("creating vault entity: %w", err)
		}

		vaultID, err := s.morphoRepo.GetOrCreateVault(ctx, tx, vault)
		if err != nil {
			return fmt.Errorf("persisting vault: %w", err)
		}

		receiptToken, err := entity.NewReceiptToken(chainID, protocolID, tokenID, blockNumber, vaultAddress, metadata.Symbol)
		if err != nil {
			return fmt.Errorf("creating receipt token entity: %w", err)
		}
		if _, err := s.receiptTokenRepo.GetOrCreateReceiptToken(ctx, tx, *receiptToken); err != nil {
			return fmt.Errorf("upserting receipt token: %w", err)
		}

		vault.ID = vaultID
		return nil
	}); err != nil {
		return fmt.Errorf("persisting vault: %w", err)
	}

	s.vaultRegistry.RegisterVault(vaultAddress, vault)
	return nil
}

// tryDiscoverVault attempts to discover a new MetaMorpho/VaultV2 vault from
// a vault-emitted log (currently only the V2 4-field AccrueInterest topic
// per IsVaultActivityEvent). Validates the log decodes, then probes and
// registers via discoverAndRegisterVault, then processes the triggering log
// against the now-known vault.
func (s *Service) tryDiscoverVault(ctx context.Context, log shared.Log, vaultAddress common.Address, chainID, blockNumber int64, blockVersion int, blockTimestamp time.Time) error {
	ctx, span := s.telemetry.StartSpan(ctx, "morpho.discoverVault",
		attribute.String("vault.address", vaultAddress.Hex()),
		attribute.String("discovery.path", "vaultActivity"))
	defer span.End()

	// Validate this is a decodable MetaMorpho event before making on-chain calls.
	if _, err := s.eventExtractor.ExtractMetaMorphoEvent(log); err != nil {
		return &ErrNotVault{Err: fmt.Errorf("event decode failed: %w", err)}
	}

	if err := s.discoverAndRegisterVault(ctx, vaultAddress, chainID, blockNumber); err != nil {
		return err
	}

	return s.processMetaMorphoLog(ctx, log, vaultAddress, chainID, blockNumber, blockVersion, blockTimestamp)
}

// discoverV1V11VaultsInReceipt is the pre-walk for V1/V1.1 vault discovery
// via the Morpho Blue caller/onBehalf path. It iterates the receipt's
// Morpho Blue logs once, extracts candidate addresses (caller, onBehalf —
// or caller, borrower for Liquidate), and probes each unknown address.
// Successful probes register the vault in the in-memory registry so the
// caller's main loop sees it as a known vault when it reaches the vault's
// own Deposit / Transfer / V1 AccrueInterest logs in the same receipt.
//
// Mirrors the morpho-vault-indexer backfiller's emitMorphoBlueCandidates,
// keeping the live and offline V1/V1.1 discovery contracts uniform — the
// backfiller is recovery-only, so the live indexer must cover V1/V1.1
// discovery itself.
//
// ErrNotVault outcomes mark the address as known-not-vault (cached) so
// repeat appearances of the same EOA / non-vault contract short-circuit.
// Transient errors propagate so the receipt fails and SQS redelivers —
// they must NOT mark the address as not-vault, or a real V1/V1.1 vault
// that was momentarily unreachable would be permanently black-holed.
//
// The seen map dedupes within the receipt: if the same address appears
// as caller AND onBehalf in one Supply, OR as caller in two Morpho Blue
// logs in the same receipt, only one probe fires per receipt. After the
// first probe the registry cache (IsKnownVault / IsKnownNotVault) handles
// further short-circuiting.
func (s *Service) discoverV1V11VaultsInReceipt(ctx context.Context, receipt shared.TransactionReceipt, chainID, blockNumber int64) error {
	morphoBlueAddr := MorphoBlueAddress
	var errs []error
	seen := make(map[common.Address]bool)

	for _, log := range receipt.Logs {
		if common.HexToAddress(log.Address) != morphoBlueAddr {
			continue
		}
		if !s.eventExtractor.IsMorphoBlueEvent(log) {
			continue
		}

		event, parseErr := s.eventExtractor.ExtractMorphoBlueEvent(log)
		if parseErr != nil {
			// Re-parse failure here is structurally impossible today —
			// the same log will be extracted again in processMorphoBlueLog
			// from the same extractor. If a future change introduces
			// non-determinism (e.g. ABI fallback, caching), this branch
			// becomes a silent discovery hole, so log a Warn rather than
			// swallow.
			s.logger.Warn("re-parse of Morpho Blue event failed in discovery pre-walk (should not happen — investigate)",
				"tx", log.TransactionHash,
				"topic", log.Topics[0],
				"error", parseErr)
			continue
		}

		for _, addr := range MorphoBlueVaultCandidates(event) {
			if seen[addr] {
				continue
			}
			seen[addr] = true
			if addr == (common.Address{}) || addr == morphoBlueAddr {
				continue
			}
			if s.vaultRegistry.IsKnownVault(addr) || s.vaultRegistry.IsKnownNotVault(addr) {
				continue
			}

			probeCtx, probeSpan := s.telemetry.StartSpan(ctx, "morpho.discoverVault",
				attribute.String("vault.address", addr.Hex()),
				attribute.String("discovery.path", "morphoBlue"))
			probeErr := s.discoverAndRegisterVault(probeCtx, addr, chainID, blockNumber)
			probeSpan.End()
			if probeErr == nil {
				continue
			}
			var nv *ErrNotVault
			if errors.As(probeErr, &nv) {
				s.vaultRegistry.MarkNotVault(addr)
				if nv.VaultShaped {
					s.logger.Warn("vault-shaped address rejected by probe (Morpho Blue path) — possible new vault flavour",
						"address", addr.Hex(),
						"reason", probeErr)
				} else {
					s.logger.Debug("not a Morpho-family vault (Morpho Blue path)",
						"address", addr.Hex(),
						"reason", probeErr)
				}
				continue
			}
			s.logger.Warn("V1/V1.1 vault discovery via Morpho Blue path failed (will retry)",
				"address", addr.Hex(),
				"error", probeErr)
			errs = append(errs, fmt.Errorf("V1/V1.1 discovery for %s in tx %s: %w", addr.Hex(), receipt.TransactionHash, probeErr))
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// handleCreateMarket handles a CreateMarket event.
func (s *Service) handleCreateMarket(ctx context.Context, e *CreateMarketEvent, chainID, blockNumber int64, blockVersion int, blockTimestamp time.Time) error {
	mp := e.Params
	if mp == nil {
		return fmt.Errorf("CreateMarket event missing marketParams")
	}

	// Fetch token metadata and initial market state.
	loanMetadata, collMetadata, err := s.blockchainSvc.getTokenPairMetadata(ctx, mp.LoanToken, mp.CollateralToken, blockNumber)
	if err != nil {
		return fmt.Errorf("getting token pair metadata: %w", err)
	}

	ms, err := s.blockchainSvc.getMarketState(ctx, e.MarketID(), blockNumber)
	if err != nil {
		return fmt.Errorf("fetching initial market state: %w", err)
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		protocolID, err := s.protocolRepo.GetOrCreateProtocol(ctx, tx, chainID, MorphoBlueAddress, "Morpho Blue", "lending", s.deployBlock)
		if err != nil {
			return fmt.Errorf("getting protocol: %w", err)
		}

		loanTokenID, err := s.tokenRepo.GetOrCreateToken(ctx, tx, chainID, mp.LoanToken, loanMetadata.Symbol, loanMetadata.Decimals, blockNumber)
		if err != nil {
			return fmt.Errorf("getting loan token: %w", err)
		}

		collTokenID, err := s.tokenRepo.GetOrCreateToken(ctx, tx, chainID, mp.CollateralToken, collMetadata.Symbol, collMetadata.Decimals, blockNumber)
		if err != nil {
			return fmt.Errorf("getting collateral token: %w", err)
		}

		market, err := entity.NewMorphoMarket(chainID, protocolID, common.Hash(e.MarketID()), loanTokenID, collTokenID, mp.Oracle, mp.Irm, mp.LLTV, blockNumber)
		if err != nil {
			return fmt.Errorf("creating market entity: %w", err)
		}

		marketID, err := s.morphoRepo.GetOrCreateMarket(ctx, tx, market)
		if err != nil {
			return fmt.Errorf("creating market: %w", err)
		}

		return s.saveMarketStateSnapshot(ctx, tx, marketID, blockNumber, blockVersion, blockTimestamp, ms, nil)
	})
}

// handlePositionEvent handles Supply, Withdraw, Borrow, Repay, SupplyCollateral, WithdrawCollateral events.
func (s *Service) handlePositionEvent(ctx context.Context, mktID [32]byte, user common.Address, eventType entity.MorphoEventType, txHash string, chainID, blockNumber int64, blockVersion int, blockTimestamp time.Time) error {
	ms, ps, err := s.blockchainSvc.getMarketAndPositionState(ctx, mktID, user, blockNumber)
	if err != nil {
		return fmt.Errorf("fetching on-chain state: %w", err)
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		marketID, err := s.ensureMarket(ctx, tx, mktID, chainID, blockNumber)
		if err != nil {
			return fmt.Errorf("ensuring market: %w", err)
		}

		// Save market state snapshot
		if err := s.saveMarketStateSnapshot(ctx, tx, marketID, blockNumber, blockVersion, blockTimestamp, ms, nil); err != nil {
			return fmt.Errorf("saving market state: %w", err)
		}

		// Save user position snapshot
		return s.savePositionSnapshot(ctx, tx, user, marketID, blockNumber, blockVersion, blockTimestamp, ps, ms, eventType, txHash, chainID)
	})
}

// handleLiquidateEvent handles Liquidate events by snapshotting both borrower and liquidator.
func (s *Service) handleLiquidateEvent(ctx context.Context, e *LiquidateEvent, chainID, blockNumber int64, blockVersion int, blockTimestamp time.Time) error {
	borrower := e.Borrower
	liquidator := e.Caller

	// Fetch market state + both positions in a single RPC call.
	ms, borrowerPos, liquidatorPos, err := s.blockchainSvc.getMarketAndTwoPositionStates(ctx, e.MarketID(), borrower, liquidator, blockNumber)
	if err != nil {
		return fmt.Errorf("fetching on-chain state: %w", err)
	}

	// Save the borrower and liquidator positions in user-address order so the
	// per-row mmp advisory locks are acquired in a transaction-stable order.
	// Today the mss lock taken inside saveMarketStateSnapshot serializes all
	// concurrent transactions on this market, which means mmp ordering between
	// the two positions can't actually deadlock — but this defensive sort
	// closes the door on a future refactor that batches events into a shared
	// transaction or otherwise re-orders the per-event tx scope. See ADR-0002 §3.
	positions := []userPosition{
		{borrower, borrowerPos, "borrower"},
		{liquidator, liquidatorPos, "liquidator"},
	}
	slices.SortFunc(positions, func(a, b userPosition) int {
		return bytes.Compare(a.user.Bytes(), b.user.Bytes())
	})

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		marketID, err := s.ensureMarket(ctx, tx, e.MarketID(), chainID, blockNumber)
		if err != nil {
			return fmt.Errorf("ensuring market: %w", err)
		}

		if err := s.saveMarketStateSnapshot(ctx, tx, marketID, blockNumber, blockVersion, blockTimestamp, ms, nil); err != nil {
			return fmt.Errorf("saving market state: %w", err)
		}

		for _, p := range positions {
			if err := s.savePositionSnapshot(ctx, tx, p.user, marketID, blockNumber, blockVersion, blockTimestamp, p.pos, ms, e.Type(), e.TxHash(), chainID); err != nil {
				return fmt.Errorf("saving %s position: %w", p.role, err)
			}
		}
		return nil
	})
}

// userPosition pairs a user address with their position state and a role
// label, for ordered per-user mmp/mvp lock acquisition (see
// handleLiquidateEvent / handleVaultTransfer). The role survives the sort so
// failures still surface as "saving <borrower|liquidator|sender|receiver>
// position: ..." regardless of which user got sorted first.
type userPosition struct {
	user common.Address
	pos  *PositionState
	role string
}

// userVaultBalance is the vault analogue of userPosition.
type userVaultBalance struct {
	user    common.Address
	balance *big.Int
	role    string
}

// handleAccrueInterest handles AccrueInterest events.
func (s *Service) handleAccrueInterest(ctx context.Context, e *AccrueInterestEvent, chainID, blockNumber int64, blockVersion int, blockTimestamp time.Time) error {
	ms, err := s.blockchainSvc.getMarketState(ctx, e.MarketID(), blockNumber)
	if err != nil {
		return fmt.Errorf("fetching market state: %w", err)
	}

	accrueData := &accrueInterestData{
		PrevBorrowRate: e.PrevBorrowRate,
		Interest:       e.Interest,
		FeeShares:      e.FeeShares,
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		marketID, err := s.ensureMarket(ctx, tx, e.MarketID(), chainID, blockNumber)
		if err != nil {
			return fmt.Errorf("ensuring market: %w", err)
		}
		return s.saveMarketStateSnapshot(ctx, tx, marketID, blockNumber, blockVersion, blockTimestamp, ms, accrueData)
	})
}

// handleVaultTransfer handles vault Transfer events.
func (s *Service) handleVaultTransfer(ctx context.Context, e *VaultTransferEvent, vaultAddress common.Address, chainID, blockNumber int64, blockVersion int, blockTimestamp time.Time) error {
	vault := s.vaultRegistry.GetVault(vaultAddress)
	if vault == nil {
		return fmt.Errorf("vault not found in registry: %s", vaultAddress.Hex())
	}

	// Filter out mint/burn (zero address) and internal vault accounting (vault address).
	// Mints and burns are already covered by Deposit/Withdraw handlers.
	hasFrom := e.From != (common.Address{}) && e.From != vaultAddress
	hasTo := e.To != (common.Address{}) && e.To != vaultAddress

	// Fetch vault state + both balances in a single RPC call when both addresses are present.
	var vs *VaultState
	var senderBalance, receiverBalance *big.Int
	var err error

	switch {
	case hasFrom && hasTo:
		vs, senderBalance, receiverBalance, err = s.blockchainSvc.getVaultStateAndTwoBalances(ctx, vaultAddress, e.From, e.To, blockNumber)
	case hasFrom:
		vs, senderBalance, err = s.blockchainSvc.getVaultStateAndBalance(ctx, vaultAddress, e.From, blockNumber)
	case hasTo:
		vs, receiverBalance, err = s.blockchainSvc.getVaultStateAndBalance(ctx, vaultAddress, e.To, blockNumber)
	default:
		vs, err = s.blockchainSvc.getVaultState(ctx, vaultAddress, blockNumber)
	}
	if err != nil {
		return fmt.Errorf("fetching vault state and balances for vault=%s from=%s to=%s block=%d: %w",
			vaultAddress.Hex(), e.From.Hex(), e.To.Hex(), blockNumber, err)
	}

	// Save sender and receiver vault positions in user-address order so the
	// per-row mvp advisory locks are acquired in a transaction-stable order;
	// same defense-in-depth rationale as handleLiquidateEvent. See ADR-0002 §3.
	balances := make([]userVaultBalance, 0, 2)
	if hasFrom {
		balances = append(balances, userVaultBalance{e.From, senderBalance, "sender"})
	}
	if hasTo {
		balances = append(balances, userVaultBalance{e.To, receiverBalance, "receiver"})
	}
	slices.SortFunc(balances, func(a, b userVaultBalance) int {
		return bytes.Compare(a.user.Bytes(), b.user.Bytes())
	})

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		if err := s.saveVaultStateSnapshotInTx(ctx, tx, vault.ID, blockNumber, blockVersion, blockTimestamp, vs, nil); err != nil {
			return fmt.Errorf("saving vault state: %w", err)
		}

		for _, b := range balances {
			if err := s.saveVaultPositionInTx(ctx, tx, b.user, vault.ID, blockNumber, blockVersion, blockTimestamp, b.balance, vs, e.Type(), e.TxHash(), chainID); err != nil {
				return fmt.Errorf("saving %s position: %w", b.role, err)
			}
		}

		return nil
	})
}

// handleVaultAccrueInterest handles vault AccrueInterest events.
func (s *Service) handleVaultAccrueInterest(ctx context.Context, e *VaultAccrueInterestEvent, vaultAddress common.Address, chainID, blockNumber int64, blockVersion int, blockTimestamp time.Time) error {
	vault := s.vaultRegistry.GetVault(vaultAddress)
	if vault == nil {
		return fmt.Errorf("vault not found in registry: %s", vaultAddress.Hex())
	}

	vs, err := s.blockchainSvc.getVaultState(ctx, vaultAddress, blockNumber)
	if err != nil {
		return fmt.Errorf("fetching vault state: %w", err)
	}

	accrueData := &vaultAccrueData{
		FeeShares:           e.FeeShares,
		NewTotalAssets:      e.NewTotalAssets,
		PreviousTotalAssets: e.PreviousTotalAssets,
		ManagementFeeShares: e.ManagementFeeShares,
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		return s.saveVaultStateSnapshotInTx(ctx, tx, vault.ID, blockNumber, blockVersion, blockTimestamp, vs, accrueData)
	})
}

// saveVaultEventSnapshot handles deposit/withdraw by saving vault state + user position.
func (s *Service) saveVaultEventSnapshot(ctx context.Context, user common.Address, vaultAddress common.Address, chainID, blockNumber int64, blockVersion int, blockTimestamp time.Time, eventType entity.MorphoEventType, txHash string) error {
	vault := s.vaultRegistry.GetVault(vaultAddress)
	if vault == nil {
		return fmt.Errorf("vault not found in registry: %s", vaultAddress.Hex())
	}

	// Fetch vault state + user balance in a single RPC call.
	vs, balance, err := s.blockchainSvc.getVaultStateAndBalance(ctx, vaultAddress, user, blockNumber)
	if err != nil {
		return fmt.Errorf("fetching vault state and balance: %w", err)
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		if err := s.saveVaultStateSnapshotInTx(ctx, tx, vault.ID, blockNumber, blockVersion, blockTimestamp, vs, nil); err != nil {
			return fmt.Errorf("saving vault state: %w", err)
		}
		return s.saveVaultPositionInTx(ctx, tx, user, vault.ID, blockNumber, blockVersion, blockTimestamp, balance, vs, eventType, txHash, chainID)
	})
}

// Helper methods

type accrueInterestData struct {
	PrevBorrowRate *big.Int
	Interest       *big.Int
	FeeShares      *big.Int
}

type vaultAccrueData struct {
	FeeShares           *big.Int // V1: single fee, V2: performanceFeeShares
	NewTotalAssets      *big.Int
	PreviousTotalAssets *big.Int // V2 only
	ManagementFeeShares *big.Int // V2 only
}

// ensureMarket ensures the market exists in the database and returns its ID.
func (s *Service) ensureMarket(ctx context.Context, tx pgx.Tx, marketID [32]byte, chainID, blockNumber int64) (int64, error) {
	// Check if market already exists
	existing, err := s.morphoRepo.GetMarketByMarketID(ctx, chainID, common.Hash(marketID))
	if err != nil {
		return 0, fmt.Errorf("checking market existence: %w", err)
	}
	if existing != nil {
		return existing.ID, nil
	}

	// Market doesn't exist yet, fetch params from chain and create it
	params, err := s.blockchainSvc.getMarketParams(ctx, marketID, blockNumber)
	if err != nil {
		return 0, fmt.Errorf("fetching market params: %w", err)
	}

	// Fetch both token metadata in a single RPC call.
	loanMd, collMd, err := s.blockchainSvc.getTokenPairMetadata(ctx, params.LoanToken, params.CollateralToken, blockNumber)
	if err != nil {
		return 0, fmt.Errorf("getting token pair metadata: %w", err)
	}

	protocolID, err := s.protocolRepo.GetOrCreateProtocol(ctx, tx, chainID, MorphoBlueAddress, "Morpho Blue", "lending", s.deployBlock)
	if err != nil {
		return 0, fmt.Errorf("getting protocol: %w", err)
	}

	loanTokenID, err := s.tokenRepo.GetOrCreateToken(ctx, tx, chainID, params.LoanToken, loanMd.Symbol, loanMd.Decimals, blockNumber)
	if err != nil {
		return 0, fmt.Errorf("getting loan token: %w", err)
	}

	collTokenID, err := s.tokenRepo.GetOrCreateToken(ctx, tx, chainID, params.CollateralToken, collMd.Symbol, collMd.Decimals, blockNumber)
	if err != nil {
		return 0, fmt.Errorf("getting collateral token: %w", err)
	}

	market, err := entity.NewMorphoMarket(chainID, protocolID, common.Hash(marketID), loanTokenID, collTokenID, params.Oracle, params.Irm, params.LLTV, blockNumber)
	if err != nil {
		return 0, fmt.Errorf("creating market entity: %w", err)
	}

	return s.morphoRepo.GetOrCreateMarket(ctx, tx, market)
}

// Contract for the Save* helpers below — read this before adding a new event
// handler or batching existing ones into a shared transaction.
//
// Each event handler today opens its own WithTransaction scope and touches at
// most one market/vault. That bounds per-tx lock acquisition to:
//   - 0 or 1 mss + 0..N mmp at a single market_id (handlePositionEvent,
//     handleLiquidateEvent), or
//   - 0 or 1 mvs + 0..N mvp at a single vault_id (handleVaultTransfer,
//     saveVaultEventSnapshot).
//
// Two invariants prevent deadlocks under cross-build contention:
//
//  1. STATE-FIRST: every handler that writes mmp MUST first write mss for
//     the same (market_id, block_number, block_version, timestamp), and
//     likewise mvs before mvp. The state lock then serialises every other
//     concurrent tx on the same market/vault, so the trailing per-user mmp
//     /mvp locks can never be held by two txs at once for that (market, …)
//     tuple.
//
//  2. SORTED-USERS: handlers that write more than one mmp/mvp in a single tx
//     (handleLiquidateEvent: borrower + liquidator; handleVaultTransfer:
//     sender + receiver) sort their per-user saves by user address before
//     iterating. That's defence-in-depth: invariant 1 already prevents
//     deadlock today, but the sort survives a future refactor that loosens
//     it (e.g. event batching across markets in one tx, or removal of the
//     state save).
//
// If you add a handler that writes mmp/mvp without first writing mss/mvs for
// the same key, OR that batches multiple markets/vaults into a shared tx,
// you MUST extend the sort to cover the per-tx lock acquisition order across
// all keys.
//
// See ADR-0002 §3 and VEC-194 PR.

func (s *Service) saveMarketStateSnapshot(ctx context.Context, tx pgx.Tx, morphoMarketID, blockNumber int64, blockVersion int, blockTimestamp time.Time, ms *MarketState, accrueData *accrueInterestData) error {
	state, err := entity.NewMorphoMarketState(morphoMarketID, blockNumber, blockVersion, blockTimestamp, ms.TotalSupplyAssets, ms.TotalSupplyShares, ms.TotalBorrowAssets, ms.TotalBorrowShares, ms.LastUpdate.Int64(), ms.Fee)
	if err != nil {
		return fmt.Errorf("creating market state entity: %w", err)
	}

	if accrueData != nil {
		state.WithAccrueInterest(accrueData.PrevBorrowRate, accrueData.Interest, accrueData.FeeShares)
	}

	return s.morphoRepo.SaveMarketState(ctx, tx, state)
}

func (s *Service) savePositionSnapshot(ctx context.Context, tx pgx.Tx, user common.Address, morphoMarketID, blockNumber int64, blockVersion int, blockTimestamp time.Time, ps *PositionState, ms *MarketState, eventType entity.MorphoEventType, txHash string, chainID int64) error {
	userID, err := s.userRepo.GetOrCreateUser(ctx, tx, entity.User{
		ChainID:        chainID,
		Address:        user,
		FirstSeenBlock: blockNumber,
	})
	if err != nil {
		return fmt.Errorf("ensuring user: %w", err)
	}

	supplyAssets := entity.ComputeSupplyAssets(ps.SupplyShares, ms.TotalSupplyAssets, ms.TotalSupplyShares)
	borrowAssets := entity.ComputeBorrowAssets(ps.BorrowShares, ms.TotalBorrowAssets, ms.TotalBorrowShares)

	position, err := entity.NewMorphoMarketPosition(userID, morphoMarketID, blockNumber, blockVersion, blockTimestamp, ps.SupplyShares, ps.BorrowShares, ps.Collateral, supplyAssets, borrowAssets)
	if err != nil {
		return fmt.Errorf("creating position entity: %w", err)
	}

	return s.morphoRepo.SaveMarketPosition(ctx, tx, position)
}

func (s *Service) saveVaultStateSnapshotInTx(ctx context.Context, tx pgx.Tx, vaultID, blockNumber int64, blockVersion int, blockTimestamp time.Time, vs *VaultState, accrueData *vaultAccrueData) error {
	state, err := entity.NewMorphoVaultState(vaultID, blockNumber, blockVersion, blockTimestamp, vs.TotalAssets, vs.TotalSupply)
	if err != nil {
		return fmt.Errorf("creating vault state entity: %w", err)
	}

	if accrueData != nil {
		state.WithAccrueInterest(accrueData.FeeShares, accrueData.NewTotalAssets, accrueData.PreviousTotalAssets, accrueData.ManagementFeeShares)
	}

	return s.morphoRepo.SaveVaultState(ctx, tx, state)
}

func (s *Service) saveVaultPositionInTx(ctx context.Context, tx pgx.Tx, user common.Address, vaultID, blockNumber int64, blockVersion int, blockTimestamp time.Time, shares *big.Int, vs *VaultState, eventType entity.MorphoEventType, txHash string, chainID int64) error {
	userID, err := s.userRepo.GetOrCreateUser(ctx, tx, entity.User{
		ChainID:        chainID,
		Address:        user,
		FirstSeenBlock: blockNumber,
	})
	if err != nil {
		return fmt.Errorf("ensuring user: %w", err)
	}

	assets := entity.ComputeVaultAssets(shares, vs.TotalAssets, vs.TotalSupply)

	position, err := entity.NewMorphoVaultPosition(userID, vaultID, blockNumber, blockVersion, blockTimestamp, shares, assets)
	if err != nil {
		return fmt.Errorf("creating vault position entity: %w", err)
	}

	return s.morphoRepo.SaveVaultPosition(ctx, tx, position)
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

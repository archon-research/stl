package morpho_indexer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
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

// errNotVault signals that an address is definitively not a MetaMorpho vault.
// Transient failures (network, DB) must NOT be wrapped with this type so that
// discovery is retried on the next event from that address.
type errNotVault struct{ err error }

func (e *errNotVault) Error() string { return e.err.Error() }
func (e *errNotVault) Unwrap() error { return e.err }

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
	config       Config
	deployBlock  int64 // resolved Morpho Blue deploy block for the configured chain
	consumer     outbound.SQSConsumer
	cache        outbound.BlockCache
	txManager    outbound.TxManager
	userRepo     outbound.UserRepository
	protocolRepo outbound.ProtocolRepository
	tokenRepo    outbound.TokenRepository
	morphoRepo   outbound.MorphoRepository
	eventRepo    outbound.EventRepository

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
	cache outbound.BlockCache,
	multicallClient outbound.Multicaller,
	txManager outbound.TxManager,
	userRepo outbound.UserRepository,
	protocolRepo outbound.ProtocolRepository,
	tokenRepo outbound.TokenRepository,
	morphoRepo outbound.MorphoRepository,
	eventRepo outbound.EventRepository,
) (*Service, error) {
	if err := validateDependencies(consumer, cache, multicallClient, txManager, userRepo, protocolRepo, tokenRepo, morphoRepo, eventRepo); err != nil {
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
		config:         config,
		deployBlock:    deployBlock,
		consumer:       consumer,
		cache:          cache,
		txManager:      txManager,
		userRepo:       userRepo,
		protocolRepo:   protocolRepo,
		tokenRepo:      tokenRepo,
		morphoRepo:     morphoRepo,
		eventRepo:      eventRepo,
		blockchainSvc:  blockchainSvc,
		eventExtractor: eventExtractor,
		vaultRegistry:  NewVaultRegistry(config.Logger),
		telemetry:      config.Telemetry,
		logger:         config.Logger.With("component", "morpho-indexer"),
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
		s.telemetry.RecordBlockProcessed(ctx, event.BlockNumber, duration, retErr)
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
		s.logger.Warn("cache miss for receipts", "block", event.BlockNumber)
		return nil
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

	var errs []error
	for _, receipt := range receipts {
		if err := s.processReceipt(ctx, receipt, event.ChainID, event.BlockNumber, event.Version); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// processReceipt processes all Morpho-related logs in a single transaction receipt.
//
// Note: eth_call reads return end-of-block state. Multiple events for the same
// entity within one block produce identical on-chain snapshots. The ON CONFLICT
// clause means only the last-written event_type/tx_hash is retained, but the
// on-chain state (shares, assets, collateral) is always correct.
func (s *Service) processReceipt(ctx context.Context, receipt shared.TransactionReceipt, chainID, blockNumber int64, blockVersion int) error {
	ctx, span := s.telemetry.StartSpan(ctx, "morpho.processReceipt",
		attribute.String("tx.hash", receipt.TransactionHash))
	defer span.End()

	var errs []error
	// Track transient vault discovery failures separately so we can discard
	// them if a later log for the same address succeeds.
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
			if err := s.processMorphoBlueLog(ctx, log, chainID, blockNumber, blockVersion); err != nil {
				s.logger.Error("failed to process Morpho Blue event", "error", err, "tx", receipt.TransactionHash)
				errs = append(errs, err)
			}

		case logAddress == morphoBlueAddr:
			s.logger.Debug("skipping morpho blue address event", "logAddress", logAddress.Hex(), "tx", receipt.TransactionHash, "topic", log.Topics[0])

		case s.vaultRegistry.IsKnownVault(logAddress) && isMetaMorpho:
			s.logger.Debug("processing MetaMorpho event", "tx", receipt.TransactionHash, "vault", logAddress.Hex(), "topic", log.Topics[0])
			if err := s.processMetaMorphoLog(ctx, log, logAddress, chainID, blockNumber, blockVersion); err != nil {
				s.logger.Error("failed to process MetaMorpho event", "error", err, "tx", receipt.TransactionHash)
				errs = append(errs, err)
			}

		default:
			s.logger.Debug("attempting vault discovery", "address", logAddress.Hex(), "tx", receipt.TransactionHash)
			if err := s.tryDiscoverVault(ctx, log, logAddress, chainID, blockNumber, blockVersion); err != nil {
				var nv *errNotVault
				if errors.As(err, &nv) {
					s.vaultRegistry.MarkNotVault(logAddress)
					s.logger.Debug("not a MetaMorpho vault", "address", logAddress.Hex(), "reason", err)
				} else {
					s.logger.Warn("vault discovery failed (will retry)", "address", logAddress.Hex(), "error", err)
					discoveryErrs[logAddress] = err
				}
			} else {
				// Discovery succeeded — discard any earlier transient failure for this address.
				delete(discoveryErrs, logAddress)
			}
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
func (s *Service) processMorphoBlueLog(ctx context.Context, log shared.Log, chainID, blockNumber int64, blockVersion int) error {
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
	if err := s.saveProtocolEvent(ctx, event, chainID, blockNumber, blockVersion, int(logIndex)); err != nil {
		return fmt.Errorf("saving protocol event: %w", err)
	}

	switch e := event.(type) {
	case *CreateMarketEvent:
		return s.handleCreateMarket(ctx, e, chainID, blockNumber, blockVersion)
	case *SupplyEvent:
		return s.handlePositionEvent(ctx, e.MarketID(), e.OnBehalf, e.Type(), e.TxHash(), chainID, blockNumber, blockVersion)
	case *WithdrawEvent:
		return s.handlePositionEvent(ctx, e.MarketID(), e.OnBehalf, e.Type(), e.TxHash(), chainID, blockNumber, blockVersion)
	case *BorrowEvent:
		return s.handlePositionEvent(ctx, e.MarketID(), e.OnBehalf, e.Type(), e.TxHash(), chainID, blockNumber, blockVersion)
	case *RepayEvent:
		return s.handlePositionEvent(ctx, e.MarketID(), e.OnBehalf, e.Type(), e.TxHash(), chainID, blockNumber, blockVersion)
	case *SupplyCollateralEvent:
		return s.handlePositionEvent(ctx, e.MarketID(), e.OnBehalf, e.Type(), e.TxHash(), chainID, blockNumber, blockVersion)
	case *WithdrawCollateralEvent:
		return s.handlePositionEvent(ctx, e.MarketID(), e.OnBehalf, e.Type(), e.TxHash(), chainID, blockNumber, blockVersion)
	case *LiquidateEvent:
		return s.handleLiquidateEvent(ctx, e, chainID, blockNumber, blockVersion)
	case *AccrueInterestEvent:
		return s.handleAccrueInterest(ctx, e, chainID, blockNumber, blockVersion)
	case *SetFeeEvent:
		// Already saved as protocol_event above
		return nil
	default:
		return nil
	}
}

// processMetaMorphoLog handles a MetaMorpho vault event log.
func (s *Service) processMetaMorphoLog(ctx context.Context, log shared.Log, vaultAddress common.Address, chainID, blockNumber int64, blockVersion int) error {
	event, err := s.eventExtractor.ExtractMetaMorphoEvent(log)
	if err != nil {
		return fmt.Errorf("extracting MetaMorpho event: %w", err)
	}

	ctx, span := s.telemetry.StartSpan(ctx, "morpho.processMetaMorphoEvent",
		attribute.String("event.type", string(event.Type())),
		attribute.String("vault.address", vaultAddress.Hex()))
	defer span.End()
	s.telemetry.RecordEventProcessed(ctx, string(event.Type()))

	s.logger.Info("MetaMorpho event detected",
		"event", event.Type(),
		"vault", vaultAddress.Hex(),
		"tx", event.TxHash(),
		"block", blockNumber)

	switch e := event.(type) {
	case *VaultDepositEvent:
		return s.saveVaultEventSnapshot(ctx, e.Owner, vaultAddress, chainID, blockNumber, blockVersion, e.Type(), e.TxHash())
	case *VaultWithdrawEvent:
		return s.saveVaultEventSnapshot(ctx, e.Owner, vaultAddress, chainID, blockNumber, blockVersion, e.Type(), e.TxHash())
	case *VaultTransferEvent:
		return s.handleVaultTransfer(ctx, e, vaultAddress, chainID, blockNumber, blockVersion)
	case *VaultAccrueInterestEvent:
		return s.handleVaultAccrueInterest(ctx, e, vaultAddress, chainID, blockNumber, blockVersion)
	default:
		return nil
	}
}

// tryDiscoverVault attempts to discover a new MetaMorpho vault from an event log.
func (s *Service) tryDiscoverVault(ctx context.Context, log shared.Log, vaultAddress common.Address, chainID, blockNumber int64, blockVersion int) error {
	ctx, span := s.telemetry.StartSpan(ctx, "morpho.discoverVault",
		attribute.String("vault.address", vaultAddress.Hex()))
	defer span.End()

	// Validate this is a decodable MetaMorpho event before making on-chain calls.
	if _, err := s.eventExtractor.ExtractMetaMorphoEvent(log); err != nil {
		return &errNotVault{err: fmt.Errorf("event decode failed: %w", err)}
	}

	// Fetch vault metadata (including version) from on-chain.
	metadata, err := s.blockchainSvc.getVaultMetadata(ctx, vaultAddress, blockNumber)
	if err != nil {
		return fmt.Errorf("fetching vault metadata: %w", err)
	}

	// Persist the vault
	var vault *entity.MorphoVault
	if err := s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		// Get or create the asset token
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

		vault.ID = vaultID
		return nil
	}); err != nil {
		return fmt.Errorf("persisting vault: %w", err)
	}

	s.vaultRegistry.RegisterVault(vaultAddress, vault)

	// Now process the event
	return s.processMetaMorphoLog(ctx, log, vaultAddress, chainID, blockNumber, blockVersion)
}

// handleCreateMarket handles a CreateMarket event.
func (s *Service) handleCreateMarket(ctx context.Context, e *CreateMarketEvent, chainID, blockNumber int64, blockVersion int) error {
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

		return s.saveMarketStateSnapshot(ctx, tx, marketID, blockNumber, blockVersion, ms, nil)
	})
}

// handlePositionEvent handles Supply, Withdraw, Borrow, Repay, SupplyCollateral, WithdrawCollateral events.
func (s *Service) handlePositionEvent(ctx context.Context, mktID [32]byte, user common.Address, eventType entity.MorphoEventType, txHash string, chainID, blockNumber int64, blockVersion int) error {
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
		if err := s.saveMarketStateSnapshot(ctx, tx, marketID, blockNumber, blockVersion, ms, nil); err != nil {
			return fmt.Errorf("saving market state: %w", err)
		}

		// Save user position snapshot
		return s.savePositionSnapshot(ctx, tx, user, marketID, blockNumber, blockVersion, ps, ms, eventType, txHash, chainID)
	})
}

// handleLiquidateEvent handles Liquidate events by snapshotting both borrower and liquidator.
func (s *Service) handleLiquidateEvent(ctx context.Context, e *LiquidateEvent, chainID, blockNumber int64, blockVersion int) error {
	borrower := e.Borrower
	liquidator := e.Caller

	// Fetch market state + both positions in a single RPC call.
	ms, borrowerPos, liquidatorPos, err := s.blockchainSvc.getMarketAndTwoPositionStates(ctx, e.MarketID(), borrower, liquidator, blockNumber)
	if err != nil {
		return fmt.Errorf("fetching on-chain state: %w", err)
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		marketID, err := s.ensureMarket(ctx, tx, e.MarketID(), chainID, blockNumber)
		if err != nil {
			return fmt.Errorf("ensuring market: %w", err)
		}

		if err := s.saveMarketStateSnapshot(ctx, tx, marketID, blockNumber, blockVersion, ms, nil); err != nil {
			return fmt.Errorf("saving market state: %w", err)
		}

		if err := s.savePositionSnapshot(ctx, tx, borrower, marketID, blockNumber, blockVersion, borrowerPos, ms, e.Type(), e.TxHash(), chainID); err != nil {
			return fmt.Errorf("saving borrower position: %w", err)
		}

		return s.savePositionSnapshot(ctx, tx, liquidator, marketID, blockNumber, blockVersion, liquidatorPos, ms, e.Type(), e.TxHash(), chainID)
	})
}

// handleAccrueInterest handles AccrueInterest events.
func (s *Service) handleAccrueInterest(ctx context.Context, e *AccrueInterestEvent, chainID, blockNumber int64, blockVersion int) error {
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
		return s.saveMarketStateSnapshot(ctx, tx, marketID, blockNumber, blockVersion, ms, accrueData)
	})
}

// handleVaultTransfer handles vault Transfer events.
func (s *Service) handleVaultTransfer(ctx context.Context, e *VaultTransferEvent, vaultAddress common.Address, chainID, blockNumber int64, blockVersion int) error {
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

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		if err := s.saveVaultStateSnapshotInTx(ctx, tx, vault.ID, blockNumber, blockVersion, vs, nil); err != nil {
			return fmt.Errorf("saving vault state: %w", err)
		}

		if hasFrom {
			if err := s.saveVaultPositionInTx(ctx, tx, e.From, vault.ID, blockNumber, blockVersion, senderBalance, vs, e.Type(), e.TxHash(), chainID); err != nil {
				return fmt.Errorf("saving sender position: %w", err)
			}
		}

		if hasTo {
			if err := s.saveVaultPositionInTx(ctx, tx, e.To, vault.ID, blockNumber, blockVersion, receiverBalance, vs, e.Type(), e.TxHash(), chainID); err != nil {
				return fmt.Errorf("saving receiver position: %w", err)
			}
		}

		return nil
	})
}

// handleVaultAccrueInterest handles vault AccrueInterest events.
func (s *Service) handleVaultAccrueInterest(ctx context.Context, e *VaultAccrueInterestEvent, vaultAddress common.Address, chainID, blockNumber int64, blockVersion int) error {
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
		return s.saveVaultStateSnapshotInTx(ctx, tx, vault.ID, blockNumber, blockVersion, vs, accrueData)
	})
}

// saveVaultEventSnapshot handles deposit/withdraw by saving vault state + user position.
func (s *Service) saveVaultEventSnapshot(ctx context.Context, user common.Address, vaultAddress common.Address, chainID, blockNumber int64, blockVersion int, eventType entity.MorphoEventType, txHash string) error {
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
		if err := s.saveVaultStateSnapshotInTx(ctx, tx, vault.ID, blockNumber, blockVersion, vs, nil); err != nil {
			return fmt.Errorf("saving vault state: %w", err)
		}
		return s.saveVaultPositionInTx(ctx, tx, user, vault.ID, blockNumber, blockVersion, balance, vs, eventType, txHash, chainID)
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

func (s *Service) saveMarketStateSnapshot(ctx context.Context, tx pgx.Tx, morphoMarketID, blockNumber int64, blockVersion int, ms *MarketState, accrueData *accrueInterestData) error {
	state, err := entity.NewMorphoMarketState(morphoMarketID, blockNumber, blockVersion, ms.TotalSupplyAssets, ms.TotalSupplyShares, ms.TotalBorrowAssets, ms.TotalBorrowShares, ms.LastUpdate.Int64(), ms.Fee)
	if err != nil {
		return fmt.Errorf("creating market state entity: %w", err)
	}

	if accrueData != nil {
		state.WithAccrueInterest(accrueData.PrevBorrowRate, accrueData.Interest, accrueData.FeeShares)
	}

	return s.morphoRepo.SaveMarketState(ctx, tx, state)
}

func (s *Service) savePositionSnapshot(ctx context.Context, tx pgx.Tx, user common.Address, morphoMarketID, blockNumber int64, blockVersion int, ps *PositionState, ms *MarketState, eventType entity.MorphoEventType, txHash string, chainID int64) error {
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

	position, err := entity.NewMorphoMarketPosition(userID, morphoMarketID, blockNumber, blockVersion, ps.SupplyShares, ps.BorrowShares, ps.Collateral, supplyAssets, borrowAssets, eventType, common.FromHex(txHash))
	if err != nil {
		return fmt.Errorf("creating position entity: %w", err)
	}

	return s.morphoRepo.SaveMarketPosition(ctx, tx, position)
}

func (s *Service) saveVaultStateSnapshotInTx(ctx context.Context, tx pgx.Tx, vaultID, blockNumber int64, blockVersion int, vs *VaultState, accrueData *vaultAccrueData) error {
	state, err := entity.NewMorphoVaultState(vaultID, blockNumber, blockVersion, vs.TotalAssets, vs.TotalSupply)
	if err != nil {
		return fmt.Errorf("creating vault state entity: %w", err)
	}

	if accrueData != nil {
		state.WithAccrueInterest(accrueData.FeeShares, accrueData.NewTotalAssets, accrueData.PreviousTotalAssets, accrueData.ManagementFeeShares)
	}

	return s.morphoRepo.SaveVaultState(ctx, tx, state)
}

func (s *Service) saveVaultPositionInTx(ctx context.Context, tx pgx.Tx, user common.Address, vaultID, blockNumber int64, blockVersion int, shares *big.Int, vs *VaultState, eventType entity.MorphoEventType, txHash string, chainID int64) error {
	userID, err := s.userRepo.GetOrCreateUser(ctx, tx, entity.User{
		ChainID:        chainID,
		Address:        user,
		FirstSeenBlock: blockNumber,
	})
	if err != nil {
		return fmt.Errorf("ensuring user: %w", err)
	}

	assets := entity.ComputeVaultAssets(shares, vs.TotalAssets, vs.TotalSupply)

	position, err := entity.NewMorphoVaultPosition(userID, vaultID, blockNumber, blockVersion, shares, assets, eventType, common.FromHex(txHash))
	if err != nil {
		return fmt.Errorf("creating vault position entity: %w", err)
	}

	return s.morphoRepo.SaveVaultPosition(ctx, tx, position)
}

func (s *Service) saveProtocolEvent(ctx context.Context, event MorphoBlueEvent, chainID, blockNumber int64, blockVersion, logIndex int) error {
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
		)
		if err != nil {
			return fmt.Errorf("creating protocol event entity: %w", err)
		}

		return s.eventRepo.SaveEvent(ctx, tx, protocolEvent)
	})
}

func validateDependencies(
	consumer outbound.SQSConsumer,
	cache outbound.BlockCache,
	multicallClient outbound.Multicaller,
	txManager outbound.TxManager,
	userRepo outbound.UserRepository,
	protocolRepo outbound.ProtocolRepository,
	tokenRepo outbound.TokenRepository,
	morphoRepo outbound.MorphoRepository,
	eventRepo outbound.EventRepository,
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
	return nil
}

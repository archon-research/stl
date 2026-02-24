package morpho_position_tracker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"

	"github.com/archon-research/stl/stl-verify/internal/common/sqsutil"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// MorphoBlueDeployBlock is the block at which Morpho Blue was deployed.
const MorphoBlueDeployBlock = 18883124

// Config holds service configuration.
type Config struct {
	shared.SQSConsumerConfig
	ChainID   int64
	Telemetry *Telemetry // optional, nil-safe
}

// ConfigDefaults returns default configuration values.
func ConfigDefaults() Config {
	return Config{
		SQSConsumerConfig: shared.SQSConsumerConfigDefaults(),
		ChainID:           1,
	}
}

// Service is the Morpho position tracker SQS consumer service.
type Service struct {
	config       Config
	consumer     outbound.SQSConsumer
	redisClient  *redis.Client
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

// NewService creates a new Morpho position tracker service.
func NewService(
	config Config,
	consumer outbound.SQSConsumer,
	redisClient *redis.Client,
	multicallClient outbound.Multicaller,
	txManager outbound.TxManager,
	userRepo outbound.UserRepository,
	protocolRepo outbound.ProtocolRepository,
	tokenRepo outbound.TokenRepository,
	morphoRepo outbound.MorphoRepository,
	eventRepo outbound.EventRepository,
) (*Service, error) {
	if err := validateDependencies(consumer, redisClient, multicallClient, txManager, userRepo, protocolRepo, tokenRepo, morphoRepo, eventRepo); err != nil {
		return nil, err
	}

	config.SQSConsumerConfig.ApplyDefaults()
	if config.ChainID == 0 {
		config.ChainID = ConfigDefaults().ChainID
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
		consumer:       consumer,
		redisClient:    redisClient,
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
		logger:         config.Logger.With("component", "morpho-position-tracker"),
	}, nil
}

// Start begins the SQS message processing loop.
func (s *Service) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	// Load known vaults from database
	if err := s.vaultRegistry.LoadFromDB(ctx, s.morphoRepo); err != nil {
		s.logger.Warn("failed to load vault registry from DB (continuing with empty registry)", "error", err)
	}

	go sqsutil.RunLoop(s.ctx, sqsutil.Config{
		Consumer:     s.consumer,
		MaxMessages:  s.config.MaxMessages,
		PollInterval: s.config.PollInterval,
		Logger:       s.logger,
	}, s.processBlockEvent)

	s.logger.Info("morpho position tracker started",
		"maxMessages", s.config.MaxMessages,
		"vaults", s.vaultRegistry.Count())
	return nil
}

// Stop stops the service.
func (s *Service) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.logger.Info("morpho position tracker stopped")
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

	cacheKey := shared.CacheKey(event.ChainID, event.BlockNumber, event.Version, "receipts")
	receiptsJSON, err := s.redisClient.Get(ctx, cacheKey).Result()
	if errors.Is(err, redis.Nil) {
		s.logger.Warn("cache key expired or not found", "key", cacheKey, "block", event.BlockNumber)
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to fetch from Redis: %w", err)
	}

	var receipts []shared.TransactionReceipt
	if err := shared.ParseCompressedJSON([]byte(receiptsJSON), &receipts); err != nil {
		return fmt.Errorf("failed to unmarshal receipts: %w", err)
	}

	span.SetAttributes(attribute.Int("receipts.count", len(receipts)))

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

func (s *Service) processReceipt(ctx context.Context, receipt shared.TransactionReceipt, chainID, blockNumber int64, blockVersion int) error {
	ctx, span := s.telemetry.StartSpan(ctx, "morpho.processReceipt",
		attribute.String("tx.hash", receipt.TransactionHash))
	defer span.End()

	var errs []error
	morphoBlueAddr := MorphoBlueAddress

	for _, log := range receipt.Logs {
		logAddress := common.HexToAddress(log.Address)

		// Check Morpho Blue events
		if logAddress == morphoBlueAddr && s.eventExtractor.IsMorphoBlueEvent(log) {
			if err := s.processMorphoBlueLog(ctx, log, chainID, blockNumber, blockVersion); err != nil {
				s.logger.Error("failed to process Morpho Blue event", "error", err, "tx", receipt.TransactionHash)
				errs = append(errs, err)
			}
			continue
		}

		// Check MetaMorpho vault events
		if s.vaultRegistry.IsKnownVault(logAddress) && s.eventExtractor.IsMetaMorphoEvent(log) {
			if err := s.processMetaMorphoLog(ctx, log, logAddress, chainID, blockNumber, blockVersion); err != nil {
				s.logger.Error("failed to process MetaMorpho event", "error", err, "tx", receipt.TransactionHash)
				errs = append(errs, err)
			}
			continue
		}

		// Try to discover new vaults from unknown addresses
		if logAddress != morphoBlueAddr && !s.vaultRegistry.IsKnownVault(logAddress) && s.eventExtractor.IsMetaMorphoEvent(log) {
			if err := s.tryDiscoverVault(ctx, log, logAddress, chainID, blockNumber, blockVersion); err != nil {
				s.logger.Debug("vault discovery attempt failed", "address", logAddress.Hex(), "error", err)
			}
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// processMorphoBlueLog handles a Morpho Blue event log.
func (s *Service) processMorphoBlueLog(ctx context.Context, log shared.Log, chainID, blockNumber int64, blockVersion int) error {
	eventData, err := s.eventExtractor.ExtractMorphoBlueEvent(log)
	if err != nil {
		return fmt.Errorf("extracting Morpho Blue event: %w", err)
	}

	ctx, span := s.telemetry.StartSpan(ctx, "morpho.processMorphoBlueEvent",
		attribute.String("event.type", string(eventData.EventType)),
		attribute.String("market.id", fmt.Sprintf("%x", eventData.MarketID[:8])))
	defer span.End()
	s.telemetry.RecordEventProcessed(ctx, string(eventData.EventType))

	s.logger.Info("Morpho Blue event detected",
		"event", eventData.EventType,
		"market", fmt.Sprintf("%x", eventData.MarketID[:8]),
		"tx", eventData.TxHash,
		"block", blockNumber)

	// Save raw protocol event
	logIndex, err := strconv.ParseInt(log.LogIndex, 0, 64)
	if err != nil {
		return fmt.Errorf("parsing log index %q: %w", log.LogIndex, err)
	}
	if err := s.saveProtocolEvent(ctx, eventData, chainID, blockNumber, blockVersion, int(logIndex)); err != nil {
		return fmt.Errorf("saving protocol event: %w", err)
	}

	switch eventData.EventType {
	case entity.MorphoEventCreateMarket:
		return s.handleCreateMarket(ctx, eventData, chainID, blockNumber, blockVersion)
	case entity.MorphoEventSupply, entity.MorphoEventWithdraw, entity.MorphoEventBorrow, entity.MorphoEventRepay:
		return s.handlePositionEvent(ctx, eventData, chainID, blockNumber, blockVersion)
	case entity.MorphoEventSupplyCollateral, entity.MorphoEventWithdrawCollateral:
		return s.handleCollateralEvent(ctx, eventData, chainID, blockNumber, blockVersion)
	case entity.MorphoEventLiquidate:
		return s.handleLiquidateEvent(ctx, eventData, chainID, blockNumber, blockVersion)
	case entity.MorphoEventAccrueInterest:
		return s.handleAccrueInterest(ctx, eventData, chainID, blockNumber, blockVersion)
	case entity.MorphoEventSetFee:
		// Already saved as protocol_event above
		return nil
	default:
		return nil
	}
}

// processMetaMorphoLog handles a MetaMorpho vault event log.
func (s *Service) processMetaMorphoLog(ctx context.Context, log shared.Log, vaultAddress common.Address, chainID, blockNumber int64, blockVersion int) error {
	eventData, err := s.eventExtractor.ExtractMetaMorphoEvent(log)
	if err != nil {
		return fmt.Errorf("extracting MetaMorpho event: %w", err)
	}

	ctx, span := s.telemetry.StartSpan(ctx, "morpho.processMetaMorphoEvent",
		attribute.String("event.type", string(eventData.EventType)),
		attribute.String("vault.address", vaultAddress.Hex()))
	defer span.End()
	s.telemetry.RecordEventProcessed(ctx, string(eventData.EventType))

	s.logger.Info("MetaMorpho event detected",
		"event", eventData.EventType,
		"vault", vaultAddress.Hex(),
		"tx", eventData.TxHash,
		"block", blockNumber)

	switch eventData.EventType {
	case entity.MorphoEventVaultDeposit:
		return s.handleVaultDeposit(ctx, eventData, vaultAddress, chainID, blockNumber, blockVersion)
	case entity.MorphoEventVaultWithdraw:
		return s.handleVaultWithdraw(ctx, eventData, vaultAddress, chainID, blockNumber, blockVersion)
	case entity.MorphoEventVaultTransfer:
		return s.handleVaultTransfer(ctx, eventData, vaultAddress, chainID, blockNumber, blockVersion)
	case entity.MorphoEventVaultAccrueInterest:
		return s.handleVaultAccrueInterest(ctx, eventData, vaultAddress, chainID, blockNumber, blockVersion)
	default:
		return nil
	}
}

// tryDiscoverVault attempts to discover a new MetaMorpho vault from an event log.
func (s *Service) tryDiscoverVault(ctx context.Context, log shared.Log, vaultAddress common.Address, chainID, blockNumber int64, blockVersion int) error {
	ctx, span := s.telemetry.StartSpan(ctx, "morpho.discoverVault",
		attribute.String("vault.address", vaultAddress.Hex()))
	defer span.End()

	eventData, err := s.eventExtractor.ExtractMetaMorphoEvent(log)
	if err != nil {
		return fmt.Errorf("event decode failed: %w", err)
	}

	// Fetch vault metadata from on-chain
	metadata, err := s.blockchainSvc.getVaultMetadata(ctx, vaultAddress, blockNumber)
	if err != nil {
		return fmt.Errorf("fetching vault metadata: %w", err)
	}

	// Detect version from AccrueInterest data length
	version := entity.MorphoVaultV1
	if eventData.EventType == entity.MorphoEventVaultAccrueInterest {
		dataLen := len(common.FromHex(log.Data))
		version = DetectVaultVersion(dataLen)
	}

	// Persist the vault
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

		protocolID, err := s.protocolRepo.GetOrCreateProtocol(ctx, tx, chainID, MorphoBlueAddress, "Morpho Blue", "lending", MorphoBlueDeployBlock)
		if err != nil {
			return fmt.Errorf("getting protocol: %w", err)
		}

		vault, err := entity.NewMorphoVault(protocolID, vaultAddress.Bytes(), metadata.Name, metadata.Symbol, tokenID, version, blockNumber)
		if err != nil {
			return fmt.Errorf("creating vault entity: %w", err)
		}

		vaultID, err := s.morphoRepo.GetOrCreateVault(ctx, tx, vault)
		if err != nil {
			return fmt.Errorf("persisting vault: %w", err)
		}

		vault.ID = vaultID
		s.vaultRegistry.RegisterVault(vaultAddress, vault)
		return nil
	}); err != nil {
		return err
	}

	// Now process the event
	return s.processMetaMorphoLog(ctx, log, vaultAddress, chainID, blockNumber, blockVersion)
}

// handleCreateMarket handles a CreateMarket event.
func (s *Service) handleCreateMarket(ctx context.Context, eventData *MorphoBlueEventData, chainID, blockNumber int64, blockVersion int) error {
	mp := eventData.MarketParams
	if mp == nil {
		return fmt.Errorf("CreateMarket event missing marketParams")
	}

	// Fetch token metadata and initial market state.
	loanMetadata, collMetadata, err := s.blockchainSvc.getTokenPairMetadata(ctx, mp.LoanToken, mp.CollateralToken, blockNumber)
	if err != nil {
		return fmt.Errorf("getting token pair metadata: %w", err)
	}

	ms, err := s.blockchainSvc.getMarketState(ctx, eventData.MarketID, blockNumber)
	if err != nil {
		return fmt.Errorf("fetching initial market state: %w", err)
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		protocolID, err := s.protocolRepo.GetOrCreateProtocol(ctx, tx, chainID, MorphoBlueAddress, "Morpho Blue", "lending", MorphoBlueDeployBlock)
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

		market, err := entity.NewMorphoMarket(protocolID, eventData.MarketID[:], loanTokenID, collTokenID, mp.Oracle.Bytes(), mp.Irm.Bytes(), mp.LLTV, blockNumber)
		if err != nil {
			return fmt.Errorf("creating market entity: %w", err)
		}

		marketID, err := s.morphoRepo.GetOrCreateMarket(ctx, tx, market)
		if err != nil {
			return err
		}

		return s.saveMarketStateSnapshot(ctx, tx, marketID, blockNumber, blockVersion, ms, nil)
	})
}

// handlePositionEvent handles Supply, Withdraw, Borrow, Repay events.
func (s *Service) handlePositionEvent(ctx context.Context, eventData *MorphoBlueEventData, chainID, blockNumber int64, blockVersion int) error {
	user := eventData.OnBehalf

	ms, ps, err := s.blockchainSvc.getMarketAndPositionState(ctx, eventData.MarketID, user, blockNumber)
	if err != nil {
		return fmt.Errorf("fetching on-chain state: %w", err)
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		marketID, err := s.ensureMarket(ctx, tx, eventData.MarketID, chainID, blockNumber)
		if err != nil {
			return err
		}

		// Save market state snapshot
		if err := s.saveMarketStateSnapshot(ctx, tx, marketID, blockNumber, blockVersion, ms, nil); err != nil {
			return err
		}

		// Save user position snapshot
		return s.savePositionSnapshot(ctx, tx, user, marketID, blockNumber, blockVersion, ps, ms, eventData.EventType, eventData.TxHash, chainID)
	})
}

// handleCollateralEvent handles SupplyCollateral and WithdrawCollateral events.
func (s *Service) handleCollateralEvent(ctx context.Context, eventData *MorphoBlueEventData, chainID, blockNumber int64, blockVersion int) error {
	user := eventData.OnBehalf

	ms, ps, err := s.blockchainSvc.getMarketAndPositionState(ctx, eventData.MarketID, user, blockNumber)
	if err != nil {
		return fmt.Errorf("fetching on-chain state: %w", err)
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		marketID, err := s.ensureMarket(ctx, tx, eventData.MarketID, chainID, blockNumber)
		if err != nil {
			return err
		}

		if err := s.saveMarketStateSnapshot(ctx, tx, marketID, blockNumber, blockVersion, ms, nil); err != nil {
			return err
		}

		return s.savePositionSnapshot(ctx, tx, user, marketID, blockNumber, blockVersion, ps, ms, eventData.EventType, eventData.TxHash, chainID)
	})
}

// handleLiquidateEvent handles Liquidate events by snapshotting both borrower and liquidator.
func (s *Service) handleLiquidateEvent(ctx context.Context, eventData *MorphoBlueEventData, chainID, blockNumber int64, blockVersion int) error {
	borrower := eventData.Borrower
	liquidator := eventData.Caller

	// Fetch market state + both positions in a single RPC call.
	ms, borrowerPos, liquidatorPos, err := s.blockchainSvc.getMarketAndTwoPositionStates(ctx, eventData.MarketID, borrower, liquidator, blockNumber)
	if err != nil {
		return fmt.Errorf("fetching on-chain state: %w", err)
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		marketID, err := s.ensureMarket(ctx, tx, eventData.MarketID, chainID, blockNumber)
		if err != nil {
			return err
		}

		if err := s.saveMarketStateSnapshot(ctx, tx, marketID, blockNumber, blockVersion, ms, nil); err != nil {
			return err
		}

		if err := s.savePositionSnapshot(ctx, tx, borrower, marketID, blockNumber, blockVersion, borrowerPos, ms, eventData.EventType, eventData.TxHash, chainID); err != nil {
			return fmt.Errorf("saving borrower position: %w", err)
		}

		return s.savePositionSnapshot(ctx, tx, liquidator, marketID, blockNumber, blockVersion, liquidatorPos, ms, eventData.EventType, eventData.TxHash, chainID)
	})
}

// handleAccrueInterest handles AccrueInterest events.
func (s *Service) handleAccrueInterest(ctx context.Context, eventData *MorphoBlueEventData, chainID, blockNumber int64, blockVersion int) error {
	ms, err := s.blockchainSvc.getMarketState(ctx, eventData.MarketID, blockNumber)
	if err != nil {
		return fmt.Errorf("fetching market state: %w", err)
	}

	accrueData := &accrueInterestData{
		PrevBorrowRate: eventData.PrevBorrowRate,
		Interest:       eventData.Interest,
		FeeShares:      eventData.FeeShares,
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		marketID, err := s.ensureMarket(ctx, tx, eventData.MarketID, chainID, blockNumber)
		if err != nil {
			return err
		}
		return s.saveMarketStateSnapshot(ctx, tx, marketID, blockNumber, blockVersion, ms, accrueData)
	})
}

// handleVaultDeposit handles vault Deposit events.
func (s *Service) handleVaultDeposit(ctx context.Context, eventData *MetaMorphoEventData, vaultAddress common.Address, chainID, blockNumber int64, blockVersion int) error {
	owner := eventData.Owner
	return s.saveVaultEventSnapshot(ctx, owner, vaultAddress, chainID, blockNumber, blockVersion, eventData.EventType, eventData.TxHash)
}

// handleVaultWithdraw handles vault Withdraw events.
func (s *Service) handleVaultWithdraw(ctx context.Context, eventData *MetaMorphoEventData, vaultAddress common.Address, chainID, blockNumber int64, blockVersion int) error {
	owner := eventData.Owner
	return s.saveVaultEventSnapshot(ctx, owner, vaultAddress, chainID, blockNumber, blockVersion, eventData.EventType, eventData.TxHash)
}

// handleVaultTransfer handles vault Transfer events.
func (s *Service) handleVaultTransfer(ctx context.Context, eventData *MetaMorphoEventData, vaultAddress common.Address, chainID, blockNumber int64, blockVersion int) error {
	vault := s.vaultRegistry.GetVault(vaultAddress)
	if vault == nil {
		return fmt.Errorf("vault not found in registry: %s", vaultAddress.Hex())
	}

	hasFrom := eventData.From != (common.Address{})
	hasTo := eventData.To != (common.Address{})

	// Fetch vault state + both balances in a single RPC call when both addresses are present.
	var vs *VaultState
	var senderBalance, receiverBalance *big.Int
	var err error

	switch {
	case hasFrom && hasTo:
		vs, senderBalance, receiverBalance, err = s.blockchainSvc.getVaultStateAndTwoBalances(ctx, vaultAddress, eventData.From, eventData.To, blockNumber)
	case hasFrom:
		vs, senderBalance, err = s.blockchainSvc.getVaultStateAndBalance(ctx, vaultAddress, eventData.From, blockNumber)
	case hasTo:
		vs, receiverBalance, err = s.blockchainSvc.getVaultStateAndBalance(ctx, vaultAddress, eventData.To, blockNumber)
	default:
		vs, err = s.blockchainSvc.getVaultState(ctx, vaultAddress, blockNumber)
	}
	if err != nil {
		return fmt.Errorf("fetching vault state and balances: %w", err)
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		if err := s.saveVaultStateSnapshotInTx(ctx, tx, vault.ID, blockNumber, blockVersion, vs, nil); err != nil {
			return err
		}

		if hasFrom {
			if err := s.saveVaultPositionInTx(ctx, tx, eventData.From, vault.ID, blockNumber, blockVersion, senderBalance, vs, eventData.EventType, eventData.TxHash, chainID); err != nil {
				return fmt.Errorf("saving sender position: %w", err)
			}
		}

		if hasTo {
			if err := s.saveVaultPositionInTx(ctx, tx, eventData.To, vault.ID, blockNumber, blockVersion, receiverBalance, vs, eventData.EventType, eventData.TxHash, chainID); err != nil {
				return fmt.Errorf("saving receiver position: %w", err)
			}
		}

		return nil
	})
}

// handleVaultAccrueInterest handles vault AccrueInterest events.
func (s *Service) handleVaultAccrueInterest(ctx context.Context, eventData *MetaMorphoEventData, vaultAddress common.Address, chainID, blockNumber int64, blockVersion int) error {
	vault := s.vaultRegistry.GetVault(vaultAddress)
	if vault == nil {
		return fmt.Errorf("vault not found in registry: %s", vaultAddress.Hex())
	}

	vs, err := s.blockchainSvc.getVaultState(ctx, vaultAddress, blockNumber)
	if err != nil {
		return fmt.Errorf("fetching vault state: %w", err)
	}

	accrueData := &vaultAccrueData{
		FeeShares:      eventData.FeeShares,
		NewTotalAssets: eventData.NewTotalAssets,
		Interest:       eventData.Interest,
		FeeAssets:      eventData.FeeAssets,
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
			return err
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
	FeeShares      *big.Int
	NewTotalAssets *big.Int
	Interest       *big.Int // V2 only
	FeeAssets      *big.Int // V2 only
}

// ensureMarket ensures the market exists in the database and returns its ID.
func (s *Service) ensureMarket(ctx context.Context, tx pgx.Tx, marketID [32]byte, chainID, blockNumber int64) (int64, error) {
	// Check if market already exists
	existing, err := s.morphoRepo.GetMarketByMarketID(ctx, marketID[:])
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

	protocolID, err := s.protocolRepo.GetOrCreateProtocol(ctx, tx, chainID, MorphoBlueAddress, "Morpho Blue", "lending", MorphoBlueDeployBlock)
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

	market, err := entity.NewMorphoMarket(protocolID, marketID[:], loanTokenID, collTokenID, params.Oracle.Bytes(), params.Irm.Bytes(), params.LLTV, blockNumber)
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

	position, err := entity.NewMorphoPosition(userID, morphoMarketID, blockNumber, blockVersion, ps.SupplyShares, ps.BorrowShares, ps.Collateral, supplyAssets, borrowAssets, eventType, common.FromHex(txHash))
	if err != nil {
		return fmt.Errorf("creating position entity: %w", err)
	}

	return s.morphoRepo.SavePosition(ctx, tx, position)
}

func (s *Service) saveVaultStateSnapshotInTx(ctx context.Context, tx pgx.Tx, vaultID, blockNumber int64, blockVersion int, vs *VaultState, accrueData *vaultAccrueData) error {
	state, err := entity.NewMorphoVaultState(vaultID, blockNumber, blockVersion, vs.TotalAssets, vs.TotalSupply)
	if err != nil {
		return fmt.Errorf("creating vault state entity: %w", err)
	}

	if accrueData != nil {
		state.WithAccrueInterest(accrueData.FeeShares, accrueData.NewTotalAssets, accrueData.Interest, accrueData.FeeAssets)
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

func (s *Service) saveProtocolEvent(ctx context.Context, eventData *MorphoBlueEventData, chainID, blockNumber int64, blockVersion, logIndex int) error {
	eventJSON, err := eventData.ToJSON()
	if err != nil {
		return fmt.Errorf("serializing event data: %w", err)
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		protocolID, err := s.protocolRepo.GetOrCreateProtocol(ctx, tx, chainID, MorphoBlueAddress, "Morpho Blue", "lending", MorphoBlueDeployBlock)
		if err != nil {
			return fmt.Errorf("getting protocol: %w", err)
		}

		event, err := entity.NewProtocolEvent(
			int(chainID),
			protocolID,
			blockNumber,
			blockVersion,
			common.FromHex(eventData.TxHash),
			logIndex,
			MorphoBlueAddress.Bytes(),
			string(eventData.EventType),
			eventJSON,
		)
		if err != nil {
			return fmt.Errorf("creating protocol event entity: %w", err)
		}

		return s.eventRepo.SaveEvent(ctx, tx, event)
	})
}

func validateDependencies(
	consumer outbound.SQSConsumer,
	redisClient *redis.Client,
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
	if redisClient == nil {
		return fmt.Errorf("redisClient is required")
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

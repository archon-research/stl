package sparklend_position_tracker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/common/sqsutil"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/multicall"
)

const (
	EventBorrow                          = entity.EventBorrow
	EventRepay                           = entity.EventRepay
	EventSupply                          = entity.EventSupply
	EventWithdraw                        = entity.EventWithdraw
	EventLiquidationCall                 = entity.EventLiquidationCall
	EventReserveUsedAsCollateralEnabled  = entity.EventReserveUsedAsCollateralEnabled
	EventReserveUsedAsCollateralDisabled = entity.EventReserveUsedAsCollateralDisabled
)

type PositionEventData struct {
	EventType                  entity.EventType
	TxHash                     string
	User                       common.Address
	Reserve                    common.Address
	Amount                     *big.Int
	Liquidator                 common.Address
	CollateralAsset            common.Address
	DebtAsset                  common.Address
	DebtToCover                *big.Int
	LiquidatedCollateralAmount *big.Int
	CollateralEnabled          bool
}

type CollateralData struct {
	Asset             common.Address
	Decimals          int
	Symbol            string
	Name              string
	ActualBalance     *big.Int
	CollateralEnabled bool
}

type Service struct {
	config       shared.SQSConsumerConfig
	consumer     outbound.SQSConsumer
	cacheReader  outbound.BlockCacheReader
	ethClient    *ethclient.Client
	txManager    outbound.TxManager
	userRepo     outbound.UserRepository
	protocolRepo outbound.ProtocolRepository
	tokenRepo    outbound.TokenRepository
	positionRepo outbound.PositionRepository
	eventRepo    outbound.EventRepository

	mu                 sync.RWMutex
	blockchainServices map[blockchain.ProtocolKey]*blockchainService
	multicallClient    outbound.Multicaller
	erc20ABI           *abi.ABI
	eventExtractor     *EventExtractor

	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger
}

func NewService(
	config shared.SQSConsumerConfig,
	consumer outbound.SQSConsumer,
	cacheReader outbound.BlockCacheReader,
	ethClient *ethclient.Client,
	txManager outbound.TxManager,
	userRepo outbound.UserRepository,
	protocolRepo outbound.ProtocolRepository,
	tokenRepo outbound.TokenRepository,
	positionRepo outbound.PositionRepository,
	eventRepo outbound.EventRepository,
) (*Service, error) {
	if err := validateDependencies(consumer, cacheReader, ethClient, txManager, userRepo, protocolRepo, tokenRepo, positionRepo, eventRepo); err != nil {
		return nil, err
	}

	config.ApplyDefaults()
	if err := config.Validate(); err != nil {
		return nil, err
	}

	mc, err := multicall.NewClient(ethClient, blockchain.Multicall3)
	if err != nil {
		return nil, fmt.Errorf("failed to create multicall client: %w", err)
	}

	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		return nil, fmt.Errorf("failed to load ERC20 ABI: %w", err)
	}

	eventExtractor, err := NewEventExtractor()
	if err != nil {
		return nil, fmt.Errorf("failed to create event extractor: %w", err)
	}

	processor := &Service{
		config:             config,
		consumer:           consumer,
		cacheReader:        cacheReader,
		ethClient:          ethClient,
		txManager:          txManager,
		userRepo:           userRepo,
		protocolRepo:       protocolRepo,
		tokenRepo:          tokenRepo,
		positionRepo:       positionRepo,
		eventRepo:          eventRepo,
		blockchainServices: make(map[blockchain.ProtocolKey]*blockchainService),
		multicallClient:    mc,
		erc20ABI:           erc20ABI,
		eventExtractor:     eventExtractor,
		logger:             config.Logger.With("component", "sparklend-position-tracker"),
	}

	return processor, nil
}

func normalizeProtocolType(protocolType string) string {
	if strings.TrimSpace(protocolType) == "" {
		return "lending"
	}

	return protocolType
}

func normalizeTokenSymbol(symbol string) string {
	if strings.TrimSpace(symbol) == "" {
		return "UNKNOWN"
	}

	return symbol
}

func (s *Service) getOrCreateBlockchainService(chainID int64, protocolAddress common.Address) (*blockchainService, error) {
	key := blockchain.ProtocolKey{ChainID: chainID, PoolAddress: protocolAddress}

	// First check: read map under read lock.
	s.mu.RLock()
	svc, exists := s.blockchainServices[key]
	s.mu.RUnlock()
	if exists {
		return svc, nil
	}

	protocolConfig, exists := blockchain.GetProtocolConfig(chainID, protocolAddress)
	if !exists {
		return nil, fmt.Errorf("unknown protocol: chainID=%d address=%s", chainID, protocolAddress.Hex())
	}

	// Construct outside the lock - this hits the network.
	newSvc, err := newBlockchainService(
		chainID,
		s.ethClient,
		s.multicallClient,
		s.erc20ABI,
		protocolConfig.UIPoolDataProvider.Address,
		protocolConfig.PoolAddress.Address,
		protocolConfig.PoolAddressesProvider.Address,
		protocolConfig.ProtocolVersion,
		s.logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create blockchain service for %s: %w", protocolConfig.Name, err)
	}

	// Second check: re-acquire lock, another goroutine may have created it.
	s.mu.Lock()
	if svc, exists = s.blockchainServices[key]; !exists {
		s.blockchainServices[key] = newSvc
		svc = newSvc
		s.logger.Info("created blockchain service",
			"protocol", protocolConfig.Name,
			"chainID", chainID,
			"address", protocolAddress.Hex())
	}
	s.mu.Unlock()

	return svc, nil
}

func (s *Service) Start(ctx context.Context) error {
	if s.consumer == nil {
		return fmt.Errorf("Start() called on service without SQS consumer")
	}
	if s.cacheReader == nil {
		return fmt.Errorf("Start() called on service without cache reader")
	}

	s.ctx, s.cancel = context.WithCancel(ctx)

	go sqsutil.RunLoop(s.ctx, sqsutil.Config{
		Consumer:     s.consumer,
		MaxMessages:  s.config.MaxMessages,
		PollInterval: s.config.PollInterval,
		Logger:       s.logger,
		ChainID:      s.config.ChainID,
	}, s.processBlockEvent)

	s.logger.Info("sparklend position tracker started",
		"maxMessages", s.config.MaxMessages)
	return nil
}

func (s *Service) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.logger.Info("sparklend position tracker stopped")
	return nil
}

func (s *Service) processBlockEvent(ctx context.Context, event outbound.BlockEvent) error {
	return s.fetchAndProcessReceipts(ctx, event)
}

func (s *Service) fetchAndProcessReceipts(ctx context.Context, event outbound.BlockEvent) error {
	start := time.Now()
	defer func() {
		s.logger.Debug("fetchAndProcessReceipts completed",
			"block", event.BlockNumber,
			"duration", time.Since(start))
	}()

	receiptsData, err := s.cacheReader.GetReceipts(ctx, event.ChainID, event.BlockNumber, event.Version)
	if err != nil {
		return fmt.Errorf("failed to fetch receipts from cache: %w", err)
	}
	if receiptsData == nil {
		return fmt.Errorf("receipts not found in cache: chainID=%d block=%d version=%d", event.ChainID, event.BlockNumber, event.Version)
	}

	var receipts []shared.TransactionReceipt
	if err := json.Unmarshal(receiptsData, &receipts); err != nil {
		return fmt.Errorf("failed to unmarshal receipts: %w", err)
	}

	return s.ProcessReceipts(ctx, event.ChainID, event.BlockNumber, event.Version, receipts)
}

// ProcessReceipts processes a slice of transaction receipts for a given block.
// It is safe to call from the backfill service without Redis or SQS.
func (s *Service) ProcessReceipts(ctx context.Context, chainID, blockNumber int64, version int, receipts []shared.TransactionReceipt) error {
	var errs []error
	for _, receipt := range receipts {
		if err := s.processReceipt(ctx, receipt, chainID, blockNumber, version); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (s *Service) processReceipt(ctx context.Context, receipt shared.TransactionReceipt, chainID, blockNumber int64, blockVersion int) error {
	var errs []error
	for _, log := range receipt.Logs {

		// Only process logs from known protocol addresses
		protocolAddress := common.HexToAddress(log.Address)
		if !blockchain.IsKnownProtocol(chainID, protocolAddress) {
			continue
		}

		if s.isPositionEvent(log) {
			if err := s.processPositionEventLog(ctx, log, receipt.TransactionHash, chainID, blockNumber, blockVersion); err != nil {
				s.logger.Error("failed to process position event", "error", err, "tx", receipt.TransactionHash)
				errs = append(errs, err)
			}
			continue
		}

		if s.isReserveEvent(log) {
			if err := s.processReserveEventLog(ctx, log, receipt.TransactionHash, chainID, blockNumber, blockVersion); err != nil {
				s.logger.Error("failed to process reserve event", "error", err, "tx", receipt.TransactionHash)
				errs = append(errs, err)
			}
			continue
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (s *Service) isPositionEvent(log shared.Log) bool {
	return s.eventExtractor.IsPositionEvent(log)
}

func (s *Service) isReserveEvent(log shared.Log) bool {
	return s.eventExtractor.IsReserveEvent(log)
}

func (s *Service) processPositionEventLog(ctx context.Context, log shared.Log, txHash string, chainID, blockNumber int64, blockVersion int) error {
	start := time.Now()
	defer func() {
		s.logger.Debug("processEventLog completed",
			"tx", txHash,
			"block", blockNumber,
			"duration", time.Since(start))
	}()

	protocolAddress := common.HexToAddress(log.Address)

	eventData, err := s.eventExtractor.ExtractEventData(log)
	if err != nil {
		return fmt.Errorf("failed to extract event data: %w", err)
	}

	s.logger.Info("Position event detected",
		"event_type", eventData.EventType,
		"user", eventData.User.Hex(),
		"protocol", protocolAddress.Hex(),
		"tx", txHash,
		"block", blockNumber)

	// Save the raw decoded event for analytics/auditability
	logIndex, err := strconv.ParseInt(log.LogIndex, 0, 64)
	if err != nil {
		return fmt.Errorf("parsing log index %q: %w", log.LogIndex, err)
	}

	if err := s.saveProtocolEvent(ctx, eventData, protocolAddress, chainID, blockNumber, blockVersion, int(logIndex)); err != nil {
		return fmt.Errorf("failed to save protocol event: %w", err)
	}

	if eventData.EventType == EventReserveUsedAsCollateralEnabled ||
		eventData.EventType == EventReserveUsedAsCollateralDisabled {
		return s.saveCollateralToggleEvent(ctx, eventData, protocolAddress, chainID, blockNumber, blockVersion)
	}

	if eventData.EventType == EventLiquidationCall {
		return s.saveLiquidationEvent(ctx, eventData, protocolAddress, chainID, blockNumber, blockVersion)
	}

	return s.savePositionSnapshot(ctx, eventData, protocolAddress, chainID, blockNumber, blockVersion)
}

func (s *Service) saveProtocolEvent(ctx context.Context, eventData *PositionEventData, protocolAddress common.Address, chainID, blockNumber int64, blockVersion int, logIndex int) error {
	eventJSON, err := eventData.ToJSON()
	if err != nil {
		return fmt.Errorf("failed to serialize event data: %w", err)
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		protocolConfig, exists := blockchain.GetProtocolConfig(chainID, protocolAddress)
		if !exists {
			return fmt.Errorf("unknown protocol: chainID=%d address=%s", chainID, protocolAddress.Hex())
		}

		protocolID, err := s.protocolRepo.GetOrCreateProtocol(ctx, tx, chainID, protocolAddress, protocolConfig.Name, normalizeProtocolType(protocolConfig.ProtocolType), blockNumber)
		if err != nil {
			return fmt.Errorf("failed to get protocol: %w", err)
		}

		event, err := entity.NewProtocolEvent(
			int(chainID),
			protocolID,
			blockNumber,
			blockVersion,
			common.FromHex(eventData.TxHash),
			logIndex,
			protocolAddress.Bytes(),
			string(eventData.EventType),
			eventJSON,
		)
		if err != nil {
			return fmt.Errorf("failed to create protocol event entity: %w", err)
		}

		return s.eventRepo.SaveEvent(ctx, tx, event)
	})
}

// processReserveEventLog handles ReserveDataUpdated events by fetching and storing reserve data.
func (s *Service) processReserveEventLog(ctx context.Context, log shared.Log, txHash string, chainID, blockNumber int64, blockVersion int) error {
	start := time.Now()
	defer func() {
		s.logger.Debug("processReserveEventLog completed",
			"tx", txHash,
			"block", blockNumber,
			"duration", time.Since(start))
	}()

	protocolAddress := common.HexToAddress(log.Address)

	reserveEventData, err := s.eventExtractor.ExtractReserveEventData(log)
	if err != nil {
		return fmt.Errorf("failed to extract reserve event data: %w", err)
	}

	s.logger.Info("ReserveDataUpdated event detected",
		"reserve", reserveEventData.Reserve.Hex(),
		"protocol", protocolAddress.Hex(),
		"tx", txHash,
		"block", blockNumber)

	return s.saveReserveDataSnapshot(ctx, reserveEventData.Reserve, protocolAddress, chainID, blockNumber, blockVersion, txHash)
}

// saveReserveDataSnapshot fetches reserve data from chain and persists it.
func (s *Service) saveReserveDataSnapshot(ctx context.Context, reserve common.Address, protocolAddress common.Address, chainID, blockNumber int64, blockVersion int, txHash string) error {
	blockchainSvc, err := s.getOrCreateBlockchainService(chainID, protocolAddress)
	if err != nil {
		return fmt.Errorf("failed to get blockchain service: %w", err)
	}

	// Get token metadata for the reserve
	tokensToFetch := map[common.Address]bool{reserve: true}
	metadataMap, err := blockchainSvc.batchGetTokenMetadata(ctx, tokensToFetch, big.NewInt(blockNumber))
	if err != nil {
		return fmt.Errorf("failed to get token metadata: %w", err)
	}

	tokenMetadata, ok := metadataMap[reserve]
	if !ok || tokenMetadata.Decimals == 0 {
		return fmt.Errorf("token metadata not found for %s", reserve.Hex())
	}

	// Fetch reserve data and configuration from chain
	reserveData, configData, err := blockchainSvc.getFullReserveData(ctx, reserve, blockNumber)
	if err != nil {
		// If reserve doesn't exist at this block or no PoolDataProvider was active,
		// log and skip (non-fatal). This can happen when:
		// - The configured PoolDataProvider address didn't exist at the historical block
		//   (e.g., contract was upgraded/redeployed)
		// - The pool was deployed before its first PoolDataProvider became active
		//   (e.g., SparkLend pool deployed at block 16568452, but first PoolDataProvider
		//   only active from block 17007086)
		if errors.Is(err, ErrReserveNotFound) || errors.Is(err, ErrNoPoolDataProvider) {
			s.logger.Warn("Reserve data unavailable at block, skipping snapshot",
				"reserve", reserve.Hex(),
				"protocol", protocolAddress.Hex(),
				"block", blockNumber,
				"tx", txHash,
				"error", err)
			return nil
		}
		return fmt.Errorf("failed to get reserve data: %w", err)
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		protocolConfig, exists := blockchain.GetProtocolConfig(chainID, protocolAddress)
		if !exists {
			return fmt.Errorf("unknown protocol: chainID=%d address=%s", chainID, protocolAddress.Hex())
		}

		protocolID, err := s.protocolRepo.GetOrCreateProtocol(ctx, tx, chainID, protocolAddress, protocolConfig.Name, normalizeProtocolType(protocolConfig.ProtocolType), blockNumber)
		if err != nil {
			return fmt.Errorf("failed to get protocol: %w", err)
		}

		// Get or create token
		tokenID, err := s.tokenRepo.GetOrCreateToken(ctx, tx, chainID, reserve, normalizeTokenSymbol(tokenMetadata.Symbol), tokenMetadata.Decimals, blockNumber)
		if err != nil {
			return fmt.Errorf("failed to get token: %w", err)
		}

		// Build and persist the entity
		sparkReserveData := s.buildReserveDataEntity(protocolID, tokenID, blockNumber, blockVersion, reserveData, configData)
		if err := s.protocolRepo.UpsertReserveData(ctx, tx, []*entity.SparkLendReserveData{sparkReserveData}); err != nil {
			return fmt.Errorf("failed to upsert reserve data: %w", err)
		}

		s.logger.Info("Saved reserve data snapshot",
			"reserve", reserve.Hex(),
			"protocol", protocolAddress.Hex(),
			"tx", txHash,
			"block", blockNumber)

		return nil
	})
}

// buildReserveDataEntity creates a SparkLendReserveData entity from fetched reserve data.
func (s *Service) buildReserveDataEntity(
	protocolID, tokenID, blockNumber int64,
	blockVersion int,
	reserveData *reserveDataFromProvider,
	configData *reserveConfigData,
) *entity.SparkLendReserveData {
	sparkReserveData := &entity.SparkLendReserveData{
		ProtocolID:   protocolID,
		TokenID:      tokenID,
		BlockNumber:  blockNumber,
		BlockVersion: blockVersion,
	}

	sparkReserveData.WithRates(
		reserveData.LiquidityRate,
		reserveData.VariableBorrowRate,
		reserveData.StableBorrowRate,
		reserveData.AverageStableBorrowRate,
	)
	sparkReserveData.WithIndexes(
		reserveData.LiquidityIndex,
		reserveData.VariableBorrowIndex,
	)
	sparkReserveData.WithTotals(
		reserveData.Unbacked,
		reserveData.AccruedToTreasuryScaled,
		reserveData.TotalAToken,
		reserveData.TotalStableDebt,
		reserveData.TotalVariableDebt,
	)
	sparkReserveData.LastUpdateTimestamp = reserveData.LastUpdateTimestamp

	sparkReserveData.WithConfiguration(
		configData.Decimals,
		configData.LTV,
		configData.LiquidationThreshold,
		configData.LiquidationBonus,
		configData.ReserveFactor,
		configData.UsageAsCollateralEnabled,
		configData.BorrowingEnabled,
		configData.StableBorrowRateEnabled,
		configData.IsActive,
		configData.IsFrozen,
	)

	return sparkReserveData
}

// saveCollateralToggleEvent handles ReserveUsedAsCollateralEnabled/Disabled events
func (s *Service) saveCollateralToggleEvent(ctx context.Context, eventData *PositionEventData, protocolAddress common.Address, chainID, blockNumber int64, blockVersion int) error {
	blockchainSvc, err := s.getOrCreateBlockchainService(chainID, protocolAddress)
	if err != nil {
		return fmt.Errorf("failed to get blockchain service: %w", err)
	}

	tokensToFetch := map[common.Address]bool{eventData.Reserve: true}
	metadataMap, err := blockchainSvc.batchGetTokenMetadata(ctx, tokensToFetch, big.NewInt(blockNumber))
	if err != nil {
		return fmt.Errorf("failed to get token metadata: %w", err)
	}

	metadata, ok := metadataMap[eventData.Reserve]
	if !ok || metadata.Decimals == 0 {
		return fmt.Errorf("token metadata not found for %s", eventData.Reserve.Hex())
	}

	collaterals, err := s.extractCollateralData(ctx, eventData.User, protocolAddress, chainID, blockNumber, eventData.TxHash)
	if err != nil {
		s.logger.Warn("failed to extract collateral data", "error", err, "tx", eventData.TxHash)
		collaterals = []CollateralData{}
	}

	var balance *big.Int
	for _, c := range collaterals {
		if c.Asset == eventData.Reserve {
			balance = c.ActualBalance
			break
		}
	}
	if balance == nil {
		balance = big.NewInt(0)
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		userID, err := s.userRepo.GetOrCreateUser(ctx, tx, entity.User{
			ChainID:        chainID,
			Address:        eventData.User,
			FirstSeenBlock: blockNumber,
		})
		if err != nil {
			return fmt.Errorf("failed to ensure user: %w", err)
		}

		protocolConfig, exists := blockchain.GetProtocolConfig(chainID, protocolAddress)
		if !exists {
			return fmt.Errorf("unknown protocol: chainID=%d address=%s", chainID, protocolAddress.Hex())
		}

		protocolID, err := s.protocolRepo.GetOrCreateProtocol(ctx, tx, chainID, protocolAddress, protocolConfig.Name, normalizeProtocolType(protocolConfig.ProtocolType), blockNumber)
		if err != nil {
			return fmt.Errorf("failed to get protocol: %w", err)
		}

		tokenID, err := s.tokenRepo.GetOrCreateToken(ctx, tx, chainID, eventData.Reserve, normalizeTokenSymbol(metadata.Symbol), metadata.Decimals, blockNumber)
		if err != nil {
			return fmt.Errorf("failed to get token: %w", err)
		}

		decimalAdjustedBalance := s.convertToDecimalAdjusted(balance, metadata.Decimals)
		if err := s.positionRepo.SaveBorrowerCollateral(ctx, tx, userID, protocolID, tokenID, blockNumber, blockVersion, decimalAdjustedBalance, string(eventData.EventType), common.FromHex(eventData.TxHash), eventData.CollateralEnabled); err != nil {
			return fmt.Errorf("failed to save collateral toggle: %w", err)
		}

		return nil
	})
}

func (s *Service) saveLiquidationEvent(ctx context.Context, eventData *PositionEventData, protocolAddress common.Address, chainID, blockNumber int64, blockVersion int) error {
	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		if err := s.snapshotUserPosition(ctx, tx, eventData.User, string(eventData.EventType), common.FromHex(eventData.TxHash), protocolAddress, chainID, blockNumber, blockVersion); err != nil {
			return fmt.Errorf("failed to snapshot borrower: %w", err)
		}

		if err := s.snapshotUserPosition(ctx, tx, eventData.Liquidator, string(eventData.EventType), common.FromHex(eventData.TxHash), protocolAddress, chainID, blockNumber, blockVersion); err != nil {
			return fmt.Errorf("failed to snapshot liquidator: %w", err)
		}

		return nil
	})
}

func (s *Service) savePositionSnapshot(ctx context.Context, eventData *PositionEventData, protocolAddress common.Address, chainID, blockNumber int64, blockVersion int) error {
	blockchainSvc, err := s.getOrCreateBlockchainService(chainID, protocolAddress)
	if err != nil {
		return fmt.Errorf("failed to get blockchain service: %w", err)
	}

	tokensToFetch := map[common.Address]bool{eventData.Reserve: true}
	metadataMap, err := blockchainSvc.batchGetTokenMetadata(ctx, tokensToFetch, big.NewInt(blockNumber))
	if err != nil {
		s.logger.Warn("failed to batch get token metadata", "error", err, "tx", eventData.TxHash, "block", blockNumber)
		metadataMap = make(map[common.Address]TokenMetadata)
	}

	tokenMetadata, ok := metadataMap[eventData.Reserve]
	if !ok || tokenMetadata.Decimals == 0 {
		s.logger.Error("Failed to get token decimals",
			"action", "skipped",
			"event_type", eventData.EventType,
			"token", eventData.Reserve.Hex(),
			"tx", eventData.TxHash,
			"block", blockNumber,
			"user", eventData.User.Hex(),
			"protocol", protocolAddress.Hex())
		return fmt.Errorf("token decimals not found for %s", eventData.Reserve.Hex())
	}

	collaterals, err := s.extractCollateralData(ctx, eventData.User, protocolAddress, chainID, blockNumber, eventData.TxHash)
	if err != nil {
		s.logger.Warn("failed to extract collateral data", "error", err, "tx", eventData.TxHash, "block", blockNumber)
		collaterals = []CollateralData{}
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		userID, err := s.userRepo.GetOrCreateUser(ctx, tx, entity.User{
			ChainID:        chainID,
			Address:        eventData.User,
			FirstSeenBlock: blockNumber,
		})
		if err != nil {
			return fmt.Errorf("failed to ensure user: %w", err)
		}

		protocolConfig, exists := blockchain.GetProtocolConfig(chainID, protocolAddress)
		if !exists {
			return fmt.Errorf("unknown protocol: chainID=%d address=%s", chainID, protocolAddress.Hex())
		}

		protocolID, err := s.protocolRepo.GetOrCreateProtocol(ctx, tx, chainID, protocolAddress, protocolConfig.Name, normalizeProtocolType(protocolConfig.ProtocolType), blockNumber)
		if err != nil {
			return fmt.Errorf("failed to get protocol: %w", err)
		}

		if eventData.EventType == EventBorrow || eventData.EventType == EventRepay {
			tokenID, err := s.tokenRepo.GetOrCreateToken(ctx, tx, chainID, eventData.Reserve, normalizeTokenSymbol(tokenMetadata.Symbol), tokenMetadata.Decimals, blockNumber)
			if err != nil {
				return fmt.Errorf("failed to get token: %w", err)
			}
			decimalAdjustedAmount := s.convertToDecimalAdjusted(eventData.Amount, tokenMetadata.Decimals)
			if err := s.positionRepo.SaveBorrower(ctx, tx, userID, protocolID, tokenID, blockNumber, blockVersion, decimalAdjustedAmount, string(eventData.EventType), common.FromHex(eventData.TxHash)); err != nil {
				return fmt.Errorf("failed to insert borrower: %w", err)
			}
		}

		records := make([]outbound.CollateralRecord, 0, len(collaterals))
		for _, col := range collaterals {
			tokenID, err := s.tokenRepo.GetOrCreateToken(ctx, tx, chainID, col.Asset, normalizeTokenSymbol(col.Symbol), col.Decimals, blockNumber)
			if err != nil {
				s.logger.Warn("failed to get collateral token", "token", col.Asset.Hex(), "error", err, "tx", eventData.TxHash)
				continue
			}

			records = append(records, outbound.CollateralRecord{
				UserID:            userID,
				ProtocolID:        protocolID,
				TokenID:           tokenID,
				BlockNumber:       blockNumber,
				BlockVersion:      blockVersion,
				Amount:            s.convertToDecimalAdjusted(col.ActualBalance, col.Decimals),
				EventType:         string(eventData.EventType),
				TxHash:            common.FromHex(eventData.TxHash),
				CollateralEnabled: col.CollateralEnabled,
			})
		}

		if err := s.positionRepo.SaveBorrowerCollaterals(ctx, tx, records); err != nil {
			return fmt.Errorf("failed to save collaterals: %w", err)
		}

		return nil
	})
}

func (s *Service) snapshotUserPosition(ctx context.Context, tx pgx.Tx, user common.Address, eventType string, txHash []byte, protocolAddress common.Address, chainID, blockNumber int64, blockVersion int) error {
	userID, err := s.userRepo.GetOrCreateUser(ctx, tx, entity.User{
		ChainID:        chainID,
		Address:        user,
		FirstSeenBlock: blockNumber,
	})
	if err != nil {
		return fmt.Errorf("failed to ensure user: %w", err)
	}

	protocolConfig, exists := blockchain.GetProtocolConfig(chainID, protocolAddress)
	if !exists {
		return fmt.Errorf("unknown protocol: chainID=%d address=%s", chainID, protocolAddress.Hex())
	}

	protocolID, err := s.protocolRepo.GetOrCreateProtocol(ctx, tx, chainID, protocolAddress, protocolConfig.Name, normalizeProtocolType(protocolConfig.ProtocolType), blockNumber)
	if err != nil {
		return fmt.Errorf("failed to get protocol: %w", err)
	}

	txHashHex := common.BytesToHash(txHash).Hex()
	collaterals, err := s.extractCollateralData(ctx, user, protocolAddress, chainID, blockNumber, txHashHex)
	if err != nil {
		s.logger.Warn("failed to extract collateral data for user", "user", user.Hex(), "error", err)
		collaterals = []CollateralData{}
	}

	records := make([]outbound.CollateralRecord, 0, len(collaterals))
	for _, col := range collaterals {
		tokenID, err := s.tokenRepo.GetOrCreateToken(ctx, tx, chainID, col.Asset, normalizeTokenSymbol(col.Symbol), col.Decimals, blockNumber)
		if err != nil {
			s.logger.Warn("failed to get collateral token", "token", col.Asset.Hex(), "error", err, "tx", txHashHex)
			continue
		}

		records = append(records, outbound.CollateralRecord{
			UserID:            userID,
			ProtocolID:        protocolID,
			TokenID:           tokenID,
			BlockNumber:       blockNumber,
			BlockVersion:      blockVersion,
			Amount:            s.convertToDecimalAdjusted(col.ActualBalance, col.Decimals),
			EventType:         eventType,
			TxHash:            txHash,
			CollateralEnabled: col.CollateralEnabled,
		})
	}

	if err := s.positionRepo.SaveBorrowerCollaterals(ctx, tx, records); err != nil {
		return fmt.Errorf("failed to save collaterals: %w", err)
	}

	s.logger.Info("Saved position snapshot", "user", user.Hex(), "tx", txHashHex, "block", blockNumber)
	return nil
}

func (s *Service) extractCollateralData(ctx context.Context, user common.Address, protocolAddress common.Address, chainID, blockNumber int64, txHash string) ([]CollateralData, error) {
	blockchainSvc, err := s.getOrCreateBlockchainService(chainID, protocolAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to get blockchain service: %w", err)
	}

	reserves, err := blockchainSvc.getUserReservesData(ctx, user, blockNumber)
	if err != nil {
		s.logger.Warn("failed to get user reserves", "error", err, "tx", txHash, "block", blockNumber)
		return []CollateralData{}, nil
	}

	collateralAssets := make([]common.Address, 0)
	for _, r := range reserves {
		if r.ScaledATokenBalance.Cmp(big.NewInt(0)) > 0 && r.UsageAsCollateralEnabledOnUser {
			if r.UnderlyingAsset != (common.Address{}) {
				collateralAssets = append(collateralAssets, r.UnderlyingAsset)
			}
		}
	}

	if len(collateralAssets) == 0 {
		return []CollateralData{}, nil
	}

	tokensToFetch := make(map[common.Address]bool)
	for _, asset := range collateralAssets {
		tokensToFetch[asset] = true
	}

	metadataMap, err := blockchainSvc.batchGetTokenMetadata(ctx, tokensToFetch, big.NewInt(blockNumber))
	if err != nil {
		s.logger.Warn("failed to batch get token metadata", "error", err, "tx", txHash, "block", blockNumber)
		return []CollateralData{}, fmt.Errorf("failed to get token metadata: %w", err)
	}

	actualDataMap, err := blockchainSvc.batchGetUserReserveData(ctx, collateralAssets, user, blockNumber)
	if err != nil {
		s.logger.Warn("failed to batch get user reserve data", "error", err, "tx", txHash, "block", blockNumber)
		return []CollateralData{}, fmt.Errorf("failed to get user reserve data: %w", err)
	}

	var collaterals []CollateralData
	for _, asset := range collateralAssets {
		metadata, ok := metadataMap[asset]
		if !ok || metadata.Decimals == 0 {
			s.logger.Error("Failed to get collateral token metadata",
				"action", "skipped",
				"token", asset.Hex(),
				"tx", txHash,
				"block", blockNumber,
				"user", user.Hex())
			continue
		}

		actualData, ok := actualDataMap[asset]
		if !ok {
			s.logger.Error("Failed to get actual balance",
				"action", "skipped",
				"token", asset.Hex(),
				"tx", txHash,
				"block", blockNumber,
				"user", user.Hex())
			continue
		}

		if actualData.UsageAsCollateralEnabled && actualData.CurrentATokenBalance.Cmp(big.NewInt(0)) > 0 {
			collaterals = append(collaterals, CollateralData{
				Asset:             asset,
				Decimals:          metadata.Decimals,
				Symbol:            metadata.Symbol,
				Name:              metadata.Name,
				ActualBalance:     actualData.CurrentATokenBalance,
				CollateralEnabled: actualData.UsageAsCollateralEnabled,
			})
		}
	}

	return collaterals, nil
}

func (s *Service) convertToDecimalAdjusted(rawAmount *big.Int, decimals int) string {
	if decimals == 0 {
		return rawAmount.String()
	}

	divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
	integerPart := new(big.Int).Div(rawAmount, divisor)
	remainder := new(big.Int).Mod(rawAmount, divisor)

	if remainder.Cmp(big.NewInt(0)) == 0 {
		return integerPart.String()
	}

	fractionalStr := fmt.Sprintf("%0*s", decimals, remainder.String())
	return fmt.Sprintf("%s.%s", integerPart.String(), fractionalStr)
}

// validateDependencies verifies that all required service dependencies are present.
// Consumer and cacheReader may be nil in backfill mode (when only ProcessReceipts is used).
// Returns an error if any of the following required dependencies is nil: ethClient,
// txManager, userRepo, protocolRepo, tokenRepo, positionRepo, or eventRepo.
func validateDependencies(
	consumer outbound.SQSConsumer,
	cacheReader outbound.BlockCacheReader,
	ethClient *ethclient.Client,
	txManager outbound.TxManager,
	userRepo outbound.UserRepository,
	protocolRepo outbound.ProtocolRepository,
	tokenRepo outbound.TokenRepository,
	positionRepo outbound.PositionRepository,
	eventRepo outbound.EventRepository,
) error {
	// consumer and cacheReader may be nil in backfill mode (ProcessReceipts only).
	if ethClient == nil {
		return fmt.Errorf("ethClient is required")
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
	if positionRepo == nil {
		return fmt.Errorf("positionRepo is required")
	}
	if eventRepo == nil {
		return fmt.Errorf("eventRepo is required")
	}
	return nil
}

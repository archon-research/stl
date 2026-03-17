package aavelike_position_tracker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/common/sqsutil"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/aavelike"
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

	reader         *aavelike.PositionReader
	eventExtractor *EventExtractor

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

	readerLogger := config.Logger.With("component", "aavelike-position-tracker")
	reader := aavelike.NewPositionReader(ethClient, mc, erc20ABI, readerLogger)

	eventExtractor, err := NewEventExtractor()
	if err != nil {
		return nil, fmt.Errorf("failed to create event extractor: %w", err)
	}

	processor := &Service{
		config:         config,
		consumer:       consumer,
		cacheReader:    cacheReader,
		ethClient:      ethClient,
		txManager:      txManager,
		userRepo:       userRepo,
		protocolRepo:   protocolRepo,
		tokenRepo:      tokenRepo,
		positionRepo:   positionRepo,
		eventRepo:      eventRepo,
		reader:         reader,
		eventExtractor: eventExtractor,
		logger:         config.Logger.With("component", "aavelike-position-tracker"),
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

	s.logger.Info("aavelike position tracker started",
		"maxMessages", s.config.MaxMessages)
	return nil
}

func (s *Service) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.logger.Info("aavelike position tracker stopped")
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
	blockchainSvc, err := s.reader.GetOrCreateBlockchainService(chainID, protocolAddress)
	if err != nil {
		return fmt.Errorf("failed to get blockchain service: %w", err)
	}

	// Get token metadata for the reserve
	tokensToFetch := map[common.Address]bool{reserve: true}
	metadataMap, err := blockchainSvc.BatchGetTokenMetadata(ctx, tokensToFetch, big.NewInt(blockNumber))
	if err != nil {
		return fmt.Errorf("failed to get token metadata: %w", err)
	}

	tokenMetadata, ok := metadataMap[reserve]
	if !ok || tokenMetadata.Decimals == 0 {
		return fmt.Errorf("token metadata not found for %s", reserve.Hex())
	}

	// Fetch reserve data and configuration from chain
	reserveData, configData, err := blockchainSvc.GetFullReserveData(ctx, reserve, blockNumber)
	if err != nil {
		// If reserve doesn't exist at this block or no PoolDataProvider was active,
		// log and skip (non-fatal). This can happen when:
		// - The configured PoolDataProvider address didn't exist at the historical block
		//   (e.g., contract was upgraded/redeployed)
		// - The pool was deployed before its first PoolDataProvider became active
		//   (e.g., SparkLend pool deployed at block 16568452, but first PoolDataProvider
		//   only active from block 17007086)
		if errors.Is(err, aavelike.ErrReserveNotFound) || errors.Is(err, aavelike.ErrNoPoolDataProvider) {
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
	reserveData *aavelike.ReserveData,
	configData *aavelike.ReserveConfigData,
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
	blockchainSvc, err := s.reader.GetOrCreateBlockchainService(chainID, protocolAddress)
	if err != nil {
		return fmt.Errorf("failed to get blockchain service: %w", err)
	}

	tokensToFetch := map[common.Address]bool{eventData.Reserve: true}
	metadataMap, err := blockchainSvc.BatchGetTokenMetadata(ctx, tokensToFetch, big.NewInt(blockNumber))
	if err != nil {
		return fmt.Errorf("failed to get token metadata: %w", err)
	}

	metadata, ok := metadataMap[eventData.Reserve]
	if !ok || metadata.Decimals == 0 {
		return fmt.Errorf("token metadata not found for %s", eventData.Reserve.Hex())
	}

	collaterals, _, err := s.extractUserPositionData(ctx, eventData.User, protocolAddress, chainID, blockNumber, eventData.TxHash)
	if err != nil {
		return fmt.Errorf("failed to extract collateral data: %w", err)
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

		change := big.NewInt(0)
		if eventData.Amount != nil {
			// Make sure we don't mutate the original
			change = big.NewInt(0).Set(eventData.Amount)
		}
		if err := s.positionRepo.SaveBorrowerCollateral(ctx, tx, userID, protocolID, tokenID, blockNumber, blockVersion, balance, change, string(eventData.EventType), common.FromHex(eventData.TxHash), eventData.CollateralEnabled); err != nil {
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
	blockchainSvc, err := s.reader.GetOrCreateBlockchainService(chainID, protocolAddress)
	if err != nil {
		return fmt.Errorf("failed to get blockchain service: %w", err)
	}

	tokensToFetch := map[common.Address]bool{eventData.Reserve: true}
	metadataMap, err := blockchainSvc.BatchGetTokenMetadata(ctx, tokensToFetch, big.NewInt(blockNumber))
	if err != nil {
		return fmt.Errorf("failed to batch get token metadata: %w", err)
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

	collaterals, debtData, err := s.extractUserPositionData(ctx, eventData.User, protocolAddress, chainID, blockNumber, eventData.TxHash)
	if err != nil {
		return fmt.Errorf("failed to extract user position data: %w", err)
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
			if err := s.saveBorrowerRecord(ctx, tx, eventData, tokenMetadata, debtData, userID, protocolID, tokenID, blockNumber, blockVersion); err != nil {
				return fmt.Errorf("failed to insert borrower: %w", err)
			}
		}

		records := make([]outbound.CollateralRecord, 0, len(collaterals))
		for _, col := range collaterals {
			tokenID, err := s.tokenRepo.GetOrCreateToken(ctx, tx, chainID, col.Asset, normalizeTokenSymbol(col.Symbol), col.Decimals, blockNumber)
			if err != nil {
				return fmt.Errorf("failed to get collateral token %s: %w", col.Asset.Hex(), err)
			}

			records = append(records, outbound.CollateralRecord{
				UserID:       userID,
				ProtocolID:   protocolID,
				TokenID:      tokenID,
				BlockNumber:  blockNumber,
				BlockVersion: blockVersion,
				Amount:       col.ActualBalance,
				// Change is zero here because these records capture the full collateral snapshot
				// across all assets triggered by a position event (Borrow, Repay, Supply, Withdraw).
				// The event delta belongs to a specific reserve, not to each collateral asset.
				Change:            big.NewInt(0),
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
	// Debts are intentionally discarded: snapshotUserPosition captures only the collateral
	// side. Debt positions are tracked separately via saveBorrowerRecord on Borrow/Repay events.
	collaterals, _, err := s.extractUserPositionData(ctx, user, protocolAddress, chainID, blockNumber, txHashHex)
	if err != nil {
		return fmt.Errorf("failed to extract collateral data for user %s: %w", user.Hex(), err)
	}

	records := make([]outbound.CollateralRecord, 0, len(collaterals))
	for _, col := range collaterals {
		tokenID, err := s.tokenRepo.GetOrCreateToken(ctx, tx, chainID, col.Asset, normalizeTokenSymbol(col.Symbol), col.Decimals, blockNumber)
		if err != nil {
			return fmt.Errorf("failed to get collateral token %s: %w", col.Asset.Hex(), err)
		}

		records = append(records, outbound.CollateralRecord{
			UserID:       userID,
			ProtocolID:   protocolID,
			TokenID:      tokenID,
			BlockNumber:  blockNumber,
			BlockVersion: blockVersion,
			Amount:       col.ActualBalance,
			// Change is zero here because snapshotUserPosition performs a full position snapshot
			// (e.g. triggered by a liquidation) with no per-asset event delta available.
			Change:            big.NewInt(0),
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

func (s *Service) extractUserPositionData(ctx context.Context, user common.Address, protocolAddress common.Address, chainID, blockNumber int64, txHash string) ([]aavelike.CollateralData, []aavelike.DebtData, error) {
	return s.reader.GetUserPositionData(ctx, user, protocolAddress, chainID, blockNumber)
}

// persistPositionData saves a full position snapshot (collaterals + debts) within an
// existing transaction. Callers are responsible for wrapping this in WithTransaction.
func (s *Service) persistPositionData(
	ctx context.Context,
	tx pgx.Tx,
	user common.Address,
	protocolAddress common.Address,
	chainID, blockNumber int64,
	blockVersion int,
	eventType string,
	txHash []byte,
	collaterals []aavelike.CollateralData,
	debts []aavelike.DebtData,
) error {
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

	for _, d := range debts {
		tokenID, err := s.tokenRepo.GetOrCreateToken(ctx, tx, chainID, d.Asset, normalizeTokenSymbol(d.Symbol), d.Decimals, blockNumber)
		if err != nil {
			return fmt.Errorf("failed to get debt token: %w", err)
		}
		if err := s.positionRepo.SaveBorrower(ctx, tx, userID, protocolID, tokenID, blockNumber, blockVersion, d.CurrentDebt, big.NewInt(0), eventType, txHash); err != nil {
			return fmt.Errorf("failed to save borrower: %w", err)
		}
	}

	records := make([]outbound.CollateralRecord, 0, len(collaterals))
	for _, col := range collaterals {
		tokenID, err := s.tokenRepo.GetOrCreateToken(ctx, tx, chainID, col.Asset, normalizeTokenSymbol(col.Symbol), col.Decimals, blockNumber)
		if err != nil {
			return fmt.Errorf("failed to get collateral token %s: %w", col.Asset.Hex(), err)
		}

		records = append(records, outbound.CollateralRecord{
			UserID:            userID,
			ProtocolID:        protocolID,
			TokenID:           tokenID,
			BlockNumber:       blockNumber,
			BlockVersion:      blockVersion,
			Amount:            col.ActualBalance,
			Change:            big.NewInt(0),
			EventType:         eventType,
			TxHash:            txHash,
			CollateralEnabled: col.CollateralEnabled,
		})
	}

	if err := s.positionRepo.SaveBorrowerCollaterals(ctx, tx, records); err != nil {
		return fmt.Errorf("failed to save collaterals: %w", err)
	}

	return nil
}

// IndexUserPosition queries the current on-chain position for a user and persists
// a full snapshot (collaterals + debts) to the database. This is the public entry
// point used by the snapshot indexer CLI.
func (s *Service) IndexUserPosition(ctx context.Context, user common.Address, protocolAddress common.Address, chainID, blockNumber int64, blockVersion int) error {
	collaterals, debts, err := s.extractUserPositionData(ctx, user, protocolAddress, chainID, blockNumber, "")
	if err != nil {
		return fmt.Errorf("failed to extract user position data: %w", err)
	}

	if len(collaterals) == 0 && len(debts) == 0 {
		return nil
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		return s.persistPositionData(ctx, tx, user, protocolAddress, chainID, blockNumber, blockVersion, "Snapshot", []byte{}, collaterals, debts)
	})
}

// PersistUserPosition saves pre-fetched position data to the database.
// Used by the batch backfill CLI which fetches data separately via
// PositionReader.GetBatchUserPositionData.
func (s *Service) PersistUserPosition(
	ctx context.Context,
	user common.Address,
	protocolAddress common.Address,
	chainID, blockNumber int64,
	blockVersion int,
	collaterals []aavelike.CollateralData,
	debts []aavelike.DebtData,
) error {
	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		return s.persistPositionData(ctx, tx, user, protocolAddress, chainID, blockNumber, blockVersion, "Snapshot", []byte{}, collaterals, debts)
	})
}

// UserPositionData holds position data for a single user, used by PersistUserPositionBatch.
type UserPositionData struct {
	User        common.Address
	Collaterals []aavelike.CollateralData
	Debts       []aavelike.DebtData
}

// PersistUserPositionBatch saves position data for multiple users in a single transaction,
// using bulk upserts for users, tokens, borrowers, and collaterals. This dramatically
// reduces DB round trips compared to calling PersistUserPosition per user.
func (s *Service) PersistUserPositionBatch(
	ctx context.Context,
	positions []UserPositionData,
	protocolAddress common.Address,
	chainID, blockNumber int64,
	blockVersion int,
) error {
	if len(positions) == 0 {
		return nil
	}

	protocolConfig, exists := blockchain.GetProtocolConfig(chainID, protocolAddress)
	if !exists {
		return fmt.Errorf("unknown protocol: chainID=%d address=%s", chainID, protocolAddress.Hex())
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		// 1. Bulk upsert all users (1 round trip).
		userEntities := make([]entity.User, len(positions))
		for i, p := range positions {
			userEntities[i] = entity.User{
				ChainID:        chainID,
				Address:        p.User,
				FirstSeenBlock: blockNumber,
			}
		}
		userIDs, err := s.userRepo.GetOrCreateUsers(ctx, tx, userEntities)
		if err != nil {
			return fmt.Errorf("failed to bulk upsert users: %w", err)
		}

		// 2. Upsert protocol (1 round trip — same protocol for all users).
		protocolID, err := s.protocolRepo.GetOrCreateProtocol(ctx, tx, chainID, protocolAddress, protocolConfig.Name, normalizeProtocolType(protocolConfig.ProtocolType), blockNumber)
		if err != nil {
			return fmt.Errorf("failed to get protocol: %w", err)
		}

		// 3. Collect all unique tokens across all users and bulk upsert (1 round trip).
		tokenInputs := make(map[common.Address]outbound.TokenInput)
		for _, p := range positions {
			for _, d := range p.Debts {
				if _, exists := tokenInputs[d.Asset]; !exists {
					tokenInputs[d.Asset] = outbound.TokenInput{
						ChainID:        chainID,
						Address:        d.Asset,
						Symbol:         normalizeTokenSymbol(d.Symbol),
						Decimals:       d.Decimals,
						CreatedAtBlock: blockNumber,
					}
				}
			}
			for _, c := range p.Collaterals {
				if _, exists := tokenInputs[c.Asset]; !exists {
					tokenInputs[c.Asset] = outbound.TokenInput{
						ChainID:        chainID,
						Address:        c.Asset,
						Symbol:         normalizeTokenSymbol(c.Symbol),
						Decimals:       c.Decimals,
						CreatedAtBlock: blockNumber,
					}
				}
			}
		}
		tokenSlice := make([]outbound.TokenInput, 0, len(tokenInputs))
		for _, t := range tokenInputs {
			tokenSlice = append(tokenSlice, t)
		}
		tokenIDs, err := s.tokenRepo.GetOrCreateTokens(ctx, tx, tokenSlice)
		if err != nil {
			return fmt.Errorf("failed to bulk upsert tokens: %w", err)
		}

		// 4. Build all borrower and collateral records, then batch insert (2 round trips).
		var borrowerRecords []outbound.BorrowerRecord
		var collateralRecords []outbound.CollateralRecord

		for _, p := range positions {
			userID, ok := userIDs[p.User]
			if !ok {
				return fmt.Errorf("missing user ID for %s", p.User.Hex())
			}

			for _, d := range p.Debts {
				tokenID, ok := tokenIDs[d.Asset]
				if !ok {
					return fmt.Errorf("missing token ID for debt asset %s", d.Asset.Hex())
				}
				borrowerRecords = append(borrowerRecords, outbound.BorrowerRecord{
					UserID:       userID,
					ProtocolID:   protocolID,
					TokenID:      tokenID,
					BlockNumber:  blockNumber,
					BlockVersion: blockVersion,
					Amount:       d.CurrentDebt,
					Change:       big.NewInt(0),
					EventType:    "Snapshot",
					TxHash:       []byte{},
				})
			}

			for _, c := range p.Collaterals {
				tokenID, ok := tokenIDs[c.Asset]
				if !ok {
					return fmt.Errorf("missing token ID for collateral asset %s", c.Asset.Hex())
				}
				collateralRecords = append(collateralRecords, outbound.CollateralRecord{
					UserID:            userID,
					ProtocolID:        protocolID,
					TokenID:           tokenID,
					BlockNumber:       blockNumber,
					BlockVersion:      blockVersion,
					Amount:            c.ActualBalance,
					Change:            big.NewInt(0),
					EventType:         "Snapshot",
					TxHash:            []byte{},
					CollateralEnabled: c.CollateralEnabled,
				})
			}
		}

		if err := s.positionRepo.SaveBorrowers(ctx, tx, borrowerRecords); err != nil {
			return fmt.Errorf("failed to batch save borrowers: %w", err)
		}
		if err := s.positionRepo.SaveBorrowerCollaterals(ctx, tx, collateralRecords); err != nil {
			return fmt.Errorf("failed to batch save collaterals: %w", err)
		}

		return nil
	})
}

// saveBorrowerRecord saves a single borrow/repay position record.
// amount is the current outstanding debt after the event (raw wei) and change is the
// raw wei event delta.
//
// Fallbacks are event-aware and explicit:
//   - Repay: if the reserve is missing from debtData, persist amount=0 because the
//     debt was fully repaid and no longer appears in the on-chain snapshot.
//   - Borrow: if the reserve is missing from debtData, return an error instead of
//     guessing the outstanding balance.
func (s *Service) saveBorrowerRecord(ctx context.Context, tx pgx.Tx, eventData *PositionEventData, tokenMetadata aavelike.TokenMetadata, debtData []aavelike.DebtData, userID, protocolID, tokenID, blockNumber int64, blockVersion int) error {
	// Look up the current outstanding debt for this reserve.
	for _, d := range debtData {
		if d.Asset == eventData.Reserve {
			return s.positionRepo.SaveBorrower(ctx, tx, userID, protocolID, tokenID, blockNumber, blockVersion, d.CurrentDebt, eventData.Amount, string(eventData.EventType), common.FromHex(eventData.TxHash))
		}
	}

	switch eventData.EventType {
	case EventRepay:
		return s.positionRepo.SaveBorrower(ctx, tx, userID, protocolID, tokenID, blockNumber, blockVersion, big.NewInt(0), eventData.Amount, string(eventData.EventType), common.FromHex(eventData.TxHash))
	case EventBorrow:
		return fmt.Errorf("missing debt data for borrow reserve %s", eventData.Reserve.Hex())
	default:
		return fmt.Errorf("unsupported borrower event type %s", eventData.EventType)
	}
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

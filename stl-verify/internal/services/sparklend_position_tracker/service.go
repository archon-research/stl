package sparklend_position_tracker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// EventType represents the type of SparkLend position-changing event
type EventType string

const (
	EventBorrow                          EventType = "Borrow"
	EventRepay                           EventType = "Repay"
	EventSupply                          EventType = "Supply"
	EventWithdraw                        EventType = "Withdraw"
	EventLiquidationCall                 EventType = "LiquidationCall"
	EventReserveUsedAsCollateralEnabled  EventType = "ReserveUsedAsCollateralEnabled"
	EventReserveUsedAsCollateralDisabled EventType = "ReserveUsedAsCollateralDisabled"
)

// ray is Aave/Spark's precision standard (10^27) used for liquidity index calculations.
// This is a protocol-defined constant for ray math operations.
var ray = new(big.Int).Exp(big.NewInt(10), big.NewInt(27), nil)

type TransactionReceipt struct {
	Type              string  `json:"type"`
	Status            string  `json:"status"`
	CumulativeGasUsed string  `json:"cumulativeGasUsed"`
	Logs              []Log   `json:"logs"`
	LogsBloom         string  `json:"logsBloom"`
	TransactionHash   string  `json:"transactionHash"`
	TransactionIndex  string  `json:"transactionIndex"`
	BlockHash         string  `json:"blockHash"`
	BlockNumber       string  `json:"blockNumber"`
	GasUsed           string  `json:"gasUsed"`
	EffectiveGasPrice string  `json:"effectiveGasPrice"`
	From              string  `json:"from"`
	To                string  `json:"to"`
	ContractAddress   *string `json:"contractAddress"`
}

type Log struct {
	Address          string   `json:"address"`
	Topics           []string `json:"topics"`
	Data             string   `json:"data"`
	BlockHash        string   `json:"blockHash"`
	BlockNumber      string   `json:"blockNumber"`
	BlockTimestamp   string   `json:"blockTimestamp"`
	TransactionHash  string   `json:"transactionHash"`
	TransactionIndex string   `json:"transactionIndex"`
	LogIndex         string   `json:"logIndex"`
	Removed          bool     `json:"removed"`
}

// PositionEventData is a unified struct for all position-changing events
type PositionEventData struct {
	EventType EventType
	TxHash    string
	// User is the primary address affected (borrower, supplier, etc.)
	User common.Address
	// Reserve is the primary asset involved
	Reserve common.Address
	// Amount is the primary amount (may be borrow amount, supply amount, etc.)
	Amount *big.Int
	// For LiquidationCall only
	Liquidator                 common.Address
	CollateralAsset            common.Address
	DebtAsset                  common.Address
	DebtToCover                *big.Int
	LiquidatedCollateralAmount *big.Int
	// For collateral toggle events - indicates new state
	CollateralEnabled bool
}

type CollateralData struct {
	Asset             common.Address
	Decimals          int
	Symbol            string
	Name              string
	ActualBalance     *big.Int
	CollateralEnabled bool
}

type BlockEvent struct {
	ChainID        int64  `json:"chainId"`
	BlockNumber    int64  `json:"blockNumber"`
	Version        int    `json:"version"`
	BlockHash      string `json:"blockHash"`
	ParentHash     string `json:"parentHash"`
	BlockTimestamp int64  `json:"blockTimestamp"`
	ReceivedAt     string `json:"receivedAt"`
	IsBackfill     bool   `json:"isBackfill"`
	IsReorg        bool   `json:"isReorg"`
}

func (e BlockEvent) CacheKey() string {
	return fmt.Sprintf("stl:%d:%d:%d:receipts", e.ChainID, e.BlockNumber, e.Version)
}

type Config struct {
	QueueURL        string
	MaxMessages     int32
	WaitTimeSeconds int32
	PollInterval    time.Duration
	Logger          *slog.Logger
}

func ConfigDefaults() Config {
	return Config{
		MaxMessages:     10,
		WaitTimeSeconds: 20,
		PollInterval:    100 * time.Millisecond,
		Logger:          slog.Default(),
	}
}

type Service struct {
	config         Config
	sqsClient      *sqs.Client
	redisClient    *redis.Client
	blockchain     *blockchainService
	txManager      *postgres.TxManager
	userRepo       *postgres.UserRepository
	protocolRepo   *postgres.ProtocolRepository
	tokenRepo      *postgres.TokenRepository
	positionRepo   *postgres.PositionRepository
	eventExtractor *EventExtractor

	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger
}

func NewService(
	config Config,
	sqsClient *sqs.Client,
	redisClient *redis.Client,
	ethClient *ethclient.Client,
	multicall3Addr common.Address,
	uiPoolDataProvider common.Address,
	poolAddressesProvider common.Address,
	txManager *postgres.TxManager,
	userRepo *postgres.UserRepository,
	protocolRepo *postgres.ProtocolRepository,
	tokenRepo *postgres.TokenRepository,
	positionRepo *postgres.PositionRepository,
) (*Service, error) {
	if err := validateDependencies(sqsClient, redisClient, ethClient, txManager, userRepo, protocolRepo, tokenRepo, positionRepo); err != nil {
		return nil, err
	}

	defaults := ConfigDefaults()
	if config.QueueURL == "" {
		return nil, fmt.Errorf("queueURL is required")
	}
	if config.MaxMessages == 0 {
		config.MaxMessages = defaults.MaxMessages
	}
	if config.WaitTimeSeconds == 0 {
		config.WaitTimeSeconds = defaults.WaitTimeSeconds
	}
	if config.PollInterval == 0 {
		config.PollInterval = defaults.PollInterval
	}
	if config.Logger == nil {
		config.Logger = defaults.Logger
	}

	blockchain, err := newBlockchainService(ethClient, multicall3Addr, uiPoolDataProvider, poolAddressesProvider, config.Logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create blockchain service: %w", err)
	}

	eventExtractor, err := NewEventExtractor()
	if err != nil {
		return nil, fmt.Errorf("failed to create event extractor: %w", err)
	}

	processor := &Service{
		config:         config,
		sqsClient:      sqsClient,
		redisClient:    redisClient,
		blockchain:     blockchain,
		txManager:      txManager,
		userRepo:       userRepo,
		protocolRepo:   protocolRepo,
		tokenRepo:      tokenRepo,
		positionRepo:   positionRepo,
		eventExtractor: eventExtractor,
		logger:         config.Logger.With("component", "sparklend-position-tracker"),
	}

	return processor, nil
}

func (s *Service) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	go s.processLoop()

	s.logger.Info("sparklend position tracker started",
		"queue", s.config.QueueURL,
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

func (s *Service) processLoop() {
	ticker := time.NewTicker(s.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if err := s.processMessages(s.ctx); err != nil {
				s.logger.Error("error processing messages", "error", err)
			}
		}
	}
}

func (s *Service) processMessages(ctx context.Context) error {
	start := time.Now()
	defer func() {
		s.logger.Debug("processMessages completed", "duration", time.Since(start))
	}()

	result, err := s.sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(s.config.QueueURL),
		MaxNumberOfMessages: s.config.MaxMessages,
		WaitTimeSeconds:     s.config.WaitTimeSeconds,
		VisibilityTimeout:   30,
	})
	if err != nil {
		return fmt.Errorf("failed to receive messages: %w", err)
	}

	if len(result.Messages) == 0 {
		return nil
	}

	s.logger.Info("received messages", "count", len(result.Messages))

	var errs []error
	for _, msg := range result.Messages {
		if err := s.processMessage(ctx, msg); err != nil {
			s.logger.Error("failed to process message", "error", err)
			errs = append(errs, err)
			continue
		}

		_, deleteErr := s.sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
			QueueUrl:      aws.String(s.config.QueueURL),
			ReceiptHandle: msg.ReceiptHandle,
		})
		if deleteErr != nil {
			s.logger.Error("failed to delete message", "error", deleteErr)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (s *Service) processMessage(ctx context.Context, msg types.Message) error {
	if msg.Body == nil {
		return fmt.Errorf("message body is nil")
	}

	var event BlockEvent
	if err := json.Unmarshal([]byte(*msg.Body), &event); err != nil {
		return fmt.Errorf("failed to parse block event: %w", err)
	}

	return s.fetchAndProcessReceipts(ctx, event)
}

func (s *Service) fetchAndProcessReceipts(ctx context.Context, event BlockEvent) error {
	start := time.Now()
	defer func() {
		s.logger.Debug("fetchAndProcessReceipts completed",
			"block", event.BlockNumber,
			"duration", time.Since(start))
	}()

	cacheKey := event.CacheKey()
	receiptsJSON, err := s.redisClient.Get(ctx, cacheKey).Result()
	if errors.Is(err, redis.Nil) {
		s.logger.Warn("cache key expired or not found", "key", cacheKey, "block", event.BlockNumber)
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to fetch from Redis: %w", err)
	}

	var receipts []TransactionReceipt
	if err := json.Unmarshal([]byte(receiptsJSON), &receipts); err != nil {
		return fmt.Errorf("failed to unmarshal receipts: %w", err)
	}

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

func (s *Service) processReceipt(ctx context.Context, receipt TransactionReceipt, chainID, blockNumber int64, blockVersion int) error {
	var errs []error
	for _, log := range receipt.Logs {
		if !s.isRelevantEvent(log) {
			continue
		}

		if err := s.processEventLog(ctx, log, receipt.TransactionHash, chainID, blockNumber, blockVersion); err != nil {
			s.logger.Error("failed to process event", "error", err, "tx", receipt.TransactionHash)
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (s *Service) isRelevantEvent(log Log) bool {
	return s.eventExtractor.IsRelevantEvent(log)
}

func (s *Service) processEventLog(ctx context.Context, log Log, txHash string, chainID, blockNumber int64, blockVersion int) error {
	start := time.Now()
	defer func() {
		s.logger.Debug("processEventLog completed",
			"tx", txHash,
			"block", blockNumber,
			"duration", time.Since(start))
	}()

	eventData, err := s.eventExtractor.ExtractEventData(log)
	if err != nil {
		return fmt.Errorf("failed to extract event data: %w", err)
	}

	protocolAddress := common.HexToAddress(log.Address)

	s.logger.Info("Position event detected",
		"event_type", eventData.EventType,
		"user", eventData.User.Hex(),
		"protocol", protocolAddress.Hex(),
		"tx", txHash,
		"block", blockNumber)

	// Handle collateral toggle events separately - they only need to update collateral state
	if eventData.EventType == EventReserveUsedAsCollateralEnabled ||
		eventData.EventType == EventReserveUsedAsCollateralDisabled {
		return s.saveCollateralToggleEvent(ctx, eventData, protocolAddress, chainID, blockNumber, blockVersion)
	}

	// Handle liquidation - snapshot both borrower AND liquidator
	if eventData.EventType == EventLiquidationCall {
		return s.saveLiquidationEvent(ctx, eventData, protocolAddress, chainID, blockNumber, blockVersion)
	}

	// All other events (Borrow, Repay, Supply, Withdraw) - standard position snapshot
	return s.savePositionSnapshot(ctx, eventData, protocolAddress, chainID, blockNumber, blockVersion)
}

// saveCollateralToggleEvent handles ReserveUsedAsCollateralEnabled/Disabled events
func (s *Service) saveCollateralToggleEvent(ctx context.Context, eventData *PositionEventData, protocolAddress common.Address, chainID, blockNumber int64, blockVersion int) error {
	// Get token metadata for the reserve
	tokensToFetch := map[common.Address]bool{eventData.Reserve: true}
	metadataMap, err := s.blockchain.batchGetTokenMetadata(ctx, tokensToFetch)
	if err != nil {
		return fmt.Errorf("failed to get token metadata: %w", err)
	}

	metadata, ok := metadataMap[eventData.Reserve]
	if !ok || metadata.Decimals == 0 {
		return fmt.Errorf("token metadata not found for %s", eventData.Reserve.Hex())
	}

	// Get current collateral balance from chain
	collaterals, err := s.extractCollateralData(ctx, eventData.User, protocolAddress, blockNumber, eventData.TxHash)
	if err != nil {
		s.logger.Warn("failed to extract collateral data", "error", err, "tx", eventData.TxHash)
		collaterals = []CollateralData{}
	}

	// Find the balance for this specific reserve
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

		protocolID, err := s.protocolRepo.GetProtocolByAddress(ctx, chainID, protocolAddress.Hex())
		if err != nil {
			return fmt.Errorf("failed to get protocol: %w", err)
		}
		if protocolID == nil {
			return fmt.Errorf("protocol not found for address %s on chain %d", protocolAddress.Hex(), chainID)
		}

		tokenID, err := s.tokenRepo.GetOrCreateToken(ctx, tx, chainID, eventData.Reserve, metadata.Symbol, metadata.Decimals, blockNumber)
		if err != nil {
			return fmt.Errorf("failed to get token: %w", err)
		}

		decimalAdjustedBalance := s.convertToDecimalAdjusted(balance, metadata.Decimals)
		if err := s.positionRepo.SaveBorrowerCollateral(ctx, tx, userID, protocolID.ID, tokenID, blockNumber, blockVersion, decimalAdjustedBalance, string(eventData.EventType), eventData.TxHash, eventData.CollateralEnabled); err != nil {
			return fmt.Errorf("failed to save collateral toggle: %w", err)
		}

		return nil
	})
}

// saveLiquidationEvent handles LiquidationCall events - snapshots both borrower and liquidator
func (s *Service) saveLiquidationEvent(ctx context.Context, eventData *PositionEventData, protocolAddress common.Address, chainID, blockNumber int64, blockVersion int) error {
	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		// Snapshot the borrower being liquidated
		if err := s.snapshotUserPosition(ctx, tx, eventData.User, string(eventData.EventType), eventData.TxHash, protocolAddress, chainID, blockNumber, blockVersion); err != nil {
			return fmt.Errorf("failed to snapshot borrower: %w", err)
		}

		// Snapshot the liquidator
		if err := s.snapshotUserPosition(ctx, tx, eventData.Liquidator, string(eventData.EventType), eventData.TxHash, protocolAddress, chainID, blockNumber, blockVersion); err != nil {
			return fmt.Errorf("failed to snapshot liquidator: %w", err)
		}

		return nil
	})
}

// savePositionSnapshot handles standard position events (Borrow, Repay, Supply, Withdraw)
func (s *Service) savePositionSnapshot(ctx context.Context, eventData *PositionEventData, protocolAddress common.Address, chainID, blockNumber int64, blockVersion int) error {
	// Get metadata for the reserve token
	tokensToFetch := map[common.Address]bool{eventData.Reserve: true}
	metadataMap, err := s.blockchain.batchGetTokenMetadata(ctx, tokensToFetch)
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

	collaterals, err := s.extractCollateralData(ctx, eventData.User, protocolAddress, blockNumber, eventData.TxHash)
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

		protocolID, err := s.protocolRepo.GetProtocolByAddress(ctx, chainID, protocolAddress.Hex())
		if err != nil {
			return fmt.Errorf("failed to get protocol: %w", err)
		}
		if protocolID == nil {
			return fmt.Errorf("protocol not found for address %s on chain %d", protocolAddress.Hex(), chainID)
		}

		// For Borrow/Repay events, save borrower position
		if eventData.EventType == EventBorrow || eventData.EventType == EventRepay {
			tokenID, err := s.tokenRepo.GetOrCreateToken(ctx, tx, chainID, eventData.Reserve, tokenMetadata.Symbol, tokenMetadata.Decimals, blockNumber)
			if err != nil {
				return fmt.Errorf("failed to get token: %w", err)
			}
			decimalAdjustedAmount := s.convertToDecimalAdjusted(eventData.Amount, tokenMetadata.Decimals)
			if err := s.positionRepo.SaveBorrower(ctx, tx, userID, protocolID.ID, tokenID, blockNumber, blockVersion, decimalAdjustedAmount, string(eventData.EventType), eventData.TxHash); err != nil {
				return fmt.Errorf("failed to insert borrower: %w", err)
			}
		}

		// Build batch of collateral records
		records := make([]postgres.CollateralRecord, 0, len(collaterals))
		for _, col := range collaterals {
			tokenID, err := s.tokenRepo.GetOrCreateToken(ctx, tx, chainID, col.Asset, col.Symbol, col.Decimals, blockNumber)
			if err != nil {
				s.logger.Warn("failed to get collateral token", "token", col.Asset.Hex(), "error", err, "tx", eventData.TxHash)
				continue
			}

			records = append(records, postgres.CollateralRecord{
				UserID:            userID,
				ProtocolID:        protocolID.ID,
				TokenID:           tokenID,
				BlockNumber:       blockNumber,
				BlockVersion:      blockVersion,
				Amount:            s.convertToDecimalAdjusted(col.ActualBalance, col.Decimals),
				EventType:         string(eventData.EventType),
				TxHash:            eventData.TxHash,
				CollateralEnabled: col.CollateralEnabled,
			})
		}

		// Save all collateral positions in a single batch insert
		if err := s.positionRepo.SaveBorrowerCollaterals(ctx, tx, records); err != nil {
			return fmt.Errorf("failed to save collaterals: %w", err)
		}

		return nil
	})
}

// snapshotUserPosition snapshots a user's full position (used for liquidation events)
func (s *Service) snapshotUserPosition(ctx context.Context, tx pgx.Tx, user common.Address, eventType, txHash string, protocolAddress common.Address, chainID, blockNumber int64, blockVersion int) error {
	userID, err := s.userRepo.GetOrCreateUser(ctx, tx, entity.User{
		ChainID:        chainID,
		Address:        user,
		FirstSeenBlock: blockNumber,
	})
	if err != nil {
		return fmt.Errorf("failed to ensure user: %w", err)
	}

	protocolID, err := s.protocolRepo.GetProtocolByAddress(ctx, chainID, protocolAddress.Hex())
	if err != nil {
		return fmt.Errorf("failed to get protocol: %w", err)
	}
	if protocolID == nil {
		return fmt.Errorf("protocol not found for address %s on chain %d", protocolAddress.Hex(), chainID)
	}

	// Get user's reserve data from chain
	collaterals, err := s.extractCollateralData(ctx, user, protocolAddress, blockNumber, txHash)
	if err != nil {
		s.logger.Warn("failed to extract collateral data for user", "user", user.Hex(), "error", err)
		collaterals = []CollateralData{}
	}

	// Build batch of collateral records
	records := make([]postgres.CollateralRecord, 0, len(collaterals))
	for _, col := range collaterals {
		tokenID, err := s.tokenRepo.GetOrCreateToken(ctx, tx, chainID, col.Asset, col.Symbol, col.Decimals, blockNumber)
		if err != nil {
			s.logger.Warn("failed to get collateral token", "token", col.Asset.Hex(), "error", err, "tx", txHash)
			continue
		}

		records = append(records, postgres.CollateralRecord{
			UserID:            userID,
			ProtocolID:        protocolID.ID,
			TokenID:           tokenID,
			BlockNumber:       blockNumber,
			BlockVersion:      blockVersion,
			Amount:            s.convertToDecimalAdjusted(col.ActualBalance, col.Decimals),
			EventType:         eventType,
			TxHash:            txHash,
			CollateralEnabled: col.CollateralEnabled,
		})
	}

	// Save all collateral positions in a single batch insert
	if err := s.positionRepo.SaveBorrowerCollaterals(ctx, tx, records); err != nil {
		return fmt.Errorf("failed to save collaterals: %w", err)
	}

	s.logger.Info("Saved position snapshot", "user", user.Hex(), "tx", txHash, "block", blockNumber)
	return nil
}

func (s *Service) extractCollateralData(ctx context.Context, user common.Address, protocolAddress common.Address, blockNumber int64, txHash string) ([]CollateralData, error) {
	reserves, err := s.blockchain.getUserReservesData(ctx, user, blockNumber)
	if err != nil {
		s.logger.Warn("failed to get user reserves", "error", err, "tx", txHash, "block", blockNumber)
		return []CollateralData{}, nil
	}

	tokensToFetch := make(map[common.Address]bool)
	assetsForReserveData := make([]common.Address, 0)

	for _, r := range reserves {
		if r.ScaledATokenBalance.Cmp(big.NewInt(0)) > 0 && r.UsageAsCollateralEnabledOnUser {
			if r.UnderlyingAsset != (common.Address{}) {
				tokensToFetch[r.UnderlyingAsset] = true
				assetsForReserveData = append(assetsForReserveData, r.UnderlyingAsset)
			}
		}
	}

	if len(tokensToFetch) == 0 {
		return []CollateralData{}, nil
	}

	metadataMap, err := s.blockchain.batchGetTokenMetadata(ctx, tokensToFetch)
	if err != nil {
		s.logger.Warn("failed to batch get token metadata", "error", err, "tx", txHash, "block", blockNumber)
		return []CollateralData{}, fmt.Errorf("failed to get token metadata: %w", err)
	}

	reserveDataMap, err := s.blockchain.batchGetReserveData(ctx, protocolAddress, assetsForReserveData, blockNumber)
	if err != nil {
		s.logger.Warn("failed to batch get reserve data", "error", err, "tx", txHash, "block", blockNumber)
		reserveDataMap = make(map[common.Address]*big.Int)
	}

	var collaterals []CollateralData
	for _, r := range reserves {
		if r.ScaledATokenBalance.Cmp(big.NewInt(0)) > 0 && r.UsageAsCollateralEnabledOnUser {
			if r.UnderlyingAsset == (common.Address{}) {
				continue
			}

			metadata, ok := metadataMap[r.UnderlyingAsset]
			if !ok || metadata.Decimals == 0 {
				s.logger.Error("Failed to get collateral token metadata",
					"action", "skipped",
					"token", r.UnderlyingAsset.Hex(),
					"tx", txHash,
					"block", blockNumber,
					"user", user.Hex())
				continue
			}

			liquidityIndex, ok := reserveDataMap[r.UnderlyingAsset]
			var actualBalance *big.Int
			if ok && liquidityIndex != nil && liquidityIndex.Cmp(big.NewInt(0)) > 0 {
				actualBalance = new(big.Int).Mul(r.ScaledATokenBalance, liquidityIndex)
				actualBalance.Div(actualBalance, ray)
			} else {
				actualBalance = r.ScaledATokenBalance
			}

			collaterals = append(collaterals, CollateralData{
				Asset:             r.UnderlyingAsset,
				Decimals:          metadata.Decimals,
				Symbol:            metadata.Symbol,
				Name:              metadata.Name,
				ActualBalance:     actualBalance,
				CollateralEnabled: r.UsageAsCollateralEnabledOnUser,
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

func validateDependencies(
	sqsClient *sqs.Client,
	redisClient *redis.Client,
	ethClient *ethclient.Client,
	txManager *postgres.TxManager,
	userRepo *postgres.UserRepository,
	protocolRepo *postgres.ProtocolRepository,
	tokenRepo *postgres.TokenRepository,
	positionRepo *postgres.PositionRepository,
) error {
	if sqsClient == nil {
		return fmt.Errorf("sqsClient is required")
	}
	if redisClient == nil {
		return fmt.Errorf("redisClient is required")
	}
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
	return nil
}

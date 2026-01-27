package borrow_processor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

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

type BorrowEventData struct {
	Reserve    common.Address
	User       common.Address
	OnBehalfOf common.Address
	Amount     *big.Int
	TxHash     string
}

type CollateralData struct {
	Asset         common.Address
	Decimals      int
	Symbol        string
	Name          string
	ActualBalance *big.Int
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
	config          Config
	sqsClient       *sqs.Client
	redisClient     *redis.Client
	blockchain      *blockchainService
	txManager       *postgres.TxManager
	userRepo        *postgres.UserRepository
	protocolRepo    *postgres.ProtocolRepository
	tokenRepo       *postgres.TokenRepository
	positionRepo    *postgres.PositionRepository
	borrowABI       *abi.ABI
	eventSignatures map[common.Hash]*abi.Event

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

	processor := &Service{
		config:          config,
		sqsClient:       sqsClient,
		redisClient:     redisClient,
		blockchain:      blockchain,
		txManager:       txManager,
		userRepo:        userRepo,
		protocolRepo:    protocolRepo,
		tokenRepo:       tokenRepo,
		positionRepo:    positionRepo,
		eventSignatures: make(map[common.Hash]*abi.Event),
		logger:          config.Logger.With("component", "borrow-processor"),
	}

	if err := processor.loadBorrowABI(); err != nil {
		return nil, err
	}

	return processor, nil
}

func (s *Service) loadBorrowABI() error {
	borrowEventABI := `[{"anonymous":false,"inputs":[{"indexed":true,"name":"reserve","type":"address"},{"indexed":false,"name":"user","type":"address"},{"indexed":true,"name":"onBehalfOf","type":"address"},{"indexed":false,"name":"amount","type":"uint256"},{"indexed":false,"name":"interestRateMode","type":"uint8"},{"indexed":false,"name":"borrowRate","type":"uint256"},{"indexed":true,"name":"referralCode","type":"uint16"}],"name":"Borrow","type":"event"}]`
	parsedBorrowABI, err := abi.JSON(strings.NewReader(borrowEventABI))
	if err != nil {
		return fmt.Errorf("failed to parse borrow ABI: %w", err)
	}
	s.borrowABI = &parsedBorrowABI

	if borrowEvent, ok := s.borrowABI.Events["Borrow"]; ok {
		s.eventSignatures[borrowEvent.ID] = &borrowEvent
	} else {
		return fmt.Errorf("borrow event not found in ABI")
	}

	return nil
}

func (s *Service) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	go s.processLoop()

	s.logger.Info("borrow event processor started",
		"queue", s.config.QueueURL,
		"maxMessages", s.config.MaxMessages)
	return nil
}

func (s *Service) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.logger.Info("borrow event processor stopped")
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

func (s *Service) processReceipt(ctx context.Context, receipt TransactionReceipt, ChainID, blockNumber int64, blockVersion int) error {
	var errs []error
	for _, log := range receipt.Logs {
		if !s.isBorrowEvent(log) {
			continue
		}

		if err := s.processBorrowLog(ctx, log, receipt.TransactionHash, ChainID, blockNumber, blockVersion); err != nil {
			s.logger.Error("failed to process borrow event", "error", err, "tx", receipt.TransactionHash)
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (s *Service) isBorrowEvent(log Log) bool {
	if len(log.Topics) == 0 {
		return false
	}
	eventSig := common.HexToHash(log.Topics[0])
	_, ok := s.eventSignatures[eventSig]
	return ok
}

func (s *Service) extractBorrowEventData(log Log) (*BorrowEventData, error) {
	if len(log.Topics) == 0 {
		return nil, fmt.Errorf("no topics")
	}

	eventSig := common.HexToHash(log.Topics[0])
	borrowEvent, ok := s.eventSignatures[eventSig]
	if !ok {
		return nil, fmt.Errorf("not a borrow event")
	}

	eventData := make(map[string]interface{})

	var indexed abi.Arguments
	for _, arg := range borrowEvent.Inputs {
		if arg.Indexed {
			indexed = append(indexed, arg)
		}
	}

	if len(indexed) > 0 {
		topics := make([]common.Hash, 0, len(log.Topics)-1)
		for i := 1; i < len(log.Topics); i++ {
			topics = append(topics, common.HexToHash(log.Topics[i]))
		}
		if err := abi.ParseTopicsIntoMap(eventData, indexed, topics); err != nil {
			return nil, fmt.Errorf("failed to parse indexed params: %w", err)
		}
	}

	var nonIndexed abi.Arguments
	for _, arg := range borrowEvent.Inputs {
		if !arg.Indexed {
			nonIndexed = append(nonIndexed, arg)
		}
	}

	if len(nonIndexed) > 0 && len(log.Data) > 2 {
		data := common.FromHex(log.Data)
		if err := nonIndexed.UnpackIntoMap(eventData, data); err != nil {
			return nil, fmt.Errorf("failed to parse non-indexed params: %w", err)
		}
	}

	return &BorrowEventData{
		Reserve:    eventData["reserve"].(common.Address),
		User:       eventData["user"].(common.Address),
		OnBehalfOf: eventData["onBehalfOf"].(common.Address),
		Amount:     eventData["amount"].(*big.Int),
		TxHash:     log.TransactionHash,
	}, nil
}

func (s *Service) processBorrowLog(ctx context.Context, log Log, txHash string, ChainID, blockNumber int64, blockVersion int) error {
	start := time.Now()
	defer func() {
		s.logger.Debug("processBorrowLog completed",
			"tx", txHash,
			"block", blockNumber,
			"duration", time.Since(start))
	}()

	borrowEvent, err := s.extractBorrowEventData(log)
	if err != nil {
		return fmt.Errorf("failed to extract borrow event: %w", err)
	}

	protocolAddress := common.HexToAddress(log.Address)

	s.logger.Info("Borrow event detected",
		"user", borrowEvent.OnBehalfOf.Hex(),
		"protocol", protocolAddress.Hex(),
		"reserve", borrowEvent.Reserve.Hex(),
		"amount", borrowEvent.Amount.String(),
		"tx", txHash,
		"block_version", blockVersion,
		"block", blockNumber)

	tokensToFetch := make(map[common.Address]bool)
	tokensToFetch[borrowEvent.Reserve] = true

	metadataMap, err := s.blockchain.batchGetTokenMetadata(ctx, tokensToFetch)
	if err != nil {
		s.logger.Warn("failed to batch get token metadata", "error", err, "tx", txHash, "block", blockNumber)
		metadataMap = make(map[common.Address]TokenMetadata)
	}

	borrowTokenMetadata, ok := metadataMap[borrowEvent.Reserve]
	if !ok || borrowTokenMetadata.Decimals == 0 {
		s.logger.Error("SKIPPING EVENT: Failed to get borrow token decimals",
			"token", borrowEvent.Reserve.Hex(),
			"tx", txHash,
			"block", blockNumber,
			"user", borrowEvent.OnBehalfOf.Hex(),
			"protocol", protocolAddress.Hex())
		return fmt.Errorf("borrow token decimals not found for %s", borrowEvent.Reserve.Hex())
	}

	collaterals, err := s.extractCollateralData(ctx, borrowEvent.OnBehalfOf, protocolAddress, ChainID, blockNumber, txHash)
	if err != nil {
		s.logger.Warn("failed to extract collateral data", "error", err, "tx", txHash, "block", blockNumber)
		collaterals = []CollateralData{}
	}

	return s.saveBorrowEvent(ctx, borrowEvent, collaterals, borrowTokenMetadata, protocolAddress, ChainID, blockNumber, blockVersion)
}

func (s *Service) extractCollateralData(ctx context.Context, user common.Address, protocolAddress common.Address, ChainID, blockNumber int64, txHash string) ([]CollateralData, error) {
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
				s.logger.Error("SKIPPING COLLATERAL: Failed to get collateral token metadata",
					"token", r.UnderlyingAsset.Hex(),
					"tx", txHash,
					"block", blockNumber,
					"user", user.Hex())
				continue
			}

			liquidityIndex, ok := reserveDataMap[r.UnderlyingAsset]
			var actualBalance *big.Int
			if ok && liquidityIndex != nil && liquidityIndex.Cmp(big.NewInt(0)) > 0 {
				RAY := new(big.Int).Exp(big.NewInt(10), big.NewInt(27), nil)
				actualBalance = new(big.Int).Mul(r.ScaledATokenBalance, liquidityIndex)
				actualBalance.Div(actualBalance, RAY)
			} else {
				actualBalance = r.ScaledATokenBalance
			}

			collaterals = append(collaterals, CollateralData{
				Asset:         r.UnderlyingAsset,
				Decimals:      metadata.Decimals,
				Symbol:        metadata.Symbol,
				Name:          metadata.Name,
				ActualBalance: actualBalance,
			})
		}
	}

	return collaterals, nil
}

func (s *Service) saveBorrowEvent(ctx context.Context, borrowEvent *BorrowEventData, collaterals []CollateralData, borrowTokenMetadata TokenMetadata, protocolAddress common.Address, chainID, blockNumber int64, blockVersion int) error {
	err := s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		userID, err := s.userRepo.GetOrCreateUserWithTX(ctx, tx, entity.User{
			ChainID:        chainID,
			Address:        borrowEvent.OnBehalfOf,
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

		borrowTokenID, err := s.tokenRepo.GetOrCreateTokenWithTX(ctx, tx, chainID, borrowEvent.Reserve, borrowTokenMetadata.Symbol, borrowTokenMetadata.Decimals, blockNumber)
		if err != nil {
			return fmt.Errorf("failed to get borrow token: %w", err)
		}
		decimalAdjustedAmount := s.convertToDecimalAdjusted(borrowEvent.Amount, borrowTokenMetadata.Decimals)
		if err := s.positionRepo.SaveBorrowerWithTX(ctx, tx, userID, protocolID.ID, borrowTokenID, blockNumber, blockVersion, decimalAdjustedAmount); err != nil {
			return fmt.Errorf("failed to insert borrower: %w", err)
		}

		for _, col := range collaterals {
			tokenID, err := s.tokenRepo.GetOrCreateTokenWithTX(ctx, tx, chainID, col.Asset, col.Symbol, col.Decimals, blockNumber)
			if err != nil {
				s.logger.Warn("failed to get collateral token", "token", col.Asset.Hex(), "error", err, "tx", borrowEvent.TxHash)
				continue
			}

			decimalAdjustedCollateral := s.convertToDecimalAdjusted(col.ActualBalance, col.Decimals)
			if err := s.positionRepo.SaveBorrowerCollateralWithTX(ctx, tx, userID, protocolID.ID, tokenID, blockNumber, blockVersion, decimalAdjustedCollateral); err != nil {
				s.logger.Warn("failed to insert collateral", "error", err, "tx", borrowEvent.TxHash)
			}
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to save borrow event: %w", err)
	}

	s.logger.Info("Saved to database", "user", borrowEvent.OnBehalfOf.Hex(), "tx", borrowEvent.TxHash, "block", blockNumber)
	return nil
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

package borrow_processor

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/multicall"
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
	config       Config
	sqsClient    *sqs.Client
	redisClient  *redis.Client
	ethClient    *ethclient.Client
	txManager    *postgres.TxManager
	userRepo     *postgres.UserRepository
	protocolRepo *postgres.ProtocolRepository
	tokenRepo    *postgres.TokenRepository
	positionRepo *postgres.PositionRepository

	blockchainServices map[common.Address]*blockchainService
	multicallClient    multicall.Multicaller
	erc20ABI           *abi.ABI

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

	mc, err := multicall.NewClient(ethClient, blockchain.Multicall3)
	if err != nil {
		return nil, fmt.Errorf("failed to create multicall client: %w", err)
	}

	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		return nil, fmt.Errorf("failed to load ERC20 ABI: %w", err)
	}

	processor := &Service{
		config:             config,
		sqsClient:          sqsClient,
		redisClient:        redisClient,
		ethClient:          ethClient,
		txManager:          txManager,
		userRepo:           userRepo,
		protocolRepo:       protocolRepo,
		tokenRepo:          tokenRepo,
		positionRepo:       positionRepo,
		blockchainServices: make(map[common.Address]*blockchainService),
		multicallClient:    mc,
		erc20ABI:           erc20ABI,
		eventSignatures:    make(map[common.Hash]*abi.Event),
		logger:             config.Logger.With("component", "borrow-processor"),
	}

	if err := processor.loadBorrowABI(); err != nil {
		return nil, err
	}

	return processor, nil
}

func (s *Service) getOrCreateBlockchainService(protocolAddress common.Address) (*blockchainService, error) {
	if svc, exists := s.blockchainServices[protocolAddress]; exists {
		return svc, nil
	}

	protocolConfig, exists := blockchain.GetProtocolConfig(protocolAddress)
	if !exists {
		return nil, fmt.Errorf("unknown protocol: %s", protocolAddress.Hex())
	}

	svc, err := newBlockchainService(
		s.ethClient,
		s.multicallClient,
		s.erc20ABI,
		protocolConfig.UIPoolDataProvider,
		protocolConfig.PoolDataProvider,
		protocolConfig.PoolAddressesProvider,
		protocolConfig.UseAaveABI, // Pass the flag
		s.logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create blockchain service for %s: %w", protocolConfig.Name, err)
	}

	s.blockchainServices[protocolAddress] = svc
	s.logger.Info("created blockchain service",
		"protocol", protocolConfig.Name,
		"address", protocolAddress.Hex(),
		"abiType", map[bool]string{true: "Aave", false: "Sparklend"}[protocolConfig.UseAaveABI])

	return svc, nil
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

	data := []byte(receiptsJSON)
	if len(data) > 2 && data[0] == 0x1f && data[1] == 0x8b {
		gr, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("failed to create gzip reader: %w", err)
		}

		decompressed, err := io.ReadAll(gr)
		closeErr := gr.Close()

		if err != nil {
			return fmt.Errorf("failed to decompress: %w", err)
		}

		if closeErr != nil {
			return fmt.Errorf("failed to close gzip reader: %w", closeErr)
		}

		data = decompressed
	}

	var receipts []TransactionReceipt
	if err := json.Unmarshal(data, &receipts); err != nil {
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
		if !s.isBorrowEvent(log) {
			continue
		}

		if err := s.processBorrowLog(ctx, log, receipt.TransactionHash, chainID, blockNumber, blockVersion); err != nil {
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

func (s *Service) processBorrowLog(ctx context.Context, log Log, txHash string, chainID, blockNumber int64, blockVersion int) error {
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

	blockchainSvc, err := s.getOrCreateBlockchainService(protocolAddress)
	if err != nil {
		s.logger.Error("unsupported protocol", "protocol", protocolAddress.Hex(), "error", err)
		return fmt.Errorf("unsupported protocol %s: %w", protocolAddress.Hex(), err)
	}

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

	metadataMap, err := blockchainSvc.batchGetTokenMetadata(ctx, tokensToFetch)
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

	collaterals, err := s.extractCollateralData(ctx, blockchainSvc, borrowEvent.OnBehalfOf, protocolAddress, chainID, blockNumber, txHash)
	if err != nil {
		s.logger.Warn("failed to extract collateral data", "error", err, "tx", txHash, "block", blockNumber)
		collaterals = []CollateralData{}
	}

	return s.saveBorrowEvent(ctx, borrowEvent, collaterals, borrowTokenMetadata, protocolAddress, chainID, blockNumber, blockVersion)
}

func (s *Service) extractCollateralData(ctx context.Context, blockchainSvc *blockchainService, user common.Address, protocolAddress common.Address, chainID, blockNumber int64, txHash string) ([]CollateralData, error) {
	// Step 1: Get user's positions (scaled values, just for asset list)
	reserves, err := blockchainSvc.getUserReservesData(ctx, user, blockNumber)
	if err != nil {
		s.logger.Warn("failed to get user reserves", "error", err, "tx", txHash, "block", blockNumber)
		return []CollateralData{}, nil
	}

	// Step 2: Filter to get only collateral assets
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

	// Step 3: Get token metadata for collateral assets
	tokensToFetch := make(map[common.Address]bool)
	for _, asset := range collateralAssets {
		tokensToFetch[asset] = true
	}

	metadataMap, err := blockchainSvc.batchGetTokenMetadata(ctx, tokensToFetch)
	if err != nil {
		s.logger.Warn("failed to batch get token metadata", "error", err, "tx", txHash, "block", blockNumber)
		return []CollateralData{}, fmt.Errorf("failed to get token metadata: %w", err)
	}

	// Step 4: Get actual balances from PoolDataProvider (unratioed)
	actualDataMap, err := blockchainSvc.batchGetUserReserveData(ctx, collateralAssets, user, blockNumber)
	if err != nil {
		s.logger.Warn("failed to batch get user reserve data", "error", err, "tx", txHash, "block", blockNumber)
		return []CollateralData{}, fmt.Errorf("failed to get user reserve data: %w", err)
	}

	// Step 5: Build collateral data
	var collaterals []CollateralData
	for _, asset := range collateralAssets {
		metadata, ok := metadataMap[asset]
		if !ok || metadata.Decimals == 0 {
			s.logger.Error("SKIPPING COLLATERAL: Failed to get collateral token metadata",
				"token", asset.Hex(),
				"tx", txHash,
				"block", blockNumber,
				"user", user.Hex())
			continue
		}

		actualData, ok := actualDataMap[asset]
		if !ok {
			s.logger.Error("SKIPPING COLLATERAL: Failed to get actual balance",
				"token", asset.Hex(),
				"tx", txHash,
				"block", blockNumber,
				"user", user.Hex())
			continue
		}

		// Only include if still has collateral enabled and balance > 0
		if actualData.UsageAsCollateralEnabled && actualData.CurrentATokenBalance.Cmp(big.NewInt(0)) > 0 {
			collaterals = append(collaterals, CollateralData{
				Asset:         asset,
				Decimals:      metadata.Decimals,
				Symbol:        metadata.Symbol,
				Name:          metadata.Name,
				ActualBalance: actualData.CurrentATokenBalance,
			})
		}
	}

	return collaterals, nil
}

func (s *Service) saveBorrowEvent(ctx context.Context, borrowEvent *BorrowEventData, collaterals []CollateralData, borrowTokenMetadata TokenMetadata, protocolAddress common.Address, chainID, blockNumber int64, blockVersion int) error {
	err := s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		userID, err := s.userRepo.GetOrCreateUser(ctx, tx, entity.User{
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

		borrowTokenID, err := s.tokenRepo.GetOrCreateToken(ctx, tx, chainID, borrowEvent.Reserve, borrowTokenMetadata.Symbol, borrowTokenMetadata.Decimals, blockNumber)
		if err != nil {
			return fmt.Errorf("failed to get borrow token: %w", err)
		}

		decimalAdjusted := blockchain.ConvertToDecimalAdjusted(borrowEvent.Amount, borrowTokenMetadata.Decimals)
		decimalAdjustedStr := decimalAdjusted.Text('f', -1)

		if err := s.positionRepo.SaveBorrower(ctx, tx, userID, protocolID.ID, borrowTokenID, blockNumber, blockVersion, decimalAdjustedStr); err != nil {
			return fmt.Errorf("failed to insert borrower: %w", err)
		}

		for _, col := range collaterals {
			tokenID, err := s.tokenRepo.GetOrCreateToken(ctx, tx, chainID, col.Asset, col.Symbol, col.Decimals, blockNumber)
			if err != nil {
				s.logger.Warn("failed to get collateral token", "token", col.Asset.Hex(), "error", err, "tx", borrowEvent.TxHash)
				continue
			}

			decimalAdjusted := blockchain.ConvertToDecimalAdjusted(col.ActualBalance, col.Decimals)
			decimalAdjustedStr := decimalAdjusted.Text('f', -1)

			if err := s.positionRepo.SaveBorrowerCollateral(ctx, tx, userID, protocolID.ID, tokenID, blockNumber, blockVersion, decimalAdjustedStr); err != nil {
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

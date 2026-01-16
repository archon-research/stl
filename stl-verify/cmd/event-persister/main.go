package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
)

type ReceiptMetadata struct {
	ChainId     int64  `json:"chainId"`
	BlockNumber int64  `json:"blockNumber"`
	Version     int    `json:"version"`
	BlockHash   string `json:"blockHash"`
	ReceivedAt  string `json:"receivedAt"`
	CacheKey    string `json:"cacheKey"`
}

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

type SNSMessage struct {
	Type      string `json:"Type"`
	MessageId string `json:"MessageId"`
	TopicArn  string `json:"TopicArn"`
	Message   string `json:"Message"`
	Timestamp string `json:"Timestamp"`
}

type UserReserveData struct {
	UnderlyingAsset                 common.Address
	ScaledATokenBalance             *big.Int
	UsageAsCollateralEnabledOnUser  bool
	StableBorrowRate                *big.Int
	ScaledVariableDebt              *big.Int
	PrincipalStableDebt             *big.Int
	StableBorrowLastUpdateTimestamp *big.Int
}

type BorrowEventProcessor struct {
	sqsClient             *sqs.Client
	redisClient           *redis.Client
	ethClient             *ethclient.Client
	lendingRepo           *postgres.LendingRepository
	queueURL              string
	maxMessages           int32
	waitTimeSeconds       int32
	logger                *slog.Logger
	verbose               bool
	borrowABI             *abi.ABI
	getUserReservesABI    *abi.ABI
	getReserveDataABI     *abi.ABI
	erc20DecimalsABI      *abi.ABI
	uiPoolDataProvider    common.Address
	poolAddressesProvider common.Address
	aavePool              common.Address
	eventSignatures       map[common.Hash]*abi.Event
}

type collateralData struct {
	asset         common.Address
	decimals      int
	actualBalance *big.Int
}

func main() {
	queueURL := flag.String("queue", "", "SQS Queue URL")
	redisAddr := flag.String("redis", "", "Redis address")
	dbURL := flag.String("db", "", "PostgreSQL connection URL")
	maxMessages := flag.Int("max", 10, "Max messages per poll")
	waitTime := flag.Int("wait", 20, "Wait time in seconds (long polling)")
	verbose := flag.Bool("verbose", false, "Enable verbose logging")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	if *verbose {
		logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))
	}

	if *queueURL == "" {
		*queueURL = os.Getenv("AWS_SQS_QUEUE_RECEIPTS")
	}
	if *queueURL == "" {
		logger.Error("queue URL not provided")
		os.Exit(1)
	}

	if *dbURL == "" {
		*dbURL = os.Getenv("DATABASE_URL")
	}
	if *dbURL == "" {
		logger.Error("database URL not provided (use -db flag or DATABASE_URL env var)")
		os.Exit(1)
	}

	alchemyAPIKey := os.Getenv("ALCHEMY_API_KEY")
	if alchemyAPIKey == "" {
		logger.Error("ALCHEMY_API_KEY environment variable is required")
		os.Exit(1)
	}
	alchemyHTTPURL := getEnv("ALCHEMY_HTTP_URL", "https://eth-mainnet.g.alchemy.com/v2")
	fullAlchemyURL := fmt.Sprintf("%s/%s", alchemyHTTPURL, alchemyAPIKey)

	if *redisAddr == "" {
		*redisAddr = os.Getenv("REDIS_ADDR")
	}
	if *redisAddr == "" {
		logger.Error("Redis address not provided (use -redis flag or REDIS_ADDR env var)")
		os.Exit(1)
	}

	chainIDStr := os.Getenv("CHAIN_ID")
	if chainIDStr == "" {
		chainIDStr = "1"
	}
	var chainID int64 = 1
	_, _ = fmt.Sscanf(chainIDStr, "%d", &chainID)

	logger.Info("starting borrow event processor",
		"queue", *queueURL,
		"redis", *redisAddr,
		"chainID", chainID)

	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(getEnv("AWS_REGION", "us-east-1")),
		config.WithCredentialsProvider(aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     getEnv("AWS_ACCESS_KEY_ID", "test"),
				SecretAccessKey: getEnv("AWS_SECRET_ACCESS_KEY", "test"),
				Source:          "Static",
			}, nil
		})),
	)
	if err != nil {
		logger.Error("failed to load AWS config", "error", err)
		os.Exit(1)
	}

	sqsClient := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		if endpoint := os.Getenv("AWS_SQS_ENDPOINT"); endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	})

	redisClient := redis.NewClient(&redis.Options{
		Addr:     *redisAddr,
		Password: getEnv("REDIS_PASSWORD", ""),
		DB:       0,
	})
	if err := redisClient.Ping(ctx).Err(); err != nil {
		logger.Error("failed to connect to Redis", "error", err)
		os.Exit(1)
	}
	logger.Info("connected to Redis")

	ethClient, err := ethclient.Dial(fullAlchemyURL)
	if err != nil {
		logger.Error("failed to connect to Ethereum node", "error", err)
		os.Exit(1)
	}
	logger.Info("connected to Ethereum node")

	db, err := sql.Open("pgx", *dbURL)
	if err != nil {
		logger.Error("failed to open database", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		logger.Error("failed to ping database", "error", err)
		os.Exit(1)
	}
	logger.Info("connected to PostgreSQL")

	lendingRepo := postgres.NewLendingRepository(db, chainID, logger)

	processor := &BorrowEventProcessor{
		sqsClient:             sqsClient,
		redisClient:           redisClient,
		ethClient:             ethClient,
		lendingRepo:           lendingRepo,
		queueURL:              *queueURL,
		maxMessages:           int32(*maxMessages),
		waitTimeSeconds:       int32(*waitTime),
		logger:                logger,
		verbose:               *verbose,
		uiPoolDataProvider:    common.HexToAddress("0x56b7A1012765C285afAC8b8F25C69Bf10ccfE978"),
		poolAddressesProvider: common.HexToAddress("0x2f39d218133AFaB8F2B819B1066c7E434Ad94E9e"),
		aavePool:              common.HexToAddress("0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"),
	}

	if err := processor.loadABIs(); err != nil {
		logger.Error("failed to load ABIs", "error", err)
		os.Exit(1)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		for {
			if err := processor.processMessages(ctx); err != nil {
				logger.Error("error processing messages", "error", err)
			}
		}
	}()

	<-sigChan
	logger.Info("shutting down gracefully")
}

func (p *BorrowEventProcessor) loadABIs() error {
	borrowEventABI := `[
       {
          "anonymous": false,
          "inputs": [
             {"indexed": true, "name": "reserve", "type": "address"},
             {"indexed": false, "name": "user", "type": "address"},
             {"indexed": true, "name": "onBehalfOf", "type": "address"},
             {"indexed": false, "name": "amount", "type": "uint256"},
             {"indexed": false, "name": "interestRateMode", "type": "uint8"},
             {"indexed": false, "name": "borrowRate", "type": "uint256"},
             {"indexed": true, "name": "referralCode", "type": "uint16"}
          ],
          "name": "Borrow",
          "type": "event"
       }
    ]`

	parsedBorrowABI, err := abi.JSON(strings.NewReader(borrowEventABI))
	if err != nil {
		return fmt.Errorf("failed to parse borrow ABI: %w", err)
	}
	p.borrowABI = &parsedBorrowABI

	p.eventSignatures = make(map[common.Hash]*abi.Event)

	if borrowEvent, ok := p.borrowABI.Events["Borrow"]; ok {
		p.eventSignatures[borrowEvent.ID] = &borrowEvent
	} else {
		return fmt.Errorf("Borrow event not found in ABI")
	}

	getUserReservesABI := `[
       {
          "inputs": [
             {"name": "provider", "type": "address"},
             {"name": "user", "type": "address"}
          ],
          "name": "getUserReservesData",
          "outputs": [
             {
                "components": [
                   {"name": "underlyingAsset", "type": "address"},
                   {"name": "scaledATokenBalance", "type": "uint256"},
                   {"name": "usageAsCollateralEnabledOnUser", "type": "bool"},
                   {"name": "stableBorrowRate", "type": "uint256"},
                   {"name": "scaledVariableDebt", "type": "uint256"},
                   {"name": "principalStableDebt", "type": "uint256"},
                   {"name": "stableBorrowLastUpdateTimestamp", "type": "uint256"}
                ],
                "name": "",
                "type": "tuple[]"
             },
             {"name": "", "type": "uint8"}
          ],
          "stateMutability": "view",
          "type": "function"
       }
    ]`

	parsedGetUserReservesABI, err := abi.JSON(strings.NewReader(getUserReservesABI))
	if err != nil {
		return fmt.Errorf("failed to parse getUserReserves ABI: %w", err)
	}
	p.getUserReservesABI = &parsedGetUserReservesABI

	getReserveDataABI := `[
       {
          "inputs": [{"name": "asset", "type": "address"}],
          "name": "getReserveData",
          "outputs": [
             {
                "components": [
                   {"name": "configuration", "type": "uint256"},
                   {"name": "liquidityIndex", "type": "uint128"},
                   {"name": "currentLiquidityRate", "type": "uint128"},
                   {"name": "variableBorrowIndex", "type": "uint128"},
                   {"name": "currentVariableBorrowRate", "type": "uint128"},
                   {"name": "currentStableBorrowRate", "type": "uint128"},
                   {"name": "lastUpdateTimestamp", "type": "uint40"},
                   {"name": "id", "type": "uint16"},
                   {"name": "aTokenAddress", "type": "address"},
                   {"name": "stableDebtTokenAddress", "type": "address"},
                   {"name": "variableDebtTokenAddress", "type": "address"},
                   {"name": "interestRateStrategyAddress", "type": "address"},
                   {"name": "accruedToTreasury", "type": "uint128"},
                   {"name": "unbacked", "type": "uint128"},
                   {"name": "isolationModeTotalDebt", "type": "uint128"}
                ],
                "name": "",
                "type": "tuple"
             }
          ],
          "stateMutability": "view",
          "type": "function"
       }
    ]`

	parsedGetReserveDataABI, err := abi.JSON(strings.NewReader(getReserveDataABI))
	if err != nil {
		return fmt.Errorf("failed to parse getReserveData ABI: %w", err)
	}
	p.getReserveDataABI = &parsedGetReserveDataABI

	erc20DecimalsABI := `[{
       "inputs": [],
       "name": "decimals",
       "outputs": [{"name": "", "type": "uint8"}],
       "stateMutability": "view",
       "type": "function"
    }]`

	parsedERC20DecimalsABI, err := abi.JSON(strings.NewReader(erc20DecimalsABI))
	if err != nil {
		return fmt.Errorf("failed to parse ERC20 decimals ABI: %w", err)
	}
	p.erc20DecimalsABI = &parsedERC20DecimalsABI

	p.logger.Info("loaded ABIs")
	return nil
}

func (p *BorrowEventProcessor) processMessages(ctx context.Context) error {
	p.logger.Debug("polling SQS queue")

	result, err := p.sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(p.queueURL),
		MaxNumberOfMessages: p.maxMessages,
		WaitTimeSeconds:     p.waitTimeSeconds,
		VisibilityTimeout:   30,
	})
	if err != nil {
		return fmt.Errorf("failed to receive messages: %w", err)
	}

	if len(result.Messages) == 0 {
		p.logger.Debug("no messages available")
		return nil
	}

	p.logger.Info("received messages", "count", len(result.Messages))

	for _, msg := range result.Messages {
		if err := p.processMessage(ctx, msg); err != nil {
			p.logger.Error("failed to process message", "error", err)
			continue
		}

		_, deleteErr := p.sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
			QueueUrl:      aws.String(p.queueURL),
			ReceiptHandle: msg.ReceiptHandle,
		})
		if deleteErr != nil {
			p.logger.Error("failed to delete message", "error", deleteErr)
		}
	}

	return nil
}

func (p *BorrowEventProcessor) processMessage(ctx context.Context, msg types.Message) error {
	if msg.Body == nil {
		return fmt.Errorf("message body is nil")
	}

	var snsMsg SNSMessage
	if err := json.Unmarshal([]byte(*msg.Body), &snsMsg); err != nil {
		return fmt.Errorf("failed to parse SNS message: %w", err)
	}

	var metadata ReceiptMetadata
	if err := json.Unmarshal([]byte(snsMsg.Message), &metadata); err != nil {
		return fmt.Errorf("failed to parse metadata: %w", err)
	}

	return p.fetchAndProcessReceipts(ctx, metadata)
}

func (p *BorrowEventProcessor) fetchAndProcessReceipts(ctx context.Context, metadata ReceiptMetadata) error {
	receiptsJSON, err := p.redisClient.Get(ctx, metadata.CacheKey).Result()
	if errors.Is(err, redis.Nil) {
		p.logger.Warn("cache key expired or not found", "key", metadata.CacheKey)
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to fetch from Redis: %w", err)
	}

	var receipts []TransactionReceipt
	if err := json.Unmarshal([]byte(receiptsJSON), &receipts); err != nil {
		return fmt.Errorf("failed to unmarshal receipts: %w", err)
	}

	for _, receipt := range receipts {
		if err := p.processReceipt(ctx, receipt, metadata.BlockNumber, metadata.Version); err != nil {
			p.logger.Error("failed to process receipt", "error", err, "tx", receipt.TransactionHash)
		}
	}

	return nil
}

func (p *BorrowEventProcessor) processReceipt(ctx context.Context, receipt TransactionReceipt, blockNumber int64, blockVersion int) error {
	for _, log := range receipt.Logs {
		if !p.isBorrowEvent(log) {
			continue
		}

		if err := p.processBorrowLog(ctx, log, blockNumber, blockVersion); err != nil {
			p.logger.Error("failed to process borrow event", "error", err, "tx", receipt.TransactionHash)
		}
	}
	return nil
}

func (p *BorrowEventProcessor) processBorrowLog(ctx context.Context, log Log, blockNumber int64, blockVersion int) error {
	if len(log.Topics) == 0 {
		return fmt.Errorf("no topics")
	}

	eventSig := common.HexToHash(log.Topics[0])
	borrowEvent, ok := p.eventSignatures[eventSig]
	if !ok {
		return fmt.Errorf("not a borrow event")
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
			return fmt.Errorf("failed to parse indexed params: %w", err)
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
			return fmt.Errorf("failed to parse non-indexed params: %w", err)
		}
	}

	protocolAddress := common.HexToAddress(log.Address)

	return p.saveBorrowEvent(ctx, protocolAddress, eventData, blockNumber, blockVersion)
}

func (p *BorrowEventProcessor) saveBorrowEvent(ctx context.Context, protocolAddress common.Address, eventData map[string]interface{}, blockNumber int64, blockVersion int) error {
	reserve := eventData["reserve"].(common.Address)
	onBehalfOf := eventData["onBehalfOf"].(common.Address)
	amount := eventData["amount"].(*big.Int)

	p.logger.Info("Borrow event detected",
		"user", onBehalfOf.Hex(),
		"protocol", protocolAddress.Hex(),
		"reserve", reserve.Hex(),
		"amount", amount.String(),
		"block_version", blockVersion,
		"block", blockNumber)

	borrowTokenDecimals, err := p.getTokenDecimals(ctx, reserve)
	if err != nil {
		p.logger.Warn("failed to get borrow token decimals, using default 18", "token", reserve.Hex(), "error", err)
		borrowTokenDecimals = 18
	}

	reserves, err := p.getUserReservesData(ctx, onBehalfOf, blockNumber)
	if err != nil {
		p.logger.Warn("failed to get user reserves", "error", err)
		reserves = []UserReserveData{}
	}

	var collaterals []collateralData
	for _, reserve := range reserves {
		if reserve.ScaledATokenBalance.Cmp(big.NewInt(0)) > 0 && reserve.UsageAsCollateralEnabledOnUser {
			if reserve.UnderlyingAsset == (common.Address{}) {
				p.logger.Debug("skipping zero address collateral")
				continue
			}

			decimals, err := p.getTokenDecimals(ctx, reserve.UnderlyingAsset)
			if err != nil {
				p.logger.Warn("failed to get collateral token decimals, using default 18", "token", reserve.UnderlyingAsset.Hex(), "error", err)
				decimals = 18
			}

			actualBalance, err := p.getActualBalance(ctx, protocolAddress, reserve.UnderlyingAsset, reserve.ScaledATokenBalance, blockNumber)
			if err != nil {
				p.logger.Warn("failed to calculate actual balance, using scaled", "token", reserve.UnderlyingAsset.Hex(), "error", err)
				actualBalance = reserve.ScaledATokenBalance
			}

			collaterals = append(collaterals, collateralData{
				asset:         reserve.UnderlyingAsset,
				decimals:      decimals,
				actualBalance: actualBalance,
			})
		}
	}

	tx, err := p.lendingRepo.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			p.logger.Warn("failed to rollback transaction", "error", rbErr)
		}
	}()

	userID, err := p.lendingRepo.EnsureUser(ctx, tx, onBehalfOf)
	if err != nil {
		return fmt.Errorf("failed to ensure user: %w", err)
	}

	protocolID, err := p.lendingRepo.GetOrCreateProtocol(ctx, tx, protocolAddress)
	if err != nil {
		return fmt.Errorf("failed to get protocol: %w", err)
	}

	borrowTokenID, err := p.lendingRepo.GetOrCreateToken(ctx, tx, reserve, borrowTokenDecimals)
	if err != nil {
		return fmt.Errorf("failed to get borrow token: %w", err)
	}

	decimalAdjustedAmount := p.convertToDecimalAdjusted(amount, borrowTokenDecimals)
	p.logger.Debug("converted borrow amount", "raw", amount.String(), "decimals", borrowTokenDecimals, "adjusted", decimalAdjustedAmount)

	if err := p.lendingRepo.SaveBorrower(ctx, tx, userID, protocolID, borrowTokenID, blockNumber, blockVersion, decimalAdjustedAmount); err != nil {
		return fmt.Errorf("failed to insert borrower: %w", err)
	}

	for _, col := range collaterals {
		tokenID, err := p.lendingRepo.GetOrCreateToken(ctx, tx, col.asset, col.decimals)
		if err != nil {
			p.logger.Warn("failed to get collateral token", "token", col.asset.Hex(), "error", err)
			continue
		}

		decimalAdjustedCollateral := p.convertToDecimalAdjusted(col.actualBalance, col.decimals)
		p.logger.Debug("converted collateral amount", "raw", col.actualBalance.String(), "decimals", col.decimals, "adjusted", decimalAdjustedCollateral)

		if err := p.lendingRepo.SaveBorrowerCollateral(ctx, tx, userID, protocolID, tokenID, blockNumber, blockVersion, decimalAdjustedCollateral); err != nil {
			p.logger.Warn("failed to insert collateral", "error", err)
		} else {
			p.logger.Debug("saved collateral", "token", col.asset.Hex(), "decimals", col.decimals, "adjusted_amount", decimalAdjustedCollateral)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	p.logger.Info("Saved to database", "user", onBehalfOf.Hex(), "block", blockNumber)
	return nil
}

func (p *BorrowEventProcessor) getUserReservesData(ctx context.Context, user common.Address, blockNumber int64) ([]UserReserveData, error) {
	data, err := p.getUserReservesABI.Pack("getUserReservesData", p.poolAddressesProvider, user)
	if err != nil {
		return nil, fmt.Errorf("failed to pack function call: %w", err)
	}

	msg := ethereum.CallMsg{
		To:   &p.uiPoolDataProvider,
		Data: data,
	}

	result, err := p.ethClient.CallContract(ctx, msg, big.NewInt(blockNumber))
	if err != nil {
		return nil, fmt.Errorf("failed to call contract: %w", err)
	}

	p.logger.Debug("raw contract response",
		"length", len(result),
		"hex", fmt.Sprintf("0x%x", result[:min(len(result), 256)]))

	if len(result) < 64 {
		return []UserReserveData{}, nil
	}

	offset := new(big.Int).SetBytes(result[0:32]).Uint64()
	p.logger.Debug("decoded offset", "offset", offset)

	if offset+32 > uint64(len(result)) {
		return []UserReserveData{}, nil
	}

	arrayLengthBytes := result[offset : offset+32]
	arrayLength := new(big.Int).SetBytes(arrayLengthBytes).Uint64()
	p.logger.Debug("decoded array length", "count", arrayLength)

	if arrayLength == 0 {
		return []UserReserveData{}, nil
	}

	reserves := make([]UserReserveData, 0, arrayLength)

	structSize := uint64(224)
	dataStart := offset + 32

	for i := uint64(0); i < arrayLength; i++ {
		structOffset := dataStart + (i * structSize)
		if structOffset+structSize > uint64(len(result)) {
			p.logger.Warn("struct data out of bounds", "i", i, "offset", structOffset, "size", structSize, "total", len(result))
			break
		}

		structData := result[structOffset : structOffset+structSize]

		underlyingAsset := common.BytesToAddress(structData[0:32])
		scaledATokenBalance := new(big.Int).SetBytes(structData[32:64])
		usageAsCollateral := new(big.Int).SetBytes(structData[64:96]).Uint64() != 0
		stableBorrowRate := new(big.Int).SetBytes(structData[96:128])
		scaledVariableDebt := new(big.Int).SetBytes(structData[128:160])
		principalStableDebt := new(big.Int).SetBytes(structData[160:192])
		stableBorrowLastUpdate := new(big.Int).SetBytes(structData[192:224])

		p.logger.Debug("decoded reserve",
			"index", i,
			"asset", underlyingAsset.Hex(),
			"scaledATokenBalance", scaledATokenBalance.String(),
			"usageAsCollateral", usageAsCollateral,
			"scaledVariableDebt", scaledVariableDebt.String())

		reserves = append(reserves, UserReserveData{
			UnderlyingAsset:                 underlyingAsset,
			ScaledATokenBalance:             scaledATokenBalance,
			UsageAsCollateralEnabledOnUser:  usageAsCollateral,
			StableBorrowRate:                stableBorrowRate,
			ScaledVariableDebt:              scaledVariableDebt,
			PrincipalStableDebt:             principalStableDebt,
			StableBorrowLastUpdateTimestamp: stableBorrowLastUpdate,
		})
	}

	return reserves, nil
}

func (p *BorrowEventProcessor) getTokenDecimals(ctx context.Context, tokenAddress common.Address) (int, error) {
	data, err := p.erc20DecimalsABI.Pack("decimals")
	if err != nil {
		return 0, fmt.Errorf("failed to pack decimals call: %w", err)
	}

	msg := ethereum.CallMsg{
		To:   &tokenAddress,
		Data: data,
	}

	result, err := p.ethClient.CallContract(ctx, msg, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to call decimals on token %s: %w", tokenAddress.Hex(), err)
	}

	var decimals uint8
	err = p.erc20DecimalsABI.UnpackIntoInterface(&decimals, "decimals", result)
	if err != nil {
		return 0, fmt.Errorf("failed to unpack decimals for token %s: %w", tokenAddress.Hex(), err)
	}

	p.logger.Debug("fetched token decimals", "token", tokenAddress.Hex(), "decimals", decimals)
	return int(decimals), nil
}

func (p *BorrowEventProcessor) getActualBalance(ctx context.Context, protocolAddress, asset common.Address, scaledBalance *big.Int, blockNumber int64) (*big.Int, error) {
	data, err := p.getReserveDataABI.Pack("getReserveData", asset)
	if err != nil {
		p.logger.Warn("failed to pack getReserveData, using scaled balance", "error", err)
		return scaledBalance, nil
	}

	msg := ethereum.CallMsg{
		To:   &protocolAddress,
		Data: data,
	}

	result, err := p.ethClient.CallContract(ctx, msg, big.NewInt(blockNumber))
	if err != nil {
		p.logger.Warn("failed to get reserve data, using scaled balance", "asset", asset.Hex(), "error", err)
		return scaledBalance, nil
	}

	if len(result) < 96 {
		p.logger.Warn("reserve data too short", "length", len(result), "asset", asset.Hex())
		return scaledBalance, nil
	}

	liquidityIndex := new(big.Int).SetBytes(result[48:64])

	p.logger.Debug("extracted liquidity index",
		"asset", asset.Hex(),
		"bytes_32_64_hex", fmt.Sprintf("0x%x", result[32:64]),
		"liquidityIndex", liquidityIndex.String())

	if liquidityIndex.Cmp(big.NewInt(0)) == 0 {
		p.logger.Warn("liquidityIndex is zero, using scaled balance", "asset", asset.Hex())
		return scaledBalance, nil
	}

	RAY := new(big.Int).Exp(big.NewInt(10), big.NewInt(27), nil)

	actualBalance := new(big.Int).Mul(scaledBalance, liquidityIndex)
	actualBalance.Div(actualBalance, RAY)

	p.logger.Debug("calculated actual balance",
		"asset", asset.Hex(),
		"scaled", scaledBalance.String(),
		"liquidityIndex", liquidityIndex.String(),
		"RAY", RAY.String(),
		"actual", actualBalance.String())

	return actualBalance, nil
}

func (p *BorrowEventProcessor) convertToDecimalAdjusted(rawAmount *big.Int, decimals int) string {
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

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func (p *BorrowEventProcessor) isBorrowEvent(log Log) bool {
	if len(log.Topics) == 0 {
		return false
	}

	eventSig := common.HexToHash(log.Topics[0])
	_, ok := p.eventSignatures[eventSig]
	return ok
}

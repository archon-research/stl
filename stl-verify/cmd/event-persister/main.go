package main

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"os/signal"
	"strings"
	"sync"
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

const (
	Multicall3Address = "0xcA11bde05977b3631167028862bE2a173976CA11"
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
	multicallABI          *abi.ABI
	uiPoolDataProvider    common.Address
	poolAddressesProvider common.Address
	aavePool              common.Address
	multicall3            common.Address
	eventSignatures       map[common.Hash]*abi.Event
	decimalsCache         map[common.Address]int
	decimalsCacheMu       sync.RWMutex
}

type collateralData struct {
	asset         common.Address
	decimals      int
	actualBalance *big.Int
}

type Call3 struct {
	Target       common.Address
	AllowFailure bool
	CallData     []byte
}

type Multicall3Result struct {
	Success    bool
	ReturnData []byte
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
		multicall3:            common.HexToAddress(Multicall3Address),
		decimalsCache:         make(map[common.Address]int),
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
		return fmt.Errorf("borrow event not found in ABI")
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

	multicallABI := `[{
       "inputs": [
          {
             "components": [
                {"internalType": "address", "name": "target", "type": "address"},
                {"internalType": "bool", "name": "allowFailure", "type": "bool"},
                {"internalType": "bytes", "name": "callData", "type": "bytes"}
             ],
             "internalType": "struct Multicall3.Call3[]",
             "name": "calls",
             "type": "tuple[]"
          }
       ],
       "name": "aggregate3",
       "outputs": [
          {
             "components": [
                {"internalType": "bool", "name": "success", "type": "bool"},
                {"internalType": "bytes", "name": "returnData", "type": "bytes"}
             ],
             "internalType": "struct Multicall3.Result[]",
             "name": "returnData",
             "type": "tuple[]"
          }
       ],
       "stateMutability": "payable",
       "type": "function"
    }]`

	parsedMulticallABI, err := abi.JSON(strings.NewReader(multicallABI))
	if err != nil {
		return fmt.Errorf("failed to parse multicall ABI: %w", err)
	}
	p.multicallABI = &parsedMulticallABI

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

		if err := p.processBorrowLog(ctx, log, receipt.TransactionHash, blockNumber, blockVersion); err != nil {
			p.logger.Error("failed to process borrow event", "error", err, "tx", receipt.TransactionHash)
		}
	}
	return nil
}

func (p *BorrowEventProcessor) processBorrowLog(ctx context.Context, log Log, txHash string, blockNumber int64, blockVersion int) error {
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

	return p.saveBorrowEvent(ctx, protocolAddress, eventData, txHash, blockNumber, blockVersion)
}

func (p *BorrowEventProcessor) saveBorrowEvent(ctx context.Context, protocolAddress common.Address, eventData map[string]interface{}, txHash string, blockNumber int64, blockVersion int) error {
	reserve := eventData["reserve"].(common.Address)
	onBehalfOf := eventData["onBehalfOf"].(common.Address)
	amount := eventData["amount"].(*big.Int)

	p.logger.Info("Borrow event detected",
		"user", onBehalfOf.Hex(),
		"protocol", protocolAddress.Hex(),
		"reserve", reserve.Hex(),
		"amount", amount.String(),
		"tx", txHash,
		"block_version", blockVersion,
		"block", blockNumber)

	reserves, err := p.getUserReservesData(ctx, onBehalfOf, blockNumber)
	if err != nil {
		p.logger.Warn("failed to get user reserves", "error", err, "tx", txHash, "block", blockNumber)
		reserves = []UserReserveData{}
	}

	tokensToFetch := make(map[common.Address]bool)
	tokensToFetch[reserve] = true

	for _, r := range reserves {
		if r.ScaledATokenBalance.Cmp(big.NewInt(0)) > 0 && r.UsageAsCollateralEnabledOnUser {
			if r.UnderlyingAsset != (common.Address{}) {
				tokensToFetch[r.UnderlyingAsset] = true
			}
		}
	}

	assetsForReserveData := make([]common.Address, 0)
	for _, r := range reserves {
		if r.ScaledATokenBalance.Cmp(big.NewInt(0)) > 0 && r.UsageAsCollateralEnabledOnUser {
			if r.UnderlyingAsset != (common.Address{}) {
				assetsForReserveData = append(assetsForReserveData, r.UnderlyingAsset)
			}
		}
	}

	decimalsMap, err := p.batchGetDecimals(ctx, tokensToFetch)
	if err != nil {
		p.logger.Warn("failed to batch get decimals", "error", err, "tx", txHash, "block", blockNumber)
		decimalsMap = make(map[common.Address]int)
	}

	reserveDataMap, err := p.batchGetReserveData(ctx, protocolAddress, assetsForReserveData, blockNumber)
	if err != nil {
		p.logger.Warn("failed to batch get reserve data", "error", err, "tx", txHash, "block", blockNumber)
		reserveDataMap = make(map[common.Address]*big.Int)
	}

	borrowTokenDecimals, ok := decimalsMap[reserve]
	if !ok {
		p.logger.Error("SKIPPING EVENT: Failed to get borrow token decimals",
			"token", reserve.Hex(),
			"tx", txHash,
			"block", blockNumber,
			"user", onBehalfOf.Hex(),
			"protocol", protocolAddress.Hex())
		return fmt.Errorf("borrow token decimals not found for %s", reserve.Hex())
	}

	var collaterals []collateralData
	for _, r := range reserves {
		if r.ScaledATokenBalance.Cmp(big.NewInt(0)) > 0 && r.UsageAsCollateralEnabledOnUser {
			if r.UnderlyingAsset == (common.Address{}) {
				continue
			}

			decimals, ok := decimalsMap[r.UnderlyingAsset]
			if !ok {
				p.logger.Error("SKIPPING COLLATERAL: Failed to get collateral token decimals",
					"token", r.UnderlyingAsset.Hex(),
					"tx", txHash,
					"block", blockNumber,
					"user", onBehalfOf.Hex())
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

			collaterals = append(collaterals, collateralData{
				asset:         r.UnderlyingAsset,
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
			p.logger.Warn("failed to get collateral token", "token", col.asset.Hex(), "error", err, "tx", txHash)
			continue
		}

		decimalAdjustedCollateral := p.convertToDecimalAdjusted(col.actualBalance, col.decimals)
		p.logger.Debug("converted collateral amount", "raw", col.actualBalance.String(), "decimals", col.decimals, "adjusted", decimalAdjustedCollateral)

		if err := p.lendingRepo.SaveBorrowerCollateral(ctx, tx, userID, protocolID, tokenID, blockNumber, blockVersion, decimalAdjustedCollateral); err != nil {
			p.logger.Warn("failed to insert collateral", "error", err, "tx", txHash)
		} else {
			p.logger.Debug("saved collateral", "token", col.asset.Hex(), "decimals", col.decimals, "adjusted_amount", decimalAdjustedCollateral)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	p.logger.Info("Saved to database", "user", onBehalfOf.Hex(), "tx", txHash, "block", blockNumber)
	return nil
}

func (p *BorrowEventProcessor) batchGetDecimals(ctx context.Context, tokens map[common.Address]bool) (map[common.Address]int, error) {
	result := make(map[common.Address]int)

	var tokensToFetch []common.Address
	p.decimalsCacheMu.RLock()
	for token := range tokens {
		if decimals, ok := p.decimalsCache[token]; ok {
			result[token] = decimals
		} else {
			tokensToFetch = append(tokensToFetch, token)
		}
	}
	p.decimalsCacheMu.RUnlock()

	if len(tokensToFetch) == 0 {
		return result, nil
	}

	p.logger.Debug("fetching decimals for tokens", "count", len(tokensToFetch))

	var calls []Call3
	for _, token := range tokensToFetch {
		callData, err := p.erc20DecimalsABI.Pack("decimals")
		if err != nil {
			p.logger.Warn("failed to pack decimals call", "token", token.Hex(), "error", err)
			continue
		}
		calls = append(calls, Call3{
			Target:       token,
			AllowFailure: true,
			CallData:     callData,
		})
	}

	if len(calls) == 0 {
		return result, nil
	}

	results, err := p.executeMulticall(ctx, calls, nil)
	if err != nil {
		return result, fmt.Errorf("multicall failed: %w", err)
	}

	p.decimalsCacheMu.Lock()
	defer p.decimalsCacheMu.Unlock()

	for i, mcResult := range results {
		if i >= len(tokensToFetch) {
			break
		}
		token := tokensToFetch[i]

		if !mcResult.Success {
			p.logger.Warn("decimals call failed (call not successful)",
				"token", token.Hex(),
				"returnDataLength", len(mcResult.ReturnData))
			continue
		}

		if len(mcResult.ReturnData) == 0 {
			p.logger.Warn("decimals call returned empty data", "token", token.Hex())
			continue
		}

		var decimals uint8
		err := p.erc20DecimalsABI.UnpackIntoInterface(&decimals, "decimals", mcResult.ReturnData)
		if err != nil {
			p.logger.Warn("failed to unpack decimals",
				"token", token.Hex(),
				"error", err,
				"returnDataHex", hex.EncodeToString(mcResult.ReturnData))
			continue
		}

		result[token] = int(decimals)
		p.decimalsCache[token] = int(decimals)
		p.logger.Debug("fetched and cached decimals", "token", token.Hex(), "decimals", decimals)
	}

	return result, nil
}

func (p *BorrowEventProcessor) batchGetReserveData(ctx context.Context, protocolAddress common.Address, assets []common.Address, blockNumber int64) (map[common.Address]*big.Int, error) {
	result := make(map[common.Address]*big.Int)

	if len(assets) == 0 {
		return result, nil
	}

	var calls []Call3
	for _, asset := range assets {
		callData, err := p.getReserveDataABI.Pack("getReserveData", asset)
		if err != nil {
			continue
		}
		calls = append(calls, Call3{
			Target:       protocolAddress,
			AllowFailure: true,
			CallData:     callData,
		})
	}

	if len(calls) == 0 {
		return result, nil
	}

	blockNum := big.NewInt(blockNumber)
	results, err := p.executeMulticall(ctx, calls, blockNum)
	if err != nil {
		return result, fmt.Errorf("multicall failed: %w", err)
	}

	for i, mcResult := range results {
		if i >= len(assets) {
			break
		}
		asset := assets[i]

		if !mcResult.Success || len(mcResult.ReturnData) < 96 {
			p.logger.Debug("getReserveData call failed", "asset", asset.Hex())
			continue
		}

		liquidityIndex := new(big.Int).SetBytes(mcResult.ReturnData[48:64])
		result[asset] = liquidityIndex

		p.logger.Debug("fetched liquidity index", "asset", asset.Hex(), "liquidityIndex", liquidityIndex.String())
	}

	return result, nil
}

func (p *BorrowEventProcessor) executeMulticall(ctx context.Context, calls []Call3, blockNumber *big.Int) ([]Multicall3Result, error) {
	if len(calls) == 0 {
		return []Multicall3Result{}, nil
	}

	data, err := p.multicallABI.Pack("aggregate3", calls)
	if err != nil {
		return nil, fmt.Errorf("failed to pack multicall: %w", err)
	}

	msg := ethereum.CallMsg{
		To:   &p.multicall3,
		Data: data,
	}

	result, err := p.ethClient.CallContract(ctx, msg, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to call multicall contract: %w", err)
	}

	unpacked, err := p.multicallABI.Unpack("aggregate3", result)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack multicall response: %w", err)
	}

	resultsRaw := unpacked[0].([]struct {
		Success    bool   `json:"success"`
		ReturnData []byte `json:"returnData"`
	})

	results := make([]Multicall3Result, len(resultsRaw))
	for i, r := range resultsRaw {
		results[i] = Multicall3Result{
			Success:    r.Success,
			ReturnData: r.ReturnData,
		}
	}

	p.logger.Debug("multicall executed", "calls", len(calls), "results", len(results))

	return results, nil
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
	if offset+32 > uint64(len(result)) {
		return []UserReserveData{}, nil
	}

	arrayLengthBytes := result[offset : offset+32]
	arrayLengthClaimed := new(big.Int).SetBytes(arrayLengthBytes).Uint64()

	const structSize = uint64(224)
	dataStart := offset + 32
	availableBytes := uint64(len(result)) - dataStart
	actualArrayLength := availableBytes / structSize

	arrayLength := arrayLengthClaimed
	if actualArrayLength < arrayLengthClaimed {
		p.logger.Warn("array length mismatch",
			"claimed", arrayLengthClaimed,
			"actual", actualArrayLength,
			"availableBytes", availableBytes)
		arrayLength = actualArrayLength
	}

	p.logger.Debug("decoded array info",
		"offset", offset,
		"claimed_length", arrayLengthClaimed,
		"actual_length", arrayLength)

	if arrayLength == 0 {
		return []UserReserveData{}, nil
	}

	reserves := make([]UserReserveData, 0, arrayLength)

	for i := uint64(0); i < arrayLength; i++ {
		structOffset := dataStart + (i * structSize)
		if structOffset+structSize > uint64(len(result)) {
			p.logger.Warn("reached end of data", "processed", i, "expected", arrayLength)
			break
		}

		structData := result[structOffset : structOffset+structSize]

		underlyingAsset := common.BytesToAddress(structData[0:32])

		if underlyingAsset == (common.Address{}) {
			continue
		}

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

	p.logger.Debug("decoded user reserves", "total_slots", arrayLength, "valid_reserves", len(reserves))
	return reserves, nil
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

package main

import (
	"context"
	"database/sql"
	"encoding/json"
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
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
)

// Metadata from SQS
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

// UserReserveData from Aave V3 UIPoolDataProvider
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
	seenTxs               map[string]bool
	uiPoolDataProvider    common.Address
	poolAddressesProvider common.Address
	aavePool              common.Address
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

	// Load environment
	_ = godotenv.Load(".env")
	_ = godotenv.Load(".env.local")

	// Get queue URL
	if *queueURL == "" {
		*queueURL = os.Getenv("AWS_SQS_QUEUE_RECEIPTS")
	}
	if *queueURL == "" {
		logger.Error("queue URL not provided")
		os.Exit(1)
	}

	// Get database URL
	if *dbURL == "" {
		*dbURL = os.Getenv("DATABASE_URL")
	}
	if *dbURL == "" {
		logger.Error("database URL not provided (use -db flag or DATABASE_URL env var)")
		os.Exit(1)
	}

	// Get Alchemy configuration
	alchemyAPIKey := os.Getenv("ALCHEMY_API_KEY")
	if alchemyAPIKey == "" {
		logger.Error("ALCHEMY_API_KEY environment variable is required")
		os.Exit(1)
	}
	alchemyHTTPURL := getEnv("ALCHEMY_HTTP_URL", "https://eth-mainnet.g.alchemy.com/v2")
	fullAlchemyURL := fmt.Sprintf("%s/%s", alchemyHTTPURL, alchemyAPIKey)

	// Get Redis address
	if *redisAddr == "" {
		*redisAddr = os.Getenv("REDIS_ADDR")
	}
	if *redisAddr == "" {
		logger.Error("Redis address not provided (use -redis flag or REDIS_ADDR env var)")
		os.Exit(1)
	}

	// Get chain ID
	chainIDStr := os.Getenv("CHAIN_ID")
	if chainIDStr == "" {
		chainIDStr = "1" // Default to mainnet
	}
	var chainID int64 = 1
	fmt.Sscanf(chainIDStr, "%d", &chainID)

	logger.Info("starting borrow event processor",
		"queue", *queueURL,
		"redis", *redisAddr,
		"chainID", chainID)

	ctx := context.Background()

	// Create AWS config
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

	// Create SQS client
	sqsClient := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		if endpoint := os.Getenv("AWS_SQS_ENDPOINT"); endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	})

	// Create Redis client
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

	// Create Ethereum client
	ethClient, err := ethclient.Dial(fullAlchemyURL)
	if err != nil {
		logger.Error("failed to connect to Ethereum node", "error", err)
		os.Exit(1)
	}
	logger.Info("connected to Ethereum node")

	// Connect to PostgreSQL
	db, err := sql.Open("postgres", *dbURL)
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

	// Create lending repository and run migration
	lendingRepo := postgres.NewLendingRepository(db, chainID, logger)
	if err := lendingRepo.Migrate(ctx); err != nil {
		logger.Error("failed to migrate lending tables", "error", err)
		os.Exit(1)
	}
	logger.Info("lending tables migrated successfully")

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
		seenTxs:               make(map[string]bool),
		uiPoolDataProvider:    common.HexToAddress("0x56b7A1012765C285afAC8b8F25C69Bf10ccfE978"),
		poolAddressesProvider: common.HexToAddress("0x2f39d218133AFaB8F2B819B1066c7E434Ad94E9e"),
		aavePool:              common.HexToAddress("0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"),
	}

	// Load ABIs
	if err := processor.loadABIs(); err != nil {
		logger.Error("failed to load ABIs", "error", err)
		os.Exit(1)
	}

	// Setup graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start processing
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
	// Borrow event ABI
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

	// UIPoolDataProvider ABI
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

	// Pool getReserveData ABI (for liquidity index)
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

	// ERC20 decimals() ABI
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

	p.logger.Info("📨 received messages", "count", len(result.Messages))

	for _, msg := range result.Messages {
		if err := p.processMessage(ctx, msg); err != nil {
			p.logger.Error("failed to process message", "error", err)
			continue
		}

		// Delete message
		//_, err := p.sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		//	QueueUrl:      aws.String(p.queueURL),
		//	ReceiptHandle: msg.ReceiptHandle,
		//})
		//if err != nil {
		//	p.logger.Error("failed to delete message", "error", err)
		//}
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
	if p.seenTxs[receipt.TransactionHash] {
		return nil
	}
	p.seenTxs[receipt.TransactionHash] = true

	// Find Borrow events
	for _, log := range receipt.Logs {
		if err := p.processBorrowLog(ctx, log, receipt.TransactionHash, blockNumber, blockVersion); err != nil {
			// Check if it's actually a borrow event that failed to save
			if strings.Contains(err.Error(), "failed to") {
				p.logger.Error("failed to process borrow event", "error", err, "tx", receipt.TransactionHash)
			} else {
				p.logger.Debug("not a borrow event", "error", err)
			}
		}
	}

	return nil
}

func (p *BorrowEventProcessor) processBorrowLog(ctx context.Context, log Log, txHash string, blockNumber int64, blockVersion int) error {
	if len(log.Topics) == 0 {
		return fmt.Errorf("no topics")
	}

	eventSig := common.HexToHash(log.Topics[0])

	// Find Borrow event
	var borrowEvent *abi.Event
	for _, event := range p.borrowABI.Events {
		if event.ID == eventSig {
			borrowEvent = &event
			break
		}
	}

	if borrowEvent == nil {
		return fmt.Errorf("not a borrow event")
	}

	// Decode event
	eventData := make(map[string]interface{})

	// Indexed parameters
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

	// Non-indexed parameters
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

	// Get contract address (protocol)
	protocolAddress := common.HexToAddress(log.Address)

	// Save to database
	return p.saveBorrowEvent(ctx, protocolAddress, eventData, blockNumber, blockVersion)
}

func (p *BorrowEventProcessor) saveBorrowEvent(ctx context.Context, protocolAddress common.Address, eventData map[string]interface{}, blockNumber int64, blockVersion int) error {
	reserve := eventData["reserve"].(common.Address)
	onBehalfOf := eventData["onBehalfOf"].(common.Address)
	amount := eventData["amount"].(*big.Int)

	p.logger.Info("💰 Borrow event detected",
		"user", onBehalfOf.Hex(),
		"protocol", protocolAddress.Hex(),
		"reserve", reserve.Hex(),
		"amount", amount.String(),
		"block", blockNumber)

	// Start transaction
	tx, err := p.lendingRepo.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// 1. Ensure user exists
	userID, err := p.lendingRepo.EnsureUser(ctx, tx, onBehalfOf)
	if err != nil {
		return fmt.Errorf("failed to ensure user: %w", err)
	}

	// 2. Get or create protocol
	protocolID, err := p.lendingRepo.GetOrCreateProtocol(ctx, tx, protocolAddress)
	if err != nil {
		return fmt.Errorf("failed to get protocol: %w", err)
	}

	// 3. Get or create token for borrowed asset
	borrowTokenDecimals, err := p.getTokenDecimals(ctx, reserve)
	if err != nil {
		p.logger.Warn("failed to get borrow token decimals, using default 18", "token", reserve.Hex(), "error", err)
		borrowTokenDecimals = 18
	}
	borrowTokenID, err := p.lendingRepo.GetOrCreateToken(ctx, tx, reserve, borrowTokenDecimals)
	if err != nil {
		return fmt.Errorf("failed to get borrow token: %w", err)
	}

	// 4. Convert raw amount to decimal-adjusted amount
	decimalAdjustedAmount := p.convertToDecimalAdjusted(amount, borrowTokenDecimals)
	p.logger.Debug("converted borrow amount", "raw", amount.String(), "decimals", borrowTokenDecimals, "adjusted", decimalAdjustedAmount)

	// 5. Insert borrower record with decimal-adjusted amount
	if err := p.lendingRepo.SaveBorrower(ctx, tx, userID, protocolID, borrowTokenID, blockNumber, blockVersion, decimalAdjustedAmount); err != nil {
		return fmt.Errorf("failed to insert borrower: %w", err)
	}

	// 6. Get user's collateral data
	reserves, err := p.getUserReservesData(ctx, onBehalfOf, blockNumber)
	if err != nil {
		p.logger.Warn("failed to get user reserves", "error", err)
		// Continue anyway - we still save the borrow
	} else {
		p.logger.Debug("found reserves", "count", len(reserves))
		// 6. Insert collateral records with ACTUAL balances (not scaled)
		for _, reserve := range reserves {
			if reserve.ScaledATokenBalance.Cmp(big.NewInt(0)) > 0 && reserve.UsageAsCollateralEnabledOnUser {
				// Skip zero address (invalid token)
				if reserve.UnderlyingAsset == (common.Address{}) {
					p.logger.Debug("skipping zero address collateral")
					continue
				}

				collateralDecimals, err := p.getTokenDecimals(ctx, reserve.UnderlyingAsset)
				if err != nil {
					p.logger.Warn("failed to get collateral token decimals, using default 18", "token", reserve.UnderlyingAsset.Hex(), "error", err)
					collateralDecimals = 18
				}

				tokenID, err := p.lendingRepo.GetOrCreateToken(ctx, tx, reserve.UnderlyingAsset, collateralDecimals)
				if err != nil {
					p.logger.Warn("failed to get collateral token", "token", reserve.UnderlyingAsset.Hex(), "error", err)
					continue
				}

				actualBalance, err := p.getActualBalance(ctx, protocolAddress, reserve.UnderlyingAsset, reserve.ScaledATokenBalance, blockNumber)
				if err != nil {
					p.logger.Warn("failed to calculate actual balance, using scaled", "token", reserve.UnderlyingAsset.Hex(), "error", err)
					actualBalance = reserve.ScaledATokenBalance
				}

				decimalAdjustedCollateral := p.convertToDecimalAdjusted(actualBalance, collateralDecimals)
				p.logger.Debug("converted collateral amount", "raw", actualBalance.String(), "decimals", collateralDecimals, "adjusted", decimalAdjustedCollateral)

				if err := p.lendingRepo.SaveBorrowerCollateral(ctx, tx, userID, protocolID, tokenID, blockNumber, blockVersion, decimalAdjustedCollateral); err != nil {
					p.logger.Warn("failed to insert collateral", "error", err)
				} else {
					p.logger.Debug("💎 saved collateral", "token", reserve.UnderlyingAsset.Hex(), "decimals", collateralDecimals, "adjusted_amount", decimalAdjustedCollateral)
				}
			}
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	p.logger.Info("✅ Saved to database", "user", onBehalfOf.Hex(), "block", blockNumber)
	return nil
}

func (p *BorrowEventProcessor) getUserReservesData(ctx context.Context, user common.Address, blockNumber int64) ([]UserReserveData, error) {
	// Pack function call
	data, err := p.getUserReservesABI.Pack("getUserReservesData", p.poolAddressesProvider, user)
	if err != nil {
		return nil, fmt.Errorf("failed to pack function call: %w", err)
	}

	// Call contract at specific block
	msg := ethereum.CallMsg{
		To:   &p.uiPoolDataProvider,
		Data: data,
	}

	result, err := p.ethClient.CallContract(ctx, msg, big.NewInt(blockNumber))
	if err != nil {
		return nil, fmt.Errorf("failed to call contract: %w", err)
	}

	p.logger.Debug("📦 raw contract response",
		"length", len(result),
		"hex", fmt.Sprintf("0x%x", result[:min(len(result), 256)]))

	if len(result) < 64 {
		return []UserReserveData{}, nil
	}

	// Manual decoding - parse the raw bytes
	// Result structure:
	// 0-32: offset to array
	// 32-64: userEModeCategory (uint8)
	// Then the array data starts

	offset := new(big.Int).SetBytes(result[0:32]).Uint64()
	p.logger.Debug("decoded offset", "offset", offset)

	if offset+32 > uint64(len(result)) {
		return []UserReserveData{}, nil
	}

	// Read array length
	arrayLengthBytes := result[offset : offset+32]
	arrayLength := new(big.Int).SetBytes(arrayLengthBytes).Uint64()
	p.logger.Debug("decoded array length", "count", arrayLength)

	if arrayLength == 0 {
		return []UserReserveData{}, nil
	}

	reserves := make([]UserReserveData, 0, arrayLength)

	// Each struct is 7 fields * 32 bytes = 224 bytes
	structSize := uint64(224)
	dataStart := offset + 32

	for i := uint64(0); i < arrayLength; i++ {
		structOffset := dataStart + (i * structSize)
		if structOffset+structSize > uint64(len(result)) {
			p.logger.Warn("struct data out of bounds", "i", i, "offset", structOffset, "size", structSize, "total", len(result))
			break
		}

		structData := result[structOffset : structOffset+structSize]

		// Parse each field (each is 32 bytes)
		underlyingAsset := common.BytesToAddress(structData[0:32])
		scaledATokenBalance := new(big.Int).SetBytes(structData[32:64])
		usageAsCollateral := new(big.Int).SetBytes(structData[64:96]).Uint64() != 0
		stableBorrowRate := new(big.Int).SetBytes(structData[96:128])
		scaledVariableDebt := new(big.Int).SetBytes(structData[128:160])
		principalStableDebt := new(big.Int).SetBytes(structData[160:192])
		stableBorrowLastUpdate := new(big.Int).SetBytes(structData[192:224])

		// Note: scaledATokenBalance is a scaled value
		// Actual balance = scaledATokenBalance * liquidityIndex / RAY
		// For now we store the scaled value - can be converted later with reserve data

		p.logger.Debug("📊 decoded reserve",
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
	// Call decimals() on the token contract
	data, err := p.erc20DecimalsABI.Pack("decimals")
	if err != nil {
		return 0, fmt.Errorf("failed to pack decimals call: %w", err)
	}

	msg := ethereum.CallMsg{
		To:   &tokenAddress,
		Data: data,
	}

	result, err := p.ethClient.CallContract(ctx, msg, nil) // Use latest block for decimals (immutable)
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
	// Query reserve data to get liquidity index
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

	// ABI encoding of the struct:
	// Bytes 0-31:   configuration (uint256)
	// Bytes 32-63:  liquidityIndex (uint128, right-aligned in 32 bytes)
	// Bytes 64-95:  currentLiquidityRate (uint128, right-aligned in 32 bytes)
	// ...

	// For uint128, the value is in the LAST 16 bytes of the 32-byte slot
	// So liquidityIndex is at bytes 48-63 (not 32-63)
	liquidityIndex := new(big.Int).SetBytes(result[48:64])

	p.logger.Debug("extracted liquidity index",
		"asset", asset.Hex(),
		"bytes_32_64_hex", fmt.Sprintf("0x%x", result[32:64]),
		"liquidityIndex", liquidityIndex.String())

	if liquidityIndex.Cmp(big.NewInt(0)) == 0 {
		p.logger.Warn("liquidityIndex is zero, using scaled balance", "asset", asset.Hex())
		return scaledBalance, nil
	}

	// Calculate actual balance: scaledBalance * liquidityIndex / RAY
	// RAY = 10^27
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

// convertToDecimalAdjusted converts a raw token amount to decimal-adjusted format
// Returns the amount as a string with decimal precision applied
// Example: 1000000000 (raw USDC with 6 decimals) -> "1000.000000"
func (p *BorrowEventProcessor) convertToDecimalAdjusted(rawAmount *big.Int, decimals int) string {
	if decimals == 0 {
		return rawAmount.String()
	}

	// Create divisor: 10^decimals
	divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)

	// Integer part: rawAmount / 10^decimals
	integerPart := new(big.Int).Div(rawAmount, divisor)

	// Fractional part: rawAmount % 10^decimals
	remainder := new(big.Int).Mod(rawAmount, divisor)

	// Format with decimal point
	if remainder.Cmp(big.NewInt(0)) == 0 {
		// No fractional part, just return integer
		return integerPart.String()
	}

	// Pad remainder to decimals length
	fractionalStr := fmt.Sprintf("%0*s", decimals, remainder.String())

	return fmt.Sprintf("%s.%s", integerPart.String(), fractionalStr)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

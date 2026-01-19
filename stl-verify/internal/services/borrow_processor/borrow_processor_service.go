package borrow_processor

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
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
	ActualBalance *big.Int
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

type BlockchainService struct {
	ethClient             *ethclient.Client
	logger                *slog.Logger
	getUserReservesABI    *abi.ABI
	getReserveDataABI     *abi.ABI
	erc20DecimalsABI      *abi.ABI
	multicallABI          *abi.ABI
	uiPoolDataProvider    common.Address
	poolAddressesProvider common.Address
	multicall3            common.Address
	decimalsCache         map[common.Address]int
	decimalsCacheMu       sync.RWMutex
}

func NewBlockchainService(ethClient *ethclient.Client, multicall3Addr, uiPoolDataProvider, poolAddressesProvider common.Address, logger *slog.Logger) (*BlockchainService, error) {
	service := &BlockchainService{
		ethClient:             ethClient,
		logger:                logger,
		uiPoolDataProvider:    uiPoolDataProvider,
		poolAddressesProvider: poolAddressesProvider,
		multicall3:            multicall3Addr,
		decimalsCache:         make(map[common.Address]int),
	}

	if err := service.loadABIs(); err != nil {
		return nil, err
	}

	return service, nil
}

func (s *BlockchainService) loadABIs() error {
	getUserReservesABI := `[{"inputs":[{"name":"provider","type":"address"},{"name":"user","type":"address"}],"name":"getUserReservesData","outputs":[{"components":[{"name":"underlyingAsset","type":"address"},{"name":"scaledATokenBalance","type":"uint256"},{"name":"usageAsCollateralEnabledOnUser","type":"bool"},{"name":"stableBorrowRate","type":"uint256"},{"name":"scaledVariableDebt","type":"uint256"},{"name":"principalStableDebt","type":"uint256"},{"name":"stableBorrowLastUpdateTimestamp","type":"uint256"}],"name":"","type":"tuple[]"},{"name":"","type":"uint8"}],"stateMutability":"view","type":"function"}]`
	parsedGetUserReservesABI, err := abi.JSON(strings.NewReader(getUserReservesABI))
	if err != nil {
		return fmt.Errorf("failed to parse getUserReserves ABI: %w", err)
	}
	s.getUserReservesABI = &parsedGetUserReservesABI

	getReserveDataABI := `[{"inputs":[{"name":"asset","type":"address"}],"name":"getReserveData","outputs":[{"components":[{"name":"configuration","type":"uint256"},{"name":"liquidityIndex","type":"uint128"},{"name":"currentLiquidityRate","type":"uint128"},{"name":"variableBorrowIndex","type":"uint128"},{"name":"currentVariableBorrowRate","type":"uint128"},{"name":"currentStableBorrowRate","type":"uint128"},{"name":"lastUpdateTimestamp","type":"uint40"},{"name":"id","type":"uint16"},{"name":"aTokenAddress","type":"address"},{"name":"stableDebtTokenAddress","type":"address"},{"name":"variableDebtTokenAddress","type":"address"},{"name":"interestRateStrategyAddress","type":"address"},{"name":"accruedToTreasury","type":"uint128"},{"name":"unbacked","type":"uint128"},{"name":"isolationModeTotalDebt","type":"uint128"}],"name":"","type":"tuple"}],"stateMutability":"view","type":"function"}]`
	parsedGetReserveDataABI, err := abi.JSON(strings.NewReader(getReserveDataABI))
	if err != nil {
		return fmt.Errorf("failed to parse getReserveData ABI: %w", err)
	}
	s.getReserveDataABI = &parsedGetReserveDataABI

	erc20DecimalsABI := `[{"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"stateMutability":"view","type":"function"}]`
	parsedERC20DecimalsABI, err := abi.JSON(strings.NewReader(erc20DecimalsABI))
	if err != nil {
		return fmt.Errorf("failed to parse ERC20 decimals ABI: %w", err)
	}
	s.erc20DecimalsABI = &parsedERC20DecimalsABI

	multicallABI := `[{"inputs":[{"components":[{"internalType":"address","name":"target","type":"address"},{"internalType":"bool","name":"allowFailure","type":"bool"},{"internalType":"bytes","name":"callData","type":"bytes"}],"internalType":"struct Multicall3.Call3[]","name":"calls","type":"tuple[]"}],"name":"aggregate3","outputs":[{"components":[{"internalType":"bool","name":"success","type":"bool"},{"internalType":"bytes","name":"returnData","type":"bytes"}],"internalType":"struct Multicall3.Result[]","name":"returnData","type":"tuple[]"}],"stateMutability":"payable","type":"function"}]`
	parsedMulticallABI, err := abi.JSON(strings.NewReader(multicallABI))
	if err != nil {
		return fmt.Errorf("failed to parse multicall ABI: %w", err)
	}
	s.multicallABI = &parsedMulticallABI

	s.logger.Info("blockchain service initialized")
	return nil
}

func (s *BlockchainService) GetUserReservesData(ctx context.Context, user common.Address, blockNumber int64) ([]UserReserveData, error) {
	data, err := s.getUserReservesABI.Pack("getUserReservesData", s.poolAddressesProvider, user)
	if err != nil {
		return nil, fmt.Errorf("failed to pack function call: %w", err)
	}

	msg := ethereum.CallMsg{
		To:   &s.uiPoolDataProvider,
		Data: data,
	}

	result, err := s.ethClient.CallContract(ctx, msg, big.NewInt(blockNumber))
	if err != nil {
		return nil, fmt.Errorf("failed to call contract: %w", err)
	}

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
		arrayLength = actualArrayLength
	}

	if arrayLength == 0 {
		return []UserReserveData{}, nil
	}

	reserves := make([]UserReserveData, 0, arrayLength)

	for i := uint64(0); i < arrayLength; i++ {
		structOffset := dataStart + (i * structSize)
		if structOffset+structSize > uint64(len(result)) {
			break
		}

		structData := result[structOffset : structOffset+structSize]
		underlyingAsset := common.BytesToAddress(structData[0:32])

		if underlyingAsset == (common.Address{}) {
			continue
		}

		reserves = append(reserves, UserReserveData{
			UnderlyingAsset:                 underlyingAsset,
			ScaledATokenBalance:             new(big.Int).SetBytes(structData[32:64]),
			UsageAsCollateralEnabledOnUser:  new(big.Int).SetBytes(structData[64:96]).Uint64() != 0,
			StableBorrowRate:                new(big.Int).SetBytes(structData[96:128]),
			ScaledVariableDebt:              new(big.Int).SetBytes(structData[128:160]),
			PrincipalStableDebt:             new(big.Int).SetBytes(structData[160:192]),
			StableBorrowLastUpdateTimestamp: new(big.Int).SetBytes(structData[192:224]),
		})
	}

	return reserves, nil
}

func (s *BlockchainService) BatchGetDecimals(ctx context.Context, tokens map[common.Address]bool) (map[common.Address]int, error) {
	result := make(map[common.Address]int)

	var tokensToFetch []common.Address
	s.decimalsCacheMu.RLock()
	for token := range tokens {
		if decimals, ok := s.decimalsCache[token]; ok {
			result[token] = decimals
		} else {
			tokensToFetch = append(tokensToFetch, token)
		}
	}
	s.decimalsCacheMu.RUnlock()

	if len(tokensToFetch) == 0 {
		return result, nil
	}

	var calls []Call3
	for _, token := range tokensToFetch {
		callData, err := s.erc20DecimalsABI.Pack("decimals")
		if err != nil {
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

	results, err := s.executeMulticall(ctx, calls, nil)
	if err != nil {
		return result, fmt.Errorf("multicall failed: %w", err)
	}

	s.decimalsCacheMu.Lock()
	defer s.decimalsCacheMu.Unlock()

	for i, mcResult := range results {
		if i >= len(tokensToFetch) {
			break
		}
		token := tokensToFetch[i]

		if !mcResult.Success || len(mcResult.ReturnData) == 0 {
			continue
		}

		var decimals uint8
		err := s.erc20DecimalsABI.UnpackIntoInterface(&decimals, "decimals", mcResult.ReturnData)
		if err != nil {
			s.logger.Warn("failed to unpack decimals", "token", token.Hex(), "error", err, "returnDataHex", hex.EncodeToString(mcResult.ReturnData))
			continue
		}

		result[token] = int(decimals)
		s.decimalsCache[token] = int(decimals)
	}

	return result, nil
}

func (s *BlockchainService) BatchGetReserveData(ctx context.Context, protocolAddress common.Address, assets []common.Address, blockNumber int64) (map[common.Address]*big.Int, error) {
	result := make(map[common.Address]*big.Int)

	if len(assets) == 0 {
		return result, nil
	}

	var calls []Call3
	for _, asset := range assets {
		callData, err := s.getReserveDataABI.Pack("getReserveData", asset)
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
	results, err := s.executeMulticall(ctx, calls, blockNum)
	if err != nil {
		return result, fmt.Errorf("multicall failed: %w", err)
	}

	for i, mcResult := range results {
		if i >= len(assets) {
			break
		}
		asset := assets[i]

		if !mcResult.Success || len(mcResult.ReturnData) < 96 {
			continue
		}

		liquidityIndex := new(big.Int).SetBytes(mcResult.ReturnData[48:64])
		result[asset] = liquidityIndex
	}

	return result, nil
}

func (s *BlockchainService) executeMulticall(ctx context.Context, calls []Call3, blockNumber *big.Int) ([]Multicall3Result, error) {
	if len(calls) == 0 {
		return []Multicall3Result{}, nil
	}

	data, err := s.multicallABI.Pack("aggregate3", calls)
	if err != nil {
		return nil, fmt.Errorf("failed to pack multicall: %w", err)
	}

	msg := ethereum.CallMsg{
		To:   &s.multicall3,
		Data: data,
	}

	result, err := s.ethClient.CallContract(ctx, msg, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to call multicall contract: %w", err)
	}

	unpacked, err := s.multicallABI.Unpack("aggregate3", result)
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

	return results, nil
}

type ProcessorConfig struct {
	QueueURL        string
	MaxMessages     int32
	WaitTimeSeconds int32
	PollInterval    time.Duration
	Logger          *slog.Logger
}

func ProcessorConfigDefaults() ProcessorConfig {
	return ProcessorConfig{
		MaxMessages:     10,
		WaitTimeSeconds: 20,
		PollInterval:    100 * time.Millisecond,
		Logger:          slog.Default(),
	}
}

type BorrowEventProcessor struct {
	config      ProcessorConfig
	sqsClient   *sqs.Client
	redisClient *redis.Client
	blockchain  *BlockchainService
	lendingRepo *postgres.LendingRepository

	borrowABI       *abi.ABI
	eventSignatures map[common.Hash]*abi.Event

	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger
}

func NewBorrowEventProcessor(
	config ProcessorConfig,
	sqsClient *sqs.Client,
	redisClient *redis.Client,
	blockchain *BlockchainService,
	lendingRepo *postgres.LendingRepository,
) (*BorrowEventProcessor, error) {
	if sqsClient == nil {
		return nil, fmt.Errorf("sqsClient is required")
	}
	if redisClient == nil {
		return nil, fmt.Errorf("redisClient is required")
	}
	if blockchain == nil {
		return nil, fmt.Errorf("blockchain is required")
	}
	if lendingRepo == nil {
		return nil, fmt.Errorf("lendingRepo is required")
	}

	defaults := ProcessorConfigDefaults()
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

	processor := &BorrowEventProcessor{
		config:          config,
		sqsClient:       sqsClient,
		redisClient:     redisClient,
		blockchain:      blockchain,
		lendingRepo:     lendingRepo,
		eventSignatures: make(map[common.Hash]*abi.Event),
		logger:          config.Logger.With("component", "borrow-processor"),
	}

	if err := processor.loadBorrowABI(); err != nil {
		return nil, err
	}

	return processor, nil
}

func (p *BorrowEventProcessor) loadBorrowABI() error {
	borrowEventABI := `[{"anonymous":false,"inputs":[{"indexed":true,"name":"reserve","type":"address"},{"indexed":false,"name":"user","type":"address"},{"indexed":true,"name":"onBehalfOf","type":"address"},{"indexed":false,"name":"amount","type":"uint256"},{"indexed":false,"name":"interestRateMode","type":"uint8"},{"indexed":false,"name":"borrowRate","type":"uint256"},{"indexed":true,"name":"referralCode","type":"uint16"}],"name":"Borrow","type":"event"}]`
	parsedBorrowABI, err := abi.JSON(strings.NewReader(borrowEventABI))
	if err != nil {
		return fmt.Errorf("failed to parse borrow ABI: %w", err)
	}
	p.borrowABI = &parsedBorrowABI

	if borrowEvent, ok := p.borrowABI.Events["Borrow"]; ok {
		p.eventSignatures[borrowEvent.ID] = &borrowEvent
	} else {
		return fmt.Errorf("borrow event not found in ABI")
	}

	return nil
}

func (p *BorrowEventProcessor) Start(ctx context.Context) error {
	p.ctx, p.cancel = context.WithCancel(ctx)

	go p.processLoop()

	p.logger.Info("borrow event processor started",
		"queue", p.config.QueueURL,
		"maxMessages", p.config.MaxMessages)
	return nil
}

func (p *BorrowEventProcessor) Stop() error {
	if p.cancel != nil {
		p.cancel()
	}
	p.logger.Info("borrow event processor stopped")
	return nil
}

func (p *BorrowEventProcessor) processLoop() {
	ticker := time.NewTicker(p.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			if err := p.processMessages(p.ctx); err != nil {
				p.logger.Error("error processing messages", "error", err)
			}
		}
	}
}

func (p *BorrowEventProcessor) processMessages(ctx context.Context) error {
	start := time.Now()
	defer func() {
		p.logger.Debug("processMessages completed", "duration", time.Since(start))
	}()

	result, err := p.sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(p.config.QueueURL),
		MaxNumberOfMessages: p.config.MaxMessages,
		WaitTimeSeconds:     p.config.WaitTimeSeconds,
		VisibilityTimeout:   30,
	})
	if err != nil {
		return fmt.Errorf("failed to receive messages: %w", err)
	}

	if len(result.Messages) == 0 {
		return nil
	}

	p.logger.Info("received messages", "count", len(result.Messages))

	var errs []error
	for _, msg := range result.Messages {
		if err := p.processMessage(ctx, msg); err != nil {
			p.logger.Error("failed to process message", "error", err)
			errs = append(errs, err)
			continue
		}

		_, deleteErr := p.sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
			QueueUrl:      aws.String(p.config.QueueURL),
			ReceiptHandle: msg.ReceiptHandle,
		})
		if deleteErr != nil {
			p.logger.Error("failed to delete message", "error", deleteErr)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
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
	start := time.Now()
	defer func() {
		p.logger.Debug("fetchAndProcessReceipts completed",
			"block", metadata.BlockNumber,
			"duration", time.Since(start))
	}()

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

	var errs []error
	for _, receipt := range receipts {
		if err := p.processReceipt(ctx, receipt, metadata.BlockNumber, metadata.Version); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (p *BorrowEventProcessor) processReceipt(ctx context.Context, receipt TransactionReceipt, blockNumber int64, blockVersion int) error {
	var errs []error
	for _, log := range receipt.Logs {
		if !p.isBorrowEvent(log) {
			continue
		}

		if err := p.processBorrowLog(ctx, log, receipt.TransactionHash, blockNumber, blockVersion); err != nil {
			p.logger.Error("failed to process borrow event", "error", err, "tx", receipt.TransactionHash)
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (p *BorrowEventProcessor) isBorrowEvent(log Log) bool {
	if len(log.Topics) == 0 {
		return false
	}
	eventSig := common.HexToHash(log.Topics[0])
	_, ok := p.eventSignatures[eventSig]
	return ok
}

func (p *BorrowEventProcessor) extractBorrowEventData(log Log) (*BorrowEventData, error) {
	if len(log.Topics) == 0 {
		return nil, fmt.Errorf("no topics")
	}

	eventSig := common.HexToHash(log.Topics[0])
	borrowEvent, ok := p.eventSignatures[eventSig]
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

func (p *BorrowEventProcessor) processBorrowLog(ctx context.Context, log Log, txHash string, blockNumber int64, blockVersion int) error {
	start := time.Now()
	defer func() {
		p.logger.Debug("processBorrowLog completed",
			"tx", txHash,
			"block", blockNumber,
			"duration", time.Since(start))
	}()

	borrowEvent, err := p.extractBorrowEventData(log)
	if err != nil {
		return fmt.Errorf("failed to extract borrow event: %w", err)
	}

	protocolAddress := common.HexToAddress(log.Address)

	p.logger.Info("Borrow event detected",
		"user", borrowEvent.OnBehalfOf.Hex(),
		"protocol", protocolAddress.Hex(),
		"reserve", borrowEvent.Reserve.Hex(),
		"amount", borrowEvent.Amount.String(),
		"tx", txHash,
		"block_version", blockVersion,
		"block", blockNumber)

	tokensToFetch := make(map[common.Address]bool)
	tokensToFetch[borrowEvent.Reserve] = true

	decimalsMap, err := p.blockchain.BatchGetDecimals(ctx, tokensToFetch)
	if err != nil {
		p.logger.Warn("failed to batch get decimals", "error", err, "tx", txHash, "block", blockNumber)
		decimalsMap = make(map[common.Address]int)
	}

	borrowTokenDecimals, ok := decimalsMap[borrowEvent.Reserve]
	if !ok {
		p.logger.Error("SKIPPING EVENT: Failed to get borrow token decimals",
			"token", borrowEvent.Reserve.Hex(),
			"tx", txHash,
			"block", blockNumber,
			"user", borrowEvent.OnBehalfOf.Hex(),
			"protocol", protocolAddress.Hex())
		return fmt.Errorf("borrow token decimals not found for %s", borrowEvent.Reserve.Hex())
	}

	collaterals, err := p.extractCollateralData(ctx, borrowEvent.OnBehalfOf, protocolAddress, blockNumber, txHash)
	if err != nil {
		p.logger.Warn("failed to extract collateral data", "error", err, "tx", txHash, "block", blockNumber)
		collaterals = []CollateralData{}
	}

	return p.saveBorrowEvent(ctx, borrowEvent, collaterals, borrowTokenDecimals, protocolAddress, blockNumber, blockVersion)
}

func (p *BorrowEventProcessor) extractCollateralData(ctx context.Context, user common.Address, protocolAddress common.Address, blockNumber int64, txHash string) ([]CollateralData, error) {
	reserves, err := p.blockchain.GetUserReservesData(ctx, user, blockNumber)
	if err != nil {
		p.logger.Warn("failed to get user reserves", "error", err, "tx", txHash, "block", blockNumber)
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

	decimalsMap, err := p.blockchain.BatchGetDecimals(ctx, tokensToFetch)
	if err != nil {
		p.logger.Warn("failed to batch get decimals", "error", err, "tx", txHash, "block", blockNumber)
		return []CollateralData{}, fmt.Errorf("failed to get decimals: %w", err)
	}

	reserveDataMap, err := p.blockchain.BatchGetReserveData(ctx, protocolAddress, assetsForReserveData, blockNumber)
	if err != nil {
		p.logger.Warn("failed to batch get reserve data", "error", err, "tx", txHash, "block", blockNumber)
		reserveDataMap = make(map[common.Address]*big.Int)
	}

	var collaterals []CollateralData
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
				Decimals:      decimals,
				ActualBalance: actualBalance,
			})
		}
	}

	return collaterals, nil
}

func (p *BorrowEventProcessor) saveBorrowEvent(ctx context.Context, borrowEvent *BorrowEventData, collaterals []CollateralData, borrowTokenDecimals int, protocolAddress common.Address, blockNumber int64, blockVersion int) error {
	tx, err := p.lendingRepo.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	userID, err := p.lendingRepo.EnsureUser(ctx, tx, borrowEvent.OnBehalfOf)
	if err != nil {
		return fmt.Errorf("failed to ensure user: %w", err)
	}

	protocolID, err := p.lendingRepo.GetOrCreateProtocol(ctx, tx, protocolAddress)
	if err != nil {
		return fmt.Errorf("failed to get protocol: %w", err)
	}

	borrowTokenID, err := p.lendingRepo.GetOrCreateToken(ctx, tx, borrowEvent.Reserve, borrowTokenDecimals)
	if err != nil {
		return fmt.Errorf("failed to get borrow token: %w", err)
	}

	decimalAdjustedAmount := p.convertToDecimalAdjusted(borrowEvent.Amount, borrowTokenDecimals)

	if err := p.lendingRepo.SaveBorrower(ctx, tx, userID, protocolID, borrowTokenID, blockNumber, blockVersion, decimalAdjustedAmount); err != nil {
		return fmt.Errorf("failed to insert borrower: %w", err)
	}

	for _, col := range collaterals {
		tokenID, err := p.lendingRepo.GetOrCreateToken(ctx, tx, col.Asset, col.Decimals)
		if err != nil {
			p.logger.Warn("failed to get collateral token", "token", col.Asset.Hex(), "error", err, "tx", borrowEvent.TxHash)
			continue
		}

		decimalAdjustedCollateral := p.convertToDecimalAdjusted(col.ActualBalance, col.Decimals)

		if err := p.lendingRepo.SaveBorrowerCollateral(ctx, tx, userID, protocolID, tokenID, blockNumber, blockVersion, decimalAdjustedCollateral); err != nil {
			p.logger.Warn("failed to insert collateral", "error", err, "tx", borrowEvent.TxHash)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	p.logger.Info("Saved to database", "user", borrowEvent.OnBehalfOf.Hex(), "tx", borrowEvent.TxHash, "block", blockNumber)
	return nil
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

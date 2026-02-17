package allocation_tracker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/redis/go-redis/v9"

	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// TransactionReceipt mirrors the JSON receipt structure stored in Redis.
type TransactionReceipt struct {
	TransactionHash string  `json:"transactionHash"`
	BlockNumber     string  `json:"blockNumber"`
	BlockHash       string  `json:"blockHash"`
	From            string  `json:"from"`
	To              string  `json:"to"`
	Status          string  `json:"status"`
	Logs            []Log   `json:"logs"`
	ContractAddress *string `json:"contractAddress"`
}

// Log mirrors a single log entry within a receipt.
type Log struct {
	Address          string   `json:"address"`
	Topics           []string `json:"topics"`
	Data             string   `json:"data"`
	BlockNumber      string   `json:"blockNumber"`
	TransactionHash  string   `json:"transactionHash"`
	TransactionIndex string   `json:"transactionIndex"`
	LogIndex         string   `json:"logIndex"`
	Removed          bool     `json:"removed"`
}

// BlockEvent represents an SQS message with block metadata.
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

// Service is the main allocation tracker worker.
type Service struct {
	config    Config
	sqsClient *sqs.Client
	redis     *redis.Client

	extractor  *TransferExtractor
	tokenCache *TokenCache
	handler    AllocationHandler

	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger
}

func NewService(
	config Config,
	sqsClient *sqs.Client,
	redisClient *redis.Client,
	tokenCache *TokenCache,
	handler AllocationHandler,
	proxies []ProxyConfig,
) (*Service, error) {
	if sqsClient == nil {
		return nil, fmt.Errorf("sqsClient is required")
	}
	if redisClient == nil {
		return nil, fmt.Errorf("redisClient is required")
	}
	if tokenCache == nil {
		return nil, fmt.Errorf("tokenCache is required")
	}
	if handler == nil {
		return nil, fmt.Errorf("handler is required")
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

	if len(proxies) == 0 {
		proxies = DefaultProxies()
	}

	return &Service{
		config:     config,
		sqsClient:  sqsClient,
		redis:      redisClient,
		extractor:  NewTransferExtractor(proxies),
		tokenCache: tokenCache,
		handler:    handler,
		logger:     config.Logger.With("component", "allocation-tracker"),
	}, nil
}

func (s *Service) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)
	go s.processLoop()

	s.logger.Info("allocation tracker started",
		"queue", s.config.QueueURL,
		"maxMessages", s.config.MaxMessages)
	return nil
}

func (s *Service) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.logger.Info("allocation tracker stopped")
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
			if err := s.pollMessages(s.ctx); err != nil {
				s.logger.Error("error polling messages", "error", err)
			}
		}
	}
}

func (s *Service) pollMessages(ctx context.Context) error {
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

	s.logger.Debug("received messages", "count", len(result.Messages))

	var errs []error
	for _, msg := range result.Messages {
		if err := s.processMessage(ctx, msg); err != nil {
			s.logger.Error("failed to process message", "error", err)
			errs = append(errs, err)
			continue
		}

		if _, deleteErr := s.sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
			QueueUrl:      aws.String(s.config.QueueURL),
			ReceiptHandle: msg.ReceiptHandle,
		}); deleteErr != nil {
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

	return s.processBlock(ctx, event)
}

func (s *Service) processBlock(ctx context.Context, event BlockEvent) error {
	start := time.Now()

	cacheKey := event.CacheKey()
	receiptsJSON, err := s.redis.Get(ctx, cacheKey).Result()
	if errors.Is(err, redis.Nil) {
		s.logger.Warn("cache miss", "key", cacheKey, "block", event.BlockNumber)
		return nil
	}
	if err != nil {
		return fmt.Errorf("redis get: %w", err)
	}

	var receipts []TransactionReceipt
	if err := shared.ParseCompressedJSON([]byte(receiptsJSON), &receipts); err != nil {
		return fmt.Errorf("parse receipts: %w", err)
	}

	// Extract all Transfer events involving proxies
	var allEvents []*AllocationEvent
	for _, receipt := range receipts {
		events := s.extractor.ExtractFromReceipt(receipt, event.ChainID, event.BlockNumber, event.Version)
		allEvents = append(allEvents, events...)
	}

	if len(allEvents) == 0 {
		s.logger.Debug("no allocation changes",
			"block", event.BlockNumber,
			"duration", time.Since(start))
		return nil
	}

	// Enrich with token metadata
	if err := s.enrichTokenMetadata(ctx, allEvents); err != nil {
		s.logger.Warn("failed to enrich token metadata", "error", err)
	}

	// Enrich with post-transfer balances
	if err := s.enrichBalances(ctx, allEvents, event.BlockNumber); err != nil {
		s.logger.Warn("failed to enrich balances", "error", err)
	}

	// Dispatch to handler
	if err := s.handler.HandleAllocationChanges(ctx, allEvents); err != nil {
		return fmt.Errorf("handler failed: %w", err)
	}

	s.logger.Debug("processed block",
		"block", event.BlockNumber,
		"events", len(allEvents),
		"duration", time.Since(start))

	return nil
}

// enrichTokenMetadata fetches metadata for all unique tokens in the events.
func (s *Service) enrichTokenMetadata(ctx context.Context, events []*AllocationEvent) error {
	tokenSet := make(map[common.Address]bool)
	for _, e := range events {
		tokenSet[e.TokenAddress] = true
	}

	tokens := make([]common.Address, 0, len(tokenSet))
	for addr := range tokenSet {
		tokens = append(tokens, addr)
	}

	metadataMap, err := s.tokenCache.FetchMissing(ctx, tokens)
	if err != nil {
		return err
	}

	for _, e := range events {
		if m, ok := metadataMap[e.TokenAddress]; ok {
			e.TokenSymbol = m.Symbol
			e.TokenDecimals = m.Decimals
			e.TokenName = m.Name
		}
	}

	return nil
}

// enrichBalances fetches balanceOf(proxy) for each unique (token, proxy) pair at the block.
func (s *Service) enrichBalances(ctx context.Context, events []*AllocationEvent, blockNumber int64) error {
	// Deduplicate: multiple events in the same block may reference the same (token, proxy) pair.
	// We fetch balanceOf once per pair — the result is the post-block balance for all of them.
	keySet := make(map[BalanceKey]bool)
	for _, e := range events {
		keySet[BalanceKey{Token: e.TokenAddress, Holder: e.ProxyAddress}] = true
	}

	keys := make([]BalanceKey, 0, len(keySet))
	for k := range keySet {
		keys = append(keys, k)
	}

	balances, err := s.tokenCache.FetchBalances(ctx, keys, blockNumber)
	if err != nil {
		return err
	}

	for _, e := range events {
		key := BalanceKey{Token: e.TokenAddress, Holder: e.ProxyAddress}
		if bal, ok := balances[key]; ok {
			e.Balance = bal
		}
	}

	return nil
}

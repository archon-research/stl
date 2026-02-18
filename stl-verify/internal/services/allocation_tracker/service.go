package allocation_tracker

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
	"github.com/redis/go-redis/v9"

	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// TransactionReceipt mirrors the JSON receipt from Redis.
type TransactionReceipt struct {
	TransactionHash string `json:"transactionHash"`
	BlockNumber     string `json:"blockNumber"`
	BlockHash       string `json:"blockHash"`
	From            string `json:"from"`
	To              string `json:"to"`
	Status          string `json:"status"`
	Logs            []Log  `json:"logs"`
}

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

// BlockEvent is an SQS message with block metadata.
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
	config      Config
	sqsClient   *sqs.Client
	redis       *redis.Client
	ethClient   EthClient
	extractor   *TransferExtractor
	registry    *SourceRegistry
	entryLookup map[EntryKey]*TokenEntry
	entries     []*TokenEntry
	handler     AllocationHandler
	ctx         context.Context
	cancel      context.CancelFunc
	logger      *slog.Logger
}

// EthClient is the minimal interface for querying block numbers.
type EthClient interface {
	BlockNumber(ctx context.Context) (uint64, error)
}

func NewService(
	config Config,
	sqsClient *sqs.Client,
	redisClient *redis.Client,
	ethClient EthClient,
	registry *SourceRegistry,
	entries []*TokenEntry,
	handler AllocationHandler,
	proxies []ProxyConfig,
) (*Service, error) {
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
	if config.SweepInterval == 0 {
		config.SweepInterval = defaults.SweepInterval
	}
	if config.Logger == nil {
		config.Logger = defaults.Logger
	}
	if len(proxies) == 0 {
		proxies = DefaultProxies()
	}
	if len(entries) == 0 {
		entries = DefaultTokenEntries()
	}

	return &Service{
		config:      config,
		sqsClient:   sqsClient,
		redis:       redisClient,
		ethClient:   ethClient,
		extractor:   NewTransferExtractor(proxies),
		registry:    registry,
		entryLookup: BuildEntryLookup(entries),
		entries:     entries,
		handler:     handler,
		logger:      config.Logger.With("component", "allocation-tracker"),
	}, nil
}

func (s *Service) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)
	go s.processLoop()
	go s.sweepLoop()
	s.logger.Info("started", "queue", s.config.QueueURL, "entries", len(s.entries), "sweep", s.config.SweepInterval)
	return nil
}

func (s *Service) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.logger.Info("stopped")
	return nil
}

// ── Event-driven loop ──

func (s *Service) processLoop() {
	ticker := time.NewTicker(s.config.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if err := s.pollMessages(s.ctx); err != nil {
				s.logger.Error("poll error", "error", err)
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
		return fmt.Errorf("receive: %w", err)
	}

	var errs []error
	for _, msg := range result.Messages {
		if err := s.processMessage(ctx, msg); err != nil {
			s.logger.Error("process failed", "error", err)
			errs = append(errs, err)
			continue
		}
		if _, deleteErr := s.sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
			QueueUrl:      aws.String(s.config.QueueURL),
			ReceiptHandle: msg.ReceiptHandle,
		}); deleteErr != nil {
			s.logger.Error("delete failed", "error", deleteErr)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (s *Service) processMessage(ctx context.Context, msg types.Message) error {
	if msg.Body == nil {
		return fmt.Errorf("nil body")
	}
	var event BlockEvent
	if err := json.Unmarshal([]byte(*msg.Body), &event); err != nil {
		return fmt.Errorf("parse event: %w", err)
	}
	return s.processBlock(ctx, event)
}

func (s *Service) processBlock(ctx context.Context, event BlockEvent) error {
	start := time.Now()

	receiptsJSON, err := s.redis.Get(ctx, event.CacheKey()).Result()
	if errors.Is(err, redis.Nil) {
		s.logger.Warn("cache miss", "block", event.BlockNumber, "chain", event.ChainID)
		return nil
	}
	if err != nil {
		return fmt.Errorf("redis get: %w", err)
	}

	var receipts []TransactionReceipt
	if err := shared.ParseCompressedJSON([]byte(receiptsJSON), &receipts); err != nil {
		return fmt.Errorf("parse receipts: %w", err)
	}

	// Extract transfers involving proxies
	var transfers []*TransferEvent
	for _, receipt := range receipts {
		transfers = append(transfers, s.extractor.Extract(receipt)...)
	}
	if len(transfers) == 0 {
		return nil
	}

	// Match to registry entries
	affected := s.matchTransfers(transfers)
	if len(affected) == 0 {
		return nil
	}

	// Fetch balances via source registry
	balances, err := s.registry.FetchAll(ctx, affected, event.BlockNumber)
	if err != nil {
		s.logger.Warn("partial balance fetch", "error", err)
	}

	// Build and dispatch snapshots
	snapshots := s.buildSnapshots(affected, balances, transfers, event)
	if len(snapshots) == 0 {
		return nil
	}

	if err := s.handler.HandleSnapshots(ctx, snapshots); err != nil {
		return fmt.Errorf("handler: %w", err)
	}

	s.logger.Debug("block processed",
		"block", event.BlockNumber,
		"chain", event.ChainID,
		"transfers", len(transfers),
		"snapshots", len(snapshots),
		"duration", time.Since(start))

	return nil
}

func (s *Service) matchTransfers(transfers []*TransferEvent) []*TokenEntry {
	seen := make(map[EntryKey]bool)
	var matched []*TokenEntry
	for _, t := range transfers {
		key := EntryKey{ContractAddress: t.TokenAddress, WalletAddress: t.ProxyAddress}
		if seen[key] {
			continue
		}
		if entry, ok := s.entryLookup[key]; ok {
			matched = append(matched, entry)
			seen[key] = true
		}
	}
	return matched
}

func (s *Service) buildSnapshots(
	entries []*TokenEntry,
	balances map[EntryKey]*PositionBalance,
	transfers []*TransferEvent,
	event BlockEvent,
) []*PositionSnapshot {
	// Build transfer lookup
	tLookup := make(map[EntryKey]*TransferEvent)
	for _, t := range transfers {
		key := EntryKey{ContractAddress: t.TokenAddress, WalletAddress: t.ProxyAddress}
		if _, exists := tLookup[key]; !exists {
			tLookup[key] = t
		}
	}

	var snapshots []*PositionSnapshot
	for _, entry := range entries {
		bal, ok := balances[entry.Key()]
		if !ok {
			continue // skip/stub source
		}

		snap := &PositionSnapshot{
			Entry:         entry,
			Balance:       bal.Balance,
			ScaledBalance: bal.ScaledBalance,
			ChainID:       event.ChainID,
			BlockNumber:   event.BlockNumber,
			BlockVersion:  event.Version,
		}
		if t, ok := tLookup[entry.Key()]; ok {
			snap.TxHash = t.TxHash
			snap.LogIndex = t.LogIndex
			snap.TxAmount = t.Amount
			snap.Direction = t.Direction
		}
		snapshots = append(snapshots, snap)
	}
	return snapshots
}

// ── Periodic sweep ──

func (s *Service) sweepLoop() {
	ticker := time.NewTicker(s.config.SweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if err := s.sweep(s.ctx); err != nil {
				s.logger.Error("sweep failed", "error", err)
			}
		}
	}
}

func (s *Service) sweep(ctx context.Context) error {
	start := time.Now()

	// Get current block number before fetching balances
	blockNum, err := s.ethClient.BlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("get block number: %w", err)
	}

	balances, err := s.registry.FetchAll(ctx, s.entries, 0) // 0 = latest
	if err != nil {
		s.logger.Warn("sweep partial failure", "error", err)
	}

	var snapshots []*PositionSnapshot
	for _, entry := range s.entries {
		bal, ok := balances[entry.Key()]
		if !ok {
			continue
		}
		snapshots = append(snapshots, &PositionSnapshot{
			Entry:         entry,
			Balance:       bal.Balance,
			ScaledBalance: bal.ScaledBalance,
			ChainID:       ChainNameToID[entry.Chain],
			BlockNumber:   int64(blockNum),
			TxAmount:      big.NewInt(0),
		})
	}

	if len(snapshots) == 0 {
		return nil
	}

	if err := s.handler.HandleSnapshots(ctx, snapshots); err != nil {
		return fmt.Errorf("sweep handler: %w", err)
	}

	s.logger.Info("sweep complete", "snapshots", len(snapshots), "duration", time.Since(start))
	return nil
}

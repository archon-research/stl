// Package oracle_price_worker provides an SQS consumer that fetches onchain oracle
// prices for each new block and stores them in the database.
package oracle_price_worker

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
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// SQSClient defines the SQS operations used by this service.
// Satisfied by *sqs.Client from the AWS SDK.
type SQSClient interface {
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
}

// Config holds configuration for the oracle price worker.
type Config struct {
	QueueURL        string
	MaxMessages     int32
	WaitTimeSeconds int32
	PollInterval    time.Duration
	OracleSource    string // e.g., "sparklend"
	Logger          *slog.Logger
}

func configDefaults() Config {
	return Config{
		MaxMessages:     10,
		WaitTimeSeconds: 20,
		PollInterval:    100 * time.Millisecond,
		OracleSource:    "sparklend",
		Logger:          slog.Default(),
	}
}

// Service processes SQS block events and fetches oracle prices for each block.
type Service struct {
	config      Config
	sqsClient   SQSClient
	repo        outbound.OnchainPriceRepository
	multicaller outbound.Multicaller

	oracleABI *abi.ABI

	// tokenAddressMap is the token_id → on-chain address mapping, provided at construction.
	tokenAddressMap map[int64]common.Address

	oracle     *entity.Oracle
	assets     []*entity.OracleAsset
	tokenAddrs []common.Address  // ordered list of token addresses for oracle call
	tokenIDs   []int64           // parallel to tokenAddrs
	oracleAddr common.Address    // oracle contract address
	priceCache map[int64]float64 // tokenID → last stored price (for change detection)

	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger
}

// NewService creates a new oracle price worker service.
// tokenAddresses maps token_id → on-chain address (loaded from the token table).
func NewService(
	config Config,
	sqsClient SQSClient,
	multicaller outbound.Multicaller,
	repo outbound.OnchainPriceRepository,
	tokenAddresses map[int64]common.Address,
) (*Service, error) {
	if sqsClient == nil {
		return nil, fmt.Errorf("sqsClient cannot be nil")
	}
	if multicaller == nil {
		return nil, fmt.Errorf("multicaller cannot be nil")
	}
	if repo == nil {
		return nil, fmt.Errorf("repo cannot be nil")
	}
	if len(tokenAddresses) == 0 {
		return nil, fmt.Errorf("tokenAddresses cannot be empty")
	}

	defaults := configDefaults()
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
	if config.OracleSource == "" {
		config.OracleSource = defaults.OracleSource
	}
	if config.Logger == nil {
		config.Logger = defaults.Logger
	}

	oracleABI, err := abis.GetSparkLendOracleABI()
	if err != nil {
		return nil, fmt.Errorf("loading SparkLend Oracle ABI: %w", err)
	}

	return &Service{
		config:          config,
		sqsClient:       sqsClient,
		repo:            repo,
		multicaller:     multicaller,
		oracleABI:       oracleABI,
		tokenAddressMap: tokenAddresses,
		priceCache:      make(map[int64]float64),
		logger:          config.Logger.With("component", "oracle-price-worker"),
	}, nil
}

// Start initializes the service and begins processing SQS messages.
func (s *Service) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	if err := s.initialize(s.ctx); err != nil {
		return fmt.Errorf("initializing: %w", err)
	}

	go s.processLoop()

	s.logger.Info("oracle price worker started",
		"queue", s.config.QueueURL,
		"oracleSource", s.config.OracleSource,
		"assets", len(s.assets))
	return nil
}

// Stop stops the service.
func (s *Service) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.logger.Info("oracle price worker stopped")
	return nil
}

func (s *Service) initialize(ctx context.Context) error {
	oracle, err := s.repo.GetOracle(ctx, s.config.OracleSource)
	if err != nil {
		return fmt.Errorf("getting oracle source: %w", err)
	}
	s.oracle = oracle
	s.oracleAddr = common.Address(oracle.Address)

	assets, err := s.repo.GetEnabledAssets(ctx, oracle.ID)
	if err != nil {
		return fmt.Errorf("getting enabled assets: %w", err)
	}
	if len(assets) == 0 {
		return fmt.Errorf("no enabled assets for oracle source %s", oracle.Name)
	}
	s.assets = assets

	// Build ordered token address and ID lists
	s.tokenAddrs = make([]common.Address, len(assets))
	s.tokenIDs = make([]int64, len(assets))
	for i, asset := range assets {
		addr, ok := s.tokenAddressMap[asset.TokenID]
		if !ok {
			return fmt.Errorf("token address not found for token_id %d", asset.TokenID)
		}
		s.tokenAddrs[i] = addr
		s.tokenIDs[i] = asset.TokenID
	}

	// Initialize price cache from DB
	cached, err := s.repo.GetLatestPrices(ctx, oracle.ID)
	if err != nil {
		return fmt.Errorf("loading latest prices: %w", err)
	}
	s.priceCache = cached

	s.logger.Info("initialized",
		"oracleSource", oracle.Name,
		"assets", len(assets),
		"cachedPrices", len(cached))

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
	result, err := s.sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(s.config.QueueURL),
		MaxNumberOfMessages: s.config.MaxMessages,
		WaitTimeSeconds:     s.config.WaitTimeSeconds,
		VisibilityTimeout:   30,
	})
	if err != nil {
		return fmt.Errorf("receiving messages: %w", err)
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

// blockEvent is the SQS message payload for block events.
type blockEvent struct {
	ChainID        int64  `json:"chainId"`
	BlockNumber    int64  `json:"blockNumber"`
	Version        int    `json:"version"`
	BlockHash      string `json:"blockHash"`
	BlockTimestamp int64  `json:"blockTimestamp"`
	IsReorg        bool   `json:"isReorg,omitempty"`
}

func (s *Service) processMessage(ctx context.Context, msg sqstypes.Message) error {
	if msg.Body == nil {
		return fmt.Errorf("message body is nil")
	}

	var event blockEvent
	if err := json.Unmarshal([]byte(*msg.Body), &event); err != nil {
		return fmt.Errorf("parsing block event: %w", err)
	}

	return s.processBlock(ctx, event)
}

func (s *Service) processBlock(ctx context.Context, event blockEvent) error {
	prices, err := blockchain.FetchOraclePrices(
		ctx,
		s.multicaller,
		s.oracleABI,
		s.oracleAddr,
		s.tokenAddrs,
		event.BlockNumber,
	)
	if err != nil {
		return fmt.Errorf("fetching oracle prices at block %d: %w", event.BlockNumber, err)
	}

	if len(prices) != len(s.tokenIDs) {
		return fmt.Errorf("price count mismatch: expected %d, got %d", len(s.tokenIDs), len(prices))
	}

	changed := s.detectChanges(prices, event)

	if len(changed) == 0 {
		s.logger.Debug("no price changes", "block", event.BlockNumber)
		return nil
	}

	if err := s.repo.UpsertPrices(ctx, changed); err != nil {
		return fmt.Errorf("storing prices at block %d: %w", event.BlockNumber, err)
	}

	s.logger.Info("stored oracle prices",
		"block", event.BlockNumber,
		"changed", len(changed),
		"total", len(s.tokenIDs))

	return nil
}

func (s *Service) detectChanges(rawPrices []*big.Int, event blockEvent) []*entity.OnchainTokenPrice {
	blockTimestamp := time.Unix(event.BlockTimestamp, 0).UTC()
	oracleID := int16(s.oracle.ID)

	var changed []*entity.OnchainTokenPrice
	for i, rawPrice := range rawPrices {
		priceUSD := blockchain.ConvertOraclePriceToUSD(rawPrice)
		tokenID := s.tokenIDs[i]

		if cachedPrice, ok := s.priceCache[tokenID]; ok && cachedPrice == priceUSD {
			continue
		}

		price, err := entity.NewOnchainTokenPrice(
			tokenID,
			oracleID,
			event.BlockNumber,
			int16(event.Version),
			blockTimestamp,
			priceUSD,
		)
		if err != nil {
			s.logger.Error("invalid price entity", "tokenID", tokenID, "error", err)
			continue
		}
		changed = append(changed, price)
		s.priceCache[tokenID] = priceUSD
	}

	return changed
}

package transfer_user_discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/common/sqsutil"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

type snapshotter interface {
	SnapshotUserWithTx(
		ctx context.Context,
		tx pgx.Tx,
		chainID int64,
		protocolAddress common.Address,
		userAddress common.Address,
		blockNumber int64,
		blockVersion int,
		eventType string,
		txHash []byte,
	) error
}

type Service struct {
	config      shared.SQSConsumerConfig
	consumer    outbound.SQSConsumer
	cacheReader outbound.BlockCacheReader
	txManager   outbound.TxManager
	userRepo    outbound.UserRepository
	snapshotter snapshotter
	extractor   *TransferExtractor
	logger      *slog.Logger
	ctx         context.Context
	cancel      context.CancelFunc
}

func NewService(
	config shared.SQSConsumerConfig,
	consumer outbound.SQSConsumer,
	cacheReader outbound.BlockCacheReader,
	txManager outbound.TxManager,
	userRepo outbound.UserRepository,
	snapshotter snapshotter,
	trackedTokens []outbound.TrackedReceiptToken,
	logger *slog.Logger,
) (*Service, error) {
	config.ApplyDefaults()
	if err := validateConfig(config); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}
	if err := validateDependencies(consumer, cacheReader, txManager, userRepo, snapshotter); err != nil {
		return nil, err
	}
	logger = resolveLogger(config, logger)

	return &Service{
		config:      config,
		consumer:    consumer,
		cacheReader: cacheReader,
		txManager:   txManager,
		userRepo:    userRepo,
		snapshotter: snapshotter,
		extractor:   NewTransferExtractor(trackedTokens),
		logger:      logger.With("component", "transfer-user-discovery"),
	}, nil
}

func validateConfig(config shared.SQSConsumerConfig) error {
	return config.Validate()
}

func validateDependencies(
	consumer outbound.SQSConsumer,
	cacheReader outbound.BlockCacheReader,
	txManager outbound.TxManager,
	userRepo outbound.UserRepository,
	snapshotter snapshotter,
) error {
	switch {
	case consumer == nil:
		return fmt.Errorf("consumer is required")
	case cacheReader == nil:
		return fmt.Errorf("cacheReader is required")
	case txManager == nil:
		return fmt.Errorf("txManager is required")
	case userRepo == nil:
		return fmt.Errorf("userRepo is required")
	case snapshotter == nil:
		return fmt.Errorf("snapshotter is required")
	default:
		return nil
	}
}

func resolveLogger(config shared.SQSConsumerConfig, logger *slog.Logger) *slog.Logger {
	if logger != nil {
		return logger
	}

	return config.Logger
}

func (s *Service) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	go sqsutil.RunLoop(s.ctx, sqsutil.Config{
		Consumer:     s.consumer,
		MaxMessages:  s.config.MaxMessages,
		PollInterval: s.config.PollInterval,
		Logger:       s.logger,
		ChainID:      s.config.ChainID,
	}, s.processBlockEvent)

	return nil
}

func (s *Service) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	return nil
}

func (s *Service) processBlockEvent(ctx context.Context, event outbound.BlockEvent) error {
	receipts, err := s.loadReceipts(ctx, event)
	if err != nil {
		return err
	}

	candidates := s.uniqueCandidates(receipts, event.BlockNumber, event.Version)
	for _, candidate := range candidates {
		if err := s.processCandidate(ctx, event.ChainID, event.BlockNumber, event.Version, candidate); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) loadReceipts(ctx context.Context, event outbound.BlockEvent) ([]shared.TransactionReceipt, error) {
	receiptsData, err := s.cacheReader.GetReceipts(ctx, event.ChainID, event.BlockNumber, event.Version)
	if err != nil {
		return nil, fmt.Errorf("fetching receipts from cache: %w", err)
	}
	if receiptsData == nil {
		return nil, fmt.Errorf("receipts not found in cache: chainID=%d block=%d version=%d", event.ChainID, event.BlockNumber, event.Version)
	}

	var receipts []shared.TransactionReceipt
	if err := json.Unmarshal(receiptsData, &receipts); err != nil {
		return nil, fmt.Errorf("unmarshalling receipts: %w", err)
	}

	return receipts, nil
}

type userCandidate struct {
	userAddress     common.Address
	protocolAddress common.Address
	txHash          []byte
}

type candidateSet struct {
	seen map[string]struct{}
	list []userCandidate
}

func (s *Service) uniqueCandidates(receipts []shared.TransactionReceipt, blockNumber int64, blockVersion int) []userCandidate {
	events := s.extractor.Extract(receipts, blockNumber, blockVersion)
	candidates := newCandidateSet(len(events) * 2)

	for _, event := range events {
		candidates.add(event.From, event.ProtocolAddress, event.TxHash)
		candidates.add(event.To, event.ProtocolAddress, event.TxHash)
	}

	return candidates.list
}

func newCandidateSet(capacity int) candidateSet {
	return candidateSet{
		seen: make(map[string]struct{}, capacity),
		list: make([]userCandidate, 0, capacity),
	}
}

func (c *candidateSet) add(userAddress common.Address, protocolAddress common.Address, txHash []byte) {
	if userAddress == (common.Address{}) {
		return
	}

	key := userAddress.Hex() + ":" + protocolAddress.Hex()
	if _, ok := c.seen[key]; ok {
		return
	}

	c.seen[key] = struct{}{}
	c.list = append(c.list, userCandidate{
		userAddress:     userAddress,
		protocolAddress: protocolAddress,
		txHash:          txHash,
	})
}

func (s *Service) processCandidate(
	ctx context.Context,
	chainID int64,
	blockNumber int64,
	blockVersion int,
	candidate userCandidate,
) error {
	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		_, created, err := s.userRepo.EnsureUser(ctx, tx, entity.User{
			ChainID:        chainID,
			Address:        candidate.userAddress,
			FirstSeenBlock: blockNumber,
		})
		if err != nil {
			return fmt.Errorf("ensuring user %s: %w", candidate.userAddress.Hex(), err)
		}
		if !created {
			return nil
		}

		if err := s.snapshotter.SnapshotUserWithTx(
			ctx,
			tx,
			chainID,
			candidate.protocolAddress,
			candidate.userAddress,
			blockNumber,
			blockVersion,
			"Transfer",
			candidate.txHash,
		); err != nil {
			return fmt.Errorf("snapshotting user %s for protocol %s: %w", candidate.userAddress.Hex(), candidate.protocolAddress.Hex(), err)
		}

		return nil
	})
}

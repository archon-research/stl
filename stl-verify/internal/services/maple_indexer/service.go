// Package maple_indexer provides an SQS consumer that fetches Maple Finance
// borrower debt and collateral snapshots on each new block, then persists the
// results to the database.
package maple_indexer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Config holds configuration for the maple indexer service.
type Config struct {
	// MaxMessages is the maximum number of SQS messages to receive per poll.
	MaxMessages int
	// PollInterval is the time between SQS polls.
	PollInterval time.Duration
	// ChainID is the chain ID for user lookups (default 1 for Ethereum mainnet).
	ChainID int64
	// ProtocolAddress identifies the Maple Finance protocol for DB lookup.
	ProtocolAddress common.Address
	// Logger is the structured logger.
	Logger *slog.Logger
}

func configDefaults() Config {
	return Config{
		MaxMessages:  10,
		PollInterval: 100 * time.Millisecond,
		ChainID:      1,
		Logger:       slog.Default(),
	}
}

// Service processes SQS block events, fetches Maple positions and collateral
// breakdowns via GraphQL, and persists the results.
type Service struct {
	config       Config
	consumer     outbound.SQSConsumer
	mapleAPI     outbound.MapleClient
	positionRepo outbound.PositionRepository
	userRepo     outbound.UserRepository
	tokenRepo    outbound.TokenRepository
	txManager    outbound.TxManager
	protocolRepo outbound.ProtocolRepository
	protocolID   int64

	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger
}

// NewService creates a new maple indexer service.
func NewService(
	config Config,
	consumer outbound.SQSConsumer,
	mapleAPI outbound.MapleClient,
	txManager outbound.TxManager,
	userRepo outbound.UserRepository,
	tokenRepo outbound.TokenRepository,
	positionRepo outbound.PositionRepository,
	protocolRepo outbound.ProtocolRepository,
) (*Service, error) {
	if consumer == nil {
		return nil, fmt.Errorf("consumer cannot be nil")
	}
	if mapleAPI == nil {
		return nil, fmt.Errorf("mapleAPI cannot be nil")
	}
	if txManager == nil {
		return nil, fmt.Errorf("txManager cannot be nil")
	}
	if userRepo == nil {
		return nil, fmt.Errorf("userRepo cannot be nil")
	}
	if tokenRepo == nil {
		return nil, fmt.Errorf("tokenRepo cannot be nil")
	}
	if positionRepo == nil {
		return nil, fmt.Errorf("positionRepo cannot be nil")
	}
	if protocolRepo == nil {
		return nil, fmt.Errorf("protocolRepo cannot be nil")
	}

	defaults := configDefaults()
	if config.MaxMessages == 0 {
		config.MaxMessages = defaults.MaxMessages
	}
	if config.PollInterval == 0 {
		config.PollInterval = defaults.PollInterval
	}
	if config.ChainID == 0 {
		config.ChainID = defaults.ChainID
	}
	if config.Logger == nil {
		config.Logger = defaults.Logger
	}
	if config.ProtocolAddress == (common.Address{}) {
		return nil, fmt.Errorf("protocolAddress must be set")
	}

	return &Service{
		config:       config,
		consumer:     consumer,
		mapleAPI:     mapleAPI,
		positionRepo: positionRepo,
		userRepo:     userRepo,
		tokenRepo:    tokenRepo,
		txManager:    txManager,
		protocolRepo: protocolRepo,
		logger:       config.Logger.With("component", "maple-indexer"),
	}, nil
}

// Start begins processing SQS messages.
func (s *Service) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	protocol, err := s.protocolRepo.GetProtocolByAddress(s.ctx, s.config.ChainID, s.config.ProtocolAddress)
	if err != nil {
		return fmt.Errorf("looking up protocol by address %s: %w", s.config.ProtocolAddress.Hex(), err)
	}
	if protocol == nil {
		return fmt.Errorf("protocol not found for address %s on chain %d", s.config.ProtocolAddress.Hex(), s.config.ChainID)
	}
	s.protocolID = protocol.ID

	go s.processLoop()

	s.logger.Info("maple indexer started",
		"protocolID", s.protocolID,
		"protocolAddress", s.config.ProtocolAddress.Hex(),
		"chainID", s.config.ChainID)
	return nil
}

// Stop stops the service.
func (s *Service) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	if err := s.consumer.Close(); err != nil {
		return fmt.Errorf("closing consumer: %w", err)
	}
	s.logger.Info("maple indexer stopped")
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
	messages, err := s.consumer.ReceiveMessages(ctx, s.config.MaxMessages)
	if err != nil {
		return fmt.Errorf("receiving messages: %w", err)
	}

	if len(messages) == 0 {
		return nil
	}

	s.logger.Info("received messages", "count", len(messages))

	var errs []error
	for _, msg := range messages {
		if err := s.processMessage(ctx, msg); err != nil {
			s.logger.Error("failed to process message",
				"messageID", msg.MessageID,
				"error", err)
			errs = append(errs, err)
			continue
		}

		if deleteErr := s.consumer.DeleteMessage(ctx, msg.ReceiptHandle); deleteErr != nil {
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

func (s *Service) processMessage(ctx context.Context, msg outbound.SQSMessage) error {
	var event blockEvent
	if err := json.Unmarshal([]byte(msg.Body), &event); err != nil {
		return fmt.Errorf("parsing block event: %w", err)
	}

	return s.processBlock(ctx, event)
}

func (s *Service) processBlock(ctx context.Context, event blockEvent) error {
	if event.BlockNumber <= 0 {
		return fmt.Errorf("invalid block number %d", event.BlockNumber)
	}

	if event.ChainID != s.config.ChainID {
		return fmt.Errorf("unexpected chain ID %d for block %d", event.ChainID, event.BlockNumber)
	}

	if event.BlockHash == "" {
		return fmt.Errorf("block hash missing for block %d", event.BlockNumber)
	}

	pools, err := s.mapleAPI.ListPools(ctx)
	if err != nil {
		return fmt.Errorf("listing maple pools: %w", err)
	}

	if len(pools) == 0 {
		s.logger.Debug("no maple pools found", "block", event.BlockNumber)
		return nil
	}

	txHash := common.FromHex(event.BlockHash)
	blockNumber := event.BlockNumber
	blockVersion := event.Version
	userCache := make(map[common.Address]int64)
	tokenCache := make(map[string]int64)

	var borrowers []*entity.Borrower
	var collaterals []*entity.BorrowerCollateral
	var errs []error

	for _, pool := range pools {
		loans, err := s.mapleAPI.GetBorrowerCollateralAtBlock(ctx, pool.Address, uint64(blockNumber))
		if err != nil {
			s.logger.Error("failed to fetch borrower collateral",
				"pool", pool.Address.Hex(),
				"error", err)
			errs = append(errs, fmt.Errorf("pool %s: %w", pool.Address.Hex(), err))
			continue
		}

		for _, loan := range loans {
			userID, err := s.getOrCreateUserID(ctx, loan.Borrower, blockNumber, userCache)
			if err != nil {
				s.logger.Error("failed to resolve borrower user",
					"borrower", loan.Borrower.Hex(),
					"pool", pool.Address.Hex(),
					"error", err)
				errs = append(errs, fmt.Errorf("borrower %s: %w", loan.Borrower.Hex(), err))
				continue
			}

			loanTokenID, err := s.getTokenID(ctx, pool.AssetSymbol, tokenCache)
			if err != nil {
				s.logger.Error("failed to resolve loan token",
					"symbol", pool.AssetSymbol,
					"pool", pool.Address.Hex(),
					"error", err)
				errs = append(errs, fmt.Errorf("loan token %s: %w", pool.AssetSymbol, err))
				continue
			}

			borrowers = append(borrowers, &entity.Borrower{
				UserID:       userID,
				ProtocolID:   s.protocolID,
				TokenID:      loanTokenID,
				BlockNumber:  blockNumber,
				BlockVersion: blockVersion,
				Amount:       loan.PrincipalOwed,
				Change:       big.NewInt(0),
				EventType:    entity.EventMapleSnapshot,
				TxHash:       txHash,
			})

			collateralTokenID, err := s.getTokenID(ctx, loan.Collateral.Asset, tokenCache)
			if err != nil {
				s.logger.Error("failed to resolve collateral token",
					"symbol", loan.Collateral.Asset,
					"pool", pool.Address.Hex(),
					"error", err)
				errs = append(errs, fmt.Errorf("collateral token %s: %w", loan.Collateral.Asset, err))
				continue
			}

			collaterals = append(collaterals, &entity.BorrowerCollateral{
				UserID:            userID,
				ProtocolID:        s.protocolID,
				TokenID:           collateralTokenID,
				BlockNumber:       blockNumber,
				BlockVersion:      blockVersion,
				Amount:            loan.Collateral.AssetAmount,
				Change:            big.NewInt(0),
				EventType:         entity.EventMapleSnapshot,
				TxHash:            txHash,
				CollateralEnabled: isCollateralEnabled(loan.Collateral.State),
			})
		}
	}


	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		if err := s.positionRepo.UpsertBorrowersTx(ctx, tx, borrowers); err != nil {
			return fmt.Errorf("persisting borrowers: %w", err)
		}

		if err := s.positionRepo.UpsertBorrowerCollateralTx(ctx, tx, collaterals); err != nil {
			return fmt.Errorf("persisting borrower collateral: %w", err)
		}
		
		return nil
	})
}

func (s *Service) getOrCreateUserID(ctx context.Context, address common.Address, blockNumber int64, cache map[common.Address]int64) (int64, error) {
	if userID, ok := cache[address]; ok {
		return userID, nil
	}

	var userID int64
	err := s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		id, err := s.userRepo.GetOrCreateUser(ctx, tx, entity.User{
			ChainID:        s.config.ChainID,
			Address:        address,
			FirstSeenBlock: blockNumber,
		})
		if err != nil {
			return err
		}
		userID = id
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("ensuring user %s: %w", address.Hex(), err)
	}

	cache[address] = userID
	return userID, nil
}

func (s *Service) getTokenID(ctx context.Context, symbol string, cache map[string]int64) (int64, error) {
	key := strings.ToUpper(symbol)
	if tokenID, ok := cache[key]; ok {
		return tokenID, nil
	}

	tokenID, err := s.tokenRepo.GetTokenIDBySymbol(ctx, s.config.ChainID, symbol)
	if err != nil {
		return 0, err
	}

	cache[key] = tokenID
	return tokenID, nil
}

func isCollateralEnabled(state string) bool {
	return strings.EqualFold(state, "Deposited")
}

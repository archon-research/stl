// Package maple_indexer provides an SQS consumer that fetches Maple Finance
// borrower debt and collateral snapshots on each new block, then persists the
// results to the database.
package maple_indexer

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/common/sqsutil"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Track loan metadata for entity creation
// only used internally while processing a block, not persisted as-is
type loanData struct {
	borrower                  common.Address
	loanProtocolAssetID       int64
	collateralProtocolAssetID int64
	loan                      outbound.MapleActiveLoan
}

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
	config            Config
	consumer          outbound.SQSConsumer
	mapleAPI          outbound.MapleClient
	positionRepo      outbound.PositionRepository
	userRepo          outbound.UserRepository
	protocolAssetRepo outbound.ProtocolAssetRepository
	txManager         outbound.TxManager
	protocolRepo      outbound.ProtocolRepository
	protocolID        int64

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
	protocolAssetRepo outbound.ProtocolAssetRepository,
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
	if protocolAssetRepo == nil {
		return nil, fmt.Errorf("protocolAssetRepo cannot be nil")
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
		config:            config,
		consumer:          consumer,
		mapleAPI:          mapleAPI,
		positionRepo:      positionRepo,
		userRepo:          userRepo,
		protocolAssetRepo: protocolAssetRepo,
		txManager:         txManager,
		protocolRepo:      protocolRepo,
		logger:            config.Logger.With("component", "maple-indexer"),
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

	go sqsutil.RunLoop(s.ctx, sqsutil.Config{
		Consumer:     s.consumer,
		MaxMessages:  s.config.MaxMessages,
		PollInterval: s.config.PollInterval,
		Logger:       s.logger,
	}, s.processBlock)

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

func (s *Service) processBlock(ctx context.Context, event outbound.BlockEvent) error {
	if event.BlockNumber <= 0 {
		return fmt.Errorf("invalid block number %d", event.BlockNumber)
	}

	if event.ChainID != s.config.ChainID {
		return fmt.Errorf("unexpected chain ID %d for block %d", event.ChainID, event.BlockNumber)
	}

	if event.BlockHash == "" {
		return fmt.Errorf("block hash missing for block %d", event.BlockNumber)
	}

	// Fetch loans from Maple API
	loans, err := s.mapleAPI.GetAllActiveLoansAtBlock(ctx, uint64(event.BlockNumber))
	if err != nil {
		return fmt.Errorf("fetching active loans: %w", err)
	}

	if len(loans) == 0 {
		s.logger.Debug("no active loans found", "block", event.BlockNumber)
		return nil
	}

	// Build entities (no transactions)
	addresses := collectUniqueBorrowerAddresses(loans)
	blockNumber := event.BlockNumber
	blockVersion := event.Version
	tokenCache := make(map[string]int64)

	// Pre-resolve protocol assets before transaction - fail fast on any error
	loansWithTokens := make([]loanData, 0, len(loans))

	for _, loan := range loans {
		// Resolve both protocol assets - fail immediately if either fails
		loanProtocolAssetID, err := s.getProtocolAssetID(ctx, loan.PoolAssetSymbol, tokenCache)
		if err != nil {
			return fmt.Errorf("resolving loan token %s for pool %s: %w", loan.PoolAssetSymbol, loan.PoolName, err)
		}

		collateralProtocolAssetID, err := s.getProtocolAssetID(ctx, loan.Collateral.Asset, tokenCache)
		if err != nil {
			return fmt.Errorf("resolving collateral token %s for pool %s: %w", loan.Collateral.Asset, loan.PoolName, err)
		}

		loansWithTokens = append(loansWithTokens, loanData{
			borrower:                  loan.Borrower,
			loanProtocolAssetID:       loanProtocolAssetID,
			collateralProtocolAssetID: collateralProtocolAssetID,
			loan:                      loan,
		})
	}

	// Persist users and positions in a single transaction
	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		// Resolve users first
		userCache, err := s.resolveUsersInTx(ctx, tx, addresses, blockNumber)
		if err != nil {
			return fmt.Errorf("resolving users: %w", err)
		}

		// Build entities with user IDs
		borrowers := make([]*entity.Borrower, 0, len(loansWithTokens))
		collaterals := make([]*entity.BorrowerCollateral, 0, len(loansWithTokens))

		for _, ld := range loansWithTokens {
			userID := userCache[ld.borrower]

			borrowers = append(borrowers, &entity.Borrower{
				UserID:          userID,
				ProtocolID:      s.protocolID,
				ProtocolAssetID: ld.loanProtocolAssetID,
				BlockNumber:     blockNumber,
				BlockVersion:    blockVersion,
				Amount:          ld.loan.PrincipalOwed,
				Change:          big.NewInt(0), // For maple we do not have incremental events, only snapshots, so change is always 0
				EventType:       entity.EventMapleSnapshot,
				TxHash:          nil,
			})

			collaterals = append(collaterals, &entity.BorrowerCollateral{
				UserID:            userID,
				ProtocolID:        s.protocolID,
				ProtocolAssetID:   ld.collateralProtocolAssetID,
				BlockNumber:       blockNumber,
				BlockVersion:      blockVersion,
				Amount:            ld.loan.Collateral.AssetAmount,
				Change:            big.NewInt(0), // For maple we do not have incremental events, only snapshots, so change is always 0
				EventType:         entity.EventMapleSnapshot,
				TxHash:            nil,
				CollateralEnabled: isCollateralEnabled(ld.loan.Collateral.State),
			})
		}

		// Persist positions
		if err := s.positionRepo.UpsertBorrowersTx(ctx, tx, borrowers); err != nil {
			return fmt.Errorf("persisting borrowers: %w", err)
		}

		if err := s.positionRepo.UpsertBorrowerCollateralTx(ctx, tx, collaterals); err != nil {
			return fmt.Errorf("persisting borrower collateral: %w", err)
		}

		return nil
	})
}

// collectUniqueBorrowerAddresses deduplicates borrower addresses from all loans.
func collectUniqueBorrowerAddresses(loans []outbound.MapleActiveLoan) []common.Address {
	seen := make(map[common.Address]struct{}, len(loans))
	addresses := make([]common.Address, 0, len(loans))
	for _, loan := range loans {
		if _, ok := seen[loan.Borrower]; !ok {
			seen[loan.Borrower] = struct{}{}
			addresses = append(addresses, loan.Borrower)
		}
	}
	return addresses
}

// resolveUsersInTx resolves all borrower addresses to user IDs within an existing transaction.
func (s *Service) resolveUsersInTx(ctx context.Context, tx pgx.Tx, addresses []common.Address, blockNumber int64) (map[common.Address]int64, error) {
	userCache := make(map[common.Address]int64, len(addresses))
	for _, addr := range addresses {
		id, err := s.userRepo.GetOrCreateUser(ctx, tx, entity.User{
			ChainID:        s.config.ChainID,
			Address:        addr,
			FirstSeenBlock: blockNumber,
		})

		if err != nil {
			return nil, fmt.Errorf("user %s: %w", addr.Hex(), err)
		}
		userCache[addr] = id
	}
	return userCache, nil
}

func (s *Service) getProtocolAssetID(ctx context.Context, symbol string, cache map[string]int64) (int64, error) {
	key := strings.ToUpper(symbol)
	if id, ok := cache[key]; ok {
		return id, nil
	}

	asset, err := s.protocolAssetRepo.GetByKey(ctx, s.protocolID, key)
	if err != nil {
		return 0, err
	}

	cache[key] = asset.ID
	return asset.ID, nil
}

func isCollateralEnabled(state string) bool {
	return strings.EqualFold(state, "Deposited")
}

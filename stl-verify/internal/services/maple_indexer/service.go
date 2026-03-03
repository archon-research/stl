// Package maple_indexer provides an SQS consumer that fetches Maple Finance
// borrower debt and collateral snapshots on each new block, then persists the
// results to the database.
package maple_indexer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/common/sqsutil"
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
	config            Config
	consumer          outbound.SQSConsumer
	mapleAPI          outbound.MapleClient
	maplePositionRepo outbound.MaplePositionRepository
	userRepo          outbound.UserRepository
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
	maplePositionRepo outbound.MaplePositionRepository,
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
	if maplePositionRepo == nil {
		return nil, fmt.Errorf("maplePositionRepo cannot be nil")
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
		maplePositionRepo: maplePositionRepo,
		userRepo:          userRepo,
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
	if err := validateBlockEvent(event, s.config.ChainID); err != nil {
		return err
	}

	loans, err := s.mapleAPI.GetAllActiveLoansAtBlock(ctx, uint64(event.BlockNumber))
	if err != nil {
		return fmt.Errorf("fetching active loans: %w", err)
	}

	if len(loans) == 0 {
		s.logger.Debug("no active loans found", "block", event.BlockNumber)
		return nil
	}

	addresses := collectUniqueBorrowerAddresses(loans)
	blockNumber := event.BlockNumber
	blockVersion := event.Version

	// Resolve users in a transaction, then build and persist entities
	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		userCache, err := s.resolveUsersInTx(ctx, tx, addresses, blockNumber)
		if err != nil {
			return fmt.Errorf("resolving users: %w", err)
		}

		borrowers, collaterals := s.buildEntities(loans, userCache, blockNumber, blockVersion)

		if err := s.maplePositionRepo.SaveBorrowerSnapshots(ctx, borrowers); err != nil {
			return fmt.Errorf("persisting borrowers: %w", err)
		}

		if err := s.maplePositionRepo.SaveCollateralSnapshots(ctx, collaterals); err != nil {
			return fmt.Errorf("persisting collateral: %w", err)
		}

		return nil
	})
}

// validateBlockEvent checks that the block event has valid fields.
func validateBlockEvent(event outbound.BlockEvent, expectedChainID int64) error {
	if event.BlockNumber <= 0 {
		return fmt.Errorf("invalid block number %d", event.BlockNumber)
	}
	if event.ChainID != expectedChainID {
		return fmt.Errorf("unexpected chain ID %d for block %d", event.ChainID, event.BlockNumber)
	}
	if event.BlockHash == "" {
		return fmt.Errorf("block hash missing for block %d", event.BlockNumber)
	}
	return nil
}

// buildEntities converts Maple API loan data into MapleBorrower and MapleCollateral entities.
func (s *Service) buildEntities(loans []outbound.MapleActiveLoan, userCache map[common.Address]int64, blockNumber int64, blockVersion int) ([]*entity.MapleBorrower, []*entity.MapleCollateral) {
	borrowers := make([]*entity.MapleBorrower, 0, len(loans))
	collaterals := make([]*entity.MapleCollateral, 0, len(loans))

	for _, loan := range loans {
		userID := userCache[loan.Borrower]

		// Extract loanMeta fields (empty strings if loanMeta is nil for external loans)
		var loanType, loanAssetSymbol, loanDexName, loanLocation, loanWalletAddress, loanWalletType string
		if loan.LoanMeta != nil {
			loanType = loan.LoanMeta.Type
			loanAssetSymbol = loan.LoanMeta.AssetSymbol
			loanDexName = loan.LoanMeta.DexName
			loanLocation = loan.LoanMeta.Location
			loanWalletAddress = loan.LoanMeta.WalletAddress
			loanWalletType = loan.LoanMeta.WalletType
		}

		borrowers = append(borrowers, &entity.MapleBorrower{
			UserID:            userID,
			ProtocolID:        s.protocolID,
			PoolAsset:         loan.PoolAssetSymbol,
			PoolDecimals:      loan.PoolAssetDecimals,
			Amount:            loan.PrincipalOwed,
			BlockNumber:       blockNumber,
			BlockVersion:      blockVersion,
			LoanType:          loanType,
			LoanAssetSymbol:   loanAssetSymbol,
			LoanDexName:       loanDexName,
			LoanLocation:      loanLocation,
			LoanWalletAddress: loanWalletAddress,
			LoanWalletType:    loanWalletType,
		})

		collaterals = append(collaterals, &entity.MapleCollateral{
			UserID:             userID,
			ProtocolID:         s.protocolID,
			CollateralAsset:    loan.Collateral.Asset,
			CollateralDecimals: loan.Collateral.Decimals,
			Amount:             loan.Collateral.AssetAmount,
			Custodian:          loan.Collateral.Custodian,
			State:              loan.Collateral.State,
			LiquidationLevel:   loan.Collateral.LiquidationLevel,
			BlockNumber:        blockNumber,
			BlockVersion:       blockVersion,
			LoanType:           loanType,
			LoanAssetSymbol:    loanAssetSymbol,
			LoanDexName:        loanDexName,
			LoanLocation:       loanLocation,
			LoanWalletAddress:  loanWalletAddress,
			LoanWalletType:     loanWalletType,
		})
	}

	return borrowers, collaterals
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

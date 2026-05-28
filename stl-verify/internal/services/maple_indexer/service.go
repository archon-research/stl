package maple_indexer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"slices"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"

	"github.com/archon-research/stl/stl-verify/internal/common/sqsutil"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// Service is the maple-indexer SQS consumer.
//
// One block-event message in → 0+ vault_state rows + 0+ vault_position rows
// out, all written inside a single Tx per (vault, block). The set of vaults
// is static (loaded once from `maple_vault` at startup); the set of users to
// refresh per block is derived from the Deposit / Withdraw / Transfer logs
// emitted by each vault.
type Service struct {
	config Config

	consumer  outbound.SQSConsumer
	cache     outbound.BlockCacheReader
	txManager outbound.TxManager
	userRepo  outbound.UserRepository
	mapleRepo outbound.MapleRepository

	blockchainSvc *BlockchainService
	extractor     *EventExtractor
	registry      *VaultRegistry
	telemetry     *Telemetry

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	logger *slog.Logger
}

// NewService validates dependencies, wires the auxiliary components and
// returns a ready-to-Start Service.
//
// Caller's responsibility: pass non-nil ports for everything except
// telemetry (which is nil-safe). The registry is NOT preloaded here —
// Start() runs LoadFromDB so the service can be constructed in unit
// tests without a DB.
func NewService(
	config Config,
	consumer outbound.SQSConsumer,
	cache outbound.BlockCacheReader,
	multicaller outbound.Multicaller,
	txManager outbound.TxManager,
	userRepo outbound.UserRepository,
	mapleRepo outbound.MapleRepository,
) (*Service, error) {
	if err := validateDependencies(consumer, cache, multicaller, txManager, userRepo, mapleRepo); err != nil {
		return nil, fmt.Errorf("validating dependencies: %w", err)
	}

	config.SQSConsumerConfig.ApplyDefaults()
	if err := config.SQSConsumerConfig.Validate(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}
	if config.Logger == nil {
		return nil, fmt.Errorf("config.Logger is required")
	}

	if _, err := MapleSyrupDeployBlock(config.ChainID); err != nil {
		return nil, fmt.Errorf("getting deploy block: %w", err)
	}

	extractor, err := NewEventExtractor(config.Logger)
	if err != nil {
		return nil, fmt.Errorf("creating event extractor: %w", err)
	}

	bs, err := NewBlockchainService(multicaller, config.Telemetry)
	if err != nil {
		return nil, fmt.Errorf("creating blockchain service: %w", err)
	}

	return &Service{
		config:        config,
		consumer:      consumer,
		cache:         cache,
		txManager:     txManager,
		userRepo:      userRepo,
		mapleRepo:     mapleRepo,
		blockchainSvc: bs,
		extractor:     extractor,
		registry:      NewVaultRegistry(config.Logger),
		telemetry:     config.Telemetry,
		logger:        config.Logger.With("component", "maple-indexer"),
	}, nil
}

// Start loads the vault registry from the DB and begins the SQS poll loop.
// Returns once the goroutine has been launched; cancel ctx to drain.
func (s *Service) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	if err := s.registry.LoadFromDB(s.ctx, s.mapleRepo, s.config.ChainID); err != nil {
		s.cancel()
		return fmt.Errorf("loading vault registry: %w", err)
	}
	if s.registry.Count() == 0 {
		s.cancel()
		return fmt.Errorf("vault registry is empty for chain %d: migration may not have run or chain_id is wrong", s.config.ChainID)
	}

	s.wg.Go(func() {
		sqsutil.RunLoop(s.ctx, sqsutil.Config{
			Consumer:     s.consumer,
			MaxMessages:  s.config.MaxMessages,
			PollInterval: s.config.PollInterval,
			Logger:       s.logger,
			ChainID:      s.config.ChainID,
		}, s.processBlockEvent)
	})

	s.logger.Info("maple indexer started",
		"maxMessages", s.config.MaxMessages,
		"vaults", s.registry.Count())
	return nil
}

// Stop cancels the internal context and waits for the SQS poll loop to exit.
func (s *Service) Stop() error {
	if s.cancel != nil {
		s.cancel()
		s.wg.Wait()
	}
	s.logger.Info("maple indexer stopped")
	return nil
}

// processBlockEvent is the per-message entry point used by sqsutil.RunLoop.
// All work happens inside fetchAndProcessReceipts; this method only exists
// as a stable handler reference for the goroutine started in Start().
func (s *Service) processBlockEvent(ctx context.Context, event outbound.BlockEvent) error {
	return s.fetchAndProcessReceipts(ctx, event)
}

// fetchAndProcessReceipts pulls the block's receipts from cache, decides
// which Syrup vaults were touched, and indexes each one.
//
// Cache miss is a hard error so the SQS message redelivers; tearing down
// the indexer when the cache TTL drops is preferable to silently skipping
// a block.
func (s *Service) fetchAndProcessReceipts(ctx context.Context, event outbound.BlockEvent) (retErr error) {
	ctx, span := s.telemetry.StartBlockSpan(ctx, event.BlockNumber)
	defer span.End()

	start := time.Now()
	defer func() {
		s.telemetry.RecordBlockProcessed(ctx, time.Since(start), retErr)
		if retErr != nil {
			SetSpanError(span, retErr, "block processing failed")
			s.telemetry.RecordError(ctx, "fetchAndProcessReceipts", retErr)
		}
	}()

	receiptsJSON, err := s.cache.GetReceipts(ctx, event.ChainID, event.BlockNumber, event.Version)
	if err != nil {
		return fmt.Errorf("fetching receipts from cache: %w", err)
	}
	if receiptsJSON == nil {
		return fmt.Errorf("receipts not found in cache for block %d (chain=%d, version=%d)",
			event.BlockNumber, event.ChainID, event.Version)
	}

	var receipts []shared.TransactionReceipt
	if err := json.Unmarshal(receiptsJSON, &receipts); err != nil {
		return fmt.Errorf("unmarshalling receipts: %w", err)
	}

	totalLogs := 0
	for _, r := range receipts {
		totalLogs += len(r.Logs)
	}
	span.SetAttributes(
		attribute.Int("receipts.count", len(receipts)),
		attribute.Int("logs.count", totalLogs),
	)

	touched := s.touchedVaults(receipts)
	if len(touched) == 0 {
		s.logger.Debug("no relevant Syrup events in block",
			"block", event.BlockNumber, "receipts", len(receipts), "logs", totalLogs)
		return nil
	}

	blockTimestamp := time.Unix(event.BlockTimestamp, 0).UTC()
	blockNumber := big.NewInt(event.BlockNumber)

	// Deterministic per-vault ordering so concurrent indexer instances
	// (e.g. blue/green deploy overlap) acquire DB locks in the same order
	// and don't deadlock on multi-vault blocks.
	vaultAddrs := make([]common.Address, 0, len(touched))
	for v := range touched {
		vaultAddrs = append(vaultAddrs, v)
	}
	slices.SortFunc(vaultAddrs, func(a, b common.Address) int {
		return bytes.Compare(a.Bytes(), b.Bytes())
	})

	var errs []error
	for _, vaultAddr := range vaultAddrs {
		users := touched[vaultAddr]
		if err := s.indexVault(ctx, vaultAddr, users, event, blockNumber, blockTimestamp); err != nil {
			errs = append(errs, fmt.Errorf("vault %s at block %d: %w", vaultAddr.Hex(), event.BlockNumber, err))
		}
	}
	return errors.Join(errs...)
}

// touchedVaults walks every log in the block and groups touched user
// addresses by the vault that emitted them. Vaults whose entry in the
// returned map is non-nil were touched at this block and need a state +
// position refresh; an empty user set means the vault emitted a relevant
// event with no user-addressable side effect (e.g. a malformed log).
func (s *Service) touchedVaults(receipts []shared.TransactionReceipt) map[common.Address]map[common.Address]struct{} {
	allLogs := make([]shared.Log, 0)
	for _, r := range receipts {
		allLogs = append(allLogs, r.Logs...)
	}
	out := make(map[common.Address]map[common.Address]struct{})
	for _, vault := range s.registry.All() {
		vaultAddr := common.BytesToAddress(vault.Address)
		users, isRelevant := s.extractor.ExtractTouchedAddresses(vaultAddr, allLogs)
		if isRelevant {
			out[vaultAddr] = users
		}
	}
	return out
}

// indexVault writes one maple_vault_state row + N maple_vault_position rows
// for the given vault at the given block, all within one DB transaction.
func (s *Service) indexVault(
	ctx context.Context,
	vaultAddr common.Address,
	users map[common.Address]struct{},
	event outbound.BlockEvent,
	blockNumber *big.Int,
	blockTimestamp time.Time,
) (retErr error) {
	ctx, span := s.telemetry.StartSpan(ctx, "maple.indexVault",
		attribute.String("vault.address", vaultAddr.Hex()),
		attribute.Int64("block.number", event.BlockNumber),
		attribute.Int("users.touched", len(users)))
	defer span.End()
	defer func() {
		if retErr != nil {
			SetSpanError(span, retErr, "indexVault failed")
			s.telemetry.RecordError(ctx, "indexVault", retErr)
		}
	}()

	vault := s.registry.GetVault(vaultAddr)
	if vault == nil {
		// touchedVaults only emits addresses that are in the registry, so
		// reaching this branch means a concurrent reload removed the vault.
		// Surface as an error so the SQS message redelivers and is reprocessed
		// against the now-current registry.
		return fmt.Errorf("vault %s not in registry (registry reload race?)", vaultAddr.Hex())
	}

	stateRaw, err := s.blockchainSvc.FetchVaultState(ctx, vaultAddr, blockNumber)
	if err != nil {
		return fmt.Errorf("fetching vault state: %w", err)
	}

	userAddrs := make([]common.Address, 0, len(users))
	for u := range users {
		userAddrs = append(userAddrs, u)
	}
	// Sort users so per-block multicall input is deterministic — handy when
	// diffing test output and also gives concurrent indexer instances a
	// stable user_id lock-acquisition order under the same defensive
	// rationale as the vault sort above. See ADR-0002 §3.
	slices.SortFunc(userAddrs, func(a, b common.Address) int {
		return bytes.Compare(a.Bytes(), b.Bytes())
	})

	positionsRaw, err := s.blockchainSvc.FetchUserPositions(ctx, vaultAddr, userAddrs, blockNumber)
	if err != nil {
		return fmt.Errorf("fetching user positions: %w", err)
	}

	stateEntity, err := entity.NewMapleVaultState(
		vault.ID, event.BlockNumber, event.Version, blockTimestamp,
		stateRaw.TotalAssets, stateRaw.TotalSupply, stateRaw.SharePrice,
	)
	if err != nil {
		return fmt.Errorf("building vault state entity: %w", err)
	}

	err = s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		if err := s.mapleRepo.SaveVaultState(ctx, tx, stateEntity); err != nil {
			return fmt.Errorf("saving vault state: %w", err)
		}

		positionEntities, err := s.buildPositionEntities(ctx, tx, vault.ID, userAddrs, positionsRaw, event, blockTimestamp)
		if err != nil {
			return err
		}

		if err := s.mapleRepo.SaveVaultPositions(ctx, tx, positionEntities); err != nil {
			return fmt.Errorf("saving vault positions: %w", err)
		}
		s.telemetry.RecordPositionWrites(ctx, int64(len(positionEntities)))
		return nil
	})
	if err != nil {
		return err
	}

	s.telemetry.RecordVaultStateWrite(ctx)
	s.logger.Debug("indexed maple vault block",
		"vault", vaultAddr.Hex(),
		"vault.id", vault.ID,
		"block", event.BlockNumber,
		"users", len(userAddrs),
		"totalAssets", stateRaw.TotalAssets.String(),
		"sharePrice", stateRaw.SharePrice.String())
	return nil
}

// buildPositionEntities upserts user rows and constructs the per-user
// position snapshots. Inputs are pre-sorted so userRepo.GetOrCreateUser
// acquires its per-row locks in a stable order across concurrent indexers.
func (s *Service) buildPositionEntities(
	ctx context.Context,
	tx pgx.Tx,
	vaultID int64,
	users []common.Address,
	positionsRaw map[common.Address]*UserPosition,
	event outbound.BlockEvent,
	blockTimestamp time.Time,
) ([]*entity.MapleVaultPosition, error) {
	out := make([]*entity.MapleVaultPosition, 0, len(users))
	for _, addr := range users {
		raw, ok := positionsRaw[addr]
		if !ok {
			return nil, fmt.Errorf("position missing for user %s", addr.Hex())
		}
		userID, err := s.userRepo.GetOrCreateUser(ctx, tx, entity.User{
			ChainID:        event.ChainID,
			Address:        addr,
			FirstSeenBlock: event.BlockNumber,
		})
		if err != nil {
			return nil, fmt.Errorf("ensuring user %s: %w", addr.Hex(), err)
		}
		pos, err := entity.NewMapleVaultPosition(
			userID, vaultID, event.BlockNumber, event.Version, blockTimestamp,
			raw.Shares, raw.Assets,
		)
		if err != nil {
			return nil, fmt.Errorf("building position entity for user %s: %w", addr.Hex(), err)
		}
		out = append(out, pos)
	}
	return out, nil
}

// validateDependencies ensures every required port is non-nil at construction
// time. We fail fast in NewService rather than nil-panic on first message.
func validateDependencies(
	consumer outbound.SQSConsumer,
	cache outbound.BlockCacheReader,
	multicaller outbound.Multicaller,
	txManager outbound.TxManager,
	userRepo outbound.UserRepository,
	mapleRepo outbound.MapleRepository,
) error {
	if consumer == nil {
		return fmt.Errorf("consumer is required")
	}
	if cache == nil {
		return fmt.Errorf("cache is required")
	}
	if multicaller == nil {
		return fmt.Errorf("multicaller is required")
	}
	if txManager == nil {
		return fmt.Errorf("txManager is required")
	}
	if userRepo == nil {
		return fmt.Errorf("userRepo is required")
	}
	if mapleRepo == nil {
		return fmt.Errorf("mapleRepo is required")
	}
	return nil
}

package allocation_tracker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/core/types"

	"github.com/archon-research/stl/stl-verify/internal/common/sqsutil"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

type TransactionReceipt struct {
	TransactionHash string      `json:"transactionHash"`
	BlockNumber     string      `json:"blockNumber"`
	BlockHash       string      `json:"blockHash"`
	From            string      `json:"from"`
	To              string      `json:"to"`
	Status          string      `json:"status"`
	Logs            []types.Log `json:"logs"`
}

type Service struct {
	config           Config
	sqsConsumer      outbound.SQSConsumer
	cache            outbound.BlockCache
	extractor        *TransferExtractor
	registry         *SourceRegistry
	entryLookup      map[EntryKey]*TokenEntry
	entries          []*TokenEntry
	handler          AllocationHandler
	ctx              context.Context
	cancel           context.CancelFunc
	logger           *slog.Logger
	blocksSinceSweep int
}

func NewService(
	config Config,
	sqsConsumer outbound.SQSConsumer,
	cache outbound.BlockCache,
	registry *SourceRegistry,
	entries []*TokenEntry,
	handler AllocationHandler,
	proxies []ProxyConfig,
) (*Service, error) {
	defaults := ConfigDefaults()
	if config.MaxMessages == 0 {
		config.MaxMessages = defaults.MaxMessages
	}
	if config.PollInterval == 0 {
		config.PollInterval = defaults.PollInterval
	}
	if config.SweepEveryNBlocks == 0 {
		config.SweepEveryNBlocks = defaults.SweepEveryNBlocks
	}
	if config.Logger == nil {
		config.Logger = defaults.Logger
	}
	if config.ChainID == 0 {
		return nil, fmt.Errorf("chain ID is required")
	}
	if len(proxies) == 0 {
		proxies = ProxiesForChainID(DefaultProxies(), config.ChainID)
	}
	if len(entries) == 0 {
		entries = EntriesForChainID(DefaultTokenEntries(), config.ChainID)
	}

	return &Service{
		config:      config,
		sqsConsumer: sqsConsumer,
		cache:       cache,
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

	go sqsutil.RunLoop(s.ctx, sqsutil.Config{
		Consumer:     s.sqsConsumer,
		MaxMessages:  s.config.MaxMessages,
		PollInterval: s.config.PollInterval,
		Logger:       s.logger,
	}, s.processBlock)

	s.logger.Info("started",
		"chainID", s.config.ChainID,
		"entries", len(s.entries),
		"sweepEveryNBlocks", s.config.SweepEveryNBlocks)
	return nil
}

func (s *Service) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.logger.Info("stopped")
	return nil
}

func (s *Service) processBlock(
	ctx context.Context,
	event outbound.BlockEvent,
) error {
	start := time.Now()

	receiptsJSON, err := s.cache.GetReceipts(ctx, event.ChainID, event.BlockNumber, event.Version)
	if err != nil {
		return fmt.Errorf("fetching receipts from cache: %w", err)
	}
	if receiptsJSON == nil {
		return fmt.Errorf("receipts not found in cache for block %d (chain=%d, version=%d)", event.BlockNumber, event.ChainID, event.Version)
	}

	var receipts []TransactionReceipt
	if err := json.Unmarshal(receiptsJSON, &receipts); err != nil {
		return fmt.Errorf("parse receipts: %w", err)
	}

	var transfers []*TransferEvent
	for _, receipt := range receipts {
		transfers = append(transfers, s.extractor.Extract(receipt)...)
	}

	// Process transfers if any matched
	if len(transfers) > 0 {
		affected := s.matchTransfers(transfers)
		if len(affected) > 0 {
			balances, err := s.registry.FetchAll(ctx, affected, event.BlockNumber)
			if err != nil {
				s.logger.Warn("partial balance fetch", "error", err)
			}

			snapshots := s.buildSnapshots(affected, balances, transfers, event)
			if len(snapshots) > 0 {
				if err := s.handler.HandleSnapshots(ctx, snapshots); err != nil {
					return fmt.Errorf("handler: %w", err)
				}
			}

			s.logger.Debug("block processed",
				"block", event.BlockNumber,
				"chain", event.ChainID,
				"transfers", len(transfers),
				"snapshots", len(snapshots),
				"duration", time.Since(start))
		}
	}

	// Periodic sweep
	s.blocksSinceSweep++
	if s.blocksSinceSweep >= s.config.SweepEveryNBlocks {
		s.blocksSinceSweep = 0
		if err := s.sweep(ctx, event.BlockNumber); err != nil {
			s.logger.Error("sweep failed", "error", err)
		}
	}

	return nil
}

func (s *Service) matchTransfers(
	transfers []*TransferEvent,
) []*TokenEntry {
	seen := make(map[EntryKey]bool)
	var matched []*TokenEntry
	for _, t := range transfers {
		key := EntryKey{
			ContractAddress: t.TokenAddress,
			WalletAddress:   t.ProxyAddress,
		}
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
	event outbound.BlockEvent,
) []*PositionSnapshot {
	tLookup := make(map[EntryKey]*TransferEvent)
	for _, t := range transfers {
		key := EntryKey{
			ContractAddress: t.TokenAddress,
			WalletAddress:   t.ProxyAddress,
		}
		if _, exists := tLookup[key]; !exists {
			tLookup[key] = t
		}
	}

	var snapshots []*PositionSnapshot
	for _, entry := range entries {
		bal, ok := balances[entry.Key()]
		if !ok {
			continue
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

// sweep runs periodic reconciliation to capture balance changes that don't
// emit Transfer events — e.g. aToken interest accrual, ERC4626 yield compounding,
// and BUIDL rebases. Without this, positions would drift between transfer-triggered
// snapshots.
func (s *Service) sweep(ctx context.Context, blockNumber int64) error {
	start := time.Now()

	balances, err := s.registry.FetchAll(ctx, s.entries, blockNumber)
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
			ChainID:       s.config.ChainID,
			BlockNumber:   blockNumber,
			TxAmount:      big.NewInt(0),
			Direction:     DirectionSweep,
		})
	}

	if len(snapshots) == 0 {
		return nil
	}

	if err := s.handler.HandleSnapshots(ctx, snapshots); err != nil {
		return fmt.Errorf("sweep handler: %w", err)
	}

	s.logger.Info("sweep complete",
		"block", blockNumber,
		"snapshots", len(snapshots),
		"duration", time.Since(start))
	return nil
}

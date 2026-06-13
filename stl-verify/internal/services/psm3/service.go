// Package psm3 reads Spark PSM3 reserve state (USDS/sUSDS/USDC balances,
// totalAssets and the sUSDS conversion rate) and writes append-only snapshots
// to Postgres.
//
// Flow:
//  1. At Start, resolve and verify the PSM3 immutables via PSM3Caller.ResolveImmutables.
//  2. Consume block events from SQS via sqsutil.RunLoop.
//  3. Every SweepEveryNBlocks, read the reserve state pinned to the event's
//     block via PSM3Caller.ReadState.
//  4. Write one snapshot row via PSM3SnapshotRepository.SaveSnapshot. Any
//     failed call aborts the whole sweep — no partial rows.
package psm3

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/common/sqsutil"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

const defaultSweepEveryNBlocks = 300

// Config holds configuration for Service.
type Config struct {
	// SweepEveryNBlocks controls how often state is read from the chain.
	// Defaults to 300.
	SweepEveryNBlocks int

	// ChainID is the expected chain ID for incoming block events.
	ChainID int64

	// PSM3Address is the PSM3 contract address recorded on every snapshot.
	PSM3Address common.Address

	// MaxMessages is the max number of SQS messages per poll.
	MaxMessages int

	// PollInterval controls how often SQS is polled for new messages.
	PollInterval time.Duration

	// Logger is the structured logger.
	Logger *slog.Logger
}

// Service consumes block events from SQS and periodically reads the PSM3
// reserve state, pinned to the event's block.
type Service struct {
	config           Config
	caller           outbound.PSM3Caller
	repo             outbound.PSM3SnapshotRepository
	sqsConsumer      outbound.SQSConsumer
	blockQuerier     entity.BlockQuerier
	logger           *slog.Logger
	blocksSinceSweep int
	wg               sync.WaitGroup
	ctx              context.Context
	cancel           context.CancelFunc
}

// NewService creates a new Service.
func NewService(
	config Config,
	caller outbound.PSM3Caller,
	repo outbound.PSM3SnapshotRepository,
	sqsConsumer outbound.SQSConsumer,
	blockQuerier entity.BlockQuerier,
) (*Service, error) {
	if caller == nil {
		return nil, fmt.Errorf("psm3 caller is required")
	}
	if repo == nil {
		return nil, fmt.Errorf("psm3 snapshot repository is required")
	}
	if sqsConsumer == nil {
		return nil, fmt.Errorf("sqs consumer is required")
	}
	if blockQuerier == nil {
		return nil, fmt.Errorf("block querier is required")
	}
	if config.ChainID <= 0 {
		return nil, fmt.Errorf("chain ID is required")
	}
	if config.PSM3Address == (common.Address{}) {
		return nil, fmt.Errorf("psm3 address is required")
	}
	if config.SweepEveryNBlocks < 0 {
		return nil, fmt.Errorf("sweep every n blocks must not be negative, got %d", config.SweepEveryNBlocks)
	}

	if config.SweepEveryNBlocks == 0 {
		config.SweepEveryNBlocks = defaultSweepEveryNBlocks
	}
	if config.MaxMessages == 0 {
		config.MaxMessages = 10
	}
	if config.PollInterval == 0 {
		config.PollInterval = 100 * time.Millisecond
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}

	return &Service{
		config:       config,
		caller:       caller,
		repo:         repo,
		sqsConsumer:  sqsConsumer,
		blockQuerier: blockQuerier,
		logger:       config.Logger.With("component", "psm3-service"),
	}, nil
}

// Start resolves the PSM3 immutables at the latest block, then starts the SQS
// processing loop. It returns when initial setup fails; the loop runs until
// ctx is cancelled.
func (s *Service) Start(ctx context.Context) error {
	blockNum, err := s.blockQuerier.BlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("get latest block for immutable resolution: %w", err)
	}

	if err := s.caller.ResolveImmutables(ctx, new(big.Int).SetUint64(blockNum)); err != nil {
		return fmt.Errorf("resolve psm3 immutables: %w", err)
	}

	s.blocksSinceSweep = s.config.SweepEveryNBlocks - 1 // first block triggers immediate read
	s.ctx, s.cancel = context.WithCancel(ctx)

	s.wg.Go(func() {
		sqsutil.RunLoop(s.ctx, sqsutil.Config{
			Consumer:     s.sqsConsumer,
			MaxMessages:  s.config.MaxMessages,
			PollInterval: s.config.PollInterval,
			Logger:       s.logger,
			ChainID:      s.config.ChainID,
		}, s.processBlock)
	})

	s.logger.Info("psm3 service started",
		"psm3", s.config.PSM3Address.Hex(),
		"sweepEveryNBlocks", s.config.SweepEveryNBlocks,
		"chainID", s.config.ChainID,
		"block", blockNum,
	)

	return nil
}

// Stop cancels the SQS processing loop and waits for the goroutine to exit.
func (s *Service) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	s.logger.Info("psm3 service stopped")
	return nil
}

// processBlock is called for every block event from SQS.
// It only reads state every SweepEveryNBlocks.
func (s *Service) processBlock(ctx context.Context, event outbound.BlockEvent) error {
	s.blocksSinceSweep++
	if s.blocksSinceSweep < s.config.SweepEveryNBlocks {
		return nil
	}

	// Reset before sweeping so a failing sweep still consumes the cadence budget.
	// Otherwise the counter stays at the threshold and every subsequent block
	// re-sweeps until one succeeds — a per-block RPC storm during an RPC outage.
	// The failed block NACKs and is retried via SQS redelivery.
	s.blocksSinceSweep = 0
	return s.sweep(ctx, event)
}

// sweep reads the reserve state pinned to the event's block and writes one
// snapshot row. Any failure aborts the sweep so the SQS message NACKs and is
// redelivered — no partial rows.
func (s *Service) sweep(ctx context.Context, event outbound.BlockEvent) error {
	start := time.Now()

	state, err := s.caller.ReadState(ctx, big.NewInt(event.BlockNumber))
	if err != nil {
		return fmt.Errorf("read psm3 state at block %d: %w", event.BlockNumber, err)
	}

	snap := &entity.PSM3Snapshot{
		ChainID:        s.config.ChainID,
		Address:        s.config.PSM3Address,
		State:          *state,
		BlockNumber:    event.BlockNumber,
		BlockVersion:   event.Version,
		BlockTimestamp: time.Unix(event.BlockTimestamp, 0).UTC(),
		Source:         "sweep",
	}
	if err := snap.Validate(); err != nil {
		return fmt.Errorf("psm3 snapshot at block %d: %w", event.BlockNumber, err)
	}

	if err := s.repo.SaveSnapshot(ctx, snap); err != nil {
		return fmt.Errorf("save psm3 snapshot at block %d: %w", event.BlockNumber, err)
	}

	s.logger.Info("psm3 sweep complete",
		"block", event.BlockNumber,
		"blockVersion", event.Version,
		"duration", time.Since(start),
	)

	return nil
}

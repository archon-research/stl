// Package sparklend_backfill provides a service for backfilling historical SparkLend
// position data by reading transaction receipts from S3 and processing them.
package sparklend_backfill

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/partition"
	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
	"golang.org/x/sync/errgroup"
)

// ReceiptProcessor processes transaction receipts for a given block.
// Satisfied by *aavelike_position_tracker.Service.
type ReceiptProcessor interface {
	ProcessReceipts(ctx context.Context, chainID, blockNumber int64, version int, receipts []shared.TransactionReceipt) error
}

// Config holds configuration for the backfill service.
type Config struct {
	Concurrency int
	Logger      *slog.Logger
}

// Service backfills historical SparkLend position data from S3.
type Service struct {
	config    Config
	s3Reader  outbound.S3Reader
	processor ReceiptProcessor
	bucket    string
	chainID   int64
}

// NewService creates a new Service, validating required fields and defaulting Concurrency to 1 if unset.
func NewService(
	config Config,
	s3Reader outbound.S3Reader,
	processor ReceiptProcessor,
	bucket string,
	chainID int64,
) (*Service, error) {
	if config.Logger == nil {
		return nil, fmt.Errorf("logger is required")
	}
	if s3Reader == nil {
		return nil, fmt.Errorf("s3Reader is required")
	}
	if processor == nil {
		return nil, fmt.Errorf("processor is required")
	}
	if bucket == "" {
		return nil, fmt.Errorf("bucket is required")
	}
	if config.Concurrency <= 0 {
		config.Concurrency = 1
	}
	if chainID <= 0 {
		return nil, fmt.Errorf("chainID must be positive, got %d", chainID)
	}
	return &Service{
		config:    config,
		s3Reader:  s3Reader,
		processor: processor,
		bucket:    bucket,
		chainID:   chainID,
	}, nil
}

// Run processes all blocks in [fromBlock, toBlock] concurrently.
func (s *Service) Run(ctx context.Context, fromBlock, toBlock int64) error {
	if toBlock < fromBlock {
		return fmt.Errorf("toBlock (%d) must be >= fromBlock (%d)", toBlock, fromBlock)
	}

	versionMap, err := s.scanVersions(ctx, fromBlock, toBlock)
	if err != nil {
		return fmt.Errorf("scanning S3 versions: %w", err)
	}

	totalBlocks := toBlock - fromBlock + 1
	logger := s.config.Logger.With("component", "sparklend-backfill")
	logger.Info("starting backfill",
		"fromBlock", fromBlock,
		"toBlock", toBlock,
		"totalBlocks", totalBlocks,
		"concurrency", s.config.Concurrency,
	)

	var processed atomic.Int64
	progressDone := make(chan struct{})
	progressCtx, stopProgressLogging := context.WithCancel(ctx)
	defer stopProgressLogging()
	startTime := time.Now()
	go func() {
		defer close(progressDone)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-progressCtx.Done():
				return
			case <-ticker.C:
				s.logProgress(logger, startTime, totalBlocks, &processed)
			}
		}
	}()

	g, workerCtx := errgroup.WithContext(ctx)
	g.SetLimit(s.config.Concurrency)

	for blockNum := fromBlock; blockNum <= toBlock; blockNum++ {
		if workerCtx.Err() != nil {
			break
		}

		g.Go(func() error {
			// Check for cancellation before starting work.
			// This is needed because g.Go queues goroutines and they may
			// start after another goroutine has already failed.
			if workerCtx.Err() != nil {
				return nil
			}

			// Check if a file exists on S3 or fail early
			version, ok := versionMap[blockNum]
			if !ok {
				return fmt.Errorf("block %d not found in S3", blockNum)
			}

			if err := s.processBlockFromS3(workerCtx, blockNum, version); err != nil {
				logger.Error("failed to process block", "block", blockNum, "error", err)
				return err
			}

			processed.Add(1)
			return nil
		})
	}

	err = g.Wait()
	stopProgressLogging()
	<-progressDone

	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return fmt.Errorf("backfill cancelled: %w", ctx.Err())
	}

	logger.Info("backfill complete",
		"processed", processed.Load(),
		"total", totalBlocks,
		"elapsed", time.Since(startTime).String(),
	)
	return nil
}

func (s *Service) logProgress(
	logger *slog.Logger,
	startTime time.Time,
	totalBlocks int64,
	processed *atomic.Int64,
) {
	p := processed.Load()
	elapsed := time.Since(startTime).Seconds()

	var rate float64
	if elapsed > 0 {
		rate = float64(p) / elapsed
	}

	var eta float64
	remaining := totalBlocks - p
	if rate > 0 {
		eta = float64(remaining) / rate
	}

	logger.Info("backfill progress",
		"processed", p,
		"total", totalBlocks,
		"rate_per_sec", fmt.Sprintf("%.2f", rate),
		"eta_sec", fmt.Sprintf("%.0f", eta),
	)
}

// buildVersionMap parses a slice of S3 key strings and returns a map from
// block number to the highest receipt-file version seen for that block.
// Keys that are not receipts files or that cannot be parsed are silently ignored.
func buildVersionMap(keys []string) map[int64]int {
	versions := make(map[int64]int)
	for _, key := range keys {
		parsed, ok := s3key.Parse(key)
		if !ok || parsed.DataType != s3key.Receipts {
			continue
		}
		if existing, seen := versions[parsed.BlockNumber]; !seen || parsed.Version > existing {
			versions[parsed.BlockNumber] = parsed.Version
		}
	}
	return versions
}

// scanVersions lists all S3 receipt keys in the partitions overlapping
// [fromBlock, toBlock] and returns a map of blockNum → highest version.
// It makes one ListPrefix call per partition (partition.BlockRangeSize blocks each).
func (s *Service) scanVersions(ctx context.Context, fromBlock, toBlock int64) (map[int64]int, error) {
	versions := make(map[int64]int)

	// Iterate over each partition (partition.BlockRangeSize blocks) that overlaps [fromBlock, toBlock].
	firstPartition := (fromBlock / partition.BlockRangeSize) * partition.BlockRangeSize
	for partStart := firstPartition; partStart <= toBlock; partStart += partition.BlockRangeSize {
		partEnd := partStart + partition.BlockRangeSize - 1
		prefix := fmt.Sprintf("%d-%d/", partStart, partEnd)

		keys, err := s.s3Reader.ListPrefix(ctx, s.bucket, prefix)
		if err != nil {
			return nil, fmt.Errorf("listing S3 prefix %s: %w", prefix, err)
		}

		for blockNum, ver := range buildVersionMap(keys) {
			if existing, seen := versions[blockNum]; !seen || ver > existing {
				versions[blockNum] = ver
			}
		}
	}

	return versions, nil
}

// processBlockFromS3 reads receipts for blockNum from S3 using the given version and processes them.
func (s *Service) processBlockFromS3(ctx context.Context, blockNum int64, version int) error {
	key := s3key.Build(blockNum, version, s3key.Receipts)

	rc, err := s.s3Reader.StreamFile(ctx, s.bucket, key)
	if err != nil {
		return fmt.Errorf("streaming S3 file %s: %w", key, err)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("reading S3 file %s: %w", key, err)
	}

	var receipts []shared.TransactionReceipt
	if err := json.Unmarshal(data, &receipts); err != nil {
		return fmt.Errorf("unmarshalling receipts for block %d: %w", blockNum, err)
	}

	if err := s.processor.ProcessReceipts(ctx, s.chainID, blockNum, version, receipts); err != nil {
		return fmt.Errorf("processing receipts for block %d: %w", blockNum, err)
	}

	return nil
}

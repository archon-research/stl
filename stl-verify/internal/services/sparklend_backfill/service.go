// Package sparklend_backfill provides a service for backfilling historical SparkLend
// position data by reading transaction receipts from S3 and processing them.
package sparklend_backfill

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/partition"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/sparklend_position_tracker"
)

// ReceiptProcessor processes transaction receipts for a given block.
// Satisfied by *sparklend_position_tracker.Service.
type ReceiptProcessor interface {
	ProcessReceipts(ctx context.Context, chainID, blockNumber int64, version int, receipts []sparklend_position_tracker.TransactionReceipt) error
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

// NewService creates a new backfill Service. Returns an error if any required
// dependency is missing. Concurrency defaults to 10 if <= 0.
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
		config.Concurrency = 10
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

	totalBlocks := toBlock - fromBlock + 1
	logger := s.config.Logger.With("component", "sparklend-backfill")
	logger.Info("starting backfill",
		"fromBlock", fromBlock,
		"toBlock", toBlock,
		"totalBlocks", totalBlocks,
		"concurrency", s.config.Concurrency,
	)

	blockCh := make(chan int64, s.config.Concurrency*2)
	errCh := make(chan error, 1)

	var processed, failed atomic.Int64

	// Progress reporter
	progressCtx, progressCancel := context.WithCancel(context.Background())
	progressDone := make(chan struct{})
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
				p := processed.Load()
				f := failed.Load()
				elapsed := time.Since(startTime).Seconds()
				var rate float64
				if elapsed > 0 {
					rate = float64(p) / elapsed
				}
				var eta float64
				remaining := totalBlocks - p - f
				if rate > 0 {
					eta = float64(remaining) / rate
				}
				logger.Info("backfill progress",
					"processed", p,
					"failed", f,
					"total", totalBlocks,
					"rate_per_sec", fmt.Sprintf("%.2f", rate),
					"eta_sec", fmt.Sprintf("%.0f", eta),
				)
			}
		}
	}()

	// Worker goroutines
	var wg sync.WaitGroup
	for range s.config.Concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for blockNum := range blockCh {
				if err := s.processBlock(ctx, blockNum); err != nil {
					logger.Error("failed to process block", "block", blockNum, "error", err)
					failed.Add(1)
					select {
					case errCh <- err:
					default:
					}
				} else {
					processed.Add(1)
				}
			}
		}()
	}

	// Feed blocks into channel
feedLoop:
	for blockNum := fromBlock; blockNum <= toBlock; blockNum++ {
		select {
		case <-ctx.Done():
			break feedLoop
		case blockCh <- blockNum:
		}
	}
	close(blockCh)

	wg.Wait()
	progressCancel()
	<-progressDone

	logger.Info("backfill complete",
		"processed", processed.Load(),
		"failed", failed.Load(),
		"total", totalBlocks,
		"elapsed", time.Since(startTime).String(),
	)

	if ctx.Err() != nil {
		return fmt.Errorf("backfill cancelled: %w", ctx.Err())
	}

	// Return first error from errCh, if any
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

// processBlock reads receipts for blockNum from S3 and processes them.
func (s *Service) processBlock(ctx context.Context, blockNum int64) error {
	key := fmt.Sprintf("%s/%d_1_receipts.json.gz", partition.GetPartition(blockNum), blockNum)

	rc, err := s.s3Reader.StreamFile(ctx, s.bucket, key)
	if err != nil {
		return fmt.Errorf("streaming S3 file %s: %w", key, err)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("reading S3 file %s: %w", key, err)
	}

	var receipts []sparklend_position_tracker.TransactionReceipt
	if err := json.Unmarshal(data, &receipts); err != nil {
		return fmt.Errorf("unmarshalling receipts for block %d: %w", blockNum, err)
	}

	if err := s.processor.ProcessReceipts(ctx, s.chainID, blockNum, 1, receipts); err != nil {
		return fmt.Errorf("processing receipts for block %d: %w", blockNum, err)
	}

	return nil
}

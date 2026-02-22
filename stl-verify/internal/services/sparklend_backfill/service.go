// Package sparklend_backfill provides a service for backfilling historical SparkLend
// position data by reading transaction receipts from S3 and processing them.
package sparklend_backfill

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
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
	config      Config
	s3Reader    outbound.S3Reader
	processor   ReceiptProcessor
	rpcFallback outbound.BlockReceiptsReader
	bucket      string
	chainID     int64
}

// NewService creates a Service configured to backfill SparkLend receipts.
// It validates that Logger, s3Reader, processor, and bucket are provided and returns an error if any are missing.
// If config.Concurrency is less than or equal to zero it is defaulted to 1.
// The returned Service is initialized with the provided s3Reader, processor, bucket, chainID, and the (possibly adjusted) config.
func NewService(
	config Config,
	s3Reader outbound.S3Reader,
	processor ReceiptProcessor,
	rpcFallback outbound.BlockReceiptsReader,
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
	if rpcFallback == nil {
		return nil, fmt.Errorf("rpcFallback is required")
	}
	if bucket == "" {
		return nil, fmt.Errorf("bucket is required")
	}
	if config.Concurrency <= 0 {
		config.Concurrency = 1
	}
	return &Service{
		config:      config,
		s3Reader:    s3Reader,
		processor:   processor,
		rpcFallback: rpcFallback,
		bucket:      bucket,
		chainID:     chainID,
	}, nil
}

// Run processes all blocks in [fromBlock, toBlock] concurrently.
func (s *Service) Run(ctx context.Context, fromBlock, toBlock int64) error {
	if toBlock < fromBlock {
		return fmt.Errorf("toBlock (%d) must be >= fromBlock (%d)", toBlock, fromBlock)
	}

	versionMap, err := s.ScanVersions(ctx, fromBlock, toBlock)
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

	blockCh := make(chan int64, s.config.Concurrency*2)
	errCh := make(chan error, 1)

	var processed, failed atomic.Int64

	// Use Background context so the progress reporter runs until Run() finishes,
	// independent of the caller's context being cancelled.
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
				s.logProgress(logger, startTime, totalBlocks, &processed, &failed)
			}
		}
	}()

	// Worker goroutines
	var wg sync.WaitGroup
	for range s.config.Concurrency {
		wg.Go(func() {
			for blockNum := range blockCh {
				version, ok := versionMap[blockNum]
				if !ok {
					logger.Info("block not found in S3, fetching from RPC", "block", blockNum)
					if err := s.processBlockFromRPC(ctx, blockNum); err != nil {
						logger.Error("failed to process block from RPC", "block", blockNum, "error", err)
						failed.Add(1)
						select {
						case errCh <- err:
						default:
						}
					} else {
						processed.Add(1)
					}
					continue
				}
				if err := s.processBlockFromS3(ctx, blockNum, version); err != nil {
					logger.Error("failed to process block from S3", "block", blockNum, "error", err)
					failed.Add(1)
					select {
					case errCh <- err:
					default:
					}
				} else {
					processed.Add(1)
				}
			}
		})
	}

	// Feed blocks into channel
	s.enqueueBlocks(ctx, blockCh, fromBlock, toBlock)

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

func (s *Service) logProgress(
	logger *slog.Logger,
	startTime time.Time,
	totalBlocks int64,
	processed *atomic.Int64,
	failed *atomic.Int64,
) {
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

func (s *Service) enqueueBlocks(ctx context.Context, blockCh chan<- int64, fromBlock, toBlock int64) {
	defer close(blockCh)
	for blockNum := fromBlock; blockNum <= toBlock; blockNum++ {
		select {
		case <-ctx.Done():
			return
		case blockCh <- blockNum:
		}
	}
}

// BuildVersionMap parses a slice of S3 key strings and returns a map from
// block number to the highest receipt-file version seen for that block.
// Keys that are not receipts files or that cannot be parsed are silently ignored.
func BuildVersionMap(keys []string) map[int64]int {
	versions := make(map[int64]int)
	for _, key := range keys {
		blockNum, version, ok := parseReceiptKey(key)
		if !ok {
			continue
		}
		if existing, seen := versions[blockNum]; !seen || version > existing {
			versions[blockNum] = version
		}
	}
	return versions
}

// parseReceiptKey parses an S3 key of the form
// "{partition}/{blockNum}_{version}_receipts.json.gz"
// and returns (blockNum, version, true) on success, or (0, 0, false) if the
// key is not a receipts file or cannot be parsed.
func parseReceiptKey(key string) (blockNum int64, version int, ok bool) {
	// Extract filename: everything after the last "/"
	slash := strings.LastIndex(key, "/")
	filename := key
	if slash >= 0 {
		filename = key[slash+1:]
	}

	// Must end in "_receipts.json.gz"
	const suffix = "_receipts.json.gz"
	if !strings.HasSuffix(filename, suffix) {
		return 0, 0, false
	}
	stem := filename[:len(filename)-len(suffix)] // e.g. "100_1"

	// Split on "_" — must have exactly two parts: blockNum and version
	parts := strings.SplitN(stem, "_", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}

	bn, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, false
	}

	ver, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, false
	}

	return bn, ver, true
}

// ScanVersions lists all S3 receipt keys in the partitions overlapping
// [fromBlock, toBlock] and returns a map of blockNum → highest version.
// It makes one ListPrefix call per partition (partition.BlockRangeSize blocks each).
// The returned map may contain block numbers outside [fromBlock, toBlock] because
// whole partitions are listed; callers should only access entries for blocks they
// intend to process.
func (s *Service) ScanVersions(ctx context.Context, fromBlock, toBlock int64) (map[int64]int, error) {
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

		for blockNum, ver := range BuildVersionMap(keys) {
			if existing, seen := versions[blockNum]; !seen || ver > existing {
				versions[blockNum] = ver
			}
		}
	}

	return versions, nil
}

// processBlockFromS3 reads receipts for blockNum from S3 using the given version and processes them.
func (s *Service) processBlockFromS3(ctx context.Context, blockNum int64, version int) error {
	key := fmt.Sprintf("%s/%d_%d_receipts.json.gz", partition.GetPartition(blockNum), blockNum, version)

	// StreamFile transparently decompresses .gz files, so no manual gzip decoding is needed.
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

	if err := s.processor.ProcessReceipts(ctx, s.chainID, blockNum, version, receipts); err != nil {
		return fmt.Errorf("processing receipts for block %d: %w", blockNum, err)
	}

	return nil
}

// processBlockFromRPC fetches receipts for blockNum via the RPC fallback and
// processes them with version 0 (canonical chain, no reorg).
func (s *Service) processBlockFromRPC(ctx context.Context, blockNum int64) error {
	raw, err := s.rpcFallback.GetBlockReceipts(ctx, blockNum)
	if err != nil {
		return fmt.Errorf("fetching receipts from RPC for block %d: %w", blockNum, err)
	}

	var receipts []sparklend_position_tracker.TransactionReceipt
	if err := json.Unmarshal(raw, &receipts); err != nil {
		return fmt.Errorf("unmarshalling RPC receipts for block %d: %w", blockNum, err)
	}

	if err := s.processor.ProcessReceipts(ctx, s.chainID, blockNum, 0, receipts); err != nil {
		return fmt.Errorf("processing RPC receipts for block %d: %w", blockNum, err)
	}

	return nil
}

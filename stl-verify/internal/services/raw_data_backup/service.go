// Package rawdatabackup provides a service that consumes block events from SQS,
// fetches the data from cache, and stores it to S3 for long-term backup.
package rawdatabackup

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// BlockRangeSize is the number of blocks per S3 partition.
// Blocks 0-1000 go in partition "0-1000", blocks 1001-2000 go in "1001-2000", etc.
const BlockRangeSize = 1000

// ChainExpectation defines what data is expected for a specific chain.
// If a data type is expected but missing from cache, the message will error
// and go to DLQ rather than retry infinitely.
type ChainExpectation struct {
	// ExpectReceipts indicates receipts data is required for this chain.
	ExpectReceipts bool
	// ExpectTraces indicates traces data is required for this chain.
	ExpectTraces bool
	// ExpectBlobs indicates blobs data is required for this chain.
	ExpectBlobs bool
}

// DefaultChainExpectations returns the default expectations for known chains.
func DefaultChainExpectations() map[int64]ChainExpectation {
	return map[int64]ChainExpectation{
		1: { // Ethereum Mainnet
			ExpectReceipts: true,
			ExpectTraces:   true,
			ExpectBlobs:    false, // Blobs are optional (post-Dencun)
		},
	}
}

// Config holds configuration for the backup service.
type Config struct {
	// ChainID is the blockchain chain ID (used for cache key prefix).
	ChainID int64

	// Bucket is the S3 bucket to write backups to.
	Bucket string

	// Workers is the number of concurrent message processors.
	Workers int

	// BatchSize is how many messages to fetch at once (max 10).
	BatchSize int

	// ChainExpectations maps chain IDs to their data expectations.
	// If not set, DefaultChainExpectations() is used.
	ChainExpectations map[int64]ChainExpectation

	// Metrics is the metrics recorder (optional).
	Metrics outbound.BackupMetricsRecorder

	// Logger for the service.
	Logger *slog.Logger
}

// ConfigDefaults returns sensible defaults for the backup service.
func ConfigDefaults() Config {
	return Config{
		ChainID:   1,
		Workers:   4,
		BatchSize: 10,
		Logger:    slog.Default(),
	}
}

// Service is the raw data backup service.
type Service struct {
	config            Config
	chainExpectations map[int64]ChainExpectation
	consumer          outbound.SQSConsumer
	cache             outbound.BlockCache
	writer            outbound.S3Writer
	metrics           outbound.BackupMetricsRecorder
	logger            *slog.Logger
	closeOnce         sync.Once
	stopCh            chan struct{}
	wg                sync.WaitGroup
}

// NewService creates a new backup service.
func NewService(
	config Config,
	consumer outbound.SQSConsumer,
	cache outbound.BlockCache,
	writer outbound.S3Writer,
) (*Service, error) {
	if consumer == nil {
		return nil, fmt.Errorf("consumer is required")
	}
	if cache == nil {
		return nil, fmt.Errorf("cache is required")
	}
	if writer == nil {
		return nil, fmt.Errorf("writer is required")
	}
	if config.Bucket == "" {
		return nil, fmt.Errorf("bucket is required")
	}

	// Apply defaults
	defaults := ConfigDefaults()
	if config.Workers <= 0 {
		config.Workers = defaults.Workers
	}
	if config.BatchSize <= 0 {
		config.BatchSize = defaults.BatchSize
	}
	if config.Logger == nil {
		config.Logger = defaults.Logger
	}

	// Set up chain expectations
	chainExpectations := config.ChainExpectations
	if chainExpectations == nil {
		chainExpectations = DefaultChainExpectations()
	}

	return &Service{
		config:            config,
		chainExpectations: chainExpectations,
		consumer:          consumer,
		cache:             cache,
		writer:            writer,
		metrics:           config.Metrics,
		logger:            config.Logger.With("component", "raw-data-backup"),
		stopCh:            make(chan struct{}),
	}, nil
}

// Run starts the backup service and blocks until the context is cancelled.
func (s *Service) Run(ctx context.Context) error {
	s.logger.Info("starting raw data backup service",
		"bucket", s.config.Bucket,
		"workers", s.config.Workers,
		"chainID", s.config.ChainID,
	)

	// Create a channel for messages to process
	msgCh := make(chan outbound.SQSMessage, s.config.Workers*2)

	// Start worker goroutines
	for i := 0; i < s.config.Workers; i++ {
		s.wg.Add(1)
		go s.worker(ctx, i, msgCh)
	}

	// Message fetcher loop
	for {
		select {
		case <-ctx.Done():
			s.logger.Info("context cancelled, stopping message fetcher")
			close(msgCh)
			s.wg.Wait()
			return ctx.Err()
		case <-s.stopCh:
			s.logger.Info("stop signal received, stopping message fetcher")
			close(msgCh)
			s.wg.Wait()
			return nil
		default:
		}

		// Fetch messages from SQS
		messages, err := s.consumer.ReceiveMessages(ctx, s.config.BatchSize)
		if err != nil {
			if ctx.Err() != nil {
				close(msgCh)
				s.wg.Wait()
				return ctx.Err()
			}
			s.logger.Error("failed to receive messages", "error", err)
			time.Sleep(5 * time.Second)
			continue
		}

		// Send messages to workers
		for _, msg := range messages {
			select {
			case msgCh <- msg:
			case <-ctx.Done():
				close(msgCh)
				s.wg.Wait()
				return ctx.Err()
			}
		}
	}
}

// Stop signals the service to stop.
func (s *Service) Stop() {
	s.closeOnce.Do(func() {
		close(s.stopCh)
	})
}

// worker processes messages from the channel.
func (s *Service) worker(ctx context.Context, id int, msgCh <-chan outbound.SQSMessage) {
	defer s.wg.Done()
	logger := s.logger.With("worker", id)

	for msg := range msgCh {
		start := time.Now()
		err := s.processMessage(ctx, msg)
		duration := time.Since(start)

		if s.metrics != nil {
			status := "success"
			if err != nil {
				status = "error"
			}
			s.metrics.RecordProcessingLatency(ctx, duration, status)
			s.metrics.RecordBlockProcessed(ctx, status)
		}

		if err != nil {
			logger.Error("failed to process message",
				"messageID", msg.MessageID,
				"error", err,
			)
			// Don't delete the message - it will become visible again after visibility timeout
			continue
		}

		// Delete the message from the queue
		if err := s.consumer.DeleteMessage(ctx, msg.ReceiptHandle); err != nil {
			logger.Error("failed to delete message",
				"messageID", msg.MessageID,
				"error", err,
			)
		}
	}
}

// processMessage handles a single SQS message.
func (s *Service) processMessage(ctx context.Context, msg outbound.SQSMessage) (retErr error) {
	// Parse the SNS wrapper if present (SQS messages from SNS have an envelope)
	var event outbound.BlockEvent
	body := msg.Body

	// Try to parse as SNS notification first
	var snsWrapper struct {
		Message string `json:"Message"`
	}
	if err := json.Unmarshal([]byte(body), &snsWrapper); err == nil && snsWrapper.Message != "" {
		body = snsWrapper.Message
	}

	// Parse the block event
	if err := json.Unmarshal([]byte(body), &event); err != nil {
		return fmt.Errorf("failed to parse block event: %w", err)
	}

	s.logger.Debug("processing block",
		"blockNumber", event.BlockNumber,
		"version", event.Version,
		"chainID", event.ChainID,
	)

	// Fetch all data from cache
	blockData, err := s.cache.GetBlock(ctx, event.ChainID, event.BlockNumber, event.Version)
	if err != nil {
		return fmt.Errorf("failed to get block from cache: %w", err)
	}

	receiptsData, err := s.cache.GetReceipts(ctx, event.ChainID, event.BlockNumber, event.Version)
	if err != nil {
		return fmt.Errorf("failed to get receipts from cache: %w", err)
	}

	tracesData, err := s.cache.GetTraces(ctx, event.ChainID, event.BlockNumber, event.Version)
	if err != nil {
		return fmt.Errorf("failed to get traces from cache: %w", err)
	}

	blobsData, err := s.cache.GetBlobs(ctx, event.ChainID, event.BlockNumber, event.Version)
	if err != nil {
		return fmt.Errorf("failed to get blobs from cache: %w", err)
	}

	// Check if we have at least block data (always required)
	if blockData == nil {
		cacheKey := shared.CacheKey(event.ChainID, event.BlockNumber, event.Version, "block")
		return fmt.Errorf("block data not found in cache for block %d (cacheKey=%s)", event.BlockNumber, cacheKey)
	}

	// Validate against chain expectations
	if expectation, ok := s.chainExpectations[event.ChainID]; ok {
		if expectation.ExpectReceipts && receiptsData == nil {
			return fmt.Errorf("receipts data expected but not found in cache for chain %d block %d", event.ChainID, event.BlockNumber)
		}
		if expectation.ExpectTraces && tracesData == nil {
			return fmt.Errorf("traces data expected but not found in cache for chain %d block %d", event.ChainID, event.BlockNumber)
		}
		if expectation.ExpectBlobs && blobsData == nil {
			return fmt.Errorf("blobs data expected but not found in cache for chain %d block %d", event.ChainID, event.BlockNumber)
		}
	}

	// Calculate the partition (block range)
	partition := s.getPartition(event.BlockNumber)

	// Determine which files are expected for a complete backup
	expectedTypes := []string{"block"}
	if expectation, ok := s.chainExpectations[event.ChainID]; ok {
		if expectation.ExpectReceipts {
			expectedTypes = append(expectedTypes, "receipts")
		}
		if expectation.ExpectTraces {
			expectedTypes = append(expectedTypes, "traces")
		}
		if expectation.ExpectBlobs {
			expectedTypes = append(expectedTypes, "blobs")
		}
	}

	// Check if all expected files already exist
	allExist := true
	for _, dataType := range expectedTypes {
		key := s.generateKey(partition, event, dataType)
		exists, err := s.writer.FileExists(ctx, s.config.Bucket, key)
		if err != nil {
			return fmt.Errorf("failed to check existence of %s: %w", key, err)
		}
		if !exists {
			allExist = false
			break
		}
	}

	if allExist {
		s.logger.Debug("all expected files exist, skipping backup", "block", event.BlockNumber)
		return nil
	}

	// Write all available data to S3 (overwriting if necessary to ensure consistency)
	if err := s.writeToS3(ctx, partition, event, "block", blockData); err != nil {
		return err
	}

	if receiptsData != nil {
		if err := s.writeToS3(ctx, partition, event, "receipts", receiptsData); err != nil {
			return err
		}
	}

	if tracesData != nil {
		if err := s.writeToS3(ctx, partition, event, "traces", tracesData); err != nil {
			return err
		}
	}

	if blobsData != nil {
		if err := s.writeToS3(ctx, partition, event, "blobs", blobsData); err != nil {
			return err
		}
	}

	s.logger.Info("backed up block to S3",
		"blockNumber", event.BlockNumber,
		"partition", partition,
	)

	return nil
}

// getPartition returns the partition string for a block number.
// Block 0-1000 -> "0-1000", block 1001-2000 -> "1001-2000", etc.
func (s *Service) getPartition(blockNumber int64) string {
	// Calculate which partition this block belongs to
	// Block 0-1000 is partition 0, block 1001-2000 is partition 1, etc.
	if blockNumber <= BlockRangeSize {
		return fmt.Sprintf("0-%d", BlockRangeSize)
	}

	partitionIndex := (blockNumber - 1) / BlockRangeSize
	start := partitionIndex*BlockRangeSize + 1
	end := (partitionIndex + 1) * BlockRangeSize

	return fmt.Sprintf("%d-%d", start, end)
}

// generateKey creates the S3 key for a given data type.
func (s *Service) generateKey(partition string, event outbound.BlockEvent, dataType string) string {
	return fmt.Sprintf("%s/%d_%d_%s.json.gz",
		partition,
		event.BlockNumber,
		event.Version,
		dataType,
	)
}

// writeToS3 writes data to S3 with the appropriate key structure.
// Key format: {partition}/{blockNumber}_{version}_{dataType}.json.gz
func (s *Service) writeToS3(ctx context.Context, partition string, event outbound.BlockEvent, dataType string, data json.RawMessage) error {
	key := s.generateKey(partition, event, dataType)

	// Write to S3 with gzip compression, only if not exists to avoid overwritten races
	if _, err := s.writer.WriteFileIfNotExists(ctx, s.config.Bucket, key, bytes.NewReader(data), true); err != nil {
		return fmt.Errorf("failed to write %s to S3: %w", dataType, err)
	}

	return nil
}

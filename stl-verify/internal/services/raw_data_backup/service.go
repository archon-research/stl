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
)

// BlockRangeSize is the number of blocks per S3 partition.
// Blocks 0-1000 go in partition "0-1000", blocks 1001-2000 go in "1001-2000", etc.
const BlockRangeSize = 1000

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
	config    Config
	consumer  outbound.SQSConsumer
	cache     outbound.BlockCache
	writer    outbound.S3Writer
	logger    *slog.Logger
	closeOnce sync.Once
	stopCh    chan struct{}
	wg        sync.WaitGroup
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

	return &Service{
		config:   config,
		consumer: consumer,
		cache:    cache,
		writer:   writer,
		logger:   config.Logger.With("component", "raw-data-backup"),
		stopCh:   make(chan struct{}),
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
		if err := s.processMessage(ctx, msg); err != nil {
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
func (s *Service) processMessage(ctx context.Context, msg outbound.SQSMessage) error {
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

	// Check if we have at least block data
	if blockData == nil {
		return fmt.Errorf("block data not found in cache for block %d", event.BlockNumber)
	}

	// Calculate the partition (block range)
	partition := s.getPartition(event.BlockNumber)

	// Write each data type to S3
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

// writeToS3 writes data to S3 with the appropriate key structure.
// Key format: chainID/{partition}/{blockNumber}_{version}_{dataType}.json.gz
func (s *Service) writeToS3(ctx context.Context, partition string, event outbound.BlockEvent, dataType string, data json.RawMessage) error {
	key := fmt.Sprintf("%d/%s/%d_%d_%s.json.gz",
		event.ChainID,
		partition,
		event.BlockNumber,
		event.Version,
		dataType,
	)

	// Check if file already exists (idempotency)
	exists, err := s.writer.FileExists(ctx, s.config.Bucket, key)
	if err != nil {
		return fmt.Errorf("failed to check if %s exists: %w", key, err)
	}
	if exists {
		s.logger.Debug("file already exists, skipping", "key", key)
		return nil
	}

	// Write to S3 with gzip compression
	if err := s.writer.WriteFile(ctx, s.config.Bucket, key, bytes.NewReader(data), true); err != nil {
		return fmt.Errorf("failed to write %s to S3: %w", dataType, err)
	}

	return nil
}

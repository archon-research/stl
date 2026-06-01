// Package rawdatabackup provides a service that consumes block events from SQS,
// fetches the data from cache, and stores it to S3 for long-term backup.
package rawdatabackup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/partition"
	"github.com/archon-research/stl/stl-verify/internal/pkg/retry"
	"github.com/archon-research/stl/stl-verify/internal/pkg/rpcutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// ErrPermanent marks a failure as permanent: the message cannot succeed on
// redelivery and is routed to the dead-letter queue (then deleted from the
// main queue) rather than left for SQS to retry. Transient failures are NOT
// wrapped with ErrPermanent and stay on the queue for redelivery.
var ErrPermanent = errors.New("permanent failure")

// errCacheMiss is an internal sentinel used only inside getCachedWithRetry to
// distinguish a cache miss (nil data, nil error) from a transient getter error.
// Only misses are retried; transient errors are returned immediately.
var errCacheMiss = errors.New("cache miss")

// Processing status labels recorded via BackupMetricsRecorder.RecordBlockProcessed.
const (
	statusSuccess          = "success"
	statusDeadLettered     = "dead_lettered"
	statusTransientError   = "transient_error"
	statusDLQPublishFailed = "dlq_publish_failed"
)

// Metric labels for processing latency.
const (
	latencyStatusSuccess = "success"
	latencyStatusError   = "error"
)

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
		43114: { // Avalanche C-Chain
			ExpectReceipts: true,
			ExpectTraces:   false,
			ExpectBlobs:    false,
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

	// CacheMissMaxRetries is the number of additional attempts (beyond the
	// first) to re-read a cache value before treating a miss as permanent.
	// This absorbs the not-in-cache-yet race between publish and consume.
	// Set to 0 to fail fast on the first miss (recommended during a backlog
	// drain). NewService does NOT default this field; an unset value is taken as
	// 0. The worker's parseConfig supplies ConfigDefaults().CacheMissMaxRetries (3)
	// when CACHE_MISS_MAX_RETRIES is unset in the environment.
	CacheMissMaxRetries int

	// Metrics is the metrics recorder (optional).
	Metrics outbound.BackupMetricsRecorder

	// Logger for the service.
	Logger *slog.Logger
}

// ConfigDefaults returns sensible defaults for the backup service.
func ConfigDefaults() Config {
	return Config{
		ChainID:             1,
		Workers:             4,
		BatchSize:           10,
		CacheMissMaxRetries: 3,
		Logger:              slog.Default(),
	}
}

// cacheMissBackoff returns the backoff used between cache-miss retries. Kept
// internal (not exposed on Config) since only the retry count is operator-tunable.
func cacheMissBackoff() retry.Config {
	return retry.Config{
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     250 * time.Millisecond,
		BackoffFactor:  2.0,
		Jitter:         true,
	}
}

// Service is the raw data backup service.
type Service struct {
	config            Config
	chainExpectations map[int64]ChainExpectation
	consumer          outbound.SQSConsumer
	cache             outbound.BlockCache
	writer            outbound.S3Writer
	deadLetter        outbound.DeadLetterPublisher
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
	deadLetter outbound.DeadLetterPublisher,
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
	if deadLetter == nil {
		return nil, fmt.Errorf("dead-letter publisher is required")
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
		deadLetter:        deadLetter,
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

	// Use sync.Once to ensure msgCh is closed exactly once, preventing panic from double-close race
	var closeMsgChOnce sync.Once
	closeMsgCh := func() {
		closeMsgChOnce.Do(func() {
			close(msgCh)
		})
	}

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
			closeMsgCh()
			s.wg.Wait()
			return ctx.Err()
		case <-s.stopCh:
			s.logger.Info("stop signal received, stopping message fetcher")
			closeMsgCh()
			s.wg.Wait()
			return nil
		default:
		}

		// Fetch messages from SQS
		messages, err := s.consumer.ReceiveMessages(ctx, s.config.BatchSize)
		if err != nil {
			if ctx.Err() != nil {
				closeMsgCh()
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
				closeMsgCh()
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
		s.recordLatency(ctx, start, err)
		s.handleResult(ctx, logger, msg, err)
	}
}

// recordLatency records the per-message processing latency, labelled by outcome.
func (s *Service) recordLatency(ctx context.Context, start time.Time, err error) {
	if s.metrics == nil {
		return
	}
	status := latencyStatusSuccess
	if err != nil {
		status = latencyStatusError
	}
	s.metrics.RecordProcessingLatency(ctx, time.Since(start), status)
}

// handleResult decides what to do with a message after processing:
//   - success: delete it.
//   - permanent failure: route to the DLQ, then delete (only if the DLQ
//     publish succeeds, otherwise preserve for redelivery).
//   - transient failure: leave it on the queue for SQS redelivery.
func (s *Service) handleResult(ctx context.Context, logger *slog.Logger, msg outbound.SQSMessage, err error) {
	if err == nil {
		s.recordProcessed(ctx, statusSuccess)
		s.deleteMessage(ctx, logger, msg)
		return
	}

	if errors.Is(err, ErrPermanent) {
		s.deadLetterMessage(ctx, logger, msg, err)
		return
	}

	// Transient: keep the message on the queue so SQS redelivers it.
	logger.Error("transient failure processing message, leaving for redelivery",
		"messageID", msg.MessageID,
		"error", err,
	)
	s.recordProcessed(ctx, statusTransientError)
}

// deadLetterMessage publishes a permanently failed message to the DLQ and, if
// that succeeds, deletes it from the main queue. If the publish fails the
// message is preserved (not deleted) so it can be redelivered and retried.
func (s *Service) deadLetterMessage(ctx context.Context, logger *slog.Logger, msg outbound.SQSMessage, cause error) {
	groupID := strconv.FormatInt(s.config.ChainID, 10)
	if pubErr := s.deadLetter.Publish(ctx, msg.Body, groupID); pubErr != nil {
		logger.Error("failed to publish permanent failure to dead-letter queue, preserving message",
			"messageID", msg.MessageID,
			"cause", cause,
			"error", pubErr,
		)
		s.recordProcessed(ctx, statusDLQPublishFailed)
		return
	}

	logger.Warn("routed permanent failure to dead-letter queue",
		"messageID", msg.MessageID,
		"cause", cause,
	)
	s.recordProcessed(ctx, statusDeadLettered)
	s.deleteMessage(ctx, logger, msg)
}

// deleteMessage removes a message from the main queue, logging on failure.
func (s *Service) deleteMessage(ctx context.Context, logger *slog.Logger, msg outbound.SQSMessage) {
	if err := s.consumer.DeleteMessage(ctx, msg.ReceiptHandle); err != nil {
		logger.Error("failed to delete message",
			"messageID", msg.MessageID,
			"error", err,
		)
	}
}

// recordProcessed increments the processed-blocks counter with the given status.
func (s *Service) recordProcessed(ctx context.Context, status string) {
	if s.metrics != nil {
		s.metrics.RecordBlockProcessed(ctx, status)
	}
}

// parseEvent decodes and validates the SQS message body. A malformed body or a
// chain-ID mismatch is permanent: redelivery cannot fix it, so it is routed to
// the DLQ.
func (s *Service) parseEvent(msg outbound.SQSMessage) (outbound.BlockEvent, error) {
	var event outbound.BlockEvent
	if err := json.Unmarshal([]byte(msg.Body), &event); err != nil {
		return outbound.BlockEvent{}, fmt.Errorf("failed to parse block event: %w: %w", err, ErrPermanent)
	}
	if event.ChainID != s.config.ChainID {
		return outbound.BlockEvent{}, fmt.Errorf("chain ID mismatch: event has %d, expected %d: %w", event.ChainID, s.config.ChainID, ErrPermanent)
	}
	return event, nil
}

// fetchExpectedData reads the always-required block plus any optional data types
// the chain expects. A cache getter error is transient (left on the queue); a
// confirmed miss after retries is permanent (routed to the DLQ).
func (s *Service) fetchExpectedData(ctx context.Context, event outbound.BlockEvent, expectation ChainExpectation) (block, receipts, traces, blobs json.RawMessage, err error) {
	block, err = s.getCachedWithRetry(ctx, s.cache.GetBlock, "block", event)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	if expectation.ExpectReceipts {
		receipts, err = s.getCachedWithRetry(ctx, s.cache.GetReceipts, "receipts", event)
		if err != nil {
			return nil, nil, nil, nil, err
		}
	}

	if expectation.ExpectTraces {
		traces, err = s.getCachedWithRetry(ctx, s.cache.GetTraces, "traces", event)
		if err != nil {
			return nil, nil, nil, nil, err
		}
	}

	if expectation.ExpectBlobs {
		blobs, err = s.getCachedWithRetry(ctx, s.cache.GetBlobs, "blobs", event)
		if err != nil {
			return nil, nil, nil, nil, err
		}
	}

	return block, receipts, traces, blobs, nil
}

// cacheGetter reads one data type for a block from the cache.
type cacheGetter func(ctx context.Context, chainID int64, blockNumber int64, version int) (json.RawMessage, error)

// getCachedWithRetry reads one data type from the cache, retrying only on a
// cache miss (to absorb the not-in-cache-yet race between publish and consume).
//
// Outcomes:
//   - data found: returned as-is.
//   - getter error (e.g. Redis down): returned unwrapped and NOT retried; this
//     is transient, so the message stays on the queue for SQS redelivery.
//   - miss persisting after CacheMissMaxRetries: translated into a descriptive
//     error wrapped with ErrPermanent and routed to the DLQ.
func (s *Service) getCachedWithRetry(ctx context.Context, get cacheGetter, dataType string, event outbound.BlockEvent) (json.RawMessage, error) {
	cfg := cacheMissBackoff()
	cfg.MaxRetries = s.config.CacheMissMaxRetries

	isRetryable := func(err error) bool {
		return errors.Is(err, errCacheMiss)
	}

	data, err := retry.Do(ctx, cfg, isRetryable, nil, func() (json.RawMessage, error) {
		value, getErr := get(ctx, event.ChainID, event.BlockNumber, event.Version)
		if getErr != nil {
			// Transient: surface immediately, do not retry, do not wrap.
			return nil, fmt.Errorf("failed to get %s from cache: %w", dataType, getErr)
		}
		if value == nil {
			return nil, errCacheMiss
		}
		return value, nil
	})
	if err != nil {
		if errors.Is(err, errCacheMiss) {
			cacheKey := shared.CacheKey(event.ChainID, event.BlockNumber, event.Version, dataType)
			return nil, fmt.Errorf("%s data not found in cache for chain %d block %d version %d (cacheKey=%s): %w",
				dataType, event.ChainID, event.BlockNumber, event.Version, cacheKey, ErrPermanent)
		}
		// Transient getter error: return as-is (already unwrapped).
		return nil, err
	}

	return data, nil
}

// processMessage handles a single SQS message. Errors it returns are transient by
// default: the worker leaves the message on the queue for SQS redelivery. A failure
// that redelivery cannot fix must be wrapped with ErrPermanent so the worker routes
// it to the DLQ and deletes it. See ErrPermanent and the classification in
// parseEvent, fetchExpectedData, and writeToS3.
func (s *Service) processMessage(ctx context.Context, msg outbound.SQSMessage) (retErr error) {
	event, err := s.parseEvent(msg)
	if err != nil {
		return err
	}

	s.logger.Debug("processing block",
		"blockNumber", event.BlockNumber,
		"version", event.Version,
		"chainID", event.ChainID,
	)

	// Determine chain expectations (used for conditional fetching and validation)
	expectation := s.chainExpectations[event.ChainID]

	blockData, receiptsData, tracesData, blobsData, err := s.fetchExpectedData(ctx, event, expectation)
	if err != nil {
		return err
	}

	// Calculate the partition (block range)
	partitionKey := partition.GetPartition(event.BlockNumber)

	// Determine which files are expected for a complete backup
	expectedTypes := []string{"block"}
	if expectation.ExpectReceipts {
		expectedTypes = append(expectedTypes, "receipts")
	}
	if expectation.ExpectTraces {
		expectedTypes = append(expectedTypes, "traces")
	}
	if expectation.ExpectBlobs {
		expectedTypes = append(expectedTypes, "blobs")
	}

	// Check if all expected files already exist
	allExist := true
	for _, dataType := range expectedTypes {
		key := s.generateKey(partitionKey, event, dataType)
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
	if err := s.writeToS3(ctx, partitionKey, event, "block", blockData); err != nil {
		return err
	}

	if receiptsData != nil {
		if err := s.writeToS3(ctx, partitionKey, event, "receipts", receiptsData); err != nil {
			return err
		}
	}

	if tracesData != nil {
		if err := s.writeToS3(ctx, partitionKey, event, "traces", tracesData); err != nil {
			return err
		}
	}

	if blobsData != nil {
		if err := s.writeToS3(ctx, partitionKey, event, "blobs", blobsData); err != nil {
			return err
		}
	}

	s.logger.Info("backed up block to S3",
		"blockNumber", event.BlockNumber,
		"partition", partitionKey,
	)

	return nil
}

// generateKey creates the S3 key for a given data type.
func (s *Service) generateKey(partition string, event outbound.BlockEvent, dataType string) string {
	return s3key.BuildWithPartition(partition, event.BlockNumber, event.Version, s3key.DataType(dataType))
}

// writeToS3 writes data to S3 with the appropriate key structure.
// Key format: {partition}/{blockNumber}_{version}_{dataType}.json.gz
func (s *Service) writeToS3(ctx context.Context, partition string, event outbound.BlockEvent, dataType string, data json.RawMessage) error {
	if rpcutil.IsNullOrEmpty(data) {
		// A null/empty payload cannot become valid on redelivery: permanent.
		return fmt.Errorf("refusing to write null/empty %s payload to S3 for block %d (chain=%d, version=%d): %w",
			dataType, event.BlockNumber, event.ChainID, event.Version, ErrPermanent)
	}

	key := s.generateKey(partition, event, dataType)

	// Write to S3 with gzip compression, only if not exists to avoid overwritten races
	if _, err := s.writer.WriteFileIfNotExists(ctx, s.config.Bucket, key, bytes.NewReader(data), true); err != nil {
		return fmt.Errorf("failed to write %s to S3: %w", dataType, err)
	}

	return nil
}

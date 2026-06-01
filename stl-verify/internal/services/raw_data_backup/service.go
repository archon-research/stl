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
	statusAlreadyBackedUp  = "already_backed_up"
	statusRPCFallback      = "rpc_fallback"
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

// DefaultChainExpectations returns the default expectations for known chains. These
// MUST mirror what each chain's watcher actually caches, since the backup reads from
// that cache: receipts are always fetched, but traces only when the watcher runs
// without --enable-traces=false. Today only the ethereum watcher fetches traces; every
// other chain's watcher sets --enable-traces=false (avalanche and arbitrum have no
// trace_block on Alchemy at all; base/optimism/unichain support it but the watcher
// still does not fetch it). Blobs are not fetched anywhere (--enable-blobs is false).
func DefaultChainExpectations() map[int64]ChainExpectation {
	return map[int64]ChainExpectation{
		1:     {ExpectReceipts: true, ExpectTraces: true, ExpectBlobs: false},  // Ethereum Mainnet
		43114: {ExpectReceipts: true, ExpectTraces: false, ExpectBlobs: false}, // Avalanche C-Chain
		8453:  {ExpectReceipts: true, ExpectTraces: false, ExpectBlobs: false}, // Base
		10:    {ExpectReceipts: true, ExpectTraces: false, ExpectBlobs: false}, // Optimism
		130:   {ExpectReceipts: true, ExpectTraces: false, ExpectBlobs: false}, // Unichain
		42161: {ExpectReceipts: true, ExpectTraces: false, ExpectBlobs: false}, // Arbitrum
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
	client            outbound.BlockchainClient
	metrics           outbound.BackupMetricsRecorder
	logger            *slog.Logger
	closeOnce         sync.Once
	stopCh            chan struct{}
	wg                sync.WaitGroup
}

// NewService creates a new backup service. The blockchain client is required:
// it backs the RPC fallback used when block data has aged out of the cache, so
// a cache miss can be re-fetched by block hash instead of being dead-lettered.
func NewService(
	config Config,
	consumer outbound.SQSConsumer,
	cache outbound.BlockCache,
	writer outbound.S3Writer,
	deadLetter outbound.DeadLetterPublisher,
	client outbound.BlockchainClient,
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
	if client == nil {
		return nil, fmt.Errorf("blockchain client is required")
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
		client:            client,
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
		status, err := s.processMessage(ctx, msg)
		s.recordLatency(ctx, start, err)
		s.handleResult(ctx, logger, msg, status, err)
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
//   - success: record the returned outcome status and delete it.
//   - permanent failure: route to the DLQ, then delete (only if the DLQ
//     publish succeeds, otherwise preserve for redelivery).
//   - transient failure: leave it on the queue for SQS redelivery.
//
// On success, status carries the specific outcome (statusSuccess,
// statusAlreadyBackedUp, statusRPCFallback) so the metric distinguishes a cache
// hit from a no-op re-delivery and from an RPC self-heal.
func (s *Service) handleResult(ctx context.Context, logger *slog.Logger, msg outbound.SQSMessage, status string, err error) {
	if err == nil {
		s.recordProcessed(ctx, status)
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
//     error that wraps the errCacheMiss sentinel (NOT ErrPermanent). The caller
//     uses errors.Is(err, errCacheMiss) to fall back to the RPC fetch; a miss is
//     no longer terminal because cached payloads expire (2h TTL) while a backlog
//     can exceed that, so real blocks would otherwise be wrongly dead-lettered.
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
				dataType, event.ChainID, event.BlockNumber, event.Version, cacheKey, errCacheMiss)
		}
		// Transient getter error: return as-is (already unwrapped).
		return nil, err
	}

	return data, nil
}

// processMessage handles a single SQS message and returns the outcome status
// alongside an error. Errors it returns are transient by default: the worker
// leaves the message on the queue for SQS redelivery. A failure that redelivery
// cannot fix must be wrapped with ErrPermanent so the worker routes it to the DLQ
// and deletes it. The status is only meaningful when err is nil and records which
// success path was taken (statusSuccess for a cache hit, statusAlreadyBackedUp for
// a no-op when S3 already holds every file, statusRPCFallback for an RPC re-fetch).
//
// Read fallback chain on a cache miss: cache -> S3 (already backed up? no-op) ->
// RPC by block hash. See the classification in parseEvent, fetchExpectedData,
// fetchFromRPC, and writeToS3.
func (s *Service) processMessage(ctx context.Context, msg outbound.SQSMessage) (string, error) {
	event, err := s.parseEvent(msg)
	if err != nil {
		return "", err
	}

	s.logger.Debug("processing block",
		"blockNumber", event.BlockNumber,
		"version", event.Version,
		"chainID", event.ChainID,
	)

	expectation := s.chainExpectations[event.ChainID]
	partitionKey := partition.GetPartition(event.BlockNumber)
	expectedTypes := s.expectedTypes(expectation)

	// S3 idempotency check first: if every expected file already exists this is a
	// no-op. Done before any read so a re-delivery (or an aged-out-but-backed-up
	// block) never triggers a needless cache read or RPC fetch.
	allExist, err := s.allExpectedFilesExist(ctx, partitionKey, event, expectedTypes)
	if err != nil {
		return "", err
	}
	if allExist {
		s.logger.Debug("all expected files exist, skipping backup", "block", event.BlockNumber)
		return statusAlreadyBackedUp, nil
	}

	// Read the data: cache first, then RPC by hash on a confirmed cache miss.
	blockData, receiptsData, tracesData, blobsData, status, err := s.fetchBlockData(ctx, event, expectation)
	if err != nil {
		return "", err
	}

	if err := s.backUpToS3(ctx, partitionKey, event, blockData, receiptsData, tracesData, blobsData); err != nil {
		return "", err
	}

	s.logger.Info("backed up block to S3",
		"blockNumber", event.BlockNumber,
		"partition", partitionKey,
		"source", status,
	)

	return status, nil
}

// expectedTypes returns the data types that must be present for a complete
// backup of the given chain. Block is always required; the rest are conditional.
func (s *Service) expectedTypes(expectation ChainExpectation) []string {
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
	return expectedTypes
}

// allExpectedFilesExist reports whether S3 already holds every expected file for
// this block. A FileExists error is transient (returned unwrapped) so the message
// stays on the queue for redelivery.
func (s *Service) allExpectedFilesExist(ctx context.Context, partitionKey string, event outbound.BlockEvent, expectedTypes []string) (bool, error) {
	for _, dataType := range expectedTypes {
		key := s.generateKey(partitionKey, event, dataType)
		exists, err := s.writer.FileExists(ctx, s.config.Bucket, key)
		if err != nil {
			return false, fmt.Errorf("failed to check existence of %s: %w", key, err)
		}
		if !exists {
			return false, nil
		}
	}
	return true, nil
}

// fetchBlockData reads the block (and any expected optional types) from the cache,
// falling back to an RPC fetch by block hash on a confirmed cache miss. It returns
// the raw payloads plus the success status: statusSuccess for a cache hit,
// statusRPCFallback when the data was re-fetched via RPC.
func (s *Service) fetchBlockData(ctx context.Context, event outbound.BlockEvent, expectation ChainExpectation) (block, receipts, traces, blobs json.RawMessage, status string, err error) {
	block, receipts, traces, blobs, err = s.fetchExpectedData(ctx, event, expectation)
	if err == nil {
		return block, receipts, traces, blobs, statusSuccess, nil
	}
	if !errors.Is(err, errCacheMiss) {
		// Transient getter error or any other non-miss failure: surface as-is.
		return nil, nil, nil, nil, "", err
	}

	// Cache miss: the payload aged out of the 2h TTL while the backlog drained.
	// Re-fetch by block hash so a real block self-heals instead of being lost.
	s.logger.Info("cache miss, falling back to RPC fetch by hash",
		"blockNumber", event.BlockNumber,
		"version", event.Version,
		"chainID", event.ChainID,
	)
	block, receipts, traces, blobs, err = s.fetchFromRPC(ctx, event, expectation)
	if err != nil {
		return nil, nil, nil, nil, "", err
	}
	return block, receipts, traces, blobs, statusRPCFallback, nil
}

// fetchFromRPC fetches a block by hash via the blockchain client and validates it
// against the chain expectation, mirroring the backfill validation:
//   - whole-call error -> transient (the message stays on the queue for retry).
//   - a per-type network/429/5xx/timeout error -> transient.
//   - a null result (ErrUpstreamNullResult or null/empty payload) for an expected
//     type, or a missing block hash -> permanent (DLQ): an aged-out block the node no
//     longer returns by hash cannot be recovered by redelivery. See validateRPCType.
//
// Fetching by hash is TOCTOU-safe: it returns data for the exact block the event
// references, even across a reorg.
func (s *Service) fetchFromRPC(ctx context.Context, event outbound.BlockEvent, expectation ChainExpectation) (block, receipts, traces, blobs json.RawMessage, err error) {
	// The fallback re-fetches by hash; without one the block can never be recovered.
	if event.BlockHash == "" {
		return nil, nil, nil, nil, fmt.Errorf("cannot RPC-fetch block %d: event has no block hash: %w", event.BlockNumber, ErrPermanent)
	}
	bd, err := s.client.GetBlockDataByHash(ctx, event.BlockNumber, event.BlockHash, true)
	if err != nil {
		// Network error / batch failure: transient, let SQS redeliver.
		return nil, nil, nil, nil, fmt.Errorf("RPC fetch by hash failed for block %d (hash=%s): %w", event.BlockNumber, event.BlockHash, err)
	}

	if err := validateRPCType(event, "block", bd.Block, bd.BlockErr); err != nil {
		return nil, nil, nil, nil, err
	}
	if expectation.ExpectReceipts {
		if err := validateRPCType(event, "receipts", bd.Receipts, bd.ReceiptsErr); err != nil {
			return nil, nil, nil, nil, err
		}
		receipts = bd.Receipts
	}
	if expectation.ExpectTraces {
		if err := validateRPCType(event, "traces", bd.Traces, bd.TracesErr); err != nil {
			return nil, nil, nil, nil, err
		}
		traces = bd.Traces
	}
	if expectation.ExpectBlobs {
		if err := validateRPCType(event, "blobs", bd.Blobs, bd.BlobsErr); err != nil {
			return nil, nil, nil, nil, err
		}
		blobs = bd.Blobs
	}

	return bd.Block, receipts, traces, blobs, nil
}

// validateRPCType classifies one RPC-fetched data type. The fallback only runs for a
// cache miss, i.e. a block that has already aged out of cache (>2h old), so:
//   - a null result (the adapter's ErrUpstreamNullResult, or a null/empty payload) is
//     PERMANENT: an aged-out block the node no longer returns by hash has been reorged
//     out or is absent and will not appear on redelivery, so it is routed to the DLQ
//     instead of retried (which would only head-of-line stall the FIFO queue). This is
//     deliberately stricter than the watcher/backfill, where a null is a recoverable
//     propagation race for a freshly announced block.
//   - any other per-type fetch error (network / 429 / 5xx / timeout) is TRANSIENT and
//     left on the queue for SQS to redeliver.
func validateRPCType(event outbound.BlockEvent, dataType string, payload json.RawMessage, typeErr error) error {
	if typeErr != nil {
		if errors.Is(typeErr, rpcutil.ErrUpstreamNullResult) {
			return fmt.Errorf("RPC returned null %s for aged-out block %d (chain=%d, version=%d, hash=%s): %w",
				dataType, event.BlockNumber, event.ChainID, event.Version, event.BlockHash, ErrPermanent)
		}
		return fmt.Errorf("RPC %s fetch failed for block %d (hash=%s): %w", dataType, event.BlockNumber, event.BlockHash, typeErr)
	}
	if rpcutil.IsNullOrEmpty(payload) {
		return fmt.Errorf("RPC returned null/empty %s for block %d (chain=%d, version=%d, hash=%s): %w",
			dataType, event.BlockNumber, event.ChainID, event.Version, event.BlockHash, ErrPermanent)
	}
	return nil
}

// backUpToS3 writes the block and each non-nil expected data type to S3. The
// writeToS3 null/empty guard stays permanent for each type.
func (s *Service) backUpToS3(ctx context.Context, partitionKey string, event outbound.BlockEvent, block, receipts, traces, blobs json.RawMessage) error {
	if err := s.writeToS3(ctx, partitionKey, event, "block", block); err != nil {
		return err
	}
	if receipts != nil {
		if err := s.writeToS3(ctx, partitionKey, event, "receipts", receipts); err != nil {
			return err
		}
	}
	if traces != nil {
		if err := s.writeToS3(ctx, partitionKey, event, "traces", traces); err != nil {
			return err
		}
	}
	if blobs != nil {
		if err := s.writeToS3(ctx, partitionKey, event, "blobs", blobs); err != nil {
			return err
		}
	}
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

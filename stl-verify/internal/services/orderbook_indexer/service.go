// Package orderbook_indexer is the worker-side of the CEX orderbook
// pipeline. It consumes raw WebSocket frames from SQS (one queue, all
// exchanges), dispatches each frame to a source-specific parser, updates
// the latest-snapshot cache write-through, and batches snapshots for
// periodic flushes to the database.
//
// The worker is source-agnostic: a single binary scales horizontally and
// handles messages from every CEX. New exchanges are added by registering
// a Parser; no per-CEX worker code or deployment.
package orderbook_indexer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/cex"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// CacheWriter is the subset of the Redis orderbook cache the worker needs.
// Defined here rather than imported as a port so testing stays trivial.
type CacheWriter interface {
	SetLatest(ctx context.Context, snap *entity.OrderbookSnapshot) error
}

// Config configures the worker.
type Config struct {
	// FlushInterval is how often buffered snapshots are written to the DB.
	// Defaults to 10 seconds.
	FlushInterval time.Duration

	// ReceiveBatch is the max messages requested per SQS receive call (1-10).
	// Defaults to 10.
	ReceiveBatch int

	Logger *slog.Logger
}

// Service consumes raw CEX frames and persists parsed snapshots.
type Service struct {
	config   Config
	consumer outbound.SQSConsumer
	parsers  map[string]cex.Parser
	repo     outbound.OrderbookRepository
	cache    CacheWriter
	logger   *slog.Logger

	bufferMu sync.Mutex
	buffer   map[string]*entity.OrderbookSnapshot

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewService constructs the worker.
func NewService(
	config Config,
	consumer outbound.SQSConsumer,
	parsers map[string]cex.Parser,
	repo outbound.OrderbookRepository,
	cache CacheWriter,
) (*Service, error) {
	if consumer == nil {
		return nil, fmt.Errorf("consumer cannot be nil")
	}
	if repo == nil {
		return nil, fmt.Errorf("repo cannot be nil")
	}
	if len(parsers) == 0 {
		return nil, fmt.Errorf("parsers cannot be empty")
	}
	if config.FlushInterval <= 0 {
		config.FlushInterval = 10 * time.Second
	}
	if config.ReceiveBatch <= 0 || config.ReceiveBatch > 10 {
		config.ReceiveBatch = 10
	}
	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{
		config:   config,
		consumer: consumer,
		parsers:  parsers,
		repo:     repo,
		cache:    cache,
		logger:   logger.With("component", "orderbook-indexer"),
		buffer:   make(map[string]*entity.OrderbookSnapshot),
	}, nil
}

// Run starts the consume + flush loops and blocks until the context is
// cancelled. The final buffer contents are flushed before return.
func (s *Service) Run(ctx context.Context) error {
	ctx, s.cancel = context.WithCancel(ctx)
	s.wg.Add(2)
	go s.consumeLoop(ctx)
	go s.flushLoop(ctx)
	s.wg.Wait()
	// Final flush after both loops have exited so we don't lose what's
	// buffered on graceful shutdown.
	s.flush(context.Background())
	return nil
}

// Stop signals shutdown and waits for the loops to drain.
func (s *Service) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	return nil
}

func (s *Service) consumeLoop(ctx context.Context) {
	defer s.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msgs, err := s.consumer.ReceiveMessages(ctx, s.config.ReceiveBatch)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.logger.Warn("receive failed", "error", err)
			// Brief pause before retrying to avoid hot-looping on SQS
			// errors (auth, throttling, etc.).
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}

		for _, msg := range msgs {
			s.processMessage(ctx, msg)
		}
	}
}

func (s *Service) processMessage(ctx context.Context, msg outbound.SQSMessage) {
	envelope, err := decodeEnvelope(msg.Body)
	if err != nil {
		s.logger.Warn("decode envelope failed; deleting unparseable message",
			"messageID", msg.MessageID, "error", err)
		s.deleteMessage(ctx, msg)
		return
	}

	parser, ok := s.parsers[envelope.Source]
	if !ok {
		s.logger.Warn("unknown source; deleting message",
			"source", envelope.Source, "messageID", msg.MessageID)
		s.deleteMessage(ctx, msg)
		return
	}

	snapshots, err := parser.ParseMessage(envelope.Payload)
	if err != nil {
		s.logger.Warn("parse failed", "source", envelope.Source, "error", err)
		// Don't delete: leave to SQS visibility timeout → DLQ retry path.
		return
	}

	for _, snap := range snapshots {
		s.bufferSnapshot(ctx, snap)
	}

	s.deleteMessage(ctx, msg)
}

func (s *Service) bufferSnapshot(ctx context.Context, snap entity.OrderbookSnapshot) {
	if s.cache != nil {
		if err := s.cache.SetLatest(ctx, &snap); err != nil {
			s.logger.Warn("cache update failed",
				"exchange", snap.Exchange, "symbol", snap.Symbol, "error", err)
		}
	}
	key := snap.Exchange + ":" + snap.Symbol
	s.bufferMu.Lock()
	s.buffer[key] = &snap
	s.bufferMu.Unlock()
}

func (s *Service) deleteMessage(ctx context.Context, msg outbound.SQSMessage) {
	if err := s.consumer.DeleteMessage(ctx, msg.ReceiptHandle); err != nil {
		s.logger.Warn("delete message failed", "messageID", msg.MessageID, "error", err)
	}
}

func (s *Service) flushLoop(ctx context.Context) {
	defer s.wg.Done()
	ticker := time.NewTicker(s.config.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.flush(ctx)
		}
	}
}

func (s *Service) flush(ctx context.Context) {
	s.bufferMu.Lock()
	if len(s.buffer) == 0 {
		s.bufferMu.Unlock()
		return
	}
	toFlush := s.buffer
	s.buffer = make(map[string]*entity.OrderbookSnapshot, len(toFlush))
	s.bufferMu.Unlock()

	snapshots := make([]*entity.OrderbookSnapshot, 0, len(toFlush))
	for _, snap := range toFlush {
		snapshots = append(snapshots, snap)
	}
	if err := s.repo.SaveSnapshots(ctx, snapshots); err != nil {
		s.logger.Error("flush failed", "count", len(snapshots), "error", err)
		return
	}
	s.logger.Info("flushed snapshots to DB", "count", len(snapshots))
}

func decodeEnvelope(body string) (entity.RawCEXMessage, error) {
	var envelope entity.RawCEXMessage
	if err := json.Unmarshal([]byte(body), &envelope); err != nil {
		return entity.RawCEXMessage{}, fmt.Errorf("unmarshal envelope: %w", err)
	}
	if envelope.Source == "" {
		return entity.RawCEXMessage{}, fmt.Errorf("envelope missing source")
	}
	return envelope, nil
}

// Package cex_feed_watcher is the watcher-side of the CEX orderbook pipeline:
// it subscribes to a single exchange's WebSocket feed and forwards every raw
// frame to a downstream publisher (typically SNS). Parsing and persistence
// live in the indexer worker.
//
// One process per exchange is the deployment model — `Source` identifies
// which exchange this instance is feeding.
package cex_feed_watcher

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Config configures the service.
type Config struct {
	// Source is the exchange identifier this watcher is feeding, used for
	// logging and metrics tagging.
	Source string
	Logger *slog.Logger
}

// Service consumes from a CEXFeedSubscriber and forwards messages to a
// CEXFeedPublisher.
type Service struct {
	config     Config
	subscriber outbound.CEXFeedSubscriber
	publisher  outbound.CEXFeedPublisher
	logger     *slog.Logger
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

// NewService constructs the service. All dependencies are required.
func NewService(config Config, subscriber outbound.CEXFeedSubscriber, publisher outbound.CEXFeedPublisher) (*Service, error) {
	if subscriber == nil {
		return nil, fmt.Errorf("subscriber cannot be nil")
	}
	if publisher == nil {
		return nil, fmt.Errorf("publisher cannot be nil")
	}
	if config.Source == "" {
		return nil, fmt.Errorf("source cannot be empty")
	}
	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{
		config:     config,
		subscriber: subscriber,
		publisher:  publisher,
		logger:     logger.With("component", "cex-feed-watcher", "source", config.Source),
	}, nil
}

// Start begins forwarding messages. Returns once the subscription is
// established; the forwarding loop runs in a background goroutine until
// the context is cancelled or Stop is called.
func (s *Service) Start(ctx context.Context) error {
	ctx, s.cancel = context.WithCancel(ctx)
	ch, err := s.subscriber.Subscribe(ctx)
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	s.wg.Add(1)
	go s.forwardLoop(ctx, ch)
	s.logger.Info("service started")
	return nil
}

// Stop signals shutdown and waits for the forwarding goroutine to exit.
func (s *Service) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	if err := s.subscriber.Close(); err != nil {
		s.logger.Warn("subscriber close error", "error", err)
	}
	return nil
}

func (s *Service) forwardLoop(ctx context.Context, ch <-chan entity.RawCEXMessage) {
	defer s.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			if err := s.publisher.Publish(ctx, msg); err != nil {
				// Drop and move on. Live data is point-in-time; the next
				// frame will carry a fresh snapshot. Surfacing the error
				// lets ops/alerts catch sustained publish failures.
				s.logger.Warn("publish failed", "error", err, "payloadBytes", len(msg.Payload))
			}
		}
	}
}

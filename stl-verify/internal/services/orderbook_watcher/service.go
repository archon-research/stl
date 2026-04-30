package orderbook_watcher

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// CacheWriter is the subset of the Redis cache the service needs.
type CacheWriter interface {
	SetLatest(ctx context.Context, snap *entity.OrderbookSnapshot) error
}

type Config struct {
	FlushInterval time.Duration
	Logger        *slog.Logger
}

type Service struct {
	config     Config
	subscriber outbound.OrderbookSubscriber
	repo       outbound.OrderbookRepository
	cache      CacheWriter
	logger     *slog.Logger
	buffer     map[string]*entity.OrderbookSnapshot
	bufferMu   sync.Mutex
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

func NewService(config Config, subscriber outbound.OrderbookSubscriber, repo outbound.OrderbookRepository, cache CacheWriter, logger *slog.Logger) (*Service, error) {
	if subscriber == nil {
		return nil, fmt.Errorf("subscriber cannot be nil")
	}
	if repo == nil {
		return nil, fmt.Errorf("repo cannot be nil")
	}
	if config.FlushInterval <= 0 {
		config.FlushInterval = 60 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{
		config:     config,
		subscriber: subscriber,
		repo:       repo,
		cache:      cache,
		logger:     logger.With("component", "orderbook-watcher"),
		buffer:     make(map[string]*entity.OrderbookSnapshot),
	}, nil
}

func (s *Service) Start(ctx context.Context) error {
	ctx, s.cancel = context.WithCancel(ctx)
	ch, err := s.subscriber.Subscribe(ctx)
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	s.wg.Add(2)
	go s.consumeLoop(ctx, ch)
	go s.flushLoop(ctx)
	s.logger.Info("service started", "flushInterval", s.config.FlushInterval)
	return nil
}

func (s *Service) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	if err := s.subscriber.Unsubscribe(); err != nil {
		s.logger.Warn("unsubscribe error", "error", err)
	}
	return nil
}

func (s *Service) consumeLoop(ctx context.Context, ch <-chan entity.OrderbookSnapshot) {
	defer s.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case snap, ok := <-ch:
			if !ok {
				return
			}
			if s.cache != nil {
				if err := s.cache.SetLatest(ctx, &snap); err != nil {
					s.logger.Warn("cache update failed", "exchange", snap.Exchange, "symbol", snap.Symbol, "error", err)
				}
			}
			key := snap.Exchange + ":" + snap.Symbol
			s.bufferMu.Lock()
			s.buffer[key] = &snap
			s.bufferMu.Unlock()
		}
	}
}

func (s *Service) flushLoop(ctx context.Context) {
	defer s.wg.Done()
	ticker := time.NewTicker(s.config.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			s.flush(context.Background())
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

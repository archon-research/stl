// Package cex_orderbook_indexer persists periodic top-N snapshots of an exchange's
// L2 order books. It drains an OrderbookProvider's update stream into the latest
// book per symbol and, on every tick, trims each book to the best N levels and
// writes one snapshot row per symbol via an OrderbookSnapshotRepository.
package cex_orderbook_indexer

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// instrumentationName scopes this service's OpenTelemetry instruments.
const instrumentationName = "github.com/archon-research/stl/stl-verify/internal/services/cex_orderbook_indexer"

// Config tunes the indexer. Depth and Interval fall back to defaults when unset.
type Config struct {
	// Symbols are the exchange pairs to subscribe to (provider-specific format,
	// e.g. "BTC-USD" for Coinbase). Must be non-empty.
	Symbols []string
	// Depth is the number of best levels kept per side per snapshot. Defaults to
	// defaultDepth when <= 0.
	Depth int
	// Interval is the snapshot cadence. Defaults to defaultInterval when <= 0.
	Interval time.Duration
	// Logger is the structured logger; defaults to slog.Default().
	Logger *slog.Logger
	// MeterProvider supplies the persist-failure counter; defaults to the global
	// provider (otel.GetMeterProvider()).
	MeterProvider metric.MeterProvider
}

const (
	defaultDepth    = 100
	defaultInterval = 5 * time.Second

	// A symbol whose last provider update is older than stalenessFactor x interval
	// (floored at minStaleness) is treated as a dropped feed and NOT re-persisted:
	// during an outage the provider stops emitting and only re-snapshots on
	// reconnect, so re-writing the frozen book every tick with a fresh persisted_at
	// would produce a flat-lined series that reads as live. The floor keeps a quiet
	// (but alive) market from being mistaken for an outage.
	stalenessFactor = 3
	minStaleness    = 30 * time.Second
)

// Service is the order book snapshot indexer. It is a lifecycle.Service: Start
// launches the drain+tick loop in a goroutine and Stop blocks until it has
// drained, flushed its final tick, and returned.
type Service struct {
	provider   outbound.OrderbookProvider
	repo       outbound.OrderbookSnapshotRepository
	symbols    []string
	depth      int
	interval   time.Duration
	staleAfter time.Duration
	logger     *slog.Logger

	persistFailures metric.Int64Counter
	persistDuration metric.Float64Histogram
	exchangeAttr    attribute.KeyValue

	cancel context.CancelFunc
	done   chan struct{}
}

// NewService builds the indexer. It fails fast on a nil dependency or empty
// symbol list rather than starting a worker that can never produce a snapshot.
func NewService(cfg Config, provider outbound.OrderbookProvider, repo outbound.OrderbookSnapshotRepository) (*Service, error) {
	if provider == nil {
		return nil, fmt.Errorf("provider cannot be nil")
	}
	if repo == nil {
		return nil, fmt.Errorf("repo cannot be nil")
	}
	if len(cfg.Symbols) == 0 {
		return nil, fmt.Errorf("symbols cannot be empty")
	}

	depth := cfg.Depth
	if depth <= 0 {
		depth = defaultDepth
	}
	interval := cfg.Interval
	if interval <= 0 {
		interval = defaultInterval
	}
	staleAfter := max(stalenessFactor*interval, minStaleness)
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	mp := cfg.MeterProvider
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	meter := mp.Meter(instrumentationName)
	persistFailures, err := meter.Int64Counter(
		"orderbook.persist.failures.total",
		metric.WithDescription("Total order book snapshot persistence failures (one per failed tick batch)"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating persist-failures counter: %w", err)
	}
	persistDuration, err := meter.Float64Histogram(
		"orderbook.persist.duration_seconds",
		metric.WithDescription("Duration of a successful snapshot batch write to the repository, in seconds"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(telemetry.SecondsDurationBuckets...),
	)
	if err != nil {
		return nil, fmt.Errorf("creating persist-duration histogram: %w", err)
	}

	return &Service{
		provider:        provider,
		repo:            repo,
		symbols:         slices.Clone(cfg.Symbols),
		depth:           depth,
		interval:        interval,
		staleAfter:      staleAfter,
		logger:          logger.With("component", "cex-orderbook-indexer", "exchange", provider.Name()),
		persistFailures: persistFailures,
		persistDuration: persistDuration,
		exchangeAttr:    attribute.String("exchange", provider.Name()),
	}, nil
}

// Start subscribes to the provider and runs the drain+tick loop until Stop is
// called or the provided context is cancelled. It returns an error only on a
// synchronous Watch failure; once the loop is running, transient issues are
// handled inside it.
func (s *Service) Start(ctx context.Context) error {
	// Watch and the run loop share one cancelable context so Stop tears down the
	// provider's WebSocket/reconnect goroutines too, not just the loop.
	loopCtx, cancel := context.WithCancel(ctx)

	updates, err := s.provider.Watch(loopCtx, s.symbols)
	if err != nil {
		cancel()
		return fmt.Errorf("watching order books: %w", err)
	}

	s.cancel = cancel
	s.done = make(chan struct{})

	go func() {
		defer close(s.done)
		s.run(loopCtx, updates)
	}()

	s.logger.Info("started", "symbols", s.symbols, "depth", s.depth, "interval", s.interval)
	return nil
}

// Stop signals the loop to finish and blocks until it has flushed its final tick
// and returned. It is safe to call when Start failed (no-op).
func (s *Service) Stop() error {
	if s.cancel == nil {
		return nil
	}
	s.cancel()
	<-s.done
	return nil
}

// run owns the latest-book map and the snapshot ticker. It drains every update
// into latest and, on each tick, persists the current book for each symbol. On
// shutdown it flushes one final tick so the last observed state is not lost.
//
// The update channel closing (provider stopped on context cancel) ends the loop:
// the loop persists a final tick first so a clean shutdown still captures the
// latest books.
func (s *Service) run(ctx context.Context, updates <-chan entity.OrderbookUpdate) {
	latest := make(map[string]entity.OrderbookUpdate)

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case upd, ok := <-updates:
			if !ok {
				s.flush(ctx, latest)
				return
			}
			latest[upd.Book.Symbol] = upd

		case <-ticker.C:
			s.persistTick(ctx, latest)

		case <-ctx.Done():
			s.flush(ctx, latest)
			return
		}
	}
}

// flushTimeout bounds the final shutdown write so a hung database cannot block
// Stop indefinitely (the lifecycle harness also enforces an outer timeout).
const flushTimeout = 10 * time.Second

// flush persists the last observed books on shutdown. It uses a context detached
// from cancellation (so the final write is not aborted by the same signal that
// stopped the loop) but with its own timeout so it cannot hang forever.
func (s *Service) flush(ctx context.Context, latest map[string]entity.OrderbookUpdate) {
	flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), flushTimeout)
	defer cancel()
	s.persistTick(flushCtx, latest)
}

// persistTick builds a trimmed snapshot for every symbol seen so far and saves
// them in one batch.
//
// Error policy (deliberate — VEC-374 PR discussion): a Save failure increments
// orderbook.persist.failures.total, is logged, and the tick is dropped; the loop
// keeps running. We do NOT bubble the error up to crash the pod:
//   - Unlike the repo's SQS-backed indexers, there is no queue to NACK to — the
//     input is a continuous stream we sample on a tick, so a dropped sample
//     self-heals on the next tick with fresher data (a transient DB blip costs
//     one row). Tearing down a healthy WebSocket would lose far more.
//   - A permanent failure (auth, missing table) fails every tick. Rather than
//     self-crash — no service in this repo does; they lean on external signals —
//     we surface it via the failure counter so the persist-failure alert fires,
//     mirroring how the SQS indexers rely on queue-depth alarms.
func (s *Service) persistTick(ctx context.Context, latest map[string]entity.OrderbookUpdate) {
	if len(latest) == 0 {
		return
	}

	now := time.Now().UTC()
	snapshots := make([]entity.OrderbookSnapshot, 0, len(latest))
	stale := 0
	for _, upd := range latest {
		// Skip a symbol whose feed has gone silent past the staleness window: its
		// book is frozen, and re-persisting it as a fresh row would silently flat-line
		// the series as if the market were live.
		if now.Sub(upd.IngestedAt) > s.staleAfter {
			stale++
			continue
		}
		snapshots = append(snapshots, s.toSnapshot(upd, now))
	}
	if stale > 0 {
		s.logger.Warn("skipping stale order books (no provider update within staleness window)",
			"stale", stale, "fresh", len(snapshots), "staleAfter", s.staleAfter)
	}
	if len(snapshots) == 0 {
		return
	}

	start := time.Now()
	if err := s.repo.Save(ctx, snapshots); err != nil {
		s.persistFailures.Add(context.Background(), 1, metric.WithAttributes(s.exchangeAttr))
		s.logger.Error("failed to persist order book snapshots", "count", len(snapshots), "error", err)
		return
	}
	// Record latency on success only: a slow *failing* write is already covered by
	// the persist-failure alert, and feeding its duration here would trip the
	// latency warning during the same outage and conflate "slow but working" with
	// "broken".
	s.persistDuration.Record(context.Background(), time.Since(start).Seconds(), metric.WithAttributes(s.exchangeAttr))
}

// toSnapshot trims both sides of the update's book to the best s.depth levels and
// assembles the persisted record. persistedAt is the shared tick time so every
// symbol in one tick lands on the same partition timestamp.
func (s *Service) toSnapshot(upd entity.OrderbookUpdate, persistedAt time.Time) entity.OrderbookSnapshot {
	return entity.OrderbookSnapshot{
		Exchange:    upd.Book.Exchange,
		Symbol:      upd.Book.Symbol,
		EventTime:   upd.Time,
		IngestedAt:  upd.IngestedAt,
		PersistedAt: persistedAt,
		Bids:        topLevels(upd.Book.Bids(), entity.Bid, s.depth),
		Asks:        topLevels(upd.Book.Asks(), entity.Ask, s.depth),
	}
}

// topLevels returns the best depth levels of one side, ordered best first: bids
// highest price first, asks lowest price first. Ordering uses exact decimal
// comparison (never float64, which could mis-order two levels differing only past
// its precision and drop the wrong one at the trim boundary). A side shorter than
// depth is returned whole.
func topLevels(levels []entity.PriceLevel, side entity.Side, depth int) []entity.PriceLevel {
	sorted := slices.Clone(levels)
	ascending := side == entity.Ask // asks: lowest price is best
	slices.SortFunc(sorted, func(a, b entity.PriceLevel) int {
		c := entity.CompareDecimal(a.Price, b.Price)
		if ascending {
			return c
		}
		return -c
	})
	if len(sorted) > depth {
		sorted = sorted[:depth]
	}
	return sorted
}

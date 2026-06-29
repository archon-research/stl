package cex_orderbook_indexer

import (
	"context"
	"sync"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// fakeProvider streams updates from a caller-controlled channel. Watch returns
// that channel (or watchErr) and records the symbols it was asked to watch.
type fakeProvider struct {
	name     string
	updates  chan entity.OrderbookUpdate
	watchErr error

	mu      sync.Mutex
	watched []string
}

func (p *fakeProvider) Name() string { return p.name }

func (p *fakeProvider) Watch(ctx context.Context, symbols []string) (<-chan entity.OrderbookUpdate, error) {
	if p.watchErr != nil {
		return nil, p.watchErr
	}
	p.mu.Lock()
	p.watched = symbols
	p.mu.Unlock()
	return p.updates, nil
}

// fakeRepo records every Save call. saveErr, when set, is returned from Save.
type fakeRepo struct {
	mu      sync.Mutex
	saves   [][]entity.OrderbookSnapshot
	saveErr error
	saved   chan struct{} // signalled (non-blocking) after each Save
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{saved: make(chan struct{}, 1024)}
}

func (r *fakeRepo) Save(ctx context.Context, snapshots []entity.OrderbookSnapshot) error {
	r.mu.Lock()
	cp := make([]entity.OrderbookSnapshot, len(snapshots))
	copy(cp, snapshots)
	r.saves = append(r.saves, cp)
	err := r.saveErr
	r.mu.Unlock()
	select {
	case r.saved <- struct{}{}:
	default:
	}
	return err
}

func (r *fakeRepo) allSnapshots() []entity.OrderbookSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []entity.OrderbookSnapshot
	for _, s := range r.saves {
		out = append(out, s...)
	}
	return out
}

func (r *fakeRepo) saveCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.saves)
}

// bookWith builds an OrderbookUpdate for symbol with the given bid and ask levels
// (price->size). eventTime zero means "no venue time".
func bookWith(symbol string, bids, asks map[string]string, eventTime time.Time) entity.OrderbookUpdate {
	ob := entity.NewOrderbook("test-exchange", symbol)
	for p, s := range bids {
		ob.ApplyLevel(entity.Bid, p, s)
	}
	for p, s := range asks {
		ob.ApplyLevel(entity.Ask, p, s)
	}
	return entity.NewOrderbookUpdate(ob, true, eventTime, time.Now())
}

func newTestService(t *testing.T, cfg Config, p *fakeProvider, r *fakeRepo) *Service {
	t.Helper()
	if cfg.Symbols == nil {
		cfg.Symbols = []string{"BTC-USD"}
	}
	svc, err := NewService(cfg, p, r)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return svc
}

func TestNewServiceValidation(t *testing.T) {
	p := &fakeProvider{name: "x", updates: make(chan entity.OrderbookUpdate)}
	r := newFakeRepo()

	tests := []struct {
		name        string
		nilProvider bool
		nilRepo     bool
		symbols     []string
		wantErr     bool
	}{
		{name: "nil provider", nilProvider: true, symbols: []string{"BTC-USD"}, wantErr: true},
		{name: "nil repo", nilRepo: true, symbols: []string{"BTC-USD"}, wantErr: true},
		{name: "empty symbols", symbols: nil, wantErr: true},
		{name: "valid", symbols: []string{"BTC-USD"}, wantErr: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var provider outbound.OrderbookProvider = p
			if tc.nilProvider {
				provider = nil
			}
			var repo outbound.OrderbookSnapshotRepository = r
			if tc.nilRepo {
				repo = nil
			}
			_, err := NewService(Config{Symbols: tc.symbols}, provider, repo)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err=%v, wantErr=%v", err, tc.wantErr)
			}
		})
	}
}

func TestStartReturnsWatchError(t *testing.T) {
	p := &fakeProvider{name: "x", watchErr: context.DeadlineExceeded}
	svc := newTestService(t, Config{}, p, newFakeRepo())
	if err := svc.Start(context.Background()); err == nil {
		t.Fatal("expected Start to surface the Watch error")
	}
	// Stop must be a safe no-op after a failed Start.
	if err := svc.Stop(); err != nil {
		t.Fatalf("Stop after failed Start: %v", err)
	}
}

// TestTickPersistsLatestPerSymbol drives two symbols through the channel and a
// short interval, then verifies each symbol's latest book is persisted on a tick.
func TestTickPersistsLatestPerSymbol(t *testing.T) {
	p := &fakeProvider{name: "test-exchange", updates: make(chan entity.OrderbookUpdate, 8)}
	r := newFakeRepo()
	svc := newTestService(t, Config{
		Symbols:  []string{"BTC-USD", "ETH-USD"},
		Depth:    100,
		Interval: 20 * time.Millisecond,
	}, p, r)

	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	p.updates <- bookWith("BTC-USD", map[string]string{"100": "1"}, map[string]string{"101": "2"}, time.Time{})
	// A later update for BTC supersedes the earlier one (latest wins).
	p.updates <- bookWith("BTC-USD", map[string]string{"100": "1", "99": "3"}, map[string]string{"101": "2"}, time.Time{})
	p.updates <- bookWith("ETH-USD", map[string]string{"50": "5"}, map[string]string{"51": "6"}, time.Time{})

	waitForSaves(t, r, 1)
	if err := svc.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	bySymbol := latestBySymbol(r.allSnapshots())
	btc, ok := bySymbol["BTC-USD"]
	if !ok {
		t.Fatal("no BTC-USD snapshot")
	}
	if len(btc.Bids) != 2 {
		t.Fatalf("BTC bids = %d, want 2 (latest update)", len(btc.Bids))
	}
	if _, ok := bySymbol["ETH-USD"]; !ok {
		t.Fatal("no ETH-USD snapshot")
	}
}

// TestTopNTrimOrderingAndBoundary checks best-first ordering, exact-decimal
// comparison at the trim boundary, and books shorter than the depth.
func TestTopNTrimOrderingAndBoundary(t *testing.T) {
	// Bids 100.10, 100.2, 100.09; asks 200.5, 200.49, 200.51. Depth 2.
	// Best 2 bids (highest): 100.2, 100.10 (100.10 > 100.09 by exact compare).
	// Best 2 asks (lowest): 200.49, 200.5.
	bids := map[string]string{"100.10": "1", "100.2": "2", "100.09": "3"}
	asks := map[string]string{"200.5": "1", "200.49": "2", "200.51": "3"}

	p := &fakeProvider{name: "test-exchange", updates: make(chan entity.OrderbookUpdate, 4)}
	r := newFakeRepo()
	svc := newTestService(t, Config{Symbols: []string{"BTC-USD"}, Depth: 2, Interval: 20 * time.Millisecond}, p, r)
	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	p.updates <- bookWith("BTC-USD", bids, asks, time.Time{})
	waitForSaves(t, r, 1)
	_ = svc.Stop()

	snap := latestBySymbol(r.allSnapshots())["BTC-USD"]
	wantBids := []string{"100.2", "100.10"}
	wantAsks := []string{"200.49", "200.5"}
	assertPrices(t, "bids", snap.Bids, wantBids)
	assertPrices(t, "asks", snap.Asks, wantAsks)
}

func TestBookShorterThanDepth(t *testing.T) {
	p := &fakeProvider{name: "test-exchange", updates: make(chan entity.OrderbookUpdate, 4)}
	r := newFakeRepo()
	svc := newTestService(t, Config{Symbols: []string{"BTC-USD"}, Depth: 100, Interval: 20 * time.Millisecond}, p, r)
	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	p.updates <- bookWith("BTC-USD", map[string]string{"100": "1"}, map[string]string{"101": "2"}, time.Time{})
	waitForSaves(t, r, 1)
	_ = svc.Stop()

	snap := latestBySymbol(r.allSnapshots())["BTC-USD"]
	if len(snap.Bids) != 1 || len(snap.Asks) != 1 {
		t.Fatalf("bids=%d asks=%d, want 1/1", len(snap.Bids), len(snap.Asks))
	}
}

// TestEventTimeNilHandling verifies a zero venue time persists as nil EventTime
// and a present one is carried through.
func TestEventTimeNilHandling(t *testing.T) {
	venueTime := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	p := &fakeProvider{name: "test-exchange", updates: make(chan entity.OrderbookUpdate, 4)}
	r := newFakeRepo()
	svc := newTestService(t, Config{Symbols: []string{"A", "B"}, Interval: 20 * time.Millisecond}, p, r)
	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	p.updates <- bookWith("A", map[string]string{"1": "1"}, map[string]string{"2": "1"}, time.Time{})
	p.updates <- bookWith("B", map[string]string{"1": "1"}, map[string]string{"2": "1"}, venueTime)
	waitForSaves(t, r, 1)
	_ = svc.Stop()

	bySymbol := latestBySymbol(r.allSnapshots())
	if bySymbol["A"].EventTime != nil {
		t.Errorf("symbol A EventTime = %v, want nil", bySymbol["A"].EventTime)
	}
	if got := bySymbol["B"].EventTime; got == nil || !got.Equal(venueTime) {
		t.Errorf("symbol B EventTime = %v, want %v", got, venueTime)
	}
}

// TestGracefulShutdownFlushesFinalTick verifies that stopping the service before
// any ticker tick still persists the buffered state via the shutdown flush.
func TestGracefulShutdownFlushesFinalTick(t *testing.T) {
	p := &fakeProvider{name: "test-exchange", updates: make(chan entity.OrderbookUpdate, 4)}
	r := newFakeRepo()
	// Long interval: the ticker will not fire before Stop, so only the flush writes.
	svc := newTestService(t, Config{Symbols: []string{"BTC-USD"}, Interval: time.Hour}, p, r)
	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	p.updates <- bookWith("BTC-USD", map[string]string{"100": "1"}, map[string]string{"101": "2"}, time.Time{})
	// Give the loop a moment to drain the update into latest before stopping.
	waitUntil(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.watched != nil
	})
	time.Sleep(20 * time.Millisecond)

	if err := svc.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if r.saveCount() == 0 {
		t.Fatal("shutdown flush did not persist the buffered tick")
	}
}

// TestChannelClosedFlushesAndExits verifies that when the provider closes the
// update channel (e.g. context cancel inside Watch), the loop flushes and exits.
func TestChannelClosedFlushesAndExits(t *testing.T) {
	updates := make(chan entity.OrderbookUpdate, 4)
	p := &fakeProvider{name: "test-exchange", updates: updates}
	r := newFakeRepo()
	svc := newTestService(t, Config{Symbols: []string{"BTC-USD"}, Interval: time.Hour}, p, r)
	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	updates <- bookWith("BTC-USD", map[string]string{"100": "1"}, map[string]string{"101": "2"}, time.Time{})
	// Closing the channel ends the loop; the loop flushes the buffered state first.
	waitUntil(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.watched != nil
	})
	time.Sleep(20 * time.Millisecond)
	close(updates)

	// Stop joins the now-finished goroutine.
	if err := svc.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if r.saveCount() == 0 {
		t.Fatal("channel-closed exit did not flush the buffered tick")
	}
}

// TestPersistErrorDoesNotKillLoop verifies a Save error is swallowed (logged) and
// the loop keeps running, persisting again on the next tick.
func TestPersistErrorDoesNotKillLoop(t *testing.T) {
	p := &fakeProvider{name: "test-exchange", updates: make(chan entity.OrderbookUpdate, 4)}
	r := newFakeRepo()
	r.saveErr = context.DeadlineExceeded
	svc := newTestService(t, Config{Symbols: []string{"BTC-USD"}, Interval: 15 * time.Millisecond}, p, r)
	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	p.updates <- bookWith("BTC-USD", map[string]string{"100": "1"}, map[string]string{"101": "2"}, time.Time{})
	// Two ticks should both attempt a save despite the error.
	waitForSaves(t, r, 2)
	_ = svc.Stop()
}

// TestPersistErrorIncrementsFailureMetric verifies a Save error bumps
// orderbook.persist.failures.total — the signal the persist-failure alert fires
// on — rather than only being logged.
func TestPersistErrorIncrementsFailureMetric(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	p := &fakeProvider{name: "test-exchange", updates: make(chan entity.OrderbookUpdate, 4)}
	r := newFakeRepo()
	r.saveErr = context.DeadlineExceeded
	svc := newTestService(t, Config{Symbols: []string{"BTC-USD"}, Interval: 15 * time.Millisecond, MeterProvider: mp}, p, r)
	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = svc.Stop() }) // don't leak the loop if an assertion below fails
	p.updates <- bookWith("BTC-USD", map[string]string{"100": "1"}, map[string]string{"101": "2"}, time.Time{})
	waitForSaves(t, r, 2)
	_ = svc.Stop() // blocks until the loop drains so all failure increments are recorded

	if got := persistFailureCount(t, reader); got < 2 {
		t.Fatalf("orderbook.persist.failures.total = %d, want >= 2", got)
	}
	// A failing Save must NOT feed the latency histogram, or the latency warning
	// would double-fire alongside the persist-failure alert during an outage.
	if got := persistDurationCount(t, reader); got != 0 {
		t.Fatalf("orderbook.persist.duration_seconds recorded %d points on failing saves, want 0", got)
	}
}

// persistFailureCount collects the manual reader and sums the persist-failure
// counter's data points.
func persistFailureCount(t *testing.T, reader sdkmetric.Reader) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "orderbook.persist.failures.total" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric is %T, want metricdata.Sum[int64]", m.Data)
			}
			var total int64
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
			return total
		}
	}
	t.Fatal("orderbook.persist.failures.total not recorded")
	return 0
}

// TestPersistRecordsLatency verifies a successful persist records a data point on
// orderbook.persist.duration_seconds — the histogram the latency alert reads.
func TestPersistRecordsLatency(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	p := &fakeProvider{name: "test-exchange", updates: make(chan entity.OrderbookUpdate, 4)}
	r := newFakeRepo()
	svc := newTestService(t, Config{Symbols: []string{"BTC-USD"}, Interval: 15 * time.Millisecond, MeterProvider: mp}, p, r)
	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = svc.Stop() })
	p.updates <- bookWith("BTC-USD", map[string]string{"100": "1"}, map[string]string{"101": "2"}, time.Time{})
	waitForSaves(t, r, 1)
	_ = svc.Stop() // blocks until the loop drains so the record has happened

	if got := persistDurationCount(t, reader); got == 0 {
		t.Fatal("orderbook.persist.duration_seconds recorded 0 data points, want >= 1")
	}
}

// persistDurationCount collects the manual reader and sums the persist-duration
// histogram's observation count.
func persistDurationCount(t *testing.T, reader sdkmetric.Reader) uint64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "orderbook.persist.duration_seconds" {
				continue
			}
			h, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				t.Fatalf("metric is %T, want metricdata.Histogram[float64]", m.Data)
			}
			var n uint64
			for _, dp := range h.DataPoints {
				n += dp.Count
			}
			return n
		}
	}
	return 0 // instrument absent = no observations recorded
}

// TestStaleBookSkipped: a symbol whose last provider update predates the
// staleness window must NOT be re-persisted (it would flat-line the series during
// an outage), while a fresh symbol in the same tick still is.
func TestStaleBookSkipped(t *testing.T) {
	p := &fakeProvider{name: "test-exchange", updates: make(chan entity.OrderbookUpdate, 4)}
	r := newFakeRepo()
	svc := newTestService(t, Config{Symbols: []string{"BTC-USD", "ETH-USD"}, Interval: 15 * time.Millisecond}, p, r)
	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Fresh BTC (IngestedAt now via bookWith) vs stale ETH (IngestedAt a minute ago,
	// well past the >= 30s staleness floor).
	p.updates <- bookWith("BTC-USD", map[string]string{"100": "1"}, map[string]string{"101": "2"}, time.Time{})
	staleBook := entity.NewOrderbook("test-exchange", "ETH-USD")
	staleBook.ApplyLevel(entity.Bid, "50", "5")
	staleBook.ApplyLevel(entity.Ask, "51", "6")
	p.updates <- entity.NewOrderbookUpdate(staleBook, true, time.Time{}, time.Now().Add(-time.Minute))

	waitForSaves(t, r, 1)
	_ = svc.Stop()

	bySymbol := latestBySymbol(r.allSnapshots())
	if _, ok := bySymbol["BTC-USD"]; !ok {
		t.Error("fresh BTC-USD should be persisted")
	}
	if _, ok := bySymbol["ETH-USD"]; ok {
		t.Error("stale ETH-USD should be skipped, not persisted as fresh")
	}
}

// TestEmptyTickWritesNothing ensures a tick with no observed updates is a no-op.
func TestEmptyTickWritesNothing(t *testing.T) {
	p := &fakeProvider{name: "test-exchange", updates: make(chan entity.OrderbookUpdate)}
	r := newFakeRepo()
	svc := newTestService(t, Config{Symbols: []string{"BTC-USD"}, Interval: 10 * time.Millisecond}, p, r)
	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	time.Sleep(40 * time.Millisecond) // several ticks, no updates
	_ = svc.Stop()
	if r.saveCount() != 0 {
		t.Fatalf("empty ticks wrote %d batches, want 0", r.saveCount())
	}
}

// --- helpers ---

func waitForSaves(t *testing.T, r *fakeRepo, n int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for r.saveCount() < n {
		select {
		case <-r.saved:
		case <-deadline:
			t.Fatalf("timed out waiting for %d saves (got %d)", n, r.saveCount())
		}
	}
}

func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for !cond() {
		select {
		case <-deadline:
			t.Fatal("condition not met before deadline")
		case <-time.After(time.Millisecond):
		}
	}
}

// latestBySymbol returns the last-seen snapshot per symbol across all saves.
func latestBySymbol(snaps []entity.OrderbookSnapshot) map[string]entity.OrderbookSnapshot {
	out := make(map[string]entity.OrderbookSnapshot)
	for _, s := range snaps {
		out[s.Symbol] = s
	}
	return out
}

func assertPrices(t *testing.T, side string, levels []entity.PriceLevel, want []string) {
	t.Helper()
	if len(levels) != len(want) {
		t.Fatalf("%s len = %d, want %d (%v)", side, len(levels), len(want), levels)
	}
	for i, w := range want {
		if levels[i].Price != w {
			t.Errorf("%s[%d].Price = %s, want %s", side, i, levels[i].Price, w)
		}
	}
}

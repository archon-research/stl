package orderbook

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
	"github.com/gorilla/websocket"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// newTestMetrics returns metrics recording into a manual reader so tests can
// collect and assert what was recorded.
func newTestMetrics(t *testing.T, exchange string) (*metrics, sdkmetric.Reader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	m, err := newMetrics(mp, exchange)
	if err != nil {
		t.Fatalf("newMetrics: %v", err)
	}
	return m, reader
}

func collectMetrics(t *testing.T, reader sdkmetric.Reader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collecting metrics: %v", err)
	}
	return rm
}

func findMetric(rm metricdata.ResourceMetrics, name string) (metricdata.Metrics, bool) {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return m, true
			}
		}
	}
	return metricdata.Metrics{}, false
}

// hasAttrs reports whether set carries every want attribute with the same value.
func hasAttrs(set attribute.Set, want []attribute.KeyValue) bool {
	for _, kv := range want {
		v, ok := set.Value(kv.Key)
		if !ok || v != kv.Value {
			return false
		}
	}
	return true
}

// counterValue sums the named int64 counter's data points that carry every
// attribute in attrs. The metric must exist: returning 0 for an absent (e.g.
// misspelled) metric would make expected-zero assertions vacuous.
func counterValue(t *testing.T, rm metricdata.ResourceMetrics, name string, attrs ...attribute.KeyValue) int64 {
	t.Helper()
	m, ok := findMetric(rm, name)
	if !ok {
		t.Fatalf("metric %q not found", name)
	}
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("metric %q is %T, want metricdata.Sum[int64]", name, m.Data)
	}
	var total int64
	for _, dp := range sum.DataPoints {
		if hasAttrs(dp.Attributes, attrs) {
			total += dp.Value
		}
	}
	return total
}

// gaugeValue returns the named float64 gauge's data point carrying every
// attribute in attrs, with ok=false when absent.
func gaugeValue(t *testing.T, rm metricdata.ResourceMetrics, name string, attrs ...attribute.KeyValue) (float64, bool) {
	t.Helper()
	m, ok := findMetric(rm, name)
	if !ok {
		return 0, false
	}
	g, ok := m.Data.(metricdata.Gauge[float64])
	if !ok {
		t.Fatalf("metric %q is %T, want metricdata.Gauge[float64]", name, m.Data)
	}
	for _, dp := range g.DataPoints {
		if hasAttrs(dp.Attributes, attrs) {
			return dp.Value, true
		}
	}
	return 0, false
}

// histogram returns the named float64 histogram's single data point, with
// ok=false when the metric was never recorded.
func histogram(t *testing.T, rm metricdata.ResourceMetrics, name string) (metricdata.HistogramDataPoint[float64], bool) {
	t.Helper()
	m, ok := findMetric(rm, name)
	if !ok {
		return metricdata.HistogramDataPoint[float64]{}, false
	}
	h, ok := m.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("metric %q is %T, want metricdata.Histogram[float64]", name, m.Data)
	}
	if len(h.DataPoints) == 0 {
		return metricdata.HistogramDataPoint[float64]{}, false
	}
	if len(h.DataPoints) != 1 {
		t.Fatalf("metric %q has %d data points, want 1", name, len(h.DataPoints))
	}
	return h.DataPoints[0], true
}

func exchangeAttr(v string) attribute.KeyValue { return attribute.String("exchange", v) }

// TestWatchRecordsMetrics drives the full pipeline through the public API and
// asserts the engine's per-update and connection-state instrumentation.
func TestWatchRecordsMetrics(t *testing.T) {
	srv := newWSTestServer(t, func(conn *websocket.Conn) {
		sendText(t, conn, `{"symbol":"X","snapshot":true,"price":100,"size":1}`)
		sendText(t, conn, `{"symbol":"X","snapshot":false,"price":101,"size":2}`)
		keepOpen(conn)
	})

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	cfg := testConfig()
	cfg.MeterProvider = mp

	p := newTestFeedProvider(t, cfg, &fakeExchange{url: srv.url}, 10)
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := p.Watch(ctx, []string{"X"})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	receiveWithin(t, ch, 2*time.Second)
	receiveWithin(t, ch, 2*time.Second)

	// connUp happens-before the first delivered update, so this read is ordered.
	rm := collectMetrics(t, reader)
	ex := exchangeAttr("fake")
	if got := counterValue(t, rm, "orderbook.connection.state", ex); got != 1 {
		t.Errorf("connection.state = %d, want 1 while connected", got)
	}

	// The emitted counter is recorded after the channel send, so assert only
	// after shutdown: channel close orders every emitted() call before this
	// collect.
	cancel()
	for range ch { //nolint:revive // drain until closed
	}
	rm = collectMetrics(t, reader)
	if got := counterValue(t, rm, "orderbook.updates.emitted.total", ex, attribute.String("type", "snapshot")); got != 1 {
		t.Errorf("emitted snapshots = %d, want 1", got)
	}
	if got := counterValue(t, rm, "orderbook.updates.emitted.total", ex, attribute.String("type", "delta")); got != 1 {
		t.Errorf("emitted deltas = %d, want 1", got)
	}
	if _, ok := gaugeValue(t, rm, "orderbook.last_update.age", ex, attribute.String("symbol", "X")); !ok {
		t.Error("last_update.age for X missing")
	}
	if got := counterValue(t, rm, "orderbook.connection.state", ex); got != 0 {
		t.Errorf("connection.state = %d after shutdown, want 0", got)
	}
	// A clean first connect is cold start, not a resync.
	if _, ok := histogram(t, rm, "orderbook.resync.duration"); ok {
		t.Error("resync.duration recorded without a connection loss, want none")
	}
}

// TestReconnectLoopRecordsReconnectAndResync covers the loss-to-resync window:
// a synced connection dies with a sequence gap, the next attempt resyncs, and
// the loop records one classified reconnection plus one resync duration sample.
func TestReconnectLoopRecordsReconnectAndResync(t *testing.T) {
	m, reader := newTestMetrics(t, "test")
	cfg := testConfig()
	cfg.InitialBackoff = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	var calls atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		reconnectLoop(ctx, cfg, cfg.Logger, m, func(_ context.Context, ready func()) error {
			if calls.Add(1) == 1 {
				ready()                                       // synced...
				return fmt.Errorf("test: %w", errSequenceGap) // ...then gapped
			}
			ready() // resynced: closes the loss window
			cancel()
			return errors.New("closing")
		})
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("reconnectLoop did not stop")
	}

	rm := collectMetrics(t, reader)
	ex := exchangeAttr("test")
	if got := counterValue(t, rm, "orderbook.reconnections.total", ex, attribute.String("reason", "sequence_gap")); got != 1 {
		t.Errorf("reconnections{sequence_gap} = %d, want 1", got)
	}
	dp, ok := histogram(t, rm, "orderbook.resync.duration")
	if !ok || dp.Count != 1 {
		t.Errorf("resync.duration count = %v (ok=%v), want exactly 1 sample", dp.Count, ok)
	}
}

func TestReconnectReasonClassification(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"sequence gap", fmt.Errorf("okx X: %w", errSequenceGap), "sequence_gap"},
		{"unexpected symbol", fmt.Errorf("okx Y: %w", errUnexpectedSymbol), "unexpected_symbol"},
		{"transport error", errors.New("websocket: eof"), "ws_error"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, reader := newTestMetrics(t, "test")
			m.reconnected(tt.err)
			rm := collectMetrics(t, reader)
			got := counterValue(t, rm, "orderbook.reconnections.total",
				exchangeAttr("test"), attribute.String("reason", tt.want))
			if got != 1 {
				t.Errorf("reconnections{reason=%s} = %d, want 1", tt.want, got)
			}
		})
	}
}

// TestEmitterRecordsEmittedAndDropped: a delivered update counts as emitted and
// refreshes the staleness clock; a buffer-full drop counts as dropped.
func TestEmitterRecordsEmittedAndDropped(t *testing.T) {
	m, reader := newTestMetrics(t, "test")
	out := make(chan entity.OrderbookUpdate, 1)
	em := newEmitter(out, testLogger(), m)

	book := entity.NewOrderbook("test", "X")
	book.ApplyLevel(entity.Bid, "100", "1")
	em.emit(book, true, time.Unix(1, 0))  // delivered
	em.emit(book, false, time.Unix(2, 0)) // buffer full: dropped

	rm := collectMetrics(t, reader)
	ex := exchangeAttr("test")
	if got := counterValue(t, rm, "orderbook.updates.emitted.total", ex); got != 1 {
		t.Errorf("emitted = %d, want 1", got)
	}
	if got := counterValue(t, rm, "orderbook.updates.dropped.total", ex); got != 1 {
		t.Errorf("dropped = %d, want 1", got)
	}
	if _, ok := gaugeValue(t, rm, "orderbook.last_update.age", ex, attribute.String("symbol", "X")); !ok {
		t.Error("last_update.age for X missing after a delivered update")
	}
}

// TestResyncDurationUsesSecondsBuckets guards the bucket-boundary regression
// (see the alchemy request-duration test): without explicit boundaries the SDK
// applies millisecond-scale defaults and quantiles over backoff-length resyncs
// saturate the top bucket.
func TestResyncDurationUsesSecondsBuckets(t *testing.T) {
	m, reader := newTestMetrics(t, "test")
	m.resynced(30 * time.Millisecond)
	rm := collectMetrics(t, reader)
	dp, ok := histogram(t, rm, "orderbook.resync.duration")
	if !ok {
		t.Fatal("orderbook.resync.duration not found")
	}
	if !slices.Equal(dp.Bounds, telemetry.SecondsDurationBuckets) {
		t.Errorf("resync.duration bounds = %v, want telemetry.SecondsDurationBuckets", dp.Bounds)
	}
}

package orderbook

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// instrumentationName is the name used for OpenTelemetry instrumentation.
const instrumentationName = "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/orderbook"

// metrics records the feed engine's OpenTelemetry metrics. One instance is
// scoped to one provider, and every metric carries an `exchange` attribute:
//
//   - orderbook.updates.emitted.total: delivered updates, by type (snapshot|delta)
//   - orderbook.updates.dropped.total: updates dropped on a full output buffer
//   - orderbook.reconnections.total: dropped connections, by reason
//     (sequence_gap|unexpected_symbol|ws_error)
//   - orderbook.connection.state: currently open WebSocket connections
//   - orderbook.resync.duration: seconds from a connection group losing data
//     flow to being fully snapshot-synced again
//   - orderbook.last_update.age: seconds since each symbol's last delivered
//     update; the alerting signal for a symbol that has silently gone dead
//
// Methods are goroutine-safe (one provider records from every connection
// goroutine) and take no context: nothing trace-scoped exists at the recording
// sites, so they record against context.Background().
type metrics struct {
	exchangeAttr attribute.KeyValue

	emittedTotal    metric.Int64Counter
	droppedTotal    metric.Int64Counter
	reconnectsTotal metric.Int64Counter
	connectionState metric.Int64UpDownCounter
	resyncDuration  metric.Float64Histogram

	mu         sync.Mutex
	lastUpdate map[string]time.Time // symbol -> wall clock of last delivered update
}

// newMetrics creates the provider's instruments on mp and registers the
// last-update age callback. The callback stays registered for the life of the
// meter provider, matching the provider's process-long lifetime.
func newMetrics(mp metric.MeterProvider, exchange string) (*metrics, error) {
	meter := mp.Meter(instrumentationName)
	m := &metrics{
		exchangeAttr: attribute.String("exchange", exchange),
		lastUpdate:   make(map[string]time.Time),
	}

	var err error
	if m.emittedTotal, err = meter.Int64Counter(
		"orderbook.updates.emitted.total",
		metric.WithDescription("Total orderbook updates delivered to the consumer"),
	); err != nil {
		return nil, fmt.Errorf("creating emitted counter: %w", err)
	}
	if m.droppedTotal, err = meter.Int64Counter(
		"orderbook.updates.dropped.total",
		metric.WithDescription("Total orderbook updates dropped because the output buffer was full"),
	); err != nil {
		return nil, fmt.Errorf("creating dropped counter: %w", err)
	}
	if m.reconnectsTotal, err = meter.Int64Counter(
		"orderbook.reconnections.total",
		metric.WithDescription("Total orderbook WebSocket connection drops, by reason"),
	); err != nil {
		return nil, fmt.Errorf("creating reconnections counter: %w", err)
	}
	if m.connectionState, err = meter.Int64UpDownCounter(
		"orderbook.connection.state",
		metric.WithDescription("Number of currently open orderbook WebSocket connections"),
	); err != nil {
		return nil, fmt.Errorf("creating connection-state counter: %w", err)
	}
	if m.resyncDuration, err = meter.Float64Histogram(
		"orderbook.resync.duration",
		metric.WithDescription("Seconds from losing a connection to its symbol group being fully synced again"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(telemetry.SecondsDurationBuckets...),
	); err != nil {
		return nil, fmt.Errorf("creating resync-duration histogram: %w", err)
	}

	lastUpdateAge, err := meter.Float64ObservableGauge(
		"orderbook.last_update.age",
		metric.WithDescription("Seconds since the last delivered orderbook update, per symbol"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating last-update-age gauge: %w", err)
	}
	if _, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		m.mu.Lock()
		defer m.mu.Unlock()
		for sym, ts := range m.lastUpdate {
			o.ObserveFloat64(lastUpdateAge, time.Since(ts).Seconds(),
				metric.WithAttributes(m.exchangeAttr, attribute.String("symbol", sym)))
		}
		return nil
	}, lastUpdateAge); err != nil {
		return nil, fmt.Errorf("registering last-update-age callback: %w", err)
	}
	return m, nil
}

// emitted records a delivered update and refreshes the symbol's staleness clock.
func (m *metrics) emitted(symbol string, snapshot bool) {
	m.mu.Lock()
	m.lastUpdate[symbol] = time.Now()
	m.mu.Unlock()
	kind := "delta"
	if snapshot {
		kind = "snapshot"
	}
	m.emittedTotal.Add(context.Background(), 1,
		metric.WithAttributes(m.exchangeAttr, attribute.String("type", kind)))
}

// dropped records an update lost to a full output buffer.
func (m *metrics) dropped() {
	m.droppedTotal.Add(context.Background(), 1, metric.WithAttributes(m.exchangeAttr))
}

// reconnected records a dropped connection, classified by what killed it.
func (m *metrics) reconnected(err error) {
	reason := "ws_error"
	switch {
	case errors.Is(err, errSequenceGap):
		reason = "sequence_gap"
	case errors.Is(err, errUnexpectedSymbol):
		reason = "unexpected_symbol"
	}
	m.reconnectsTotal.Add(context.Background(), 1,
		metric.WithAttributes(m.exchangeAttr, attribute.String("reason", reason)))
}

// connUp records a connection becoming established.
func (m *metrics) connUp() {
	m.connectionState.Add(context.Background(), 1, metric.WithAttributes(m.exchangeAttr))
}

// connDown records a connection ending.
func (m *metrics) connDown() {
	m.connectionState.Add(context.Background(), -1, metric.WithAttributes(m.exchangeAttr))
}

// resynced records the data-gap window: connection lost to group fully synced.
func (m *metrics) resynced(d time.Duration) {
	m.resyncDuration.Record(context.Background(), d.Seconds(), metric.WithAttributes(m.exchangeAttr))
}

// telemetry.go provides OpenTelemetry instrumentation for the Alchemy adapters.
//
// It defines metrics and tracing for both the HTTP client and WebSocket subscriber:
//
// HTTP Client metrics:
//   - alchemy.client.request.duration: Histogram of request latencies
//   - alchemy.client.requests.total: Counter of total requests by method/status
//   - alchemy.client.retries.total: Counter of retry attempts
//   - alchemy.client.batch.size: Histogram of batch request sizes
//
// WebSocket Subscriber metrics:
//   - alchemy.subscriber.reconnections.total: Counter of reconnection events
//   - alchemy.subscriber.blocks.received.total: Counter of blocks received
//   - alchemy.subscriber.blocks.dropped.total: Counter of dropped blocks (buffer full)
//   - alchemy.subscriber.connection.state: Gauge of connection state (1=connected, 0=disconnected)
package alchemy

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const (
	// instrumentationName is the name used for OpenTelemetry instrumentation.
	instrumentationName = "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/alchemy"
)

// Telemetry provides OpenTelemetry metrics and tracing for the Alchemy adapters.
type Telemetry struct {
	tracer trace.Tracer
	meter  metric.Meter

	// HTTP Client metrics
	requestDuration metric.Float64Histogram
	requestsTotal   metric.Int64Counter
	retriesTotal    metric.Int64Counter
	batchSize       metric.Int64Histogram

	// WebSocket Subscriber metrics
	reconnectionsTotal  metric.Int64Counter
	blocksReceivedTotal metric.Int64Counter
	blocksDroppedTotal  metric.Int64Counter
	connectionState     metric.Int64UpDownCounter
}

// NewTelemetry creates a new Telemetry instance with OpenTelemetry instrumentation.
// Uses the global tracer and meter providers by default.
func NewTelemetry() (*Telemetry, error) {
	return NewTelemetryWithProviders(
		otel.GetTracerProvider(),
		otel.GetMeterProvider(),
	)
}

// NewTelemetryWithProviders creates a new Telemetry instance with custom providers.
func NewTelemetryWithProviders(tp trace.TracerProvider, mp metric.MeterProvider) (*Telemetry, error) {
	tracer := tp.Tracer(instrumentationName)
	meter := mp.Meter(instrumentationName)

	t := &Telemetry{
		tracer: tracer,
		meter:  meter,
	}

	var err error

	// HTTP Client metrics
	t.requestDuration, err = meter.Float64Histogram(
		"alchemy.client.request.duration",
		metric.WithDescription("Duration of HTTP RPC requests in seconds"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	t.requestsTotal, err = meter.Int64Counter(
		"alchemy.client.requests.total",
		metric.WithDescription("Total number of HTTP RPC requests"),
	)
	if err != nil {
		return nil, err
	}

	t.retriesTotal, err = meter.Int64Counter(
		"alchemy.client.retries.total",
		metric.WithDescription("Total number of retry attempts"),
	)
	if err != nil {
		return nil, err
	}

	t.batchSize, err = meter.Int64Histogram(
		"alchemy.client.batch.size",
		metric.WithDescription("Size of batch RPC requests"),
	)
	if err != nil {
		return nil, err
	}

	// WebSocket Subscriber metrics
	t.reconnectionsTotal, err = meter.Int64Counter(
		"alchemy.subscriber.reconnections.total",
		metric.WithDescription("Total number of WebSocket reconnections"),
	)
	if err != nil {
		return nil, err
	}

	t.blocksReceivedTotal, err = meter.Int64Counter(
		"alchemy.subscriber.blocks.received.total",
		metric.WithDescription("Total number of block headers received"),
	)
	if err != nil {
		return nil, err
	}

	t.blocksDroppedTotal, err = meter.Int64Counter(
		"alchemy.subscriber.blocks.dropped.total",
		metric.WithDescription("Total number of block headers dropped due to full channel"),
	)
	if err != nil {
		return nil, err
	}

	t.connectionState, err = meter.Int64UpDownCounter(
		"alchemy.subscriber.connection.state",
		metric.WithDescription("Current connection state (1=connected, 0=disconnected)"),
	)
	if err != nil {
		return nil, err
	}

	return t, nil
}

// --- HTTP Client instrumentation ---

// StartSpan starts a new span for an RPC method call.
func (t *Telemetry) StartSpan(ctx context.Context, method string) (context.Context, trace.Span) {
	// Use descriptive span name that X-Ray will display
	spanName := "alchemy." + method
	return t.tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("rpc.system", "jsonrpc"),
			attribute.String("rpc.method", method),
		),
	)
}

// RecordRequest records metrics for an HTTP RPC request.
func (t *Telemetry) RecordRequest(ctx context.Context, method string, duration time.Duration, err error) {
	attrs := []attribute.KeyValue{
		attribute.String("rpc.method", method),
	}

	status := "success"
	if err != nil {
		status = "error"
	}
	attrs = append(attrs, attribute.String("status", status))

	t.requestDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(attrs...))
	t.requestsTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// RecordRetry records a retry attempt.
func (t *Telemetry) RecordRetry(ctx context.Context, method string, attempt int) {
	t.retriesTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("rpc.method", method),
		attribute.Int("attempt", attempt),
	))
}

// RecordBatchSize records the size of a batch request.
func (t *Telemetry) RecordBatchSize(ctx context.Context, size int) {
	t.batchSize.Record(ctx, int64(size))
}

// --- WebSocket Subscriber instrumentation ---

// RecordReconnection records a WebSocket reconnection event.
func (t *Telemetry) RecordReconnection(ctx context.Context) {
	t.reconnectionsTotal.Add(ctx, 1)
}

// RecordBlockReceived records a block header being received.
func (t *Telemetry) RecordBlockReceived(ctx context.Context, blockNum int64) {
	t.blocksReceivedTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.Int64("block.number", blockNum),
	))
}

// RecordBlockDropped records a block header being dropped due to full channel.
func (t *Telemetry) RecordBlockDropped(ctx context.Context, blockNum int64) {
	t.blocksDroppedTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.Int64("block.number", blockNum),
	))
}

// RecordConnectionUp records the connection becoming established.
func (t *Telemetry) RecordConnectionUp(ctx context.Context) {
	t.connectionState.Add(ctx, 1)
}

// RecordConnectionDown records the connection being lost.
func (t *Telemetry) RecordConnectionDown(ctx context.Context) {
	t.connectionState.Add(ctx, -1)
}

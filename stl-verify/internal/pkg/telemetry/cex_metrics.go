package telemetry

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// latencyBucketsSeconds covers µs-scale parser work up to multi-second
// DB flushes. The OTel SDK default boundaries are designed for
// millisecond-unit values, so sub-millisecond operations otherwise collapse
// into a single bucket and the distribution becomes unreadable.
var latencyBucketsSeconds = []float64{
	0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05,
	0.1, 0.25, 0.5, 1, 2.5, 5,
}

// payloadBytesBuckets spans tiny heartbeat/ack frames (~50 B) up through the
// large deep-book snapshots from Bybit/OKX that can reach 50 KB+.
var payloadBytesBuckets = []float64{
	64, 256, 512, 1024, 2048, 4096, 8192,
	16384, 32768, 65536, 131072,
}

// CEXMetrics records throughput and latency for the CEX feed pipeline:
// the watcher's publish side and the indexer's consume/parse/flush side.
//
// All methods are safe to call on a nil receiver — services accept a
// *CEXMetrics and call through unconditionally; tests pass nil.
type CEXMetrics struct {
	// Indexer side: every SQS message that flows through processMessage.
	messagesProcessed metric.Int64Counter
	processingLatency metric.Float64Histogram
	snapshotsProduced metric.Int64Counter
	flushBatchSize    metric.Int64Histogram
	flushLatency      metric.Float64Histogram

	// Watcher side: every frame the forward loop publishes.
	messagesForwarded metric.Int64Counter
	publishLatency    metric.Float64Histogram
	messageBytes      metric.Int64Histogram

	// Static, set-once descriptor of each exchange's wire format. Carries
	// the value 1 so it's always present in the scrape; the value itself is
	// meaningless — the labels are the payload.
	exchangeInfo metric.Int64Gauge
}

// NewCEXMetrics constructs a CEXMetrics bound to the global meter provider
// (set up by InitMetrics). meterName is typically the service name.
func NewCEXMetrics(meterName string) (*CEXMetrics, error) {
	meter := otel.Meter(meterName)

	processed, err := meter.Int64Counter(
		"cex_messages_processed_total",
		metric.WithDescription("CEX feed messages processed by the indexer, labelled by exchange and outcome"),
	)
	if err != nil {
		return nil, fmt.Errorf("create cex_messages_processed_total: %w", err)
	}

	procLatency, err := meter.Float64Histogram(
		"cex_processing_duration_seconds",
		metric.WithDescription("Time to decode + parse a single CEX feed message"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(latencyBucketsSeconds...),
	)
	if err != nil {
		return nil, fmt.Errorf("create cex_processing_duration_seconds: %w", err)
	}

	snapshots, err := meter.Int64Counter(
		"cex_snapshots_produced_total",
		metric.WithDescription("Orderbook snapshots emitted by parsers, labelled by exchange"),
	)
	if err != nil {
		return nil, fmt.Errorf("create cex_snapshots_produced_total: %w", err)
	}

	flushSize, err := meter.Int64Histogram(
		"cex_flush_batch_size",
		metric.WithDescription("Number of snapshots written to the DB per flush"),
	)
	if err != nil {
		return nil, fmt.Errorf("create cex_flush_batch_size: %w", err)
	}

	flushLat, err := meter.Float64Histogram(
		"cex_flush_duration_seconds",
		metric.WithDescription("Time to flush the snapshot buffer to the DB"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(latencyBucketsSeconds...),
	)
	if err != nil {
		return nil, fmt.Errorf("create cex_flush_duration_seconds: %w", err)
	}

	forwarded, err := meter.Int64Counter(
		"cex_messages_forwarded_total",
		metric.WithDescription("Raw CEX frames forwarded by the watcher, labelled by exchange and outcome"),
	)
	if err != nil {
		return nil, fmt.Errorf("create cex_messages_forwarded_total: %w", err)
	}

	pubLatency, err := meter.Float64Histogram(
		"cex_publish_duration_seconds",
		metric.WithDescription("Time to publish a single raw frame from the watcher"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(latencyBucketsSeconds...),
	)
	if err != nil {
		return nil, fmt.Errorf("create cex_publish_duration_seconds: %w", err)
	}

	msgBytes, err := meter.Int64Histogram(
		"cex_message_bytes",
		metric.WithDescription("Raw WebSocket frame size in bytes, labelled by exchange"),
		metric.WithUnit("By"),
		metric.WithExplicitBucketBoundaries(payloadBytesBuckets...),
	)
	if err != nil {
		return nil, fmt.Errorf("create cex_message_bytes: %w", err)
	}

	info, err := meter.Int64Gauge(
		"cex_exchange_info",
		metric.WithDescription("Static descriptor of an exchange's wire format and snapshot semantics; value is always 1"),
	)
	if err != nil {
		return nil, fmt.Errorf("create cex_exchange_info: %w", err)
	}

	return &CEXMetrics{
		messagesProcessed: processed,
		processingLatency: procLatency,
		snapshotsProduced: snapshots,
		flushBatchSize:    flushSize,
		flushLatency:      flushLat,
		messagesForwarded: forwarded,
		publishLatency:    pubLatency,
		messageBytes:      msgBytes,
		exchangeInfo:      info,
	}, nil
}

// RecordMessageProcessed records one indexer message with its outcome and
// the time taken to process it. status is one of: "success", "decode_error",
// "unknown_source", "parse_error".
func (m *CEXMetrics) RecordMessageProcessed(ctx context.Context, exchange, status string, duration time.Duration) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(
		attribute.String("exchange", exchange),
		attribute.String("status", status),
	)
	m.messagesProcessed.Add(ctx, 1, attrs)
	m.processingLatency.Record(ctx, duration.Seconds(), attrs)
}

// RecordSnapshotsProduced increments the snapshot counter by count for the
// given exchange. Called once per parse success — count = len(snapshots).
func (m *CEXMetrics) RecordSnapshotsProduced(ctx context.Context, exchange string, count int) {
	if m == nil || count == 0 {
		return
	}
	m.snapshotsProduced.Add(ctx, int64(count),
		metric.WithAttributes(attribute.String("exchange", exchange)))
}

// RecordFlush records one DB flush: batch size and duration, plus status
// ("success" or "error").
func (m *CEXMetrics) RecordFlush(ctx context.Context, count int, duration time.Duration, status string) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(attribute.String("status", status))
	m.flushBatchSize.Record(ctx, int64(count), attrs)
	m.flushLatency.Record(ctx, duration.Seconds(), attrs)
}

// RecordMessageForwarded records one watcher publish attempt. status is
// "success" or "error".
func (m *CEXMetrics) RecordMessageForwarded(ctx context.Context, exchange, status string, duration time.Duration) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(
		attribute.String("exchange", exchange),
		attribute.String("status", status),
	)
	m.messagesForwarded.Add(ctx, 1, attrs)
	m.publishLatency.Record(ctx, duration.Seconds(), attrs)
}

// RecordMessageBytes records the raw frame size in bytes for one published
// message. Call alongside RecordMessageForwarded.
func (m *CEXMetrics) RecordMessageBytes(ctx context.Context, exchange string, bytes int) {
	if m == nil || bytes <= 0 {
		return
	}
	m.messageBytes.Record(ctx, int64(bytes),
		metric.WithAttributes(attribute.String("exchange", exchange)))
}

// RecordExchangeInfo emits a value-of-1 gauge whose labels describe the
// exchange's static wire format. Call once at watcher startup; the gauge
// stays in the meter for the lifetime of the process.
//
// wireFormat is a short identifier of the protocol envelope (e.g.
// "combined_stream", "v5_public_orderbook"). kind classifies the frame
// semantics: "snapshot", "delta", or "mixed" (exchange tags each frame).
func (m *CEXMetrics) RecordExchangeInfo(ctx context.Context, exchange, wireFormat, kind string) {
	if m == nil {
		return
	}
	m.exchangeInfo.Record(ctx, 1, metric.WithAttributes(
		attribute.String("exchange", exchange),
		attribute.String("wire_format", wireFormat),
		attribute.String("kind", kind),
	))
}

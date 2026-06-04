package telemetry

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Metrics implements the MetricsRecorder interface using OpenTelemetry.
type Metrics struct {
	processingLatency metric.Float64Histogram
	blocksProcessed   metric.Int64Counter

	// chainAttr is the constant per-chain attribute attached to every metric.
	// One process serves one chain, so the value is fixed at construction. It
	// surfaces as the `chain` Prometheus label that the Vector backup-worker
	// alerts group by; without it those alerts render an empty chain.
	chainAttr attribute.KeyValue
}

// NewMetrics creates a new OpenTelemetry metrics recorder.
// meterName should typically be the package name or service name. chain is the
// chain name (e.g. "arbitrum") attached as the `chain` label on every metric.
func NewMetrics(meterName, chain string) (*Metrics, error) {
	meter := otel.Meter(meterName)

	latency, err := meter.Float64Histogram(
		"processing_duration_seconds",
		metric.WithDescription("Time taken to process a block backup message"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(SecondsDurationBuckets...),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create processing_duration_seconds histogram: %w", err)
	}

	blocks, err := meter.Int64Counter(
		"blocks_processed_total",
		metric.WithDescription("Total number of blocks processed"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create blocks_processed_total counter: %w", err)
	}

	return &Metrics{
		processingLatency: latency,
		blocksProcessed:   blocks,
		chainAttr:         attribute.String("chain", chain),
	}, nil
}

// RecordProcessingLatency records the duration of message processing.
func (m *Metrics) RecordProcessingLatency(ctx context.Context, duration time.Duration, status string) {
	m.processingLatency.Record(ctx, duration.Seconds(), metric.WithAttributes(m.chainAttr, attribute.String("status", status)))
}

// RecordBlockProcessed increments the blocks processed counter.
func (m *Metrics) RecordBlockProcessed(ctx context.Context, status string) {
	m.blocksProcessed.Add(ctx, 1, metric.WithAttributes(m.chainAttr, attribute.String("status", status)))
}

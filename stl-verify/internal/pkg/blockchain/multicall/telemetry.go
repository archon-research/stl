// telemetry.go provides OpenTelemetry instrumentation for the multicall client.
//
//	multicall.batch.size: histogram of calls per Execute(). _count is the number
//	of multicalls (= batched S3 PUTs/sec when archiving batches per Execute),
//	_sum is the number of individual SC calls (= current per-call S3 PUTs/sec),
//	and the bucket distribution gives the worst-case burst a single Execute can
//	produce.
package multicall

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const instrumentationName = "github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/multicall"

// batchSizeBuckets are explicit boundaries for the call-count histogram. Unlike
// the seconds-unit latency histograms (see telemetry.SecondsDurationBuckets),
// this counts calls per multicall, which range from 1 to several hundred. The
// boundaries give resolution across that range so histogram_quantile resolves a
// meaningful p99/p100 burst size. The unit "{call}" keeps the seconds view in
// telemetry.InitMetrics from overriding these.
var batchSizeBuckets = []float64{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000}

// Telemetry records multicall metrics. The zero value is unusable; build with
// NewTelemetry or NewTelemetryWithProvider.
type Telemetry struct {
	batchSize metric.Int64Histogram
	chainAttr attribute.KeyValue
}

// NewTelemetry builds Telemetry against the global meter provider. chain is the
// chain name (e.g. "mainnet") attached as the `chain` label on every metric.
func NewTelemetry(chain string) (*Telemetry, error) {
	return NewTelemetryWithProvider(otel.GetMeterProvider(), chain)
}

// NewTelemetryWithProvider builds Telemetry against a caller-supplied meter
// provider. Used by tests to inject a manual reader.
func NewTelemetryWithProvider(mp metric.MeterProvider, chain string) (*Telemetry, error) {
	meter := mp.Meter(instrumentationName)
	batchSize, err := meter.Int64Histogram(
		"multicall.batch.size",
		metric.WithDescription("Number of calls in a single multicall Execute"),
		metric.WithUnit("{call}"),
		metric.WithExplicitBucketBoundaries(batchSizeBuckets...),
	)
	if err != nil {
		return nil, fmt.Errorf("registering multicall.batch.size histogram: %w", err)
	}
	return &Telemetry{batchSize: batchSize, chainAttr: attribute.String("chain", chain)}, nil
}

// RecordBatch records one multicall Execute holding size individual calls.
func (t *Telemetry) RecordBatch(ctx context.Context, size int) {
	t.batchSize.Record(ctx, int64(size), metric.WithAttributes(t.chainAttr))
}

package telemetry

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
)

// MetricConfig holds configuration for the metrics.
type MetricConfig struct {
	// ServiceName is the name of the service.
	ServiceName string

	// ServiceVersion is the version of the service.
	ServiceVersion string

	// Environment is the deployment environment.
	Environment string

	// OTLPEndpoint is the OTLP gRPC endpoint.
	OTLPEndpoint string
}

// InitMetrics initializes the OpenTelemetry meter provider.
func InitMetrics(ctx context.Context, config MetricConfig) (shutdown func(context.Context) error, err error) {
	if config.OTLPEndpoint == "" {
		// If no endpoint, use a no-op provider or stdout?
		// For now, we'll just return with no error and default no-op provider.
		return func(_ context.Context) error { return nil }, nil
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(config.ServiceName),
			semconv.ServiceVersion(config.ServiceVersion),
			semconv.DeploymentEnvironmentNameKey.String(config.Environment),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// Use WithEndpointURL which accepts full URL with scheme (e.g., "http://localhost:4317")
	// This is the proper way to handle OTEL_EXPORTER_OTLP_ENDPOINT which includes the scheme
	exporter, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpointURL(config.OTLPEndpoint),
		otlpmetricgrpc.WithInsecure(), // Assuming internal communication
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP metric exporter: %w", err)
	}

	meterProvider := metric.NewMeterProvider(
		metric.WithResource(res),
		metric.WithReader(metric.NewPeriodicReader(exporter, metric.WithInterval(15*time.Second))),
		metric.WithView(secondsHistogramView()),
	)

	otel.SetMeterProvider(meterProvider)

	return meterProvider.Shutdown, nil
}

// SecondsDurationBuckets are explicit histogram bucket boundaries (in seconds)
// for latency/duration instruments recorded with unit "s". It is the canonical
// bucket set for every such histogram in this codebase; secondsHistogramView
// applies it to every seconds-unit histogram as a backstop.
//
// The OpenTelemetry SDK's default histogram buckets are
// [0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000],
// which are sized for milliseconds. Applied to a metric recorded in seconds,
// every observation up to 5s collapses into the first (0, 5] bucket, so
// histogram_quantile() has no sub-second resolution and any pXX interpolates to
// 0.99*5 = 4.95s. That permanently trips latency alerts while staying blind to
// real regressions below 5s.
//
// These boundaries span ~1ms to 300s: fine resolution in the sub-second region
// where RPC and block-processing durations normally sit, plus coarse buckets out
// to 300s so a longer-running latency (e.g. the backup worker, alerted above
// 30s) still resolves a p99 above its threshold instead of clamping at the top
// bucket.
var SecondsDurationBuckets = []float64{
	0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300,
}

// secondsHistogramView forces every seconds-unit (Unit:"s") histogram onto
// SecondsDurationBuckets. A matching view replaces the per-instrument
// aggregation unconditionally, so this is not merely a fallback for instruments
// that forget explicit boundaries: it overrides whatever boundaries an
// instrument declares and keeps every seconds histogram on one canonical set.
// The instruments here pass the same boundaries explicitly too, so the override
// is a no-op in value terms; a seconds histogram that genuinely needs a
// different layout must use a different unit or its own named view. See
// SecondsDurationBuckets for why the SDK defaults are wrong for seconds-valued
// latencies.
func secondsHistogramView() metric.View {
	return metric.NewView(
		metric.Instrument{
			Kind: metric.InstrumentKindHistogram,
			Unit: "s",
		},
		metric.Stream{
			Aggregation: metric.AggregationExplicitBucketHistogram{
				Boundaries: SecondsDurationBuckets,
			},
		},
	)
}

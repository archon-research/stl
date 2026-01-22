package telemetry

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
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
			semconv.DeploymentEnvironmentName(config.Environment),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	exporter, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(config.OTLPEndpoint),
		otlpmetricgrpc.WithInsecure(), // Assuming internal communication
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP metric exporter: %w", err)
	}

	meterProvider := metric.NewMeterProvider(
		metric.WithResource(res),
		metric.WithReader(metric.NewPeriodicReader(exporter, metric.WithInterval(15*time.Second))),
	)

	otel.SetMeterProvider(meterProvider)

	return meterProvider.Shutdown, nil
}

// tracer.go provides OpenTelemetry tracing initialization and configuration.
//
// This adapter sets up distributed tracing with support for:
//   - OTLP gRPC export to Jaeger or other collectors
//   - Stdout export for local development/debugging
//   - Configurable sampling rates
//   - Service metadata (name, version, environment)
//
// Usage:
//
//	shutdown, err := telemetry.InitTracer(ctx, telemetry.TracerConfig{
//	    ServiceName:    "stl-watcher",
//	    JaegerEndpoint: "localhost:4317",
//	})
//	defer shutdown(ctx)
//
// The returned shutdown function should be called on application exit
// to flush any pending spans.
package telemetry

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TracerConfig holds configuration for the tracer.
type TracerConfig struct {
	// ServiceName is the name of the service (e.g., "stl-watcher").
	ServiceName string

	// ServiceVersion is the version of the service.
	ServiceVersion string

	// Environment is the deployment environment (e.g., "development", "production").
	Environment string

	// JaegerEndpoint is the OTLP gRPC endpoint for Jaeger (e.g., "localhost:4317").
	// If empty, traces are exported to stdout.
	JaegerEndpoint string

	// SampleRate is the sampling rate (0.0 to 1.0). Default is 1.0 (sample everything).
	SampleRate float64
}

// TracerConfigDefaults returns default configuration.
func TracerConfigDefaults() TracerConfig {
	return TracerConfig{
		ServiceName:    "stl-watcher",
		ServiceVersion: "0.1.0",
		Environment:    "development",
		JaegerEndpoint: "localhost:4317",
		SampleRate:     1.0,
	}
}

// InitTracer initializes the OpenTelemetry tracer with Jaeger exporter.
// Returns a shutdown function that should be called on application exit.
func InitTracer(ctx context.Context, config TracerConfig) (shutdown func(context.Context) error, err error) {
	// Apply defaults
	if config.ServiceName == "" {
		config.ServiceName = TracerConfigDefaults().ServiceName
	}
	if config.SampleRate == 0 {
		config.SampleRate = 1.0
	}

	// Create resource with service information
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

	// Create exporter
	var exporter trace.SpanExporter
	if config.JaegerEndpoint != "" {
		// OTLP gRPC exporter for Jaeger
		conn, err := grpc.NewClient(
			config.JaegerEndpoint,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create gRPC connection: %w", err)
		}

		exporter, err = otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
		if err != nil {
			return nil, fmt.Errorf("failed to create OTLP exporter: %w", err)
		}
	} else {
		// Fallback to stdout exporter
		exporter, err = stdouttrace.New(stdouttrace.WithPrettyPrint())
		if err != nil {
			return nil, fmt.Errorf("failed to create stdout exporter: %w", err)
		}
	}

	// Create sampler
	var sampler trace.Sampler
	if config.SampleRate >= 1.0 {
		sampler = trace.AlwaysSample()
	} else if config.SampleRate <= 0 {
		sampler = trace.NeverSample()
	} else {
		sampler = trace.TraceIDRatioBased(config.SampleRate)
	}

	// Create tracer provider
	tp := trace.NewTracerProvider(
		trace.WithBatcher(exporter,
			trace.WithBatchTimeout(5*time.Second),
		),
		trace.WithResource(res),
		trace.WithSampler(sampler),
	)

	// Set global tracer provider
	otel.SetTracerProvider(tp)

	// Set global propagator for distributed tracing
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	// Return shutdown function
	shutdown = func(ctx context.Context) error {
		return tp.Shutdown(ctx)
	}

	return shutdown, nil
}

// InitTracerWithDefaults initializes the tracer with default configuration.
func InitTracerWithDefaults(ctx context.Context) (shutdown func(context.Context) error, err error) {
	return InitTracer(ctx, TracerConfigDefaults())
}

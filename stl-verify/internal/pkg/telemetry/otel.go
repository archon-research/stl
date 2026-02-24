package telemetry

import (
	"context"
	"log/slog"

	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
)

// OTELConfig holds the common parameters for OTEL initialization.
type OTELConfig struct {
	ServiceName    string
	ServiceVersion string
	BuildTime      string
	Logger         *slog.Logger
}

// InitOTEL initializes both OpenTelemetry tracing and metrics.
// It reads JAEGER_ENDPOINT, OTEL_EXPORTER_OTLP_ENDPOINT, and ENVIRONMENT
// from environment variables. Returns a shutdown function that should be
// deferred by the caller.
func InitOTEL(ctx context.Context, config OTELConfig) func(context.Context) {
	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}

	environment := env.Get("ENVIRONMENT", "development")

	var shutdowns []func(context.Context) error

	// Tracer
	traceEndpoint := env.Get("JAEGER_ENDPOINT", "localhost:4317")
	shutdownTracer, err := InitTracer(ctx, TracerConfig{
		ServiceName:    config.ServiceName,
		ServiceVersion: config.ServiceVersion,
		BuildTime:      config.BuildTime,
		Environment:    environment,
		JaegerEndpoint: traceEndpoint,
	})
	if err != nil {
		logger.Warn("failed to init tracer, continuing without tracing", "error", err)
	} else {
		shutdowns = append(shutdowns, shutdownTracer)
		logger.Info("tracer initialized", "endpoint", traceEndpoint)
	}

	// Metrics
	otelEndpoint := env.Get("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	shutdownMetrics, err := InitMetrics(ctx, MetricConfig{
		ServiceName:    config.ServiceName,
		ServiceVersion: config.ServiceVersion,
		Environment:    environment,
		OTLPEndpoint:   otelEndpoint,
	})
	if err != nil {
		logger.Warn("failed to init metrics, continuing without metrics export", "error", err)
	} else {
		shutdowns = append(shutdowns, shutdownMetrics)
		if otelEndpoint != "" {
			logger.Info("metrics initialized", "endpoint", otelEndpoint)
		}
	}

	return func(ctx context.Context) {
		for _, fn := range shutdowns {
			if err := fn(ctx); err != nil {
				logger.Warn("failed to shutdown telemetry", "error", err)
			}
		}
	}
}

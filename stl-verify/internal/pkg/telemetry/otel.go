package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
)

// ShutdownFlushTimeout bounds the final telemetry flush: with an unreachable
// collector the exporters block process exit for their own defaults (10-30s
// each). It is deferred, so it shares lifecycle.ShutdownTailBudget.
const ShutdownFlushTimeout = 10 * time.Second

// OTELConfig holds the common parameters for OTEL initialization.
type OTELConfig struct {
	ServiceName    string
	ServiceVersion string
	BuildTime      string
	Logger         *slog.Logger
}

// assertNoInstrumentsPredateTelemetry fails startup when a component that
// records metrics was built before this call.
//
// Until the meter provider is installed, otel hands out delegating placeholders
// whose Add has no delegate, and the measurements written through them are
// dropped, not replayed. OnMeterProviderReady rescues a component's zero seed
// because a seed is a value the component can restate later; a real measurement
// is an event, and the event that matters most here is the failure that ends
// the process during startup — a database refusing every connection can
// crash-loop a service while db_query_errors_total stays flat, which is exactly
// the shape of the incident these counters exist to make visible.
//
// So the ordering is a correctness requirement, not a preference, and it is
// invisible in a running process: everything looks healthy precisely when it is
// broken. This asserts it instead. It is a runtime check rather than a lint
// over cmd/**/main.go because half the affected binaries open their pool inside
// a shared helper (cmd/workers/internal/dexbootstrap), where per-main source
// order says nothing.
//
// A binary that trips this moves its telemetry.InitOTEL call above the named
// component; cmd/base/watcher/main.go is the reference order.
func assertNoInstrumentsPredateTelemetry() error {
	owners := markTelemetryStarted()
	if len(owners) == 0 {
		return nil
	}
	return fmt.Errorf(
		"telemetry initialized after %s: measurements recorded before InitOTEL are dropped, "+
			"so a startup failure there would never be exported — initialize telemetry first",
		strings.Join(owners, ", "))
}

// InitOTEL initializes both OpenTelemetry tracing and metrics.
// It reads JAEGER_ENDPOINT, OTEL_EXPORTER_OTLP_ENDPOINT, and ENVIRONMENT
// from environment variables. Returns a shutdown function that should be
// deferred by the caller.
func InitOTEL(ctx context.Context, config OTELConfig) (func(context.Context), error) {
	if err := assertNoInstrumentsPredateTelemetry(); err != nil {
		return nil, err
	}

	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}

	environment := env.Get("ENVIRONMENT", "development")

	var shutdowns []func(context.Context) error

	// Tracer
	traceEndpoint := env.Get("JAEGER_ENDPOINT", "")
	if traceEndpoint == "" {
		logger.Warn("JAEGER_ENDPOINT is not set; traces are exported to stdout (pretty-printed span JSON in the process logs)")
	}
	shutdownTracer, err := InitTracer(ctx, TracerConfig{
		ServiceName:    config.ServiceName,
		ServiceVersion: config.ServiceVersion,
		BuildTime:      config.BuildTime,
		Environment:    environment,
		JaegerEndpoint: traceEndpoint,
	})
	if err != nil {
		return nil, fmt.Errorf("initializing tracer: %w", err)
	}
	shutdowns = append(shutdowns, shutdownTracer)
	logger.Info("tracer initialized", "endpoint", traceEndpoint)

	// Metrics
	otelEndpoint := env.Get("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	shutdownMetrics, err := InitMetrics(ctx, MetricConfig{
		ServiceName:    config.ServiceName,
		ServiceVersion: config.ServiceVersion,
		Environment:    environment,
		OTLPEndpoint:   otelEndpoint,
	})
	if err != nil {
		return nil, fmt.Errorf("initializing metrics: %w", err)
	}
	shutdowns = append(shutdowns, shutdownMetrics)
	if otelEndpoint != "" {
		logger.Info("metrics initialized", "endpoint", otelEndpoint)
	}

	return func(ctx context.Context) {
		ctx, cancel := context.WithTimeout(ctx, ShutdownFlushTimeout)
		defer cancel()
		for _, fn := range shutdowns {
			if err := fn(ctx); err != nil {
				logger.Warn("failed to shutdown telemetry", "error", err)
			}
		}
	}, nil
}

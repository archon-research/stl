package temporal

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	mnoop "go.opentelemetry.io/otel/metric/noop"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// TestRunCronjob_InitializesOTEL pins the OTel bootstrap in RunCronjob:
// service telemetry (e.g. the maple indexer's) creates instruments from the
// GLOBAL providers, so if RunCronjob stopped initializing them every cronjob
// metric would silently become a no-op again. The database opener returns a
// sentinel error so the run stops before dialing Temporal — by then the
// providers must already be set.
func TestRunCronjob_InitializesOTEL(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
	t.Setenv("JAEGER_ENDPOINT", "localhost:4317")

	prevTP := otel.GetTracerProvider()
	prevMP := otel.GetMeterProvider()
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetMeterProvider(prevMP)
	})

	sentinel := errors.New("sentinel: stop before temporal dial")
	err := RunCronjob(context.Background(), BuildMeta{Commit: "test"}, CronjobConfig{
		Name:            "otel-test-cronjob",
		IntervalDefault: "10m",
		OpenDatabase: func(context.Context) (*pgxpool.Pool, error) {
			return nil, sentinel
		},
		Setup: func(context.Context, Dependencies) (Runner, error) {
			return RunnerFunc(func(context.Context) error { return nil }), nil
		},
	})
	if err == nil || !strings.Contains(err.Error(), "sentinel") {
		t.Fatalf("expected sentinel database error, got %v", err)
	}

	if _, ok := otel.GetTracerProvider().(*sdktrace.TracerProvider); !ok {
		t.Errorf("global tracer provider = %T, want *sdktrace.TracerProvider", otel.GetTracerProvider())
	}
	if _, ok := otel.GetMeterProvider().(mnoop.MeterProvider); ok {
		t.Error("global meter provider is the no-op implementation; cronjob metrics would record nothing")
	}
}

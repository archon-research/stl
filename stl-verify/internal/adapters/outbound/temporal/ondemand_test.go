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
	"go.temporal.io/sdk/worker"
)

func validWorkerConfig() WorkerConfig {
	return WorkerConfig{
		Name:         "test-on-demand",
		OpenDatabase: func(context.Context) (*pgxpool.Pool, error) { return nil, nil },
		Register:     func(context.Context, Dependencies, worker.Registry) error { return nil },
	}
}

func TestWorkerConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*WorkerConfig)
		wantErr string
	}{
		{name: "complete config", mutate: func(*WorkerConfig) {}},
		{
			name:    "missing name",
			mutate:  func(c *WorkerConfig) { c.Name = "" },
			wantErr: "Name is required",
		},
		{
			name:    "missing database opener",
			mutate:  func(c *WorkerConfig) { c.OpenDatabase = nil },
			wantErr: "OpenDatabase is required",
		},
		{
			name:    "missing register hook",
			mutate:  func(c *WorkerConfig) { c.Register = nil },
			wantErr: "Register is required",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validWorkerConfig()
			tc.mutate(&cfg)

			err := cfg.validate()

			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

// RunWorker must install the global OTel providers before anything else is built,
// for the same reason RunCronjob must: service telemetry creates its instruments
// from the global providers at construction time, so a later init leaves them
// bound to no-ops for the process lifetime. The database opener returns a sentinel
// so the run stops before dialing Temporal, by which point the providers must
// already be set.
func TestRunWorker_InitializesOTELBeforeOpeningTheDatabase(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
	t.Setenv("JAEGER_ENDPOINT", "localhost:4317")

	prevTP := otel.GetTracerProvider()
	prevMP := otel.GetMeterProvider()
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetMeterProvider(prevMP)
	})

	sentinel := errors.New("sentinel: stop before temporal dial")
	cfg := validWorkerConfig()
	cfg.OpenDatabase = func(context.Context) (*pgxpool.Pool, error) { return nil, sentinel }

	err := RunWorker(context.Background(), BuildMeta{Commit: "test"}, cfg)

	if err == nil || !strings.Contains(err.Error(), "sentinel") {
		t.Fatalf("expected the sentinel database error, got %v", err)
	}
	if _, ok := otel.GetTracerProvider().(*sdktrace.TracerProvider); !ok {
		t.Errorf("global tracer provider = %T, want *sdktrace.TracerProvider", otel.GetTracerProvider())
	}
	if _, ok := otel.GetMeterProvider().(mnoop.MeterProvider); ok {
		t.Error("global meter provider is the no-op implementation; metrics would record nothing")
	}
}

func TestRunWorker_RejectsInvalidConfigBeforeAnySetup(t *testing.T) {
	opened := false
	cfg := validWorkerConfig()
	cfg.Name = ""
	cfg.OpenDatabase = func(context.Context) (*pgxpool.Pool, error) {
		opened = true
		return nil, nil
	}

	err := RunWorker(context.Background(), BuildMeta{}, cfg)

	if err == nil || !strings.Contains(err.Error(), "validating worker config") {
		t.Fatalf("error = %v, want a config validation error", err)
	}
	if opened {
		t.Error("database was opened despite an invalid config")
	}
}

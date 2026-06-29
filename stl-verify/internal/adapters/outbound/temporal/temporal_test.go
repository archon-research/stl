package temporal

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

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

func TestBuildScheduleSpec_Offset(t *testing.T) {
	tests := []struct {
		name       string
		cfg        CronjobConfig
		env        map[string]string
		wantEvery  time.Duration
		wantOffset time.Duration
		wantErr    bool
	}{
		{
			name:       "no offset env configured",
			cfg:        CronjobConfig{IntervalDefault: "1h"},
			wantEvery:  time.Hour,
			wantOffset: 0,
		},
		{
			name:       "offset env set",
			cfg:        CronjobConfig{IntervalDefault: "1h", IntervalOffsetEnv: "OFFSET"},
			env:        map[string]string{"OFFSET": "5m"},
			wantEvery:  time.Hour,
			wantOffset: 5 * time.Minute,
		},
		{
			name:       "offset env empty falls back to zero",
			cfg:        CronjobConfig{IntervalDefault: "1h", IntervalOffsetEnv: "OFFSET"},
			env:        map[string]string{},
			wantEvery:  time.Hour,
			wantOffset: 0,
		},
		{
			name:       "interval env overrides default",
			cfg:        CronjobConfig{IntervalEnv: "INTERVAL", IntervalDefault: "1h"},
			env:        map[string]string{"INTERVAL": "30m"},
			wantEvery:  30 * time.Minute,
			wantOffset: 0,
		},
		{
			name:    "invalid offset errors",
			cfg:     CronjobConfig{IntervalDefault: "1h", IntervalOffsetEnv: "OFFSET"},
			env:     map[string]string{"OFFSET": "not-a-duration"},
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			getenv := func(k string) string { return tc.env[k] }
			spec, err := buildScheduleSpec(tc.cfg, getenv)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			got := spec.Intervals[0]
			if got.Every != tc.wantEvery || got.Offset != tc.wantOffset {
				t.Fatalf("got {Every:%s Offset:%s}, want {Every:%s Offset:%s}",
					got.Every, got.Offset, tc.wantEvery, tc.wantOffset)
			}
		})
	}
}

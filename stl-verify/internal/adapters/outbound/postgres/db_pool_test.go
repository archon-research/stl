package postgres

import (
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
)

// unreachableDSN parses as a valid pool config but is never dialled: these tests
// only inspect what buildPoolConfig produced.
const unreachableDSN = "postgres://user:pass@localhost:5432/db"

// TestBuildPoolConfig_TimeoutsNotStartupParams pins the invariant behind the
// 2026-06-19 staging crashloop: lock_timeout/statement_timeout must NOT ride the
// startup packet. The indexers connect through a pgbouncer-style pooler, which
// rejects unknown startup parameters ("FATAL: unsupported startup parameter:
// lock_timeout"), so the timeouts must be applied with a post-connect SET via
// AfterConnect instead.
func TestBuildPoolConfig_TimeoutsNotStartupParams(t *testing.T) {
	cfg := WorkerDBConfig(unreachableDSN)
	cfg.StatementTimeout = 45 * time.Second

	pc, err := buildPoolConfig(cfg)
	if err != nil {
		t.Fatalf("buildPoolConfig: %v", err)
	}

	for _, guc := range []string{"lock_timeout", "statement_timeout"} {
		if v, ok := pc.ConnConfig.RuntimeParams[guc]; ok {
			t.Errorf("%s=%q present in startup RuntimeParams; a pooler rejects unknown startup params, apply it via AfterConnect SET instead", guc, v)
		}
	}

	if pc.AfterConnect == nil {
		t.Error("AfterConnect is nil with timeouts configured; they would never be applied")
	}
}

func TestBuildPoolConfig_NoTimeoutsNoAfterConnect(t *testing.T) {
	pc, err := buildPoolConfig(DefaultDBConfig(unreachableDSN))
	if err != nil {
		t.Fatalf("buildPoolConfig: %v", err)
	}

	if pc.AfterConnect != nil {
		t.Error("AfterConnect set for DefaultDBConfig (no timeouts); want nil to avoid a wasted round-trip on every new connection")
	}
}

func TestBuildPoolConfig_AttachesQueryTracer(t *testing.T) {
	poolConfig, err := buildPoolConfig(DefaultDBConfig(unreachableDSN))
	if err != nil {
		t.Fatalf("buildPoolConfig: %v", err)
	}

	if poolConfig.ConnConfig.Tracer == nil {
		t.Fatal("ConnConfig.Tracer is nil, want the query error tracer attached")
	}
	if _, ok := poolConfig.ConnConfig.Tracer.(*queryErrorTracer); !ok {
		t.Errorf("ConnConfig.Tracer = %T, want *queryErrorTracer", poolConfig.ConnConfig.Tracer)
	}
}

func TestBuildPoolConfig_UsesTheInjectedMeterProvider(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	cfg := DefaultDBConfig(unreachableDSN)
	cfg.MeterProvider = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	if _, err := buildPoolConfig(cfg); err != nil {
		t.Fatalf("buildPoolConfig: %v", err)
	}

	counts := countsByAttr(t, reader, "db.query.errors.total", "error_class")
	if len(counts) != len(errorClasses) {
		t.Errorf("seeded classes on the injected provider = %v, want %v", counts, errorClasses)
	}
}

// Most binaries open their pool before telemetry installs the exporting meter
// provider, and a seed written at build time then reaches no exporter. See
// telemetry.OnMeterProviderReady.
//
// This must stay the only test in the binary that reaches
// telemetry.SetMeterProvider: otel.SetMeterProvider takes effect once per
// process, and the first call also drains every pending seed.
func TestBuildPoolConfig_SeedsErrorClassesWhenTelemetryStartsLast(t *testing.T) {
	if _, err := buildPoolConfig(DefaultDBConfig(unreachableDSN)); err != nil {
		t.Fatalf("buildPoolConfig: %v", err)
	}

	reader := sdkmetric.NewManualReader()
	telemetry.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))

	counts := countsByAttr(t, reader, "db.query.errors.total", "error_class")
	for _, class := range errorClasses {
		if got, ok := counts[class]; !ok || got != 0 {
			t.Errorf("error_class %q seeded = (%d, %v), want (0, true)", class, got, ok)
		}
	}
}

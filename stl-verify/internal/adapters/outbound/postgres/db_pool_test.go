package postgres

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

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

// A pool built before an exporting meter provider is installed — a one-shot
// backfiller, a test — writes its seed into a placeholder that exports nothing.
// See telemetry.OnMeterProviderReady.
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

// The handler is opt-in: every service shares this pool builder, and one that did not ask must not
// start logging server notices. The position materializer asks, to log its withheld-position warnings.
func TestBuildPoolConfig_LogsServerNoticesOnlyWhenAsked(t *testing.T) {
	const url = "postgres://u:p@localhost:5432/d?sslmode=disable"
	plain, err := buildPoolConfig(DBConfig{URL: url})
	if err != nil {
		t.Fatalf("buildPoolConfig() error: %v", err)
	}
	if plain.ConnConfig.OnNotice != nil {
		t.Error("a pool without NoticeLogger logs server notices")
	}
	asked, err := buildPoolConfig(DBConfig{URL: url, NoticeLogger: slog.Default()})
	if err != nil {
		t.Fatalf("buildPoolConfig() error: %v", err)
	}
	if asked.ConnConfig.OnNotice == nil {
		t.Error("a pool with NoticeLogger discards server WARNINGs")
	}
}

// Only warnings are logged, to the configured logger, judged on the unlocalized severity so a server
// with a non-English lc_messages still logs a WARNING as a warning.
func TestNoticeLogger_LogsOnlyWarnings(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})).With("component", "c")

	poolConfig, err := buildPoolConfig(DBConfig{URL: "postgres://u:p@localhost:5432/d?sslmode=disable", NoticeLogger: logger})
	if err != nil {
		t.Fatalf("buildPoolConfig() error: %v", err)
	}
	onNotice := poolConfig.ConnConfig.OnNotice
	onNotice(nil, &pgconn.Notice{Severity: "AVERTISSEMENT", SeverityUnlocalized: "WARNING", Message: "withheld 3 positions"})
	onNotice(nil, &pgconn.Notice{Severity: "NOTICE", SeverityUnlocalized: "NOTICE", Message: "relation exists, skipping"})

	var lines []map[string]any
	for l := range bytes.SplitSeq(bytes.TrimSpace(buf.Bytes()), []byte("\n")) {
		if len(l) == 0 {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal(l, &m); err != nil {
			t.Fatalf("log line %q: %v", l, err)
		}
		lines = append(lines, m)
	}
	if len(lines) != 1 {
		t.Fatalf("logged %d lines, want only the warning: %v", len(lines), lines)
	}
	if lines[0]["level"] != "WARN" || lines[0]["message"] != "withheld 3 positions" || lines[0]["severity"] != "WARNING" || lines[0]["component"] != "c" {
		t.Errorf("logged %v; want the warning at WARN with severity WARNING, on the configured logger", lines[0])
	}
}

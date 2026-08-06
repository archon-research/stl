//go:build integration

package db_statement_stats

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// startStatsPostgres starts a Postgres with pg_stat_statements preloaded.
//
// The shared testutil helper cannot be reused: pg_stat_statements is a shared
// library that must be listed in shared_preload_libraries at server start, which
// means an extra `-c` argument, and the shared helper's container is reused across
// packages via TestMain. This container is package-local so the extra flag cannot
// perturb anything else.
func startStatsPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        testutil.ImageTimescaleDB,
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_USER":     "test",
				"POSTGRES_PASSWORD": "test",
				"POSTGRES_DB":       "testdb",
			},
			Cmd: []string{
				"postgres",
				// timescaledb is already preloaded by the image's own config; naming it
				// here keeps it loaded alongside pg_stat_statements.
				"-c", "shared_preload_libraries=timescaledb,pg_stat_statements",
				"-c", "pg_stat_statements.track=top",
			},
			WaitingFor: wait.ForAll(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).
					WithStartupTimeout(90*time.Second),
				wait.ForListeningPort("5432/tcp").
					WithStartupTimeout(90*time.Second),
			),
		},
		Started: true,
	})
	if err != nil {
		testutil.HandleContainerRuntimeError(t, err, "start container")
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("get host: %v", err)
	}
	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatalf("get port: %v", err)
	}

	pool := testutil.ConnectPool(t, fmt.Sprintf(
		"postgres://test:test@%s:%s/testdb?sslmode=disable", host, port.Port()))
	t.Cleanup(pool.Close)

	if _, err := pool.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS pg_stat_statements`); err != nil {
		t.Fatalf("creating pg_stat_statements (is it in shared_preload_libraries?): %v", err)
	}
	return pool
}

// insertShapeOne and insertShapeTwo write to the same table through two different
// statement shapes, so Postgres fingerprints them as two queryids that the
// exporter must aggregate onto one series.
const (
	insertShapeOne = `INSERT INTO stats_probe_a (v) VALUES ($1)`
	insertShapeTwo = `INSERT INTO stats_probe_a (v, w) VALUES ($1, $2)`
	insertProbeB   = `INSERT INTO stats_probe_b (v) VALUES ($1)`
)

func execN(ctx context.Context, t *testing.T, pool *pgxpool.Pool, sql string, n int) {
	t.Helper()
	for i := range n {
		args := []any{i}
		if sql == insertShapeTwo {
			args = []any{i, i}
		}
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
}

// rawExecMillis sums the cumulative total_exec_time (milliseconds, as Postgres
// reports it) for INSERTs into table. The exporter's own conversion is checked
// against this, so the test reads the raw column rather than trusting the adapter.
func rawExecMillis(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table string) float64 {
	t.Helper()
	var ms float64
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(sum(s.total_exec_time), 0)
		FROM pg_stat_statements s
		JOIN pg_database d ON d.oid = s.dbid
		WHERE d.datname = current_database()
		  AND s.query ~* ('^\s*INSERT\s+INTO\s+' || $1)`, table).Scan(&ms); err != nil {
		t.Fatalf("reading raw total_exec_time for %s: %v", table, err)
	}
	return ms
}

// TestRunOnce_AgainstRealPgStatStatements exercises the whole path against a real
// Postgres: the adapter's query and unit conversion, the delta tracker across
// three ticks, and per-table aggregation of two statement shapes.
//
// The tick-3 assertions are the point of the test: they pin that what reaches the
// metric is each window's increment, not the counters' running totals.
func TestRunOnce_AgainstRealPgStatStatements(t *testing.T) {
	pool := startStatsPostgres(t)
	ctx := context.Background()

	for _, ddl := range []string{
		`CREATE TABLE stats_probe_a (v int, w int)`,
		`CREATE TABLE stats_probe_b (v int)`,
	} {
		if _, err := pool.Exec(ctx, ddl); err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
	}

	metricReader := newDeltaReader()
	telemetry, err := NewTelemetryWithProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader)))
	if err != nil {
		t.Fatalf("NewTelemetryWithProvider: %v", err)
	}
	service, err := NewService(ServiceConfig{}, postgres.NewStatementStatsRepository(pool), telemetry)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	// Phase 1: both tables get one live statement shape, so their fingerprints
	// already exist when the first tick takes its baseline.
	execN(ctx, t, pool, insertShapeOne, 3)
	execN(ctx, t, pool, insertProbeB, 1)

	baseMillisB := rawExecMillis(ctx, t, pool, "stats_probe_b")
	first := runTick(t, service, metricReader)
	for _, table := range []string{"stats_probe_a", "stats_probe_b"} {
		if v := first.table(metricCalls, table); v != 0 {
			t.Errorf("first tick emitted %v calls for %s, want 0 (cumulative history is a baseline)", v, table)
		}
	}

	// Phase 2: more of the known shapes, plus a shape never seen before
	// (insertShapeTwo), which must itself be baselined rather than counted.
	execN(ctx, t, pool, insertShapeOne, 2)
	execN(ctx, t, pool, insertShapeTwo, 4)
	execN(ctx, t, pool, insertProbeB, 5)

	afterMillisB := rawExecMillis(ctx, t, pool, "stats_probe_b")
	second := runTick(t, service, metricReader)

	if v := second.table(metricCalls, "stats_probe_a"); v != 2 {
		t.Errorf("second tick calls for stats_probe_a = %v, want 2 "+
			"(only the already-baselined shape counts; the new shape is baselined)", v)
	}
	if v := second.table(metricRows, "stats_probe_a"); v != 2 {
		t.Errorf("second tick rows for stats_probe_a = %v, want 2", v)
	}
	if v := second.table(metricCalls, "stats_probe_b"); v != 5 {
		t.Errorf("second tick calls for stats_probe_b = %v, want 5", v)
	}
	if v := second.table(metricRows, "stats_probe_b"); v != 5 {
		t.Errorf("second tick rows for stats_probe_b = %v, want 5", v)
	}

	// The exec-time metric must be the millisecond column divided by 1000. Comparing
	// against the raw column catches a dropped or doubled conversion, which a
	// "greater than zero" assertion would wave through.
	wantSeconds := (afterMillisB - baseMillisB) / 1000.0
	gotSeconds := second.table(metricExecTime, "stats_probe_b")
	if math.Abs(gotSeconds-wantSeconds) > 1e-9 {
		t.Errorf("second tick exec time for stats_probe_b = %v s, want %v s "+
			"(pg reports milliseconds; the adapter must convert)", gotSeconds, wantSeconds)
	}
	if gotSeconds <= 0 {
		t.Errorf("exec time for stats_probe_b = %v, want a positive duration", gotSeconds)
	}

	// Phase 3: the previously-new shape is now tracked, so both shapes on table a
	// contribute. Table b is untouched and must report 0, not its running total.
	execN(ctx, t, pool, insertShapeOne, 1)
	execN(ctx, t, pool, insertShapeTwo, 3)

	third := runTick(t, service, metricReader)

	if v := third.table(metricCalls, "stats_probe_a"); v != 4 {
		t.Errorf("third tick calls for stats_probe_a = %v, want 4 (1 + 3 across both shapes; "+
			"a running total would read 10)", v)
	}
	if v := third.table(metricRows, "stats_probe_a"); v != 4 {
		t.Errorf("third tick rows for stats_probe_a = %v, want 4", v)
	}
	if v := third.table(metricCalls, "stats_probe_b"); v != 0 {
		t.Errorf("third tick calls for stats_probe_b = %v, want 0 (untouched this window; "+
			"a running total would read 6)", v)
	}
}

// repoWithoutExtension starts a stats-enabled Postgres and then drops the extension,
// which is the state a restore or a lost preload leaves behind.
func repoWithoutExtension(t *testing.T) *postgres.StatementStatsRepository {
	t.Helper()
	pool := startStatsPostgres(t)
	if _, err := pool.Exec(context.Background(), `DROP EXTENSION pg_stat_statements`); err != nil {
		t.Fatalf("dropping extension: %v", err)
	}
	return postgres.NewStatementStatsRepository(pool)
}

// TestInsertStatements_MissingExtensionIsAnError pins the adapter's contract: an
// unreadable view is an error, not an empty slice, which would be indistinguishable
// from an idle database.
func TestInsertStatements_MissingExtensionIsAnError(t *testing.T) {
	if _, err := repoWithoutExtension(t).InsertStatements(context.Background()); err == nil {
		t.Fatal("InsertStatements succeeded without the extension; want an error")
	}
}

// TestRunOnce_MissingExtensionFailsTick pins that the adapter's error actually fails
// the tick, rather than being absorbed into a run that reports success having
// measured nothing.
func TestRunOnce_MissingExtensionFailsTick(t *testing.T) {
	service, err := NewService(ServiceConfig{}, repoWithoutExtension(t), nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	if err := service.RunOnce(context.Background()); err == nil {
		t.Fatal("RunOnce succeeded without the extension; want the tick to fail")
	}
}

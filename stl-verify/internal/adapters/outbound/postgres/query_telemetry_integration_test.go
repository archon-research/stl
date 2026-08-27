//go:build integration

package postgres

import (
	"context"
	"net/url"
	"testing"

	"github.com/jackc/pgx/v5"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// The unit tests drive the tracer's methods directly, so they cannot show that
// pgx reaches them: SendBatch dispatches through BatchTracer alone, and a
// failure raised while preparing the batch is traced twice.
func TestOpenPool_TracesQueryAndBatchErrors(t *testing.T) {
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()

	cfg := DefaultDBConfig(sharedDSN)
	cfg.MeterProvider = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	pool, err := OpenPool(ctx, cfg)
	if err != nil {
		t.Fatalf("OpenPool: %v", err)
	}
	defer pool.Close()

	if _, err := pool.Exec(ctx, "SELECT 1 FROM stl_no_such_table"); err == nil {
		t.Fatal("Exec on a missing table returned no error")
	}

	// Fails while preparing, before any result is read: SendBatch traces it and
	// Close traces the same error again.
	prepareFailure := &pgx.Batch{}
	prepareFailure.Queue("SELECT 1 FROM stl_no_such_table")
	if err := pool.SendBatch(ctx, prepareFailure).Close(); err == nil {
		t.Fatal("batch against a missing table returned no error")
	}

	// Fails at execution, after the statement was read.
	runtimeFailure := &pgx.Batch{}
	runtimeFailure.Queue("SELECT 1 / 0")
	results := pool.SendBatch(ctx, runtimeFailure)
	if _, err := results.Exec(); err == nil {
		t.Fatal("batched divide by zero returned no error")
	}
	if err := results.Close(); err == nil {
		t.Fatal("closing a failed batch returned no error")
	}

	counts := countsByAttr(t, reader, "db.query.errors.by_sqlstate.total", "sqlstate")
	if got := counts["42P01"]; got != 2 {
		t.Errorf("sqlstate 42P01 count = %d, want 2 (one Exec, one batch counted once)", got)
	}
	if got := counts["22012"]; got != 1 {
		t.Errorf("sqlstate 22012 count = %d, want 1 (SendBatch path not traced)", got)
	}
}

// A connect failure is returned by pgxpool before any query runs, so it reaches
// the tracer only through ConnectTracer — the path 53300 too_many_connections
// takes.
func TestOpenPool_TracesConnectErrors(t *testing.T) {
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()

	cfg := DefaultDBConfig(dsnForDatabase(t, sharedDSN, "stl_no_such_database"))
	cfg.MeterProvider = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	pool, err := OpenPool(ctx, cfg)
	if err == nil {
		pool.Close()
		t.Fatal("OpenPool against a missing database returned no error")
	}

	counts := countsByAttr(t, reader, "db.query.errors.by_sqlstate.total", "sqlstate")
	if counts["3D000"] < 1 {
		t.Errorf("sqlstate 3D000 count = %d, want at least 1; counted %v", counts["3D000"], counts)
	}
}

func dsnForDatabase(t *testing.T, baseDSN, database string) string {
	t.Helper()

	u, err := url.Parse(baseDSN)
	if err != nil {
		t.Fatalf("parsing %q: %v", baseDSN, err)
	}
	u.Path = "/" + database
	return u.String()
}

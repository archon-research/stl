//go:build integration

package postgres

import (
	"context"
	"net/url"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// tracedPool opens a pool whose metrics land on a reader the caller can collect.
func tracedPool(t *testing.T, dsn string) (*pgxpool.Pool, sdkmetric.Reader) {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	cfg := DefaultDBConfig(dsn)
	cfg.MeterProvider = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	pool, err := OpenPool(context.Background(), cfg)
	if err != nil {
		t.Fatalf("OpenPool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool, reader
}

func TestOpenPool_TracesQueryErrors(t *testing.T) {
	pool, reader := tracedPool(t, sharedDSN)

	if _, err := pool.Exec(context.Background(), "SELECT 1 FROM stl_no_such_table"); err == nil {
		t.Fatal("Exec on a missing table returned no error")
	}

	counts := countsByAttr(t, reader, "db.query.errors.by_sqlstate.total", "sqlstate")
	if got := counts["42P01"]; got != 1 {
		t.Errorf("sqlstate 42P01 count = %d, want 1", got)
	}
}

// The unit tests drive the tracer's methods directly, so they cannot show that
// pgx reaches them: SendBatch dispatches through BatchTracer alone, and a
// QueryTracer-only implementation counts nothing here while passing them all.
func TestOpenPool_TracesBatchErrors(t *testing.T) {
	pool, reader := tracedPool(t, sharedDSN)

	batch := &pgx.Batch{}
	batch.Queue("SELECT 1 / 0")
	results := pool.SendBatch(context.Background(), batch)
	if _, err := results.Exec(); err == nil {
		t.Fatal("batched divide by zero returned no error")
	}
	if err := results.Close(); err == nil {
		t.Fatal("closing a failed batch returned no error")
	}

	counts := countsByAttr(t, reader, "db.query.errors.by_sqlstate.total", "sqlstate")
	if got := counts["22012"]; got != 1 {
		t.Errorf("sqlstate 22012 count = %d, want 1", got)
	}
}

// A batch that fails while preparing reaches TraceBatchEnd twice, once from
// SendBatch and once from Close.
func TestOpenPool_CountsAPrepareFailingBatchOnce(t *testing.T) {
	pool, reader := tracedPool(t, sharedDSN)

	batch := &pgx.Batch{}
	batch.Queue("SELECT 1 FROM stl_no_such_table")
	if err := pool.SendBatch(context.Background(), batch).Close(); err == nil {
		t.Fatal("batch against a missing table returned no error")
	}

	counts := countsByAttr(t, reader, "db.query.errors.by_sqlstate.total", "sqlstate")
	if got := counts["42P01"]; got != 1 {
		t.Errorf("sqlstate 42P01 count = %d, want 1", got)
	}
}

// 53300 too_many_connections fails the connect itself, which pgxpool returns
// before any query runs.
func TestOpenPool_TracesConnectErrors(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	cfg := DefaultDBConfig(dsnForDatabase(t, sharedDSN, "stl_no_such_database"))
	cfg.MeterProvider = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	pool, err := OpenPool(context.Background(), cfg)
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

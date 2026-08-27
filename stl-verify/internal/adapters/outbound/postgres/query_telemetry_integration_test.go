//go:build integration

package postgres

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// The unit tests drive the tracer's methods directly, so they cannot show that
// pgx reaches them: SendBatch dispatches through BatchTracer alone, and a
// QueryTracer-only implementation counts nothing for a batch while still
// passing every unit test.
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

	// Distinct SQLSTATEs so each path is identifiable in the breakdown:
	// 42P01 undefined_table for the query, 22012 division_by_zero for the batch.
	if _, err := pool.Exec(ctx, "SELECT 1 FROM stl_no_such_table"); err == nil {
		t.Fatal("Exec on a missing table returned no error")
	}

	batch := &pgx.Batch{}
	batch.Queue("SELECT 1 / 0")
	results := pool.SendBatch(ctx, batch)
	if _, err := results.Exec(); err == nil {
		t.Fatal("batched divide by zero returned no error")
	}
	if err := results.Close(); err == nil {
		t.Fatal("closing a failed batch returned no error")
	}

	counts := countsByAttr(t, reader, "db.query.errors.by_sqlstate.total", "sqlstate")
	if got := counts["42P01"]; got != 1 {
		t.Errorf("sqlstate 42P01 count = %d, want 1 (Query path not traced)", got)
	}
	if got := counts["22012"]; got != 1 {
		t.Errorf("sqlstate 22012 count = %d, want 1 (SendBatch path not traced)", got)
	}
}

//go:build integration

package postgres

import (
	"context"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
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

// A startup connect failure is recorded and then the process exits: the
// periodic reader's next tick never comes, so the shutdown flush is the only
// thing that can carry the measurement out. This drives that whole path — an
// exporting provider installed globally the way InitOTEL installs one, a pool
// opened against a database that refuses, and a Shutdown standing in for the
// shutdownOTEL that run() defers before it opens anything.
func TestOpenPool_ExportsAStartupConnectFailureThroughTheShutdownFlush(t *testing.T) {
	exporter := &capturingExporter{}
	// An interval longer than the test, so a tick cannot be what exports.
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(
		sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(time.Hour)),
	))
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() { otel.SetMeterProvider(previous) })

	pool, err := OpenPool(context.Background(), DefaultDBConfig(dsnForDatabase(t, sharedDSN, "stl_no_such_database")))
	if err == nil {
		pool.Close()
		t.Fatal("OpenPool against a missing database returned no error")
	}
	if len(exporter.exported()) != 0 {
		t.Fatalf("metrics were exported before shutdown: %v", exporter.exported())
	}

	if err := provider.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutting the meter provider down: %v", err)
	}

	counts := exporter.countsByAttr(t, "db.query.errors.by_sqlstate.total", "sqlstate")
	if counts["3D000"] < 1 {
		t.Errorf("sqlstate 3D000 exported = %d, want at least 1; exported %v", counts["3D000"], counts)
	}
}

// The same connect failure, recorded with the pool built first: the tracer's
// instruments come from otel's placeholder provider, whose Add has no delegate,
// and installing a real provider afterwards re-delegates the instruments but
// replays nothing. Nothing distinguishes this from a healthy service, which is
// why telemetry.InitOTEL refuses to start in this order rather than relying on
// each binary to get it right.
func TestOpenPool_LosesAStartupConnectFailureWhenTelemetryStartsLast(t *testing.T) {
	exporter := &capturingExporter{}
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(
		sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(time.Hour)),
	))
	previous := otel.GetMeterProvider()
	t.Cleanup(func() { otel.SetMeterProvider(previous) })

	pool, err := OpenPool(context.Background(), DefaultDBConfig(dsnForDatabase(t, sharedDSN, "stl_no_such_database")))
	if err == nil {
		pool.Close()
		t.Fatal("OpenPool against a missing database returned no error")
	}

	otel.SetMeterProvider(provider)
	if err := provider.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutting the meter provider down: %v", err)
	}

	if counts := exporter.countsByAttr(t, "db.query.errors.by_sqlstate.total", "sqlstate"); len(counts) != 0 {
		t.Errorf("exported %v, want nothing: a measurement recorded before the provider was installed cannot be replayed", counts)
	}
}

// capturingExporter keeps what the SDK hands an exporter, so a test can tell
// "exported" from "recorded but still sitting in the reader".
type capturingExporter struct {
	mu      sync.Mutex
	batches []metricdata.ResourceMetrics
}

var _ sdkmetric.Exporter = (*capturingExporter)(nil)

func (e *capturingExporter) Temporality(k sdkmetric.InstrumentKind) metricdata.Temporality {
	return sdkmetric.DefaultTemporalitySelector(k)
}

func (e *capturingExporter) Aggregation(k sdkmetric.InstrumentKind) sdkmetric.Aggregation {
	return sdkmetric.DefaultAggregationSelector(k)
}

// The SDK reuses the ResourceMetrics it passes, so the scope metrics are copied
// out rather than retained.
func (e *capturingExporter) Export(_ context.Context, rm *metricdata.ResourceMetrics) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	copied := *rm
	copied.ScopeMetrics = append([]metricdata.ScopeMetrics(nil), rm.ScopeMetrics...)
	e.batches = append(e.batches, copied)
	return nil
}

func (e *capturingExporter) ForceFlush(context.Context) error { return nil }

func (e *capturingExporter) Shutdown(context.Context) error { return nil }

func (e *capturingExporter) exported() []metricdata.ResourceMetrics {
	e.mu.Lock()
	defer e.mu.Unlock()

	return append([]metricdata.ResourceMetrics(nil), e.batches...)
}

func (e *capturingExporter) countsByAttr(t *testing.T, name, attr string) map[string]int64 {
	t.Helper()

	counts := map[string]int64{}
	for _, rm := range e.exported() {
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if m.Name != name {
					continue
				}
				sum, ok := m.Data.(metricdata.Sum[int64])
				if !ok {
					t.Fatalf("%s data = %T, want Sum[int64]", name, m.Data)
				}
				for _, dp := range sum.DataPoints {
					value, ok := dp.Attributes.Value(attribute.Key(attr))
					if !ok {
						t.Fatalf("%s data point missing the %s attribute: %v", name, attr, dp.Attributes)
					}
					counts[value.AsString()] += dp.Value
				}
			}
		}
	}
	return counts
}

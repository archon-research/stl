package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// collectSum collects one counter by name, or reports false when the
// instrument recorded nothing at all.
func collectSum(t *testing.T, reader sdkmetric.Reader, name string) (metricdata.Sum[int64], bool) {
	t.Helper()

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s data = %T, want Sum[int64]", name, m.Data)
			}
			return sum, true
		}
	}
	return metricdata.Sum[int64]{}, false
}

// countsByAttr returns a counter's value per value of one attribute.
func countsByAttr(t *testing.T, reader sdkmetric.Reader, name, attr string) map[string]int64 {
	t.Helper()

	counts := map[string]int64{}
	sum, ok := collectSum(t, reader, name)
	if !ok {
		return counts
	}
	for _, dp := range sum.DataPoints {
		value, ok := dp.Attributes.Value(attribute.Key(attr))
		if !ok {
			t.Fatalf("%s data point missing the %s attribute: %v", name, attr, dp.Attributes)
		}
		counts[value.AsString()] += dp.Value
	}
	return counts
}

// counterTotal returns a counter's value summed across every attribute set.
func counterTotal(t *testing.T, reader sdkmetric.Reader, name string) int64 {
	t.Helper()

	sum, ok := collectSum(t, reader, name)
	if !ok {
		return 0
	}
	var total int64
	for _, dp := range sum.DataPoints {
		total += dp.Value
	}
	return total
}

func newTestTracer(t *testing.T) (*queryErrorTracer, sdkmetric.Reader) {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	tracer, err := newQueryErrorTracer(mp)
	if err != nil {
		t.Fatalf("newQueryErrorTracer: %v", err)
	}
	return tracer, reader
}

func pgErr(code string) error {
	return &pgconn.PgError{Code: code, Message: code}
}

// Without a series at 0 from startup, a class's first error is also its series'
// first sample, and increase() reports 0 for an increment it never saw begin.
func TestQueryErrorTracerSeedsEveryErrorClass(t *testing.T) {
	_, reader := newTestTracer(t)

	counts := countsByAttr(t, reader, "db.query.errors.total", "error_class")

	for _, class := range errorClasses {
		if got, ok := counts[class]; !ok || got != 0 {
			t.Errorf("error_class %q seeded = (%d, %v), want (0, true)", class, got, ok)
		}
	}
	if len(counts) != len(errorClasses) {
		t.Errorf("seeded classes = %v, want exactly %v", counts, errorClasses)
	}
}

func TestQueryErrorTracerClassifiesSQLState(t *testing.T) {
	for _, tc := range []struct {
		name  string
		err   error
		class string
	}{
		{name: "out of memory", err: pgErr("53200"), class: errorClassResources},
		{name: "too many connections", err: pgErr("53300"), class: errorClassResources},
		{name: "serialization failure", err: pgErr("40001"), class: errorClassRetryable},
		{name: "deadlock detected", err: pgErr("40P01"), class: errorClassRetryable},
		{name: "insufficient privilege", err: pgErr("42501"), class: errorClassOther},
		{name: "no sqlstate", err: errors.New("connection reset by peer"), class: errorClassUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tracer, reader := newTestTracer(t)

			tracer.TraceQueryEnd(context.Background(), nil, pgx.TraceQueryEndData{Err: tc.err})

			counts := countsByAttr(t, reader, "db.query.errors.total", "error_class")
			if got := counts[tc.class]; got != 1 {
				t.Errorf("error_class %q count = %d, want 1", tc.class, got)
			}
		})
	}
}

func TestQueryErrorTracerRecordsSQLState(t *testing.T) {
	tracer, reader := newTestTracer(t)

	tracer.TraceQueryEnd(context.Background(), nil, pgx.TraceQueryEndData{Err: pgErr("53200")})

	counts := countsByAttr(t, reader, "db.query.errors.by_sqlstate.total", "sqlstate")
	if got := counts["53200"]; got != 1 {
		t.Errorf("sqlstate 53200 count = %d, want 1", got)
	}
}

func TestQueryErrorTracerLabelsNonPostgresErrorsUnknown(t *testing.T) {
	tracer, reader := newTestTracer(t)

	tracer.TraceQueryEnd(context.Background(), nil, pgx.TraceQueryEndData{
		Err: errors.New("connection reset by peer"),
	})

	counts := countsByAttr(t, reader, "db.query.errors.by_sqlstate.total", "sqlstate")
	if got := counts[sqlStateUnknown]; got != 1 {
		t.Errorf("sqlstate unknown count = %d, want 1", got)
	}
}

// The error ratio the warning rule alerts on is meaningless without a
// denominator that counts the queries that succeeded.
func TestQueryErrorTracerCountsSuccessesInTheDenominator(t *testing.T) {
	tracer, reader := newTestTracer(t)
	ctx := context.Background()

	tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{})
	tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{Err: pgx.ErrNoRows})
	tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{Err: pgErr("53200")})

	if got := counterTotal(t, reader, "db.query.total"); got != 3 {
		t.Errorf("db.query.total = %d, want 3", got)
	}
}

func TestQueryErrorTracerIgnoresNonErrorOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "success", err: nil},
		{name: "no rows", err: pgx.ErrNoRows},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tracer, reader := newTestTracer(t)

			tracer.TraceQueryEnd(context.Background(), nil, pgx.TraceQueryEndData{Err: tc.err})

			if got := counterTotal(t, reader, "db.query.errors.total"); got != 0 {
				t.Errorf("db.query.errors.total = %d, want 0", got)
			}
		})
	}
}

// pgx dispatches SendBatch through BatchTracer alone and never calls
// TraceQueryEnd, so the bulk-write paths a resource storm kills are invisible
// without this.
func TestQueryErrorTracerCountsBatchQueryErrors(t *testing.T) {
	tracer, reader := newTestTracer(t)

	ctx := tracer.TraceBatchStart(context.Background(), nil, pgx.TraceBatchStartData{})
	tracer.TraceBatchQuery(ctx, nil, pgx.TraceBatchQueryData{Err: pgErr("53200")})

	counts := countsByAttr(t, reader, "db.query.errors.total", "error_class")
	if got := counts[errorClassResources]; got != 1 {
		t.Errorf("error_class resources count = %d, want 1", got)
	}
}

// A batch that fails before any statement is read reaches TraceBatchEnd only.
func TestQueryErrorTracerCountsBatchEarlyErrors(t *testing.T) {
	tracer, reader := newTestTracer(t)

	ctx := tracer.TraceBatchStart(context.Background(), nil, pgx.TraceBatchStartData{})
	tracer.TraceBatchEnd(ctx, nil, pgx.TraceBatchEndData{Err: pgErr("53200")})

	counts := countsByAttr(t, reader, "db.query.errors.total", "error_class")
	if got := counts[errorClassResources]; got != 1 {
		t.Errorf("error_class resources count = %d, want 1", got)
	}
}

// pgx reports one failed statement to TraceBatchQuery and again to
// TraceBatchEnd; counting both would inflate the error ratio.
func TestQueryErrorTracerCountsOneBatchFailureOnce(t *testing.T) {
	tracer, reader := newTestTracer(t)

	ctx := tracer.TraceBatchStart(context.Background(), nil, pgx.TraceBatchStartData{})
	tracer.TraceBatchQuery(ctx, nil, pgx.TraceBatchQueryData{Err: pgErr("53200")})
	tracer.TraceBatchEnd(ctx, nil, pgx.TraceBatchEndData{Err: pgErr("53200")})

	if got := counterTotal(t, reader, "db.query.errors.total"); got != 1 {
		t.Errorf("db.query.errors.total = %d, want 1", got)
	}
}

func TestQueryErrorTracerCountsCopyFromErrors(t *testing.T) {
	tracer, reader := newTestTracer(t)

	ctx := tracer.TraceCopyFromStart(context.Background(), nil, pgx.TraceCopyFromStartData{})
	tracer.TraceCopyFromEnd(ctx, nil, pgx.TraceCopyFromEndData{Err: pgErr("53200")})

	counts := countsByAttr(t, reader, "db.query.errors.total", "error_class")
	if got := counts[errorClassResources]; got != 1 {
		t.Errorf("error_class resources count = %d, want 1", got)
	}
}

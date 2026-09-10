package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
)

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
	for _, tc := range []struct {
		name  string
		err   error
		state string
	}{
		{name: "postgres error", err: pgErr("53200"), state: "53200"},
		{name: "wrapped postgres error", err: fmt.Errorf("saving block: %w", pgErr("40001")), state: "40001"},
		{name: "non-postgres error", err: errors.New("connection reset by peer"), state: sqlStateUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tracer, reader := newTestTracer(t)

			tracer.TraceQueryEnd(context.Background(), nil, pgx.TraceQueryEndData{Err: tc.err})

			counts := countsByAttr(t, reader, "db.query.errors.by_sqlstate.total", "sqlstate")
			if got := counts[tc.state]; got != 1 {
				t.Errorf("sqlstate %q count = %d, want 1", tc.state, got)
			}
		})
	}
}

// The error ratio the warning rule alerts on is meaningless without a
// denominator that counts the queries that succeeded.
func TestQueryErrorTracerCountsSuccessesInTheDenominator(t *testing.T) {
	tracer, reader := newTestTracer(t)
	ctx := context.Background()

	tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{})
	tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{Err: pgErr("53200")})

	if got := counterTotal(t, reader, "db.query.total"); got != 2 {
		t.Errorf("db.query.total = %d, want 2", got)
	}
}

func TestQueryErrorTracerIgnoresSuccesses(t *testing.T) {
	tracer, reader := newTestTracer(t)

	tracer.TraceQueryEnd(context.Background(), nil, pgx.TraceQueryEndData{})

	if got := counterTotal(t, reader, "db.query.errors.total"); got != 0 {
		t.Errorf("db.query.errors.total = %d, want 0", got)
	}
}

// A QueuedQuery.Fn callback returning ErrNoRows is the only way it reaches a
// tracer; it means "no row", not a fault.
func TestQueryErrorTracerIgnoresBatchNoRows(t *testing.T) {
	tracer, reader := newTestTracer(t)

	ctx := tracer.TraceBatchStart(context.Background(), nil, pgx.TraceBatchStartData{})
	tracer.TraceBatchEnd(ctx, nil, pgx.TraceBatchEndData{Err: pgx.ErrNoRows})

	if got := counterTotal(t, reader, "db.query.errors.total"); got != 0 {
		t.Errorf("db.query.errors.total = %d, want 0", got)
	}
}

func TestQueryErrorTracerCountsBatchQueryErrors(t *testing.T) {
	tracer, reader := newTestTracer(t)

	ctx := tracer.TraceBatchStart(context.Background(), nil, pgx.TraceBatchStartData{})
	tracer.TraceBatchQuery(ctx, nil, pgx.TraceBatchQueryData{Err: pgErr("53200")})

	counts := countsByAttr(t, reader, "db.query.errors.total", "error_class")
	if got := counts[errorClassResources]; got != 1 {
		t.Errorf("error_class resources count = %d, want 1", got)
	}
}

func TestQueryErrorTracerCountsBatchEarlyErrors(t *testing.T) {
	tracer, reader := newTestTracer(t)

	ctx := tracer.TraceBatchStart(context.Background(), nil, pgx.TraceBatchStartData{})
	tracer.TraceBatchEnd(ctx, nil, pgx.TraceBatchEndData{Err: pgErr("53200")})

	counts := countsByAttr(t, reader, "db.query.errors.total", "error_class")
	if got := counts[errorClassResources]; got != 1 {
		t.Errorf("error_class resources count = %d, want 1", got)
	}
}

func TestQueryErrorTracerCountsOneBatchFailureOnce(t *testing.T) {
	tracer, reader := newTestTracer(t)

	ctx := tracer.TraceBatchStart(context.Background(), nil, pgx.TraceBatchStartData{})
	tracer.TraceBatchQuery(ctx, nil, pgx.TraceBatchQueryData{Err: pgErr("53200")})
	tracer.TraceBatchEnd(ctx, nil, pgx.TraceBatchEndData{Err: pgErr("53200")})

	if got := counterTotal(t, reader, "db.query.errors.total"); got != 1 {
		t.Errorf("db.query.errors.total = %d, want 1", got)
	}
}

func TestQueryErrorTracerCountsOneEarlyBatchFailureOnce(t *testing.T) {
	tracer, reader := newTestTracer(t)

	ctx := tracer.TraceBatchStart(context.Background(), nil, pgx.TraceBatchStartData{})
	tracer.TraceBatchEnd(ctx, nil, pgx.TraceBatchEndData{Err: pgErr("53200")})
	tracer.TraceBatchEnd(ctx, nil, pgx.TraceBatchEndData{Err: pgErr("53200")})

	if got := counterTotal(t, reader, "db.query.errors.total"); got != 1 {
		t.Errorf("db.query.errors.total = %d, want 1", got)
	}
	if got := counterTotal(t, reader, "db.query.total"); got != 1 {
		t.Errorf("db.query.total = %d, want 1", got)
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

func TestQueryErrorTracerCountsConnectErrors(t *testing.T) {
	tracer, reader := newTestTracer(t)

	ctx := tracer.TraceConnectStart(context.Background(), pgx.TraceConnectStartData{})
	tracer.TraceConnectEnd(ctx, pgx.TraceConnectEndData{Err: pgErr("53300")})

	counts := countsByAttr(t, reader, "db.query.errors.total", "error_class")
	if got := counts[errorClassResources]; got != 1 {
		t.Errorf("error_class resources count = %d, want 1", got)
	}
}

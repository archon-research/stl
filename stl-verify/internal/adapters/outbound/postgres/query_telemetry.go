package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const instrumentationName = "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"

// sqlStateUnknown labels an error the server never assigned a SQLSTATE to: a
// cancelled context, a connection reset mid-statement. Pool acquisition fails
// before the tracer runs, so dial failures are not counted here at all.
const sqlStateUnknown = "unknown"

// Error classes partition every SQLSTATE into a domain small enough to seed, so
// alert rules key on a series that exists before the first error does.
const (
	errorClassResources = "resources"
	errorClassRetryable = "retryable"
	errorClassOther     = "other"
	errorClassUnknown   = "unknown"
)

var errorClasses = []string{errorClassResources, errorClassRetryable, errorClassOther, errorClassUnknown}

// queryErrorTracer counts database outcomes for every pool this package builds,
// so a fault in the shared database is visible wherever it lands rather than
// only on the services that happen to carry a sensitive alert.
type queryErrorTracer struct {
	queriesTotal     metric.Int64Counter
	errorsTotal      metric.Int64Counter
	errorsBySQLState metric.Int64Counter
}

var (
	_ pgx.QueryTracer    = (*queryErrorTracer)(nil)
	_ pgx.BatchTracer    = (*queryErrorTracer)(nil)
	_ pgx.CopyFromTracer = (*queryErrorTracer)(nil)
)

func newQueryErrorTracer(mp metric.MeterProvider) (*queryErrorTracer, error) {
	meter := mp.Meter(instrumentationName)

	queriesTotal, err := meter.Int64Counter(
		"db.query.total",
		metric.WithDescription("Database operations traced by the pgx tracer; the denominator of the error ratio"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating db.query.total counter: %w", err)
	}

	errorsTotal, err := meter.Int64Counter(
		"db.query.errors.total",
		metric.WithDescription("Failed database operations, labelled by error class (resources|retryable|other|unknown)"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating db.query.errors.total counter: %w", err)
	}

	// SQLSTATE is open-ended, so it cannot be seeded and cannot carry an alert
	// that must fire on a first occurrence; it rides a sibling counter for
	// breakdowns instead of widening the alertable series.
	errorsBySQLState, err := meter.Int64Counter(
		"db.query.errors.by_sqlstate.total",
		metric.WithDescription("Failed database operations, broken down by Postgres SQLSTATE"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating db.query.errors.by_sqlstate.total counter: %w", err)
	}

	t := &queryErrorTracer{
		queriesTotal:     queriesTotal,
		errorsTotal:      errorsTotal,
		errorsBySQLState: errorsBySQLState,
	}
	t.seedErrorClasses()
	return t, nil
}

// seedErrorClasses exports every error-class series at 0 on startup. Without it
// a class's first error is also its series' first sample, so increase() reports
// 0 for an increment it never observed beginning — and every deploy mints fresh
// series, resetting the blind spot.
func (t *queryErrorTracer) seedErrorClasses() {
	ctx := context.Background()
	for _, class := range errorClasses {
		t.errorsTotal.Add(ctx, 0, metric.WithAttributes(attribute.String("error_class", class)))
	}
}

func (t *queryErrorTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	return ctx
}

func (t *queryErrorTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	t.record(ctx, data.Err)
}

// TraceBatchStart seeds the per-batch dedupe flag TraceBatchEnd reads. pgx
// type-asserts BatchTracer separately and routes SendBatch — every bulk write in
// this package — through it alone, never through TraceQueryEnd.
func (t *queryErrorTracer) TraceBatchStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceBatchStartData) context.Context {
	return context.WithValue(ctx, batchStateKey{}, &batchState{})
}

func (t *queryErrorTracer) TraceBatchQuery(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchQueryData) {
	if t.record(ctx, data.Err) {
		markBatchErrorCounted(ctx)
	}
}

// TraceBatchEnd catches a batch that failed before any statement was read, which
// reaches no TraceBatchQuery; pgx repeats an already-traced statement error here,
// so a counted one is skipped.
func (t *queryErrorTracer) TraceBatchEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchEndData) {
	if data.Err == nil || batchErrorCounted(ctx) {
		return
	}
	t.record(ctx, data.Err)
}

func (t *queryErrorTracer) TraceCopyFromStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceCopyFromStartData) context.Context {
	return ctx
}

func (t *queryErrorTracer) TraceCopyFromEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceCopyFromEndData) {
	t.record(ctx, data.Err)
}

type batchStateKey struct{}

type batchState struct {
	errorCounted bool
}

func markBatchErrorCounted(ctx context.Context) {
	if s, ok := ctx.Value(batchStateKey{}).(*batchState); ok {
		s.errorCounted = true
	}
}

func batchErrorCounted(ctx context.Context) bool {
	s, ok := ctx.Value(batchStateKey{}).(*batchState)
	return ok && s.errorCounted
}

// record counts one traced operation and, when it failed, the failure against
// its class and SQLSTATE. An empty result set is an ordinary outcome, not a
// fault, and folding it in would leave the ratio too noisy to alert on.
func (t *queryErrorTracer) record(ctx context.Context, err error) bool {
	t.queriesTotal.Add(ctx, 1)
	if err == nil || errors.Is(err, pgx.ErrNoRows) {
		return false
	}

	state := sqlState(err)
	class := errorClass(state)
	t.errorsTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("error_class", class)))
	t.errorsBySQLState.Add(ctx, 1, metric.WithAttributes(
		attribute.String("sqlstate", state),
		attribute.String("error_class", class),
	))
	return true
}

// sqlState extracts the five-character SQLSTATE from a Postgres error. SQLSTATE
// is a closed set of codes, so it is safe to use as a metric attribute.
func sqlState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return sqlStateUnknown
}

// errorClass folds a SQLSTATE into the closed domain the alert rules key on.
// Class 53 is insufficient_resources; 40001 and 40P01 are the serialization and
// deadlock failures the blockstate repository retries.
func errorClass(state string) string {
	switch {
	case state == sqlStateUnknown:
		return errorClassUnknown
	case strings.HasPrefix(state, "53"):
		return errorClassResources
	case state == "40001", state == "40P01":
		return errorClassRetryable
	default:
		return errorClassOther
	}
}

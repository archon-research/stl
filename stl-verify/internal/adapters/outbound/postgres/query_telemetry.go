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
// cancelled context, a connection reset mid-statement.
const sqlStateUnknown = "unknown"

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
	_ pgx.ConnectTracer  = (*queryErrorTracer)(nil)
)

func newQueryErrorTracer(mp metric.MeterProvider) (*queryErrorTracer, error) {
	meter := mp.Meter(instrumentationName)

	queriesTotal, err := counter(meter, "db.query.total",
		"Database operations traced by the pgx tracer: queries, batch statements, copies, connection attempts, and a transaction's implicit BEGIN/COMMIT/ROLLBACK. The denominator of the error ratio")
	if err != nil {
		return nil, err
	}

	errorsTotal, err := counter(meter, "db.query.errors.total",
		"Failed database operations, labelled by error class (resources|retryable|other|unknown)")
	if err != nil {
		return nil, err
	}

	// SQLSTATE is open-ended, so this counter's series cannot be seeded and it
	// carries no alert condition; see seedErrorClasses.
	errorsBySQLState, err := counter(meter, "db.query.errors.by_sqlstate.total",
		"Failed database operations, broken down by Postgres SQLSTATE")
	if err != nil {
		return nil, err
	}

	t := &queryErrorTracer{
		queriesTotal:     queriesTotal,
		errorsTotal:      errorsTotal,
		errorsBySQLState: errorsBySQLState,
	}
	t.seedErrorClasses()
	return t, nil
}

func counter(meter metric.Meter, name, description string) (metric.Int64Counter, error) {
	c, err := meter.Int64Counter(name, metric.WithDescription(description))
	if err != nil {
		return nil, fmt.Errorf("creating %s counter: %w", name, err)
	}
	return c, nil
}

// seedErrorClasses exports every error-class series at 0. Without it a class's
// first error is also its series' first sample, so increase() reports 0 for an
// increment it never observed beginning, the ratio rule's numerator has no
// series to divide, and every deploy mints fresh series that reset the blind
// spot. It is why the alert rules key on the four fixed classes rather than on
// the open-ended sqlstate label, which cannot be enumerated in advance.
//
// Whether this reaches an exporter depends on the meter provider being the real
// one; attachQueryTracer owns that.
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

// TraceBatchStart seeds the per-batch dedupe flag the other batch callbacks
// read. pgx type-asserts BatchTracer separately and routes SendBatch — every
// bulk write in this package — through it alone, never through TraceQueryEnd.
func (t *queryErrorTracer) TraceBatchStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceBatchStartData) context.Context {
	return context.WithValue(ctx, batchStateKey{}, &batchState{})
}

func (t *queryErrorTracer) TraceBatchQuery(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchQueryData) {
	if t.record(ctx, data.Err) {
		markBatchErrorCounted(ctx)
	}
}

// TraceBatchEnd catches a batch that failed before any statement was read, and
// pgx delivers a prepare-time failure here twice: SendBatch traces it without
// setting endTraced, so Close traces the same error again. One flag per batch
// keeps that pair counted once, at the cost of dropping a genuinely different
// second error in simple protocol (pgx v5.10.0 batch.go:139-151), which the
// repo does not use.
func (t *queryErrorTracer) TraceBatchEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchEndData) {
	if data.Err == nil || batchErrorCounted(ctx) {
		return
	}
	if t.record(ctx, data.Err) {
		markBatchErrorCounted(ctx)
	}
}

func (t *queryErrorTracer) TraceCopyFromStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceCopyFromStartData) context.Context {
	return ctx
}

func (t *queryErrorTracer) TraceCopyFromEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceCopyFromEndData) {
	t.record(ctx, data.Err)
}

func (t *queryErrorTracer) TraceConnectStart(ctx context.Context, _ pgx.TraceConnectStartData) context.Context {
	return ctx
}

// TraceConnectEnd is where 53300 too_many_connections lands: pgx fails the
// connect inside pgxpool's acquire, so no query callback ever runs for it and
// the class-53 signal would otherwise miss the one code that is about the pool.
func (t *queryErrorTracer) TraceConnectEnd(ctx context.Context, data pgx.TraceConnectEndData) {
	t.record(ctx, data.Err)
}

type batchStateKey struct{}

type batchState struct {
	errorCounted bool
}

// markBatchErrorCounted no-ops off a context pgx did not build in
// TraceBatchStart, which it always calls first on a traced batch.
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

// sqlState extracts the five-character SQLSTATE from a Postgres error, looking
// through the *pgconn.ConnectError that wraps a connect-time failure.
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

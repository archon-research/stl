// Package transform_worker runs the transformation layer's incremental
// materialization on a schedule. Each invocation lists the transformed tables
// and calls each one's generated run function; the per-table
// queue-drain/transform/upsert logic lives in the database functions, so this
// service is the scheduler around them.
package transform_worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// drainBudget bounds the total time RunOnce spends draining queues in one tick, so
// a large backlog is worked off across ticks instead of overrunning the Temporal
// activity (StartToCloseTimeout 10m) and degrading to timeout + retry noise. Left
// as a var so tests can shrink it. Kept comfortably under the activity timeout to
// leave headroom for the queue-depth and parity reads afterwards.
var drainBudget = 8 * time.Minute

// minDrainSlice floors the per-source time slice. A slice too short to finish even
// one drain batch would cancel the statement mid-flight, roll back, and repeat the
// identical work every tick, so that source never commits progress while its metrics
// still read healthy. When the fair split (remaining budget / sources left) falls
// below this floor, RunOnce serves fewer sources this tick at the floor and carries
// the rest forward, rather than hand out doomed sub-batch slices. Conservative
// starting value: one 10k-row batch's realistic worst case. Tune against observed
// drain latency (transform.queue.rows_consumed / run duration). Left as a var so
// tests can shrink it.
var minDrainSlice = 60 * time.Second

// Service materializes the transformed layer incrementally, once per RunOnce.
type Service struct {
	runner    outbound.TransformRunner
	logger    *slog.Logger
	telemetry *Telemetry
}

// NewService creates a Service. runner is required; logger defaults to
// slog.Default(); telemetry may be nil (its metrics become no-ops).
func NewService(runner outbound.TransformRunner, logger *slog.Logger, telemetry *Telemetry) (*Service, error) {
	if runner == nil {
		return nil, fmt.Errorf("transform runner is required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{
		runner:    runner,
		logger:    logger.With("component", "transform-worker"),
		telemetry: telemetry,
	}, nil
}

// RunOnce runs every transformed table's incremental upsert once.
//
// A single table's failure is logged and recorded but does not abort the rest:
// a periodic job should still advance the tables it can rather than let one bad
// table starve the others. The failures are joined and returned so the run is
// still marked failed and retried on the next tick.
//
// An empty source list is treated as a failure, not a clean no-op: the migration
// seeds one transformed._sources row per table, so zero sources means the
// migration has not been applied or the worker is pointed at the wrong database.
// Returning an error surfaces that instead of silently reporting success.
func (s *Service) RunOnce(ctx context.Context) error {
	sources, err := s.runner.ListSources(ctx)
	if err != nil {
		return fmt.Errorf("listing transform sources: %w", err)
	}
	if len(sources) == 0 {
		return fmt.Errorf("no transform sources in transformed._sources (migration not applied or wrong database?)")
	}

	// Bound the whole drain phase to a per-tick budget so the tick fits inside the
	// Temporal activity. Within it each source gets a fair slice of the time left,
	// floored at minDrainSlice so a slice can never be too short to finish one batch.
	// A source that finishes early leaves slack; a second pass spends that slack on
	// the sources whose slice expired with backlog, looping until the budget is gone,
	// so a backlog in one source is worked off with the whole budget rather than a
	// single 1/N slice. Slice expiry is a clean stop (the rest is picked up next
	// tick); parent-context cancellation (shutdown / activity cancel) is a real abort
	// that must fail the run.
	drainCtx, cancel := context.WithTimeout(ctx, drainBudget)
	defer cancel()
	deadline, _ := drainCtx.Deadline()

	states := make(map[string]*drainState, len(sources))
	var (
		errs           []error
		total          int64
		budgetExceeded bool
		cancelled      bool
	)

	// drain runs one source, accumulates its outcome onto its state (telemetry and
	// logging happen once at the end, so a source drained across both passes counts
	// as one run), and reports whether it still has backlog.
	drain := func(source string, slice time.Duration) (backlogged bool) {
		out := s.drainTable(ctx, source, slice)
		st := states[source]
		if st == nil {
			st = &drainState{}
			states[source] = st
		}
		st.ran = true
		st.consumed += out.consumed
		st.upserted += out.upserted
		total += out.consumed
		switch {
		case out.parentCancelled:
			errs = append(errs, ctx.Err())
			cancelled = true
			st.status = statusCancelled
		case out.err != nil:
			s.logger.Error("transform run failed", "source", source, "error", out.err)
			errs = append(errs, out.err)
			st.status = statusFailure
		case out.backlogged:
			budgetExceeded = true
			st.status = statusBudgetExpired
			return true
		default:
			st.status = statusSuccess
		}
		return false
	}

	// First pass: fair split across every source, floored.
	var backlog []string
	for i, source := range sources {
		if ctx.Err() != nil {
			errs = append(errs, ctx.Err())
			cancelled = true
			break
		}
		remaining := time.Until(deadline)
		if remaining < minDrainSlice {
			// Not enough budget left for even one batch; serve fewer sources this tick
			// and carry the rest forward rather than hand out a doomed sub-batch slice.
			budgetExceeded = true
			break
		}
		slice := max(remaining/time.Duration(len(sources)-i), minDrainSlice)
		if drain(source, slice) {
			backlog = append(backlog, source)
		}
		if cancelled {
			break
		}
	}

	// Second pass: spend the slack early finishers left on the backlogged sources,
	// looping until the budget is spent.
	for len(backlog) > 0 && !cancelled {
		if time.Until(deadline) < minDrainSlice {
			break
		}
		var next []string
		for i, source := range backlog {
			if ctx.Err() != nil {
				errs = append(errs, ctx.Err())
				cancelled = true
				next = append(next, backlog[i:]...)
				break
			}
			remaining := time.Until(deadline)
			if remaining < minDrainSlice {
				next = append(next, backlog[i:]...)
				break
			}
			slice := max(remaining/time.Duration(len(backlog)-i), minDrainSlice)
			if drain(source, slice) {
				next = append(next, source)
			}
			if cancelled {
				break
			}
		}
		backlog = next
	}

	if budgetExceeded {
		s.logger.Warn("transform drain budget exceeded; remaining queues picked up next tick", "budget", drainBudget)
		s.telemetry.RecordDrainBudgetExceeded(ctx)
	}

	// Record each source's final outcome once, in a stable order. A source stopped by
	// the budget is tagged budget_expired (not success) so it is distinguishable and
	// leaves a log line (N13); a parent-cancelled source is an aborted run and gets no
	// per-source metric.
	for _, source := range sources {
		st := states[source]
		if st == nil || !st.ran {
			continue
		}
		switch st.status {
		case statusSuccess:
			s.logger.Info("transform run complete", "source", source, "consumed", st.consumed, "upserted", st.upserted)
			s.telemetry.RecordTableSuccess(ctx, source, st.consumed, st.upserted)
		case statusBudgetExpired:
			s.logger.Warn("transform drain budget expired for source; backlog carried to next tick",
				"source", source, "consumed", st.consumed, "upserted", st.upserted)
			s.telemetry.RecordTableBudgetExpired(ctx, source, st.consumed, st.upserted)
		case statusFailure:
			s.telemetry.RecordTableFailure(ctx, source)
		}
	}

	// The queue-depth and parity reads are the silent-failure backstop, so a failure
	// to READ them must itself surface: otherwise a permanently broken parity or
	// queue query leaves the gauges stale and the alerts looking healthy. Join their
	// errors into the run result; the drain is idempotent, so the retry is safe.
	if err := s.recordQueueDepth(ctx); err != nil {
		errs = append(errs, err)
	}
	if err := s.recordParity(ctx, sources); err != nil {
		errs = append(errs, err)
	}

	s.logger.Info("transform cycle complete",
		"tables", len(sources), "errors", len(errs), "rows", total)

	if len(errs) > 0 {
		return fmt.Errorf("transform cycle: %d error(s): %w", len(errs), errors.Join(errs...))
	}
	return nil
}

// runStatus is a source's final disposition for one RunOnce.
type runStatus int

const (
	statusSuccess runStatus = iota
	statusBudgetExpired
	statusFailure
	statusCancelled
)

// drainState accumulates one source's progress and final disposition across the (up
// to two) drain passes in a single RunOnce, so it is recorded exactly once.
type drainState struct {
	consumed, upserted int64
	status             runStatus
	ran                bool
}

// tableOutcome classifies a single RunTable invocation: a clean finish, a clean
// slice expiry with backlog remaining (retryable next tick), a parent-context
// cancellation (a real abort), or a genuine run error.
type tableOutcome struct {
	consumed, upserted int64
	backlogged         bool
	parentCancelled    bool
	err                error
}

// drainTable runs one source under its time slice and classifies the result. A done
// parent context surfaces as parentCancelled (must fail the run); a slice-only expiry
// surfaces as backlogged (a clean carry-forward).
func (s *Service) drainTable(ctx context.Context, source string, slice time.Duration) tableOutcome {
	srcCtx, srcCancel := context.WithTimeout(ctx, slice)
	consumed, upserted, runErr := s.runner.RunTable(srcCtx, source)
	srcCancel()

	out := tableOutcome{consumed: consumed, upserted: upserted}
	if runErr == nil {
		return out
	}
	switch {
	case ctx.Err() != nil:
		out.parentCancelled = true
	case errors.Is(runErr, context.DeadlineExceeded):
		out.backlogged = true
	default:
		out.err = runErr
	}
	return out
}

// recordQueueDepth emits the per-source backlog gauges that back the
// stalled-transform alert, and returns any read error so RunOnce can surface it: a
// permanently failing queue read would otherwise leave the gauges stale while the
// alerts read healthy.
func (s *Service) recordQueueDepth(ctx context.Context) error {
	depths, err := s.runner.QueueStatus(ctx)
	if err != nil {
		return fmt.Errorf("reading transform queue status: %w", err)
	}
	for _, d := range depths {
		s.telemetry.RecordQueueDepth(ctx, d.Source, d.Pending, d.OldestAgeSecs)
	}
	return nil
}

// recordParity refreshes each source's parity ledger, emits the parity gauges, and
// logs any nonzero drift (a row that reached neither the transformed table nor the
// queue). Refresh and read errors are joined and returned so RunOnce surfaces them:
// the parity ledger is the correctness backstop, so a silently failing refresh or
// read must not leave a stale gauge looking healthy.
func (s *Service) recordParity(ctx context.Context, sources []string) error {
	var errs []error
	for _, source := range sources {
		if err := s.runner.RefreshParity(ctx, source); err != nil {
			errs = append(errs, fmt.Errorf("refreshing transform parity %q: %w", source, err))
		}
	}
	rows, err := s.runner.ParityStatus(ctx)
	if err != nil {
		errs = append(errs, fmt.Errorf("reading transform parity status: %w", err))
		return errors.Join(errs...)
	}
	for _, p := range rows {
		s.telemetry.RecordParity(ctx, p.Source, p.RawRows, p.TransformedRows, p.Drift)
		if p.Drift != 0 {
			s.logger.Warn("transform parity drift",
				"source", p.Source, "raw", p.RawRows, "transformed", p.TransformedRows,
				"pending", p.PendingRows, "drift", p.Drift)
		}
	}
	return errors.Join(errs...)
}

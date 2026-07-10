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
	// Temporal activity, then hand each source an equal slice of the time that is
	// left. A source that finishes early donates its slack (the slice is recomputed
	// from the sources still to go), so a large backlog in an early source cannot
	// use the whole budget and starve the later queues. Slice expiry is a clean stop
	// (the rest is picked up next tick); parent-context cancellation (shutdown /
	// activity cancel) is a real abort that must fail the run.
	drainCtx, cancel := context.WithTimeout(ctx, drainBudget)
	defer cancel()
	deadline, _ := drainCtx.Deadline()

	var (
		errs           []error
		total          int64
		budgetExceeded bool
	)
	for i, source := range sources {
		// Parent cancellation (shutdown): abort and mark the run failed so it retries.
		if ctxErr := ctx.Err(); ctxErr != nil {
			errs = append(errs, ctxErr)
			break
		}
		slice := time.Until(deadline) / time.Duration(len(sources)-i)
		if slice <= 0 {
			budgetExceeded = true
			break
		}
		srcCtx, srcCancel := context.WithTimeout(ctx, slice)
		consumed, upserted, runErr := s.runner.RunTable(srcCtx, source)
		srcCancel()
		total += consumed

		if runErr != nil {
			switch {
			case ctx.Err() != nil:
				// Parent cancelled mid-drain (possibly on the last source, where there
				// is no next-iteration check): a real abort, not the budget.
				errs = append(errs, ctx.Err())
			case errors.Is(runErr, context.DeadlineExceeded):
				// This source's time slice expired with backlog remaining; carry the
				// rest to the next tick and still credit the partial progress.
				budgetExceeded = true
				s.telemetry.RecordTableSuccess(ctx, source, consumed, upserted)
			default:
				s.logger.Error("transform run failed", "source", source, "error", runErr)
				s.telemetry.RecordTableFailure(ctx, source)
				errs = append(errs, runErr)
			}
			if ctx.Err() != nil {
				break
			}
			continue
		}
		s.logger.Info("transform run complete", "source", source, "consumed", consumed, "upserted", upserted)
		s.telemetry.RecordTableSuccess(ctx, source, consumed, upserted)
	}

	if budgetExceeded {
		s.logger.Warn("transform drain budget exceeded; remaining queues picked up next tick", "budget", drainBudget)
		s.telemetry.RecordDrainBudgetExceeded(ctx)
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

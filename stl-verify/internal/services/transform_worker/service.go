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

	// Bound the whole drain phase to a per-tick budget shared across sources, so the
	// tick fits inside the Temporal activity. Budget expiry is a clean stop (the rest
	// is picked up next tick), distinct from parent-context cancellation (shutdown),
	// which is a real abort.
	drainCtx, cancel := context.WithTimeout(ctx, drainBudget)
	defer cancel()

	var (
		errs           []error
		total          int64
		budgetExceeded bool
	)
	for _, source := range sources {
		// Parent cancellation (shutdown): abort and mark the run failed so it retries.
		if ctxErr := ctx.Err(); ctxErr != nil {
			errs = append(errs, ctxErr)
			break
		}
		// Drain budget spent: stop cleanly without failing the run.
		if drainCtx.Err() != nil {
			budgetExceeded = true
			break
		}
		rows, err := s.runner.RunTable(drainCtx, source)
		if err != nil {
			s.logger.Error("transform run failed", "source", source, "error", err)
			s.telemetry.RecordTableFailure(ctx, source)
			errs = append(errs, err)
			continue
		}
		total += rows
		s.logger.Info("transform run complete", "source", source, "rows", rows)
		s.telemetry.RecordTableSuccess(ctx, source, rows)
	}

	if budgetExceeded {
		s.logger.Warn("transform drain budget exceeded; remaining queues picked up next tick", "budget", drainBudget)
		s.telemetry.RecordDrainBudgetExceeded(ctx)
	}

	s.recordQueueDepth(ctx)
	s.recordParity(ctx, sources)

	s.logger.Info("transform cycle complete",
		"tables", len(sources), "failed", len(errs), "rows", total)

	if len(errs) > 0 {
		return fmt.Errorf("transform cycle: %d of %d tables failed: %w",
			len(errs), len(sources), errors.Join(errs...))
	}
	return nil
}

// recordQueueDepth emits the per-source backlog gauges that back the
// stalled-transform alert. A read failure is logged but does not fail the run:
// materialization already succeeded, so a metrics-read hiccup should not mark the
// cycle failed; the next tick re-emits.
func (s *Service) recordQueueDepth(ctx context.Context) {
	depths, err := s.runner.QueueStatus(ctx)
	if err != nil {
		s.logger.Warn("reading transform queue status failed; queue-depth metrics skipped this tick", "error", err)
		return
	}
	for _, d := range depths {
		s.telemetry.RecordQueueDepth(ctx, d.Source, d.Pending, d.OldestAgeSecs)
	}
}

// recordParity incrementally refreshes each source's parity ledger, then emits the
// parity gauges and logs any source whose drift is nonzero (a row that reached
// neither the transformed table nor the queue). A refresh or read failure is
// logged, not fatal: materialization already succeeded, and a stale ledger still
// reports the last-known drift.
func (s *Service) recordParity(ctx context.Context, sources []string) {
	for _, source := range sources {
		if err := s.runner.RefreshParity(ctx, source); err != nil {
			s.logger.Warn("transform parity refresh failed; ledger may be stale this tick", "source", source, "error", err)
		}
	}
	rows, err := s.runner.ParityStatus(ctx)
	if err != nil {
		s.logger.Warn("reading transform parity status failed; parity metrics skipped this tick", "error", err)
		return
	}
	for _, p := range rows {
		s.telemetry.RecordParity(ctx, p.Source, p.RawRows, p.TransformedRows, p.Drift)
		if p.Drift != 0 {
			s.logger.Warn("transform parity drift",
				"source", p.Source, "raw", p.RawRows, "transformed", p.TransformedRows,
				"pending", p.PendingRows, "drift", p.Drift)
		}
	}
}

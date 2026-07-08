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

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

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

	var (
		errs  []error
		total int64
	)
	for _, source := range sources {
		// Stop cleanly on shutdown rather than running (and failing) every
		// remaining table against a cancelled context, which would inflate the
		// failure count. The already-joined errs still mark the run failed.
		if ctxErr := ctx.Err(); ctxErr != nil {
			errs = append(errs, ctxErr)
			break
		}
		rows, err := s.runner.RunTable(ctx, source)
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

	s.logger.Info("transform cycle complete",
		"tables", len(sources), "failed", len(errs), "rows", total)

	if len(errs) > 0 {
		return fmt.Errorf("transform cycle: %d of %d tables failed: %w",
			len(errs), len(sources), errors.Join(errs...))
	}
	return nil
}

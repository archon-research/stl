// Package position_materializer runs the position projections on a schedule.
// Each invocation calls the shared materialize_position_projection() database
// function once per configured projection view; the contract validation, recency
// guard, and classification upsert all live in that function (VEC-402), so this
// service is the scheduler around it.
//
// The write path is the full-projection upsert: every run re-projects and
// re-upserts each view's whole history, so the FIRST scheduled run is also the
// history bootstrap — there is no separate bootstrap job. The incremental
// (trigger-fed) write path and compression are VEC-566 and replace the write
// path under this same runner.
package position_materializer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Service materializes the configured position projections, once per RunOnce.
type Service struct {
	views        []string
	materializer outbound.PositionMaterializer
	reason       string
	logger       *slog.Logger
	telemetry    *Telemetry
}

// NewService creates a Service. materializer is required. views is the ordered
// list of projection view names to run; it must be non-empty (an empty list
// means the deployment is misconfigured, not that there is nothing to do), with
// no blank or duplicate entries (a duplicate is a config typo — reruns are
// idempotent but a silent double-run hides the mistake). reason must be
// non-empty: it is stamped as change_reason provenance on every classification
// write, and the database function rejects a blank one anyway — failing here is
// earlier and clearer. logger defaults to slog.Default(); telemetry may be nil
// (its metrics become no-ops).
func NewService(views []string, materializer outbound.PositionMaterializer, reason string, logger *slog.Logger, telemetry *Telemetry) (*Service, error) {
	if materializer == nil {
		return nil, fmt.Errorf("position materializer is required")
	}
	if len(views) == 0 {
		return nil, fmt.Errorf("no projection views configured (POSITION_PROJECTIONS empty or unset?)")
	}
	seen := make(map[string]bool, len(views))
	for _, v := range views {
		if strings.TrimSpace(v) == "" {
			return nil, fmt.Errorf("projection view list contains a blank entry")
		}
		if seen[v] {
			return nil, fmt.Errorf("projection view %q configured twice", v)
		}
		seen[v] = true
	}
	if strings.TrimSpace(reason) == "" {
		return nil, fmt.Errorf("change_reason is required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{
		views:        views,
		materializer: materializer,
		reason:       reason,
		logger:       logger.With("component", "position-materializer"),
		telemetry:    telemetry,
	}, nil
}

// RunOnce materializes every configured projection view once, sequentially.
//
// Sequential is load-bearing, not a simplification: the shared function's
// per-view advisory lock is held to transaction commit, and its contract is
// AT MOST ONE view per transaction — each Materialize call is a single
// statement (one transaction), and running views one after another means this
// process can never hold two view locks at once.
//
// A single view's failure is logged and recorded but does not abort the rest: a
// periodic job should still advance the projections it can rather than let one
// bad view starve the others (a poisoned source row wedges only its own
// protocol). The failures are joined and returned so the run is still marked
// failed and retried on the next tick. Parent-context cancellation aborts the
// remaining views immediately.
func (s *Service) RunOnce(ctx context.Context) error {
	var errs []error
	for _, view := range s.views {
		if err := ctx.Err(); err != nil {
			errs = append(errs, fmt.Errorf("aborting before %s: %w", view, err))
			break
		}
		start := time.Now()
		changed, err := s.materializer.Materialize(ctx, view, s.reason)
		if err != nil {
			s.logger.Error("projection materialization failed", "view", view, "error", err)
			s.telemetry.RecordRun(ctx, view, "error", 0)
			errs = append(errs, fmt.Errorf("view %s: %w", view, err))
			continue
		}
		s.logger.Info("projection materialized",
			"view", view, "rows_changed", changed, "duration", time.Since(start))
		s.telemetry.RecordRun(ctx, view, "ok", changed)
	}
	return errors.Join(errs...)
}

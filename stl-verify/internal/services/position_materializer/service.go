// Package position_materializer calls one materialize_<projection>() wrapper per configured projection
// on a schedule. Each run re-projects whole history and appends only unseen observation keys, so the
// first run is the bootstrap; p_window bounds the batch, not the read (materialize_position_projection).
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
	materializers []string
	materializer  outbound.PositionMaterializer
	buildID       int
	runID         int64
	logger        *slog.Logger
	telemetry     *Telemetry
}

// NewService creates a Service. materializer is required. materializers is the ordered
// list of materialize_<projection> function names to run; it must be non-empty (an empty list
// means the deployment is misconfigured, not that there is nothing to do), with
// no blank or duplicate entries (a duplicate is a config typo — reruns are
// idempotent but a silent double-run hides the mistake). buildID and runID are
// stamped on every appended row, as the ADR-0002 code-provenance record and the
// ADR-0006 §2 writer run. buildID may be 0, the reserved pre-tracking build;
// runID may not, because a run is opened at startup and a zero would name a
// writer_run row that does not exist. logger defaults to slog.Default();
// telemetry may be nil (its metrics become no-ops).
func NewService(materializers []string, materializer outbound.PositionMaterializer, buildID int, runID int64, logger *slog.Logger, telemetry *Telemetry) (*Service, error) {
	if materializer == nil {
		return nil, fmt.Errorf("position materializer is required")
	}
	if len(materializers) == 0 {
		return nil, fmt.Errorf("no projection materializers configured (POSITION_PROJECTIONS empty or unset?)")
	}
	seen := make(map[string]bool, len(materializers))
	for _, v := range materializers {
		if strings.TrimSpace(v) == "" {
			return nil, fmt.Errorf("projection materializer list contains a blank entry")
		}
		if seen[v] {
			return nil, fmt.Errorf("projection materializer %q configured twice", v)
		}
		seen[v] = true
	}
	if buildID < 0 {
		return nil, fmt.Errorf("buildID must not be negative, got %d", buildID)
	}
	// A run is opened at startup, so a zero here means the wiring skipped it and every row this
	// process appends would be unattributable.
	if runID <= 0 {
		return nil, fmt.Errorf("runID must be a writer_run id, got %d", runID)
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{
		materializers: materializers,
		materializer:  materializer,
		buildID:       buildID,
		runID:         runID,
		logger:        logger.With("component", "position-materializer"),
		telemetry:     telemetry,
	}, nil
}

// CheckConfigured fails when a configured materializer does not exist, so a list naming a wrapper
// whose migration has not shipped stops the worker at startup instead of failing every tick.
func (s *Service) CheckConfigured(ctx context.Context) error {
	missing, err := s.materializer.MissingMaterializers(ctx, s.materializers)
	if err != nil {
		return fmt.Errorf("checking configured materializers: %w", err)
	}
	if len(missing) > 0 {
		return fmt.Errorf("configured materializers not callable (absent, ambiguous, or not taking a build and a writer run): %s",
			strings.Join(missing, ", "))
	}
	return nil
}

// runStatus classifies a failed projection. Only the parent's cancellation is canceled; a deadline,
// the parent's or the projection's own, is an error. It reads ctx, not the error, as the Temporal
// adapter's runStatusAttr does, so a failure that coincides with a rollout also counts as canceled.
func runStatus(ctx context.Context) string {
	if errors.Is(ctx.Err(), context.Canceled) {
		return statusCanceled
	}
	return statusError
}

// RunOnce runs every configured projection materializer once, sequentially.
//
// Sequential is load-bearing, not a simplification: the shared function's
// per-view advisory lock is held to transaction commit, and its contract is
// AT MOST ONE projection per transaction — each Materialize call is a single
// statement (one transaction), and running projections one after another means this
// process can never hold two view locks at once.
//
// A single projection's failure is logged and recorded but does not abort the rest: a
// periodic job should still advance the projections it can rather than let one
// bad projection starve the others (a poisoned source row wedges only its own
// protocol). The failures are joined and returned so the run is still marked
// failed and retried on the next tick. Parent-context cancellation aborts the
// remaining projections immediately.
func (s *Service) RunOnce(ctx context.Context) error {
	var errs []error
	for _, m := range s.materializers {
		if err := ctx.Err(); err != nil {
			errs = append(errs, fmt.Errorf("aborting before %s: %w", m, err))
			break
		}
		start := time.Now()
		changed, err := s.materializer.Materialize(ctx, m, s.buildID, s.runID)
		if err != nil {
			s.logger.Error("projection materialization failed", "materializer", m, "error", err)
			s.telemetry.RecordRun(ctx, m, runStatus(ctx), 0)
			errs = append(errs, fmt.Errorf("materializer %s: %w", m, err))
			continue
		}
		s.logger.Info("projection materialized",
			"materializer", m, "rows_changed", changed, "duration", time.Since(start))
		s.telemetry.RecordRun(ctx, m, statusOK, changed)
	}
	// Both reads are skipped on a cancelled context: the loop above has already recorded the abort,
	// and a shutdown mid-run would otherwise log a spurious read failure on every deploy.
	if ctx.Err() == nil {
		s.publishWithheld(ctx)
		s.publishCacheRows(ctx)
	}
	return errors.Join(errs...)
}

// publishCacheRows reports the size of each trigger-fed cache derived from position_state. Those
// caches are plain tables, and db/migrations/AGENTS.md makes a row-growth tripwire the price of
// that: nothing else notices a plain table growing. They are written by database triggers, so this
// run cannot count what they persisted — it reads their level instead, once per run.
//
// A failure here does not fail the run, for the same reason as publishWithheld: the projections
// did their work and the rows are committed. It is logged. Note it does NOT show as the gauge going
// absent: the SDK exports cumulatively, so the last good level stands for the life of the pod. A
// read that keeps failing is visible only in the logs until it gets its own signal.
func (s *Service) publishCacheRows(ctx context.Context) {
	estimates, err := s.materializer.CacheRowEstimates(ctx)
	if err != nil {
		s.logger.Error("reading cache row estimates failed; the projections themselves succeeded", "error", err)
		return
	}
	for table, rows := range estimates {
		s.telemetry.RecordCacheRows(ctx, table, rows)
	}
}

// publishWithheld reports each projection's positions_refused from its latest run: positions whose
// new observations were withheld, and positions whose re-emitted stored key was declined. The
// shared function does both and continues, so without this a projection reports success every tick
// while a position sits at a stale value or the view and the spine disagree, and nothing says so.
// The two classes are told apart by position_projection_refusal.reason, not by this number.
//
// A failure here does not fail the run: the projections did their work and the rows are committed.
// It is logged, and a read that keeps failing shows up as the gauge going absent.
func (s *Service) publishWithheld(ctx context.Context) {
	refused, err := s.materializer.RefusedByProjection(ctx, s.runID)
	if err != nil {
		s.logger.Error("reading withheld positions failed; the projections themselves succeeded", "error", err)
		return
	}
	for projection, n := range refused {
		s.telemetry.RecordRefused(ctx, projection, n)
		if n > 0 {
			s.logger.Warn("projection withheld or declined observations for some positions",
				"projection", projection, "positions_refused", n)
		}
	}
}

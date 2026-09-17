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
		return fmt.Errorf("configured materializers not callable (absent, ambiguous, not returning one bigint, or not taking a build and a writer run): %s",
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

// refusedReadSlack widens the withheld read past the tick's elapsed time, covering the read's own
// latency. It stays far below the schedule interval, so the previous tick's rows are never included.
const refusedReadSlack = time.Minute

// RunOnce runs every configured projection materializer once, sequentially.
//
// Sequential is load-bearing: the shared function's per-view advisory lock is held to commit, and
// each Materialize call is one transaction, so this process never holds two view locks at once.
//
// A projection's failure is logged and recorded but does not stop the rest, so one poisoned source
// row wedges only its own protocol; the failures are joined and returned so the run is marked
// failed. Once the parent context ends, every remaining projection is recorded as not run with the
// same status, so a starved projection is visible by name.
func (s *Service) RunOnce(ctx context.Context) error {
	tickStart := time.Now()
	var errs []error
	for _, m := range s.materializers {
		if err := ctx.Err(); err != nil {
			s.telemetry.RecordRun(ctx, m, runStatus(ctx), 0)
			errs = append(errs, fmt.Errorf("skipping %s: %w", m, err))
			continue
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
	// On an ended context both reads are skipped, so a shutdown does not count a read failure, and both
	// gauges are cleared, so they do not keep the previous tick's levels.
	if ctx.Err() != nil {
		s.telemetry.SetRefused(nil)
		s.telemetry.SetCacheRows(nil)
		return errors.Join(errs...)
	}
	s.publishWithheld(ctx, time.Since(tickStart)+refusedReadSlack)
	s.publishCacheRows(ctx)
	return errors.Join(errs...)
}

// publishCacheRows exports the size of each trigger-fed cache derived from position_state, the level
// the plain-table tripwire compares to its budget (db/migrations/AGENTS.md).
//
// A failed read does not fail the run, whose rows are committed. It is logged and counted in
// read_failures, and the gauge goes absent until a read succeeds.
func (s *Service) publishCacheRows(ctx context.Context) {
	estimates, err := s.materializer.CacheRowEstimates(ctx)
	if err != nil {
		s.logger.Error("reading cache row estimates failed; the projections themselves succeeded", "error", err)
		s.telemetry.RecordReadFailure(ctx, readCacheRows)
		s.telemetry.SetCacheRows(nil)
		return
	}
	s.telemetry.SetCacheRows(estimates)
}

// publishWithheld exports positions_refused for each projection that completed a run within the tick:
// positions whose new observations were withheld, and positions whose re-emitted stored key was
// declined. The shared function does both and reports success, so this level is what shows it; the
// two classes are told apart by position_projection_refusal.reason.
//
// A projection that did not complete this tick is absent, so its gauge does not hold an old level.
// A failed read does not fail the run: it is logged, counted in read_failures, and every level goes
// absent until a read succeeds.
func (s *Service) publishWithheld(ctx context.Context, within time.Duration) {
	refused, err := s.materializer.RefusedByProjection(ctx, s.runID, within)
	if err != nil {
		s.logger.Error("reading withheld positions failed; the projections themselves succeeded", "error", err)
		s.telemetry.RecordReadFailure(ctx, readRefused)
		s.telemetry.SetRefused(nil)
		return
	}
	s.telemetry.SetRefused(refused)
	for projection, n := range refused {
		if n > 0 {
			s.logger.Warn("projection withheld or declined observations for some positions",
				"projection", projection, "positions_refused", n)
		}
	}
}

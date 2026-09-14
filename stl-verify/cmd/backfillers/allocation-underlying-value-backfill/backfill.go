package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/activity"
	temporalsdk "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// maxPassesPerRun bounds one workflow's activity count so a stalled or
// mistuned range fails fast instead of outgrowing Temporal's ~51,200-event
// history ceiling. At a handful of history events per pass this is far past
// any range seen in practice (13 passes over ~104k rows on a VEC-759 local
// slice), with room to spare.
const maxPassesPerRun = 8_000

// BackfillParams is the JSON an operator supplies in the Temporal UI's Input
// box:
//
//	{"after":"2026-01-01T00:00:00Z","limit":5000,"write":true}
//
// Before, primeId, limit and maxPriceBlockLag default exactly like the CLI's
// matching flags (-before, -prime-id, -limit, -max-price-block-lag) when
// omitted. Write defaults to false (dry run): a JSON bool cannot distinguish
// "omitted" from "explicitly false", so the field is named for the dangerous
// state rather than the safe one — the same safety the CLI gets by defaulting
// -dry-run to true.
type BackfillParams struct {
	After            time.Time `json:"after,omitzero"`
	Before           time.Time `json:"before,omitzero"`
	PrimeID          int64     `json:"primeId,omitempty"`
	Limit            int       `json:"limit,omitempty"`
	Write            bool      `json:"write,omitempty"`
	MaxPriceBlockLag int64     `json:"maxPriceBlockLag,omitempty"`
}

// resolve validates params and fills in defaults, so the same range behaves
// identically whether it is started from the CLI or the UI. now is threaded
// through rather than read from the clock: this runs in workflow code, where
// only workflow.Now is deterministic across replay.
func (p BackfillParams) resolve(now time.Time) (passParams, error) {
	before := p.Before
	if before.IsZero() {
		parsed, err := time.Parse(time.RFC3339, defaultBeforeCutover)
		if err != nil {
			return passParams{}, err
		}
		before = parsed
	}
	if !p.After.IsZero() && !before.After(p.After) {
		return passParams{}, fmt.Errorf("before (%s) must be after after (%s)",
			before.Format(time.RFC3339), p.After.Format(time.RFC3339))
	}
	if before.After(now) {
		return passParams{}, fmt.Errorf("before (%s) is in the future (now %s)",
			before.Format(time.RFC3339), now.Format(time.RFC3339))
	}

	limit := p.Limit
	if limit == 0 {
		limit = defaultLimit
	}
	if limit < 0 {
		return passParams{}, fmt.Errorf("limit must be positive, got %d", limit)
	}

	maxPriceLag := p.MaxPriceBlockLag
	if maxPriceLag == 0 {
		maxPriceLag = defaultMaxPriceLag
	}

	return passParams{
		After: p.After, Before: before, PrimeID: p.PrimeID,
		Limit: limit, Write: p.Write, MaxPriceBlockLag: maxPriceLag,
	}, nil
}

// passParams is what the workflow hands one RunPass activity: params.resolve
// with After pinned to this pass's own cursor position. Exported fields only —
// Temporal serializes activity input as JSON, so an unexported field (like
// cliConfig's dbURL, which the activity does not need) would silently vanish.
type passParams struct {
	After            time.Time `json:"after"`
	Before           time.Time `json:"before"`
	PrimeID          int64     `json:"primeId"`
	Limit            int       `json:"limit"`
	Write            bool      `json:"write"`
	MaxPriceBlockLag int64     `json:"maxPriceBlockLag"`
}

func (p passParams) toCliConfig() cliConfig {
	return cliConfig{
		after: p.After, before: p.Before, primeID: p.PrimeID,
		limit: p.Limit, dryRun: !p.Write, maxPriceLag: p.MaxPriceBlockLag,
	}
}

// passActivityResult is what RunPass reports back to the workflow: passResult
// with exported fields for the same JSON-boundary reason as passParams.
type passActivityResult struct {
	Fetched int       `json:"fetched"`
	Written int       `json:"written"`
	Cursor  time.Time `json:"cursor"`
}

// backfillProgress is the progress query's answer. This job matters more than
// most for this query: it processes in -limit sized passes over a range that
// can be orders of magnitude larger than one pass, and Temporal discards a
// workflow's result payload when it returns an error — so this query is the
// only way an operator sees how far a failing run got.
type backfillProgress struct {
	PassesDone  int       `json:"passesDone"`
	RowsFetched int       `json:"rowsFetched"`
	RowsWritten int       `json:"rowsWritten"`
	Cursor      time.Time `json:"cursor"`
}

// BackfillResult is the workflow's return value, shown in the UI's Result
// panel on a successful run.
type BackfillResult struct {
	PassesRun   int       `json:"passesRun"`
	RowsFetched int       `json:"rowsFetched"`
	RowsWritten int       `json:"rowsWritten"`
	Cursor      time.Time `json:"cursor"`
}

func backfillWorkflow(ctx workflow.Context, params BackfillParams) (BackfillResult, error) {
	logger := workflow.GetLogger(ctx)

	// Registered before validation so the Query tab answers for every run. Skip
	// it and a rejected run replies "unknown queryType progress", which reads
	// like a broken worker rather than a rejected request.
	var state backfillProgress
	if err := workflow.SetQueryHandler(ctx, progressQueryName, func() (backfillProgress, error) {
		return state, nil
	}); err != nil {
		return BackfillResult{}, fmt.Errorf("registering %q query handler: %w", progressQueryName, err)
	}

	resolved, err := params.resolve(workflow.Now(ctx))
	if err != nil {
		// Bad input fails identically on every attempt, so retrying it would
		// only bury the mistake under the retry envelope.
		return BackfillResult{}, temporalsdk.NewNonRetryableApplicationError(
			"invalid backfill parameters", "InvalidParams", err)
	}
	state.Cursor = resolved.After

	// Read from state at every return point, so the reported counts can never
	// lag the work actually done. On a FAILING run they do not reach the
	// Result panel at all — Temporal discards a workflow's result payload when
	// it returns a non-nil error — so the progress query is the channel an
	// operator uses to see how far the run got.
	resultOf := func() BackfillResult {
		return BackfillResult{
			PassesRun: state.PassesDone, RowsFetched: state.RowsFetched,
			RowsWritten: state.RowsWritten,
			Cursor:      state.Cursor,
		}
	}

	logger.Info("starting allocation underlying value backfill",
		"after", resolved.After.Format(time.RFC3339Nano), "before", resolved.Before.Format(time.RFC3339),
		"limit", resolved.Limit, "write", resolved.Write)

	if err := runPasses(ctx, resolved, &state); err != nil {
		return resultOf(), err
	}

	logger.Info("backfill complete",
		"passes", state.PassesDone, "rowsFetched", state.RowsFetched,
		"rowsWritten", state.RowsWritten,
		"cursor", state.Cursor.Format(time.RFC3339Nano))
	return resultOf(), nil
}

// runPasses walks resolved forward one RunPass activity at a time, each
// covering up to resolved.Limit rows starting at state.Cursor, until a pass
// comes back short of a full page (the range is exhausted) or empty (nothing
// left to fetch).
//
// Sequential on purpose, for resumability rather than speed: every completed
// pass is already in the workflow's event history, so a retry or a rolled pod
// resumes at the next pass instead of redoing the range.
func runPasses(ctx workflow.Context, resolved passParams, state *backfillProgress) error {
	ctx = workflow.WithActivityOptions(ctx, passActivityOptions())
	var activities *backfillActivities

	for {
		if state.PassesDone >= maxPassesPerRun {
			return fmt.Errorf("hit the %d-pass safety limit without exhausting the range: "+
				"narrow -before and resume the rest from the reported cursor instead of one open-ended run",
				maxPassesPerRun)
		}

		thisPass := resolved
		thisPass.After = state.Cursor

		var pass passActivityResult
		if err := workflow.ExecuteActivity(ctx, activities.RunPass, thisPass).Get(ctx, &pass); err != nil {
			return err
		}

		state.PassesDone++
		state.RowsFetched += pass.Fetched
		state.RowsWritten += pass.Written

		if pass.Fetched == 0 {
			return nil
		}
		if !pass.Cursor.After(state.Cursor) {
			// -after has only created_at resolution: a full page whose rows all
			// share one created_at can never be advanced past, so retrying (or
			// looping again) would re-fetch the identical page forever.
			return temporalsdk.NewNonRetryableApplicationError(
				fmt.Sprintf("pass fetched %d row(s) that all share created_at %s: -after cannot resolve "+
					"finer than created_at, so a %d-row page here can never advance the cursor — narrow "+
					"-limit or split the run by -prime-id",
					pass.Fetched, state.Cursor.Format(time.RFC3339Nano), resolved.Limit),
				"CursorStalled", nil)
		}
		state.Cursor = pass.Cursor
		if pass.Fetched < resolved.Limit {
			return nil
		}
	}
}

func passActivityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		// One pass fetches up to -limit rows, classifies them (including any
		// erc4626 archive multicalls, one per distinct block among the batch)
		// and writes them in a single transaction.
		StartToCloseTimeout: 10 * time.Minute,

		// Total time for one pass INCLUDING retries. This, not a small attempt
		// cap, is what bounds a pathological pass: an attempt cap turns
		// slow-but-progressing work into a hard failure, whereas an envelope
		// lets a transient archive/DB blip retry while still refusing to hang
		// the run forever.
		ScheduleToCloseTimeout: 30 * time.Minute,

		RetryPolicy: &temporalsdk.RetryPolicy{
			InitialInterval:    2 * time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
			// Deliberately no MaximumAttempts — ScheduleToCloseTimeout above is
			// the bound.
		},
	}
}

// backfillActivities holds the dependencies every pass shares across the
// worker's whole lifetime: one Postgres pool, one writer run (so every pass
// of every workflow execution lands under the same build_id), and one
// erc4626 archive resolver (so its per-chain multicaller is dialed once).
type backfillActivities struct {
	pool            *pgxpool.Pool
	deps            runnerDeps
	archiveResolver *erc4626ArchiveResolver
}

// RunPass executes one pass and reports what it did.
func (a *backfillActivities) RunPass(ctx context.Context, params passParams) (passActivityResult, error) {
	result, err := runPass(ctx, a.pool, a.deps, a.archiveResolver, params.toCliConfig())
	if err != nil {
		return passActivityResult{}, err
	}

	activity.GetLogger(ctx).Info("pass complete",
		"fetched", result.fetched, "written", result.written,
		"cursor", result.cursor.Format(time.RFC3339Nano))
	return passActivityResult{
		Fetched: result.fetched, Written: result.written,
		Cursor: result.cursor,
	}, nil
}

package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"go.temporal.io/sdk/activity"
	temporalsdk "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/archon-research/stl/stl-verify/internal/services/offchain_price_fetcher"
)

// maxChunksPerRun bounds one workflow's activity count. Activities run
// sequentially with no ContinueAsNew, and each contributes several history
// events, so an unbounded run can outgrow Temporal's history limit and be
// terminated mid-flight. 2000 chunks is ~160 years of two-asset backfill —
// far past any real request, but close enough to catch a mistyped year.
const maxChunksPerRun = 2000

// BackfillParams is the JSON an operator supplies in the Temporal UI's Input box:
//
//	{"assets":["weth","wrapped-bitcoin"],"from":"2020-01-01T00:00:00Z","to":"2026-08-05T00:00:00Z"}
//
// assets are CoinGecko IDs as registered in offchain_price_asset.source_asset_id,
// not token symbols.
type BackfillParams struct {
	Assets []string  `json:"assets"`
	From   time.Time `json:"from"`
	To     time.Time `json:"to"`
}

func (p BackfillParams) validate() error {
	if len(p.Assets) == 0 {
		return fmt.Errorf("assets must list at least one CoinGecko ID")
	}
	if slices.Contains(p.Assets, "") {
		return fmt.Errorf("assets must not contain an empty ID")
	}
	if dupes := len(p.Assets) - len(slices.Compact(slices.Sorted(slices.Values(p.Assets)))); dupes > 0 {
		return fmt.Errorf("assets contains %d duplicate ID(s); each would be fetched again and double-counted", dupes)
	}
	if p.From.IsZero() || p.To.IsZero() {
		return fmt.Errorf("from and to are both required, as RFC3339 timestamps (e.g. 2020-01-01T00:00:00Z)")
	}
	if !p.From.Before(p.To) {
		return fmt.Errorf("from (%s) must be before to (%s)",
			p.From.Format(time.RFC3339), p.To.Format(time.RFC3339))
	}
	// A dropped digit in a year ("0020-01-01") expands to ~24,000 sequential
	// activities. Temporal terminates a run whose history outgrows its limit, so
	// without this the operator's typo surfaces as a run killed mid-flight rather
	// than an immediate, readable rejection.
	if n := len(chunkWindows(p)); n > maxChunksPerRun {
		return fmt.Errorf("this request expands to %d chunks, over the %d limit: split it by asset or by year",
			n, maxChunksPerRun)
	}
	return nil
}

// assetCoverage is what a run actually achieved for one asset, as opposed to what
// it was asked to do. It exists because "how many points came back" alone cannot
// distinguish a complete backfill from one that silently covered only its tail:
// CoinGecko answers a window it cannot serve with HTTP 200 and empty arrays.
type assetCoverage struct {
	Points int `json:"points"`
	Chunks int `json:"chunks"`

	// EmptyLeading counts empty windows before the first window that returned
	// data. Ambiguous by nature — an asset listed part-way through the requested
	// range legitimately has none earlier, and so does a range that reaches past
	// the API plan's historical entitlement. Reported rather than rejected, with
	// CoveredFrom so the operator can see what they actually got.
	EmptyLeading int `json:"emptyLeading"`

	// EmptyAfterData counts empty windows *after* data has already been seen.
	// These are never legitimate: an asset cannot un-list itself, so an interior
	// or trailing hole is a real gap and fails the run.
	EmptyAfterData int `json:"emptyAfterData"`

	// CoveredFrom is the start of the first window that returned data. Compare it
	// with the requested `from` to see at a glance whether the range was truncated.
	CoveredFrom *time.Time `json:"coveredFrom,omitempty"`
}

// BackfillResult is the workflow's return value, shown in the UI's Result panel.
type BackfillResult struct {
	Coverage  map[string]assetCoverage `json:"coverage"`
	ChunksRun int                      `json:"chunksRun"`
}

type backfillProgress struct {
	ChunksTotal int                      `json:"chunksTotal"`
	ChunksDone  int                      `json:"chunksDone"`
	Coverage    map[string]assetCoverage `json:"coverage"`
}

// chunkWindow is one unit of work: a single asset over a window narrow enough to
// come back at hourly resolution.
type chunkWindow struct {
	Asset string    `json:"asset"`
	From  time.Time `json:"from"`
	To    time.Time `json:"to"`
}

func backfillWorkflow(ctx workflow.Context, params BackfillParams) (BackfillResult, error) {
	logger := workflow.GetLogger(ctx)

	if err := params.validate(); err != nil {
		// Bad input fails identically on every attempt, so retrying it only
		// obscures the mistake behind five backoffs.
		return BackfillResult{}, temporalsdk.NewNonRetryableApplicationError(
			"invalid backfill parameters", "InvalidParams", err)
	}

	windows := chunkWindows(params)
	state := backfillProgress{ChunksTotal: len(windows), Coverage: map[string]assetCoverage{}}
	if err := workflow.SetQueryHandler(ctx, progressQueryName, func() (backfillProgress, error) {
		return state, nil
	}); err != nil {
		return BackfillResult{}, fmt.Errorf("registering %q query handler: %w", progressQueryName, err)
	}

	logger.Info("starting backfill",
		"assets", params.Assets,
		"from", params.From.Format(time.DateOnly),
		"to", params.To.Format(time.DateOnly),
		"chunks", len(windows),
	)

	result := BackfillResult{Coverage: state.Coverage, ChunksRun: state.ChunksDone}
	if err := runChunks(ctx, windows, &state); err != nil {
		return result, err
	}
	result.ChunksRun = state.ChunksDone

	if err := assertCoverage(params, state); err != nil {
		// Returned alongside the error so a partially-successful run does not lose
		// the counts an operator needs to decide what to re-run.
		return result, err
	}

	// A truncated range is a success, but never a quiet one: the operator asked for
	// a window the provider only partly covered, and that has to be the headline of
	// the run rather than a detail buried in per-chunk pod logs.
	if truncated := truncatedAssets(params, state); len(truncated) > 0 {
		logger.Warn("backfill complete but the requested range was only partly covered",
			"assets", truncated,
			"requestedFrom", params.From.Format(time.DateOnly),
			"coverage", state.Coverage,
		)
	}

	logger.Info("backfill complete", "coverage", state.Coverage, "chunks", state.ChunksDone)
	return result, nil
}

// runChunks executes the windows one at a time, recording per-asset coverage.
//
// Sequential on purpose, for resumability rather than speed: a serial history
// makes a resumed run trivially easy to reason about, because every completed
// chunk is already in the event history and is not repeated. Parallelism would in
// fact be faster — the shared CoinGecko limiter is a 450 req/min ceiling, not a
// mutex, and the scheduled sweep does fan out at ServiceConfig.Concurrency — but
// ~162 chunks finish in minutes, so there is nothing to buy.
//
// Windows arrive grouped by asset and in ascending time order (see chunkWindows),
// which is what lets an empty window be classified as leading or after-data.
func runChunks(ctx workflow.Context, windows []chunkWindow, state *backfillProgress) error {
	ctx = workflow.WithActivityOptions(ctx, chunkActivityOptions())

	var activities *backfillActivities
	for _, w := range windows {
		var stored int
		if err := workflow.ExecuteActivity(ctx, activities.FetchChunk, w).Get(ctx, &stored); err != nil {
			return fmt.Errorf("backfilling %s from %s to %s: %w",
				w.Asset, w.From.Format(time.DateOnly), w.To.Format(time.DateOnly), err)
		}
		state.ChunksDone++
		state.Coverage[w.Asset] = recordChunk(state.Coverage[w.Asset], w, stored)
	}
	return nil
}

func recordChunk(c assetCoverage, w chunkWindow, stored int) assetCoverage {
	c.Chunks++
	switch {
	case stored > 0:
		c.Points += stored
		if c.CoveredFrom == nil {
			from := w.From
			c.CoveredFrom = &from
		}
	case c.CoveredFrom == nil:
		c.EmptyLeading++
	default:
		c.EmptyAfterData++
	}
	return c
}

// assertCoverage turns a silently incomplete result into a failure.
//
// Two distinct failures, because they need different verdicts. An asset that
// returned nothing at all is always wrong. An asset with a hole *after* data
// began is also always wrong — an asset cannot un-list itself, so an interior or
// trailing empty window is a genuine gap, not a coverage boundary.
//
// A leading run of empty windows is the one ambiguous case: it looks identical
// whether the asset was listed part-way through the range or the range reached
// past the API plan's historical entitlement. That is reported via CoveredFrom
// rather than rejected, so the operator can compare it with what they asked for.
func assertCoverage(params BackfillParams, state backfillProgress) error {
	for _, asset := range params.Assets {
		c := state.Coverage[asset]

		if c.Points == 0 {
			return fmt.Errorf(
				"asset %q returned no price points across the whole range %s to %s: "+
					"check the CoinGecko ID is registered in offchain_price_asset and that the "+
					"range falls inside CoinGecko's historical entitlement",
				asset, params.From.Format(time.DateOnly), params.To.Format(time.DateOnly))
		}

		if c.EmptyAfterData > 0 {
			return fmt.Errorf(
				"asset %q has %d empty window(s) after data began (coverage starts %s): "+
					"an interior or trailing gap is not a coverage boundary, so this is a real hole "+
					"in the series and must not be recorded as a complete backfill",
				asset, c.EmptyAfterData, c.CoveredFrom.Format(time.DateOnly))
		}
	}
	return nil
}

// truncatedAssets lists assets whose data starts later than the requested from,
// so the caller can say so prominently instead of reporting a bare success.
func truncatedAssets(params BackfillParams, state backfillProgress) []string {
	var truncated []string
	for _, asset := range params.Assets {
		if c := state.Coverage[asset]; c.EmptyLeading > 0 {
			truncated = append(truncated, asset)
		}
	}
	return truncated
}

func chunkActivityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		// One chunk is a single rate-limited HTTP call plus one batched upsert. The
		// ceiling is generous because the CoinGecko client spends its own retry and
		// rate-limit budget inside this window.
		StartToCloseTimeout: 3 * time.Minute,
		RetryPolicy: &temporalsdk.RetryPolicy{
			InitialInterval:    2 * time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    5,
		},
	}
}

// chunkWindows splits the request into per-asset windows no wider than the limit
// that still yields hourly data. Pure and deterministic, so it is safe to call
// from workflow code and testable without a Temporal test environment.
func chunkWindows(params BackfillParams) []chunkWindow {
	var windows []chunkWindow
	for _, asset := range params.Assets {
		for start := params.From; start.Before(params.To); {
			end := start.Add(offchain_price_fetcher.HistoricalChunkWidth)
			if end.After(params.To) {
				end = params.To
			}
			windows = append(windows, chunkWindow{Asset: asset, From: start, To: end})
			start = end
		}
	}
	return windows
}

type backfillActivities struct {
	service *offchain_price_fetcher.Service
}

// FetchChunk stores one window and reports how many points landed, leaving the
// emptiness judgement to the workflow (see assertEveryAssetProducedData).
//
// Idempotent *within one build*, which is what makes Temporal's activity retries
// safe. The write is ON CONFLICT DO NOTHING on the primary key
// (token_id, source_id, processing_version, timestamp), and the
// assign_processing_version_offchain_token_price trigger reuses the existing
// version only when a row with the same natural key AND the same build_id exists.
// Re-running a range from a *different* build therefore lands a second copy at
// processing_version+1 rather than doing nothing — additive by design, and read
// paths order by processing_version DESC. See ADR-0002 §3.
func (a *backfillActivities) FetchChunk(ctx context.Context, w chunkWindow) (int, error) {
	stored, err := a.service.BackfillChunk(ctx, w.Asset, w.From, w.To)
	if err != nil {
		wrapped := fmt.Errorf("backfilling %s from %s to %s: %w",
			w.Asset, w.From.Format(time.DateOnly), w.To.Format(time.DateOnly), err)

		// A mistyped ID or a malformed window fails identically on all five
		// attempts. Retrying buries the real cause under half a minute of backoff
		// and makes an operator error read like a flaky upstream.
		if errors.Is(err, offchain_price_fetcher.ErrInvalidRequest) {
			return 0, temporalsdk.NewNonRetryableApplicationError(
				wrapped.Error(), "InvalidRequest", err)
		}
		return 0, wrapped
	}

	activity.GetLogger(ctx).Info("stored chunk",
		"asset", w.Asset,
		"from", w.From.Format(time.DateOnly),
		"to", w.To.Format(time.DateOnly),
		"points", stored,
	)
	return stored, nil
}

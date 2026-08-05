package main

import (
	"context"
	"fmt"
	"slices"
	"time"

	"go.temporal.io/sdk/activity"
	temporalsdk "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/archon-research/stl/stl-verify/internal/services/offchain_price_fetcher"
)

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
	if p.From.IsZero() || p.To.IsZero() {
		return fmt.Errorf("from and to are both required, as RFC3339 timestamps (e.g. 2020-01-01T00:00:00Z)")
	}
	if !p.From.Before(p.To) {
		return fmt.Errorf("from (%s) must be before to (%s)",
			p.From.Format(time.RFC3339), p.To.Format(time.RFC3339))
	}
	return nil
}

// BackfillResult is the workflow's return value, shown in the UI's Result panel.
type BackfillResult struct {
	PointsByAsset map[string]int `json:"pointsByAsset"`
	ChunksRun     int            `json:"chunksRun"`
}

type backfillProgress struct {
	ChunksTotal   int            `json:"chunksTotal"`
	ChunksDone    int            `json:"chunksDone"`
	PointsByAsset map[string]int `json:"pointsByAsset"`
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
	state := backfillProgress{ChunksTotal: len(windows), PointsByAsset: map[string]int{}}
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

	if err := runChunks(ctx, windows, &state); err != nil {
		return BackfillResult{}, err
	}
	if err := assertEveryAssetProducedData(params, state); err != nil {
		return BackfillResult{}, err
	}

	logger.Info("backfill complete", "pointsByAsset", state.PointsByAsset, "chunks", state.ChunksDone)
	return BackfillResult{PointsByAsset: state.PointsByAsset, ChunksRun: state.ChunksDone}, nil
}

// runChunks executes the windows one at a time, updating state as it goes.
//
// Sequential on purpose. The CoinGecko rate limiter lives in the client, so
// parallel activities in one worker would contend on it rather than go faster,
// and a serial history makes a resumed run trivially easy to reason about: every
// completed chunk is already in the event history and is not repeated.
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
		state.PointsByAsset[w.Asset] += stored
	}
	return nil
}

// assertEveryAssetProducedData turns a silently empty result into a failure.
//
// CoinGecko answers a range it cannot serve — an unknown ID, or a window older
// than the plan's historical entitlement — with HTTP 200 and empty arrays. The
// workflow is the only layer that sees all of an asset's chunks, so it is the
// only one that can tell that from a legitimately empty leading chunk (an asset
// listed part-way through the requested range).
func assertEveryAssetProducedData(params BackfillParams, state backfillProgress) error {
	for _, asset := range params.Assets {
		if state.PointsByAsset[asset] == 0 {
			return fmt.Errorf(
				"asset %q stored no price points across the whole range %s to %s: "+
					"check the CoinGecko ID is registered in offchain_price_asset and that the "+
					"range falls inside CoinGecko's historical entitlement",
				asset, params.From.Format(time.DateOnly), params.To.Format(time.DateOnly))
		}
	}
	return nil
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
// Idempotent, which is what makes Temporal's retries and a re-run of the same
// range safe: writes are ON CONFLICT DO NOTHING on the natural key.
func (a *backfillActivities) FetchChunk(ctx context.Context, w chunkWindow) (int, error) {
	stored, err := a.service.BackfillChunk(ctx, w.Asset, w.From, w.To)
	if err != nil {
		return 0, fmt.Errorf("backfilling %s from %s to %s: %w",
			w.Asset, w.From.Format(time.DateOnly), w.To.Format(time.DateOnly), err)
	}

	activity.GetLogger(ctx).Info("stored chunk",
		"asset", w.Asset,
		"from", w.From.Format(time.DateOnly),
		"to", w.To.Format(time.DateOnly),
		"points", stored,
	)
	return stored, nil
}

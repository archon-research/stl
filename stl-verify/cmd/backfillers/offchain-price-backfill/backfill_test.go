package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"

	"github.com/archon-research/stl/stl-verify/internal/services/offchain_price_fetcher"
)

func day(y int, m time.Month, d int) time.Time {
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// daysAfter expresses ranges as day counts rather than calendar dates, so the
// expected chunk count stays obvious and does not shift with month lengths or
// leap years.
func daysAfter(t time.Time, n int) time.Time {
	return t.Add(time.Duration(n) * 24 * time.Hour)
}

// params builds a valid request, overridable per test.
func params(assets []string, from, to time.Time) BackfillParams {
	return BackfillParams{Assets: assets, From: from, To: to}
}

// registerChunkActivity stands in for the real activity, returning pointsPerChunk
// for every window and recording what it was asked to fetch.
func registerChunkActivity(env *testsuite.TestWorkflowEnvironment, pointsPerChunk func(chunkWindow) (int, error)) *[]chunkWindow {
	var seen []chunkWindow
	env.RegisterActivityWithOptions(
		func(_ context.Context, w chunkWindow) (int, error) {
			seen = append(seen, w)
			return pointsPerChunk(w)
		},
		activity.RegisterOptions{Name: "FetchChunk"},
	)
	return &seen
}

func TestChunkWindows(t *testing.T) {
	base := day(2020, time.January, 1)

	tests := []struct {
		name        string
		in          BackfillParams
		wantCount   int
		wantFirstTo time.Time
		wantLastTo  time.Time
	}{
		{
			name:        "range shorter than one chunk stays a single window",
			in:          params([]string{"weth"}, base, daysAfter(base, 10)),
			wantCount:   1,
			wantFirstTo: daysAfter(base, 10),
			wantLastTo:  daysAfter(base, 10),
		},
		{
			name:        "range of exactly one chunk stays a single window",
			in:          params([]string{"weth"}, base, daysAfter(base, 30)),
			wantCount:   1,
			wantFirstTo: daysAfter(base, 30),
			wantLastTo:  daysAfter(base, 30),
		},
		{
			name:        "ragged range ends on the requested boundary, never past it",
			in:          params([]string{"weth"}, base, daysAfter(base, 75)),
			wantCount:   3,
			wantFirstTo: daysAfter(base, 30),
			wantLastTo:  daysAfter(base, 75),
		},
		{
			name:        "each asset gets its own windows",
			in:          params([]string{"weth", "wrapped-bitcoin"}, base, daysAfter(base, 75)),
			wantCount:   6,
			wantFirstTo: daysAfter(base, 30),
			wantLastTo:  daysAfter(base, 75),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := chunkWindows(tc.in)

			if len(got) != tc.wantCount {
				t.Fatalf("window count = %d, want %d", len(got), tc.wantCount)
			}
			if !got[0].To.Equal(tc.wantFirstTo) {
				t.Errorf("first window To = %s, want %s", got[0].To, tc.wantFirstTo)
			}
			if last := got[len(got)-1]; !last.To.Equal(tc.wantLastTo) {
				t.Errorf("last window To = %s, want %s", last.To, tc.wantLastTo)
			}
		})
	}
}

// No window may exceed the hourly-resolution limit, or CoinGecko silently
// downgrades that chunk to daily data.
func TestChunkWindows_NeverExceedsHourlyResolutionLimit(t *testing.T) {
	got := chunkWindows(params([]string{"weth"}, day(2020, time.January, 1), day(2026, time.August, 5)))

	if len(got) == 0 {
		t.Fatal("expected windows for a multi-year range")
	}
	for _, w := range got {
		if width := w.To.Sub(w.From); width > offchain_price_fetcher.HistoricalChunkWidth {
			t.Fatalf("window %s..%s is %s wide, over the %s limit",
				w.From, w.To, width, offchain_price_fetcher.HistoricalChunkWidth)
		}
	}
}

// Consecutive windows must abut exactly: a gap silently skips hours, an overlap
// re-fetches them.
func TestChunkWindows_WindowsAbutWithoutGapOrOverlap(t *testing.T) {
	got := chunkWindows(params([]string{"weth"}, day(2020, time.January, 1), day(2020, time.June, 1)))

	for i := 1; i < len(got); i++ {
		if !got[i].From.Equal(got[i-1].To) {
			t.Errorf("window %d starts at %s but previous ended at %s", i, got[i].From, got[i-1].To)
		}
	}
}

func TestBackfillParams_Validate(t *testing.T) {
	from := day(2020, time.January, 1)
	to := day(2020, time.February, 1)

	tests := []struct {
		name    string
		in      BackfillParams
		wantErr bool
	}{
		{name: "valid request", in: params([]string{"weth"}, from, to)},
		{name: "no assets", in: params(nil, from, to), wantErr: true},
		{name: "empty asset ID", in: params([]string{""}, from, to), wantErr: true},
		{name: "missing from", in: params([]string{"weth"}, time.Time{}, to), wantErr: true},
		{name: "missing to", in: params([]string{"weth"}, from, time.Time{}), wantErr: true},
		{name: "from after to", in: params([]string{"weth"}, to, from), wantErr: true},
		{name: "from equals to", in: params([]string{"weth"}, from, from), wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.in.validate()

			if tc.wantErr && err == nil {
				t.Fatal("expected a validation error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestBackfillWorkflow_RunsEveryChunkAndTotalsPointsPerAsset(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	seen := registerChunkActivity(env, func(chunkWindow) (int, error) { return 100, nil })

	base := day(2020, time.January, 1)
	in := params([]string{"weth", "wrapped-bitcoin"}, base, daysAfter(base, 75))
	env.ExecuteWorkflow(backfillWorkflow, in)

	if !env.IsWorkflowCompleted() {
		t.Fatal("expected the workflow to complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("unexpected workflow error: %v", err)
	}

	var got BackfillResult
	if err := env.GetWorkflowResult(&got); err != nil {
		t.Fatalf("reading workflow result: %v", err)
	}

	if len(*seen) != 6 {
		t.Errorf("activity ran %d times, want 6 (3 chunks x 2 assets)", len(*seen))
	}
	if got.ChunksRun != 6 {
		t.Errorf("ChunksRun = %d, want 6", got.ChunksRun)
	}
	for _, asset := range in.Assets {
		if got.PointsByAsset[asset] != 300 {
			t.Errorf("PointsByAsset[%s] = %d, want 300", asset, got.PointsByAsset[asset])
		}
	}
}

// The whole point of the guard: CoinGecko returns HTTP 200 with empty arrays for
// an unknown ID or an out-of-entitlement window, so a run that stored nothing
// must fail rather than report a successful backfill of zero rows.
func TestBackfillWorkflow_FailsWhenAnAssetStoredNothing(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	registerChunkActivity(env, func(w chunkWindow) (int, error) {
		if w.Asset == "not-a-coin" {
			return 0, nil
		}
		return 100, nil
	})

	env.ExecuteWorkflow(backfillWorkflow, params(
		[]string{"weth", "not-a-coin"}, day(2020, time.January, 1), day(2020, time.February, 1)))

	if !env.IsWorkflowCompleted() {
		t.Fatal("expected the workflow to complete")
	}
	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("expected an error when an asset stored no price points")
	}
	if !strings.Contains(err.Error(), "not-a-coin") {
		t.Errorf("error should name the empty asset, got: %v", err)
	}
}

// A run where every asset produced data must not be failed by the guard just
// because some individual chunk was empty — an asset listed part-way through the
// range legitimately has none before its listing date.
func TestBackfillWorkflow_SucceedsWhenOnlySomeChunksAreEmpty(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	call := 0
	registerChunkActivity(env, func(chunkWindow) (int, error) {
		call++
		if call == 1 {
			return 0, nil
		}
		return 50, nil
	})

	base := day(2020, time.January, 1)
	env.ExecuteWorkflow(backfillWorkflow, params([]string{"weth"}, base, daysAfter(base, 75)))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("unexpected workflow error: %v", err)
	}
}

func TestBackfillWorkflow_RejectsInvalidParams(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	seen := registerChunkActivity(env, func(chunkWindow) (int, error) { return 100, nil })

	env.ExecuteWorkflow(backfillWorkflow, params(nil, day(2020, time.January, 1), day(2020, time.February, 1)))

	if err := env.GetWorkflowError(); err == nil {
		t.Fatal("expected an error for parameters that fail validation")
	}
	if len(*seen) != 0 {
		t.Errorf("activity ran %d times for invalid params, want 0", len(*seen))
	}
}

func TestBackfillWorkflow_PropagatesChunkFailure(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	registerChunkActivity(env, func(chunkWindow) (int, error) {
		return 0, errors.New("coingecko unreachable")
	})

	env.ExecuteWorkflow(backfillWorkflow, params(
		[]string{"weth"}, day(2020, time.January, 1), day(2020, time.February, 1)))

	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("expected the workflow to fail when a chunk activity fails")
	}
	if !strings.Contains(err.Error(), "weth") {
		t.Errorf("error should name the failing asset, got: %v", err)
	}
}

// Progress must be queryable mid-run: it is the only way to see how far a long
// backfill has got from the UI without reading raw event history.
func TestBackfillWorkflow_ExposesProgressQuery(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	registerChunkActivity(env, func(chunkWindow) (int, error) { return 10, nil })

	base := day(2020, time.January, 1)
	env.ExecuteWorkflow(backfillWorkflow, params([]string{"weth"}, base, daysAfter(base, 75)))

	encoded, err := env.QueryWorkflow(progressQueryName)
	if err != nil {
		t.Fatalf("querying %q: %v", progressQueryName, err)
	}

	var got backfillProgress
	if err := encoded.Get(&got); err != nil {
		t.Fatalf("decoding progress: %v", err)
	}
	if got.ChunksTotal != 3 {
		t.Errorf("ChunksTotal = %d, want 3", got.ChunksTotal)
	}
	if got.ChunksDone != 3 {
		t.Errorf("ChunksDone = %d, want 3", got.ChunksDone)
	}
}

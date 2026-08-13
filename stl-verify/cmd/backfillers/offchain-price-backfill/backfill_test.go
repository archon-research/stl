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

// Consecutive windows must be adjacent but not overlapping. CoinGecko's range is
// inclusive at both ends, so a window starting exactly where the last ended
// double-counts the seam hour; starting a second later cannot skip an hourly
// point, because no point falls inside that second.
func TestChunkWindows_WindowsAbutWithoutGapOrOverlap(t *testing.T) {
	got := chunkWindows(params([]string{"weth"}, day(2020, time.January, 1), day(2020, time.June, 1)))

	for i := 1; i < len(got); i++ {
		gap := got[i].From.Sub(got[i-1].To)
		if gap != time.Second {
			t.Errorf("window %d starts %s after the previous window ended, want exactly 1s "+
				"(0 would re-fetch the seam hour, more could skip a point)", i, gap)
		}
	}
}

func TestBackfillParams_Validate(t *testing.T) {
	from := day(2020, time.January, 1)
	to := day(2020, time.February, 1)
	now := day(2026, time.August, 5)

	// chunksWide builds a request expanding to exactly n chunks, so the ceiling
	// cases stay pinned to maxChunksPerRun rather than to a hand-computed date.
	// Anchored backwards from now, because n chunks at the ceiling span ~164
	// years — measured forwards it would trip the future-range guard instead.
	chunksWide := func(n int) BackfillParams {
		// Each window past the first starts a second late, so the range has to
		// carry those seconds to still divide into exactly n chunks.
		width := time.Duration(n)*offchain_price_fetcher.HistoricalChunkWidth + time.Duration(n-1)*time.Second
		return params([]string{"weth"}, now.Add(-width), now)
	}

	tests := []struct {
		name            string
		in              BackfillParams
		wantErrContains string
	}{
		{name: "valid request", in: params([]string{"weth"}, from, to)},
		{name: "no assets", in: params(nil, from, to), wantErrContains: "at least one CoinGecko ID"},
		{name: "empty asset ID", in: params([]string{""}, from, to), wantErrContains: "must not contain an empty ID"},
		{name: "missing from", in: params([]string{"weth"}, time.Time{}, to), wantErrContains: "both required"},
		{name: "missing to", in: params([]string{"weth"}, from, time.Time{}), wantErrContains: "both required"},
		{name: "from after to", in: params([]string{"weth"}, to, from), wantErrContains: "must be before"},
		{name: "from equals to", in: params([]string{"weth"}, from, from), wantErrContains: "must be before"},
		// A repeated ID would be fetched twice and double-counted into one
		// coverage entry, so the run would report points it never separately saw.
		{name: "duplicate asset IDs", in: params([]string{"weth", "weth"}, from, to), wantErrContains: "duplicate ID"},
		// A dropped digit in the year: the guard exists so this is rejected up
		// front rather than terminated mid-flight for outgrowing the history limit.
		{name: "mistyped year expands past the chunk ceiling", in: params([]string{"weth"}, day(20, time.January, 1), to), wantErrContains: "over the 2000 limit"},
		// A future range is served as 200-with-empty-arrays, indistinguishable
		// from a genuine hole, so it must not reach the coverage check.
		{name: "to in the future", in: params([]string{"weth"}, from, now.AddDate(0, 0, 1)), wantErrContains: "in the future"},
		// Both sides of the ceiling, so the boundary itself is pinned: without
		// these, flipping `>` to `>=` passes the suite.
		{name: "exactly at the chunk ceiling is accepted", in: chunksWide(maxChunksPerRun)},
		{name: "one chunk past the ceiling is rejected", in: chunksWide(maxChunksPerRun + 1), wantErrContains: "over the 2000 limit"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.in.validate(now)

			if tc.wantErrContains == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected a validation error containing %q", tc.wantErrContains)
			}
			// Matching the message, not just non-nil: eight rejection rows span
			// distinct guards, and a bare wantErr lets a row pass on the wrong one.
			if !strings.Contains(err.Error(), tc.wantErrContains) {
				t.Errorf("error = %q, want it to contain %q", err, tc.wantErrContains)
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
		c := got.Coverage[asset]
		if c.Points != 300 {
			t.Errorf("Coverage[%s].Points = %d, want 300", asset, c.Points)
		}
		if c.EmptyLeading != 0 || c.EmptyAfterData != 0 {
			t.Errorf("Coverage[%s] reported empty windows on a fully-covered run: %+v", asset, c)
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

// A LEADING run of empty windows is genuinely ambiguous — an asset listed
// part-way through the range has none earlier, and so does a range reaching past
// the provider's historical entitlement — so it succeeds. But it must not be
// reported as full coverage: the result has to say where data actually starts.
func TestBackfillWorkflow_SucceedsButReportsTruncationWhenLeadingWindowsAreEmpty(t *testing.T) {
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

	var got BackfillResult
	if err := env.GetWorkflowResult(&got); err != nil {
		t.Fatalf("reading workflow result: %v", err)
	}

	c := got.Coverage["weth"]
	if c.EmptyLeading != 1 {
		t.Errorf("EmptyLeading = %d, want 1 — a truncated range must be visible in the result", c.EmptyLeading)
	}
	// The second window, which is where data first appeared. It opens one second
	// past the first window's close — see chunkWindows on the inclusive seam.
	wantCoveredFrom := daysAfter(base, 30).Add(time.Second)
	if c.CoveredFrom == nil || !c.CoveredFrom.Equal(wantCoveredFrom) {
		t.Errorf("CoveredFrom = %v, want %v so the operator can see the range was truncated",
			c.CoveredFrom, wantCoveredFrom)
	}
}

// An empty window AFTER data has begun cannot be a coverage boundary — an asset
// does not un-list itself — so it is a real hole and must fail. Without this, an
// out-of-entitlement or partially-served range reports a clean success while
// silently missing years.
func TestBackfillWorkflow_FailsOnEmptyWindowAfterDataBegan(t *testing.T) {
	tests := []struct {
		name       string
		emptyCalls map[int]bool
	}{
		{name: "interior hole", emptyCalls: map[int]bool{2: true}},
		{name: "trailing hole", emptyCalls: map[int]bool{3: true}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
			call := 0
			registerChunkActivity(env, func(chunkWindow) (int, error) {
				call++
				if tc.emptyCalls[call] {
					return 0, nil
				}
				return 50, nil
			})

			base := day(2020, time.January, 1)
			env.ExecuteWorkflow(backfillWorkflow, params([]string{"weth"}, base, daysAfter(base, 75)))

			err := env.GetWorkflowError()
			if err == nil {
				t.Fatal("expected the workflow to fail on a gap after data began")
			}
			if !strings.Contains(err.Error(), "after data began") {
				t.Errorf("error should name the gap, got: %v", err)
			}
		})
	}
}

// A failing run must still expose the counts an operator needs to decide what to
// re-run.
//
// Asserted through the progress query, not the workflow result: Temporal
// discards the result payload of a workflow that returns a non-nil error, so the
// query is the only channel that survives a failure. Reading the result here
// would assert nothing.
func TestBackfillWorkflow_ExposesPartialCountsAfterFailure(t *testing.T) {
	base := day(2020, time.January, 1)
	// Keyed on the window rather than a call counter: the activity is retried, so
	// a counter-based stub would fail a different window on each attempt.
	failFromThirdWindow := func(w chunkWindow) (int, error) {
		if !w.From.Before(daysAfter(base, 60)) {
			return 0, errors.New("coingecko unreachable")
		}
		return 100, nil
	}

	tests := []struct {
		name           string
		in             BackfillParams
		chunk          func(chunkWindow) (int, error)
		wantChunksDone int
		wantPoints     map[string]int
	}{
		{
			name: "the coverage check rejects an asset that returned nothing",
			in:   params([]string{"weth", "not-a-coin"}, base, daysAfter(base, 30)),
			chunk: func(w chunkWindow) (int, error) {
				if w.Asset == "not-a-coin" {
					return 0, nil
				}
				return 100, nil
			},
			wantChunksDone: 2,
			wantPoints:     map[string]int{"weth": 100, "not-a-coin": 0},
		},
		{
			name:           "a chunk fails part-way through the range",
			in:             params([]string{"weth"}, base, daysAfter(base, 90)),
			chunk:          failFromThirdWindow,
			wantChunksDone: 2,
			wantPoints:     map[string]int{"weth": 200},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
			registerChunkActivity(env, tc.chunk)

			env.ExecuteWorkflow(backfillWorkflow, tc.in)

			if env.GetWorkflowError() == nil {
				t.Fatal("expected the run to fail")
			}

			encoded, err := env.QueryWorkflow(progressQueryName)
			if err != nil {
				t.Fatalf("querying %q after a failed run: %v", progressQueryName, err)
			}
			var got backfillProgress
			if err := encoded.Get(&got); err != nil {
				t.Fatalf("decoding progress: %v", err)
			}

			if got.ChunksDone != tc.wantChunksDone {
				t.Errorf("ChunksDone = %d, want %d — chunks that completed before the failure must stay visible",
					got.ChunksDone, tc.wantChunksDone)
			}
			for asset, want := range tc.wantPoints {
				if p := got.Coverage[asset].Points; p != want {
					t.Errorf("Coverage[%s].Points = %d, want %d", asset, p, want)
				}
			}
		})
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

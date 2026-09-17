package position_materializer

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

func newRecordingTelemetry(t *testing.T) (*Telemetry, sdkmetric.Reader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	tel, err := NewTelemetryWithProvider(mp, nil)
	if err != nil {
		t.Fatalf("NewTelemetryWithProvider() error: %v", err)
	}
	return tel, reader
}

// A run that appends nothing must still produce the rows-changed series.
// VectorPositionMaterializerSilentlyEmpty reads
// increase(position_materializer_rows_changed_total)[6h] == 0 alongside a
// non-zero run count, so a series that only exists once rows are appended
// leaves the alert with an empty vector in exactly the case it is for.
func TestRecordRun_RowsChangedSeriesExistsWhenNothingWasAppended(t *testing.T) {
	tel, reader := newRecordingTelemetry(t)

	tel.RecordRun(context.Background(), "materialize_morpho_market", "ok", 0)

	byView := testutil.CollectCounterByAttr(t, reader, "position_materializer.rows_changed.total", "materializer")
	got, ok := byView["materialize_morpho_market"]
	if !ok {
		t.Fatalf("no rows_changed series for a run that appended 0 rows; series present: %v", byView)
	}
	if got != 0 {
		t.Errorf("rows_changed = %d, want 0", got)
	}
}

// A projection that reports success while withholding positions is the failure this gauge exists
// for, so the level has to reach the metric on every pass, not only when something errors.
func TestRunOnce_PublishesTheWithheldLevel(t *testing.T) {
	tel, reader := newRecordingTelemetry(t)
	mm := &mockMaterializer{
		fn:      func(context.Context, string, int, int64) (int64, error) { return 0, nil },
		refused: map[string]int64{"public.position_sky_prime_debt": 3, "public.position_morpho_vault": 0},
	}
	s, err := NewService([]string{"materialize_sky_prime_debt"}, mm, 0, 77, nil, tel)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if err := s.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	byProjection := testutil.CollectGaugeByAttr(t, reader, "position_materializer.positions_refused", "projection")
	got, ok := byProjection["public.position_sky_prime_debt"]
	if !ok {
		t.Fatalf("no withheld level published; series present: %v", byProjection)
	}
	if got != 3 {
		t.Errorf("withheld level = %d, want 3", got)
	}
	if _, ok := byProjection["public.position_morpho_vault"]; !ok {
		t.Error("a healthy projection publishes no zero, so the alert cannot tell healthy from absent")
	}
}

// The caches are plain tables, and db/migrations/AGENTS.md makes a row-growth tripwire the price of
// that. They are written by triggers, so the level published here is the only signal the alert has.
func TestRunOnce_PublishesEachCacheTableSize(t *testing.T) {
	tel, reader := newRecordingTelemetry(t)
	mm := &mockMaterializer{
		fn:        func(context.Context, string, int, int64) (int64, error) { return 0, nil },
		cacheRows: map[string]int64{"position_current": 1234, "some_other_cache": 0},
	}
	s, err := NewService([]string{"materialize_sky_prime_debt"}, mm, 0, 77, nil, tel)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if err := s.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	byTable := testutil.CollectGaugeByAttr(t, reader, "position_materializer.cache_rows", "table")
	got, ok := byTable["position_current"]
	if !ok {
		t.Fatalf("no size published for position_current; series present: %v", byTable)
	}
	if got != 1234 {
		t.Errorf("position_current cache_rows = %d, want 1234", got)
	}
	// An empty cache still reports: the alert compares a level, so a table that has not grown must
	// read as zero rather than as an absent series indistinguishable from a runner that stopped.
	if _, ok := byTable["some_other_cache"]; !ok {
		t.Error("an empty cache published no series, so the alert cannot tell empty from absent")
	}
}

// The projections committed their rows before this read runs, so a failure here must not turn a
// successful run into a failed one -- the next run republishes the level.
func TestRunOnce_CacheSizeReadFailureDoesNotFailTheRun(t *testing.T) {
	tel, reader := newRecordingTelemetry(t)
	mm := &mockMaterializer{
		fn:       func(context.Context, string, int, int64) (int64, error) { return 5, nil },
		cacheErr: errors.New("relation does not exist"),
	}
	s, err := NewService([]string{"materialize_sky_prime_debt"}, mm, 0, 77, nil, tel)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if err := s.RunOnce(context.Background()); err != nil {
		t.Errorf("RunOnce returned %v; a failed cache-size read must not fail a run whose rows are committed", err)
	}
	// And the run's own work is still reported, so the failure is contained to this one reading.
	byView := testutil.CollectCounterByAttr(t, reader, "position_materializer.rows_changed.total", "materializer")
	if got := byView["materialize_sky_prime_debt"]; got != 5 {
		t.Errorf("rows_changed = %d, want 5 -- the run's own metrics must survive the failed read", got)
	}
}

// A nil Telemetry is the documented no-op, and the service passes nil when no meter is wired.
func TestRecordCacheRows_NilTelemetryIsANoOp(t *testing.T) {
	var tel *Telemetry
	tel.RecordCacheRows(context.Background(), "position_current", 42) // must not panic
}

// Unseeded, a counter first appears at its first value and increase() misses the 0->1 after a pod
// start, so ViewFailing missed a first error and SilentlyEmpty false-fired after a restart.
func TestNewTelemetry_SeedsEveryConfiguredMaterializer(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	if _, err := NewTelemetryWithProvider(mp, []string{"materialize_a", "materialize_b"}); err != nil {
		t.Fatalf("NewTelemetryWithProvider: %v", err)
	}

	runs := map[string]int64{}
	for _, dp := range testutil.CollectSumDataPoints(t, reader, "position_materializer.projection_runs.total") {
		runs[testutil.AttrValue(dp, "materializer")+"/"+testutil.AttrValue(dp, "status")] = dp.Value
	}
	for _, key := range []string{"materialize_a/ok", "materialize_a/error", "materialize_a/canceled", "materialize_b/ok", "materialize_b/error", "materialize_b/canceled"} {
		if v, ok := runs[key]; !ok || v != 0 {
			t.Errorf("projection_runs %s = %d (present %v); want a seeded 0", key, v, ok)
		}
	}
	rows := testutil.CollectCounterByAttr(t, reader, "position_materializer.rows_changed.total", "materializer")
	for _, m := range []string{"materialize_a", "materialize_b"} {
		if v, ok := rows[m]; !ok || v != 0 {
			t.Errorf("rows_changed %s = %d (present %v); want a seeded 0", m, v, ok)
		}
	}
}

// A run interrupted by a shutdown is not a broken view. Recorded as an error, every deploy that
// landed mid-run fired VectorPositionMaterializerViewFailing for a view that had nothing wrong.
// Only the parent's cancellation is canceled: a deadline, the parent's or the projection's own, is an
// error.
func TestRunOnce_ClassifiesAFailedProjection(t *testing.T) {
	boom := errors.New("relation does not exist")
	tests := []struct {
		name       string
		parent     func() (context.Context, context.CancelFunc)
		fail       func(ctx context.Context, cancel context.CancelFunc) error
		wantStatus string
	}{
		{
			name:   "parent cancelled mid-projection",
			parent: func() (context.Context, context.CancelFunc) { return context.WithCancel(context.Background()) },
			fail: func(ctx context.Context, cancel context.CancelFunc) error {
				cancel()
				return ctx.Err()
			},
			wantStatus: statusCanceled,
		},
		{
			name:   "unrelated error while the parent is cancelled",
			parent: func() (context.Context, context.CancelFunc) { return context.WithCancel(context.Background()) },
			fail: func(_ context.Context, cancel context.CancelFunc) error {
				cancel()
				return boom
			},
			wantStatus: statusCanceled,
		},
		{
			name:       "projection's own deadline on a live parent",
			parent:     func() (context.Context, context.CancelFunc) { return context.WithCancel(context.Background()) },
			fail:       func(context.Context, context.CancelFunc) error { return context.DeadlineExceeded },
			wantStatus: statusError,
		},
		{
			name: "parent deadline expires mid-projection",
			parent: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 10*time.Millisecond)
			},
			fail: func(ctx context.Context, _ context.CancelFunc) error {
				<-ctx.Done()
				return ctx.Err()
			},
			wantStatus: statusError,
		},
		{
			name:       "plain failure",
			parent:     func() (context.Context, context.CancelFunc) { return context.WithCancel(context.Background()) },
			fail:       func(context.Context, context.CancelFunc) error { return boom },
			wantStatus: statusError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tel, reader := newRecordingTelemetry(t)
			ctx, cancel := tt.parent()
			defer cancel()
			mat := &mockMaterializer{fn: func(ctx context.Context, view string, _ int, _ int64) (int64, error) {
				if view == "materialize_a" {
					return 0, tt.fail(ctx, cancel)
				}
				return 1, nil
			}}
			svc, err := NewService([]string{"materialize_a", "materialize_b"}, mat, 0, 77, nil, tel)
			if err != nil {
				t.Fatalf("NewService: %v", err)
			}
			if err := svc.RunOnce(ctx); err == nil {
				t.Fatal("RunOnce = nil; want the failure surfaced")
			}

			runs := map[string]int64{}
			for _, dp := range testutil.CollectSumDataPoints(t, reader, "position_materializer.projection_runs.total") {
				runs[testutil.AttrValue(dp, "materializer")+"/"+testutil.AttrValue(dp, "status")] += dp.Value
			}
			for _, status := range runStatuses {
				want := int64(0)
				if status == tt.wantStatus {
					want = 1
				}
				if got := runs["materialize_a/"+status]; got != want {
					t.Errorf("materialize_a/%s = %d; want %d (all runs: %v)", status, got, want, runs)
				}
			}
			// A view the cancellation skipped never ran, so it records nothing.
			if tt.wantStatus == statusCanceled {
				for key, v := range runs {
					if strings.HasPrefix(key, "materialize_b/") && v != 0 {
						t.Errorf("%s = %d; want nothing recorded for a view that never ran", key, v)
					}
				}
			}
		})
	}
}

// Every status RunOnce records must be one the seed exports, or its series first appears at 1 and
// increase() misses it. Collected from real runs, not from runStatuses, so the two cannot drift.
func TestRunOnce_RecordsOnlySeededStatuses(t *testing.T) {
	tel, reader := newRecordingTelemetry(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mat := &mockMaterializer{fn: func(ctx context.Context, view string, _ int, _ int64) (int64, error) {
		switch view {
		case "materialize_ok":
			return 1, nil
		case "materialize_error":
			return 0, errors.New("boom")
		default:
			cancel()
			return 0, ctx.Err()
		}
	}}
	svc, err := NewService([]string{"materialize_ok", "materialize_error", "materialize_cancel"}, mat, 0, 77, nil, tel)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	_ = svc.RunOnce(ctx) // the joined failures are the point; the statuses are what is checked

	seeded := map[string]bool{}
	for _, s := range runStatuses {
		seeded[s] = true
	}
	recorded := testutil.CollectCounterByAttr(t, reader, "position_materializer.projection_runs.total", "status")
	if len(recorded) != len(runStatuses) {
		t.Errorf("RunOnce recorded statuses %v; want one run in each of %v", recorded, runStatuses)
	}
	for status := range recorded {
		if !seeded[status] {
			t.Errorf("RunOnce recorded status %q, which NewTelemetry does not seed", status)
		}
	}
}

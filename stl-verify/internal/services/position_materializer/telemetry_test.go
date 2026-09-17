package position_materializer

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
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
func TestTelemetry_NilSettersAreNoOps(t *testing.T) {
	var tel *Telemetry
	tel.SetCacheRows(map[string]int64{"position_current": 42}) // must not panic
	tel.SetRefused(map[string]int64{"p": 1})
	tel.RecordReadFailure(context.Background(), readRefused)
}

// A gauge exports only what the latest tick set. With a synchronous gauge the last level stood for
// the life of the pod, so a projection that stopped completing, or a read that kept failing, still
// exported its old withheld level and fired VectorPositionMaterializerWithholdingPositions on it.
func TestRunOnce_GaugesGoAbsentWhenTheLatestTickDidNotReportThem(t *testing.T) {
	tel, reader := newRecordingTelemetry(t)
	mm := &mockMaterializer{
		fn:        func(context.Context, string, int, int64) (int64, error) { return 0, nil },
		refused:   map[string]int64{"public.position_a": 12, "public.position_b": 0},
		cacheRows: map[string]int64{"position_current": 7},
	}
	s, err := NewService([]string{"materialize_a"}, mm, 0, 77, nil, tel)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	tick := func() {
		if err := s.RunOnce(context.Background()); err != nil {
			t.Fatalf("RunOnce: %v", err)
		}
	}
	tick()
	if got := gaugeLevels(t, reader, "position_materializer.positions_refused", "projection"); len(got) != 2 || got["public.position_a"] != 12 {
		t.Fatalf("first tick levels = %v; want position_a 12 and position_b 0", got)
	}

	mm.refused = map[string]int64{"public.position_b": 0} // position_a did not complete this tick
	tick()
	if got := gaugeLevels(t, reader, "position_materializer.positions_refused", "projection"); len(got) != 1 {
		t.Errorf("levels after position_a stopped completing = %v; want only position_b", got)
	}

	mm.refusedErr, mm.cacheErr = errors.New("permission denied"), errors.New("permission denied")
	tick()
	for name, key := range map[string]string{"position_materializer.positions_refused": "projection", "position_materializer.cache_rows": "table"} {
		if got := gaugeLevels(t, reader, name, key); len(got) != 0 {
			t.Errorf("%s after a failed read = %v; want absent", name, got)
		}
	}
	failures := testutil.CollectCounterByAttr(t, reader, "position_materializer.read_failures.total", "read")
	if failures[readRefused] != 1 || failures[readCacheRows] != 1 {
		t.Errorf("read_failures = %v; want one of each read", failures)
	}
}

// gaugeLevels returns the int64 gauge levels of name by key, empty when the SDK exported no series.
func gaugeLevels(t *testing.T, reader sdkmetric.Reader, name, key string) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collecting metrics: %v", err)
	}
	out := map[string]int64{}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			g, ok := m.Data.(metricdata.Gauge[int64])
			if !ok {
				t.Fatalf("metric %q is %T, want metricdata.Gauge[int64]", name, m.Data)
			}
			for _, dp := range g.DataPoints {
				out[testutil.AttrValue(dp, key)] = dp.Value
			}
		}
	}
	return out
}

// Recording must land on the seeded series. testutil.CollectCounterByAttr sums data points, so a
// recording on a parallel attribute set reads back the same total; the data-point count does not.
func TestRecordRun_WritesTheSeededSeries(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	tel, err := NewTelemetryWithProvider(mp, []string{"materialize_a"})
	if err != nil {
		t.Fatalf("NewTelemetryWithProvider: %v", err)
	}
	for _, status := range runStatuses {
		tel.RecordRun(context.Background(), "materialize_a", status, 5)
	}
	tel.RecordReadFailure(context.Background(), readRefused)
	for name, want := range map[string]int{
		"position_materializer.projection_runs.total": len(runStatuses),
		"position_materializer.rows_changed.total":    1,
		"position_materializer.read_failures.total":   len(reads),
	} {
		if got := len(testutil.CollectSumDataPoints(t, reader, name)); got != want {
			t.Errorf("%s has %d series after recording; want the %d seeded ones", name, got, want)
		}
	}
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
	failures := testutil.CollectCounterByAttr(t, reader, "position_materializer.read_failures.total", "read")
	for _, r := range reads {
		if v, ok := failures[r]; !ok || v != 0 {
			t.Errorf("read_failures %s = %d (present %v); want a seeded 0", r, v, ok)
		}
	}
}

// A view skipped because the parent ended must still be recorded, or a view starved by a long one
// before it emits nothing and every alert stays silent. Each skipped view is named, not only the first.
func TestRunOnce_RecordsEveryViewSkippedByCancellation(t *testing.T) {
	tel, reader := newRecordingTelemetry(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mat := &mockMaterializer{fn: func(context.Context, string, int, int64) (int64, error) {
		cancel()
		return 1, nil
	}}
	svc, err := NewService([]string{"materialize_a", "materialize_b", "materialize_c"}, mat, 0, 77, nil, tel)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if err := svc.RunOnce(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("RunOnce = %v; want context.Canceled", err)
	}
	if got := strings.Join(mat.calls, ","); got != "materialize_a" {
		t.Errorf("calls = %s; want only materialize_a", got)
	}
	runs := runsByViewAndStatus(t, reader)
	assertOneRunIn(t, runs, "materialize_a", statusOK)
	assertOneRunIn(t, runs, "materialize_b", statusCanceled)
	assertOneRunIn(t, runs, "materialize_c", statusCanceled)
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
		wantB      string // the projection after it: skipped under an ended parent, run otherwise
	}{
		{"parent cancelled mid-projection", cancellableParent, cancelThenReturnCtxErr, statusCanceled, statusCanceled},
		{"unrelated error while the parent is cancelled", cancellableParent, cancelThenReturn(boom), statusCanceled, statusCanceled},
		{"projection's own deadline on a live parent", cancellableParent, returnErr(context.DeadlineExceeded), statusError, statusOK},
		{"parent deadline expires mid-projection", shortDeadlineParent, waitForParent, statusError, statusError},
		{"plain failure", cancellableParent, returnErr(boom), statusError, statusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tel, reader := newRecordingTelemetry(t)
			ctx, cancel := tt.parent()
			defer cancel()
			mat := &mockMaterializer{fn: failFirstView(tt.fail, cancel)}
			svc, err := NewService([]string{"materialize_a", "materialize_b"}, mat, 0, 77, nil, tel)
			if err != nil {
				t.Fatalf("NewService: %v", err)
			}
			if err := svc.RunOnce(ctx); err == nil {
				t.Fatal("RunOnce = nil; want the failure surfaced")
			}
			runs := runsByViewAndStatus(t, reader)
			assertOneRunIn(t, runs, "materialize_a", tt.wantStatus)
			assertOneRunIn(t, runs, "materialize_b", tt.wantB)
		})
	}
}

func cancellableParent() (context.Context, context.CancelFunc) {
	return context.WithCancel(context.Background())
}

func shortDeadlineParent() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 10*time.Millisecond)
}

func cancelThenReturnCtxErr(ctx context.Context, cancel context.CancelFunc) error {
	cancel()
	return ctx.Err()
}

func cancelThenReturn(err error) func(context.Context, context.CancelFunc) error {
	return func(_ context.Context, cancel context.CancelFunc) error {
		cancel()
		return err
	}
}

func returnErr(err error) func(context.Context, context.CancelFunc) error {
	return func(context.Context, context.CancelFunc) error { return err }
}

func waitForParent(ctx context.Context, _ context.CancelFunc) error {
	<-ctx.Done()
	return ctx.Err()
}

// failFirstView fails materialize_a with fail and lets every other view succeed.
func failFirstView(fail func(context.Context, context.CancelFunc) error, cancel context.CancelFunc) func(context.Context, string, int, int64) (int64, error) {
	return func(ctx context.Context, view string, _ int, _ int64) (int64, error) {
		if view == "materialize_a" {
			return 0, fail(ctx, cancel)
		}
		return 1, nil
	}
}

func runsByViewAndStatus(t *testing.T, reader sdkmetric.Reader) map[string]int64 {
	t.Helper()
	runs := map[string]int64{}
	for _, dp := range testutil.CollectSumDataPoints(t, reader, "position_materializer.projection_runs.total") {
		runs[testutil.AttrValue(dp, "materializer")+"/"+testutil.AttrValue(dp, "status")] += dp.Value
	}
	return runs
}

// assertOneRunIn checks view recorded exactly one run, in wantStatus, and none in any other status.
func assertOneRunIn(t *testing.T, runs map[string]int64, view, wantStatus string) {
	t.Helper()
	for _, status := range runStatuses {
		want := int64(0)
		if status == wantStatus {
			want = 1
		}
		if got := runs[view+"/"+status]; got != want {
			t.Errorf("%s/%s = %d; want %d (all runs: %v)", view, status, got, want, runs)
		}
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

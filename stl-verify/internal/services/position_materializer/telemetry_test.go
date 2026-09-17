package position_materializer

import (
	"context"
	"errors"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

func newRecordingTelemetry(t *testing.T) (*Telemetry, sdkmetric.Reader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	tel, err := NewTelemetryWithProvider(mp)
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

// An unseeded counter first appears at its first value, so increase() misses the 0->1 after every
// pod start: ViewFailing could not fire on a process's first error, and SilentlyEmpty fired for 6h
// after a restart whose first run appended rows. Every configured materializer starts at zero.
func TestNewTelemetry_SeedsEveryConfiguredMaterializer(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	if _, err := NewTelemetryWithProvider(mp, "materialize_a", "materialize_b"); err != nil {
		t.Fatalf("NewTelemetryWithProvider: %v", err)
	}

	runs := map[string]int64{}
	for _, dp := range testutil.CollectSumDataPoints(t, reader, "position_materializer.projection_runs.total") {
		runs[testutil.AttrValue(dp, "materializer")+"/"+testutil.AttrValue(dp, "status")] = dp.Value
	}
	for _, key := range []string{"materialize_a/ok", "materialize_a/error", "materialize_b/ok", "materialize_b/error"} {
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

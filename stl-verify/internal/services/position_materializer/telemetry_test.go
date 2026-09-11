package position_materializer

import (
	"context"
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

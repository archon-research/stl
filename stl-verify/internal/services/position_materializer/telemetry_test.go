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

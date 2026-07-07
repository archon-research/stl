package temporal

import (
	"context"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// Guards the startup seed (telemetry.SeedStatusCounter in
// newCronjobMetricsWithProvider): both terminal-status series must exist at 0
// before the first run, so increase() can see the first real increment.
func TestNewCronjobMetrics_SeedsBothStatusSeriesAtZero(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	if _, err := newCronjobMetricsWithProvider(mp); err != nil {
		t.Fatalf("newCronjobMetricsWithProvider() error: %v", err)
	}

	got := testutil.CollectCounterByAttr(t, reader, "cronjob.runs.total", "status")
	for _, status := range []string{"success", "error"} {
		v, ok := got[status]
		if !ok {
			t.Errorf("cronjob.runs.total is missing the status=%q series before any run", status)
			continue
		}
		if v != 0 {
			t.Errorf("cronjob.runs.total{status=%q} = %d, want 0", status, v)
		}
	}
}

// After seeding, the first successful run must land on the seeded success
// series as 0->1 (not orphan it into a parallel series), which is the increment
// increase() needs to see. This holds only while RecordRun and the startup seed
// (telemetry.SeedStatusCounter) derive the status label the same way
// (telemetry.StatusAttr); this guards that.
func TestRecordRun_LandsSuccessOnSeededSeriesAsOne(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	m, err := newCronjobMetricsWithProvider(mp)
	if err != nil {
		t.Fatalf("newCronjobMetricsWithProvider() error: %v", err)
	}

	m.RecordRun(context.Background(), time.Second, nil)

	got := testutil.CollectCounterByAttr(t, reader, "cronjob.runs.total", "status")
	if got["success"] != 1 {
		t.Errorf("cronjob.runs.total{status=\"success\"} = %d, want 1", got["success"])
	}
	if got["error"] != 0 {
		t.Errorf("cronjob.runs.total{status=\"error\"} = %d, want 0", got["error"])
	}
}

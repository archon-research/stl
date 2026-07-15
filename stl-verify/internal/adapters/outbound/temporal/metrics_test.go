package temporal

import (
	"context"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// Guards the startup seed in seedStatusSeries: both terminal-status series must
// exist at 0 before the first run, so increase() can see the first real
// increment. See seedStatusSeries for the full rationale.
func TestNewCronjobMetrics_SeedsBothStatusSeriesAtZero(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	if _, err := newCronjobMetricsWithProvider(mp); err != nil {
		t.Fatalf("newCronjobMetricsWithProvider() error: %v", err)
	}

	got := collectCounterByStatus(t, reader, "cronjob.runs.total")
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
// increase() needs to see. This holds only while RecordRun and seedStatusSeries
// derive the status label the same way (telemetry.StatusAttr); this guards that.
func TestRecordRun_LandsSuccessOnSeededSeriesAsOne(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	m, err := newCronjobMetricsWithProvider(mp)
	if err != nil {
		t.Fatalf("newCronjobMetricsWithProvider() error: %v", err)
	}

	m.RecordRun(context.Background(), time.Second, nil)

	got := collectCounterByStatus(t, reader, "cronjob.runs.total")
	if got["success"] != 1 {
		t.Errorf("cronjob.runs.total{status=\"success\"} = %d, want 1", got["success"])
	}
	if got["error"] != 0 {
		t.Errorf("cronjob.runs.total{status=\"error\"} = %d, want 0", got["error"])
	}
}

func collectCounterByStatus(t *testing.T, reader sdkmetric.Reader, name string) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collecting metrics: %v", err)
	}
	out := make(map[string]int64)
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric %q is %T, want metricdata.Sum[int64]", name, m.Data)
			}
			for _, dp := range sum.DataPoints {
				status, _ := dp.Attributes.Value("status")
				out[status.AsString()] = dp.Value
			}
		}
	}
	return out
}

package temporal

import (
	"context"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// A freshly-created cronjob worker must export both terminal-status series of
// cronjob.runs.total at 0 before it has run anything. Without this seed the
// {status="success"} series only appears on the first success, so Prometheus
// never observes the 0->1 transition and increase()/rate() report 0 successes
// for up to a full window after a pod (re)start -- which trips
// VectorCronjobAllRunsFailing spuriously on every rollover.
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

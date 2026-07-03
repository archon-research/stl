package telemetry

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func newSeedTestCounter(t *testing.T) (metric.Int64Counter, sdkmetric.Reader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	c, err := mp.Meter("seed-test").Int64Counter("test.counter")
	if err != nil {
		t.Fatalf("creating counter: %v", err)
	}
	return c, reader
}

func collectSeedDataPoints(t *testing.T, reader sdkmetric.Reader) []metricdata.DataPoint[int64] {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collecting metrics: %v", err)
	}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "test.counter" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric is %T, want metricdata.Sum[int64]", m.Data)
			}
			return sum.DataPoints
		}
	}
	return nil
}

func TestSeedCounter_ExportsZeroSeriesWithAttrs(t *testing.T) {
	c, reader := newSeedTestCounter(t)

	SeedCounter(context.Background(), c, attribute.String("chain", "base"))

	dps := collectSeedDataPoints(t, reader)
	if len(dps) != 1 {
		t.Fatalf("got %d data points, want 1", len(dps))
	}
	if dps[0].Value != 0 {
		t.Errorf("seeded value = %d, want 0", dps[0].Value)
	}
	if chain, _ := dps[0].Attributes.Value("chain"); chain.AsString() != "base" {
		t.Errorf("chain attr = %q, want \"base\"", chain.AsString())
	}
}

func TestSeedStatusCounter_ExportsBothStatusSeriesAtZeroWithBaseAttrs(t *testing.T) {
	c, reader := newSeedTestCounter(t)

	SeedStatusCounter(context.Background(), c, attribute.String("chain", "base"))

	got := map[string]int64{}
	for _, dp := range collectSeedDataPoints(t, reader) {
		status, _ := dp.Attributes.Value("status")
		if chain, _ := dp.Attributes.Value("chain"); chain.AsString() != "base" {
			t.Errorf("status=%q series: chain attr = %q, want \"base\"", status.AsString(), chain.AsString())
		}
		got[status.AsString()] = dp.Value
	}
	for _, status := range []string{"success", "error"} {
		v, ok := got[status]
		if !ok {
			t.Errorf("missing status=%q series", status)
			continue
		}
		if v != 0 {
			t.Errorf("status=%q = %d, want 0", status, v)
		}
	}
}

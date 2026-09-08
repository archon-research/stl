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
	t.Fatalf("metric %q not found; a nil return here would make every assertion below vacuous", "test.counter")
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

// The seeded series and the series a recorder writes must be ONE series. If the
// recorder attaches an attribute the seed did not, both exist: the seeded one
// stays flat at 0 while real counts accumulate on a second, unseeded series,
// and increase() is back to missing the first increment — the bug seeding
// exists to prevent, now wearing a healthy-looking series. Every collector in
// this repo sums across data points, so the value alone cannot see that; the
// data-point count is the load-bearing assertion.
func TestSeedCounter_RecordLandsOnTheSeededSeriesNotAParallelOne(t *testing.T) {
	c, reader := newSeedTestCounter(t)
	base := attribute.String("chain", "base")
	ctx := context.Background()

	SeedCounter(ctx, c, base)
	c.Add(ctx, 1, metric.WithAttributes(base))

	dps := collectSeedDataPoints(t, reader)
	if len(dps) != 1 {
		t.Fatalf("got %d data points, want 1 — the record orphaned the seeded series", len(dps))
	}
	if dps[0].Value != 1 {
		t.Errorf("value = %d, want 1 (0 seeded then 1 recorded)", dps[0].Value)
	}
}

// Same invariant for the status-labelled helper: a recorder deriving its status
// from StatusAttr must land on exactly the series SeedStatusCounter created,
// leaving two series and not four.
func TestSeedStatusCounter_RecordLandsOnTheSeededStatusSeries(t *testing.T) {
	c, reader := newSeedTestCounter(t)
	base := attribute.String("chain", "base")
	ctx := context.Background()

	SeedStatusCounter(ctx, c, base)
	c.Add(ctx, 1, metric.WithAttributes(base, StatusAttr(nil)))

	dps := collectSeedDataPoints(t, reader)
	if len(dps) != 2 {
		t.Fatalf("got %d data points, want 2 — the record orphaned a seeded status series", len(dps))
	}
	for _, dp := range dps {
		status, _ := dp.Attributes.Value("status")
		want := int64(0)
		if status.AsString() == SuccessStatusAttr().Value.AsString() {
			want = 1
		}
		if dp.Value != want {
			t.Errorf("status=%q = %d, want %d", status.AsString(), dp.Value, want)
		}
	}
}

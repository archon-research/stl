package testutil

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// CollectSumDataPoints collects the current state of reader and returns the
// int64 sum data points of the metric named name. It fails the test if the
// metric is absent or is not an int64 Sum.
func CollectSumDataPoints(t *testing.T, reader sdkmetric.Reader, name string) []metricdata.DataPoint[int64] {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collecting metrics: %v", err)
	}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric %q is %T, want metricdata.Sum[int64]", name, m.Data)
			}
			return sum.DataPoints
		}
	}
	t.Fatalf("metric %q not found", name)
	return nil
}

// AttrValue returns the string value of key on dp's attribute set, or "" if
// key is absent.
func AttrValue(dp metricdata.DataPoint[int64], key string) string {
	v, _ := dp.Attributes.Value(attribute.Key(key))
	return v.AsString()
}

// CollectCounterByAttr collects the named metric's int64 sum data points and
// groups their values by the string value of attrKey (e.g. "status"),
// summing values from data points that share the same attrKey value.
func CollectCounterByAttr(t *testing.T, reader sdkmetric.Reader, name, attrKey string) map[string]int64 {
	t.Helper()
	out := make(map[string]int64)
	for _, dp := range CollectSumDataPoints(t, reader, name) {
		out[AttrValue(dp, attrKey)] += dp.Value
	}
	return out
}

// Package metrictest provides shared helpers for asserting OpenTelemetry metric
// attributes in tests. It exists so the per-package telemetry tests do not each
// re-implement the same metricdata traversal when checking a label.
package metrictest

import (
	"testing"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// ChainValue returns the value of the "chain" attribute on the first data point
// of the named metric in rm, or ("", false) if the metric or the attribute is
// absent. It handles the int64 counter, float64 histogram, and float64 gauge
// shapes the vector pipeline metrics use.
func ChainValue(rm metricdata.ResourceMetrics, metricName string) (string, bool) {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != metricName {
				continue
			}
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					if v, ok := dp.Attributes.Value("chain"); ok {
						return v.AsString(), true
					}
				}
			case metricdata.Histogram[float64]:
				for _, dp := range data.DataPoints {
					if v, ok := dp.Attributes.Value("chain"); ok {
						return v.AsString(), true
					}
				}
			case metricdata.Gauge[float64]:
				for _, dp := range data.DataPoints {
					if v, ok := dp.Attributes.Value("chain"); ok {
						return v.AsString(), true
					}
				}
			}
		}
	}
	return "", false
}

// RequireChain asserts that every named metric in rm carries a "chain" attribute
// equal to want, reporting a test error per metric that is missing it or carries
// the wrong value.
func RequireChain(t *testing.T, rm metricdata.ResourceMetrics, want string, metricNames ...string) {
	t.Helper()
	for _, name := range metricNames {
		got, ok := ChainValue(rm, name)
		if !ok {
			t.Errorf("%s: missing chain attribute", name)
			continue
		}
		if got != want {
			t.Errorf("%s: chain = %q, want %q", name, got, want)
		}
	}
}

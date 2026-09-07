package testutil

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// InstallMeterProvider makes a manual-reader meter provider the global one for
// the test, so a binary's run() that builds instruments off the global provider
// records into a reader the test can inspect. The previous provider is
// restored on cleanup.
func InstallMeterProvider(t testing.TB) sdkmetric.Reader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(previous)
		if err := provider.Shutdown(context.Background()); err != nil {
			t.Errorf("shutting down the test meter provider: %v", err)
		}
	})
	return reader
}

// CounterValue sums the named int64 counter's points whose attributes include
// every entry of want. Attributes outside want are ignored, so a test asserts
// only the labels it cares about; an instrument never recorded sums to zero.
func CounterValue(t testing.TB, reader sdkmetric.Reader, name string, want map[string]string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collecting metrics: %v", err)
	}
	var total int64
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
				if hasAttributes(dp.Attributes, want) {
					total += dp.Value
				}
			}
		}
	}
	return total
}

func hasAttributes(set attribute.Set, want map[string]string) bool {
	for key, value := range want {
		got, ok := set.Value(attribute.Key(key))
		if !ok || got.AsString() != value {
			return false
		}
	}
	return true
}

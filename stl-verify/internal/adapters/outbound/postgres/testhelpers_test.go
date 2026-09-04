package postgres

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// collectSum reports false when the named instrument recorded nothing at all,
// which is a different outcome from a series sitting at 0.
func collectSum(t *testing.T, reader sdkmetric.Reader, name string) (metricdata.Sum[int64], bool) {
	t.Helper()

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s data = %T, want Sum[int64]", name, m.Data)
			}
			return sum, true
		}
	}
	return metricdata.Sum[int64]{}, false
}

func countsByAttr(t *testing.T, reader sdkmetric.Reader, name, attr string) map[string]int64 {
	t.Helper()

	counts := map[string]int64{}
	sum, ok := collectSum(t, reader, name)
	if !ok {
		return counts
	}
	for _, dp := range sum.DataPoints {
		value, ok := dp.Attributes.Value(attribute.Key(attr))
		if !ok {
			t.Fatalf("%s data point missing the %s attribute: %v", name, attr, dp.Attributes)
		}
		counts[value.AsString()] += dp.Value
	}
	return counts
}

func counterTotal(t *testing.T, reader sdkmetric.Reader, name string) int64 {
	t.Helper()

	sum, ok := collectSum(t, reader, name)
	if !ok {
		return 0
	}
	var total int64
	for _, dp := range sum.DataPoints {
		total += dp.Value
	}
	return total
}

func newTestTracer(t *testing.T) (*queryErrorTracer, sdkmetric.Reader) {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	tracer, err := newQueryErrorTracer(mp)
	if err != nil {
		t.Fatalf("newQueryErrorTracer: %v", err)
	}
	return tracer, reader
}

func pgErr(code string) error {
	return &pgconn.PgError{Code: code, Message: code}
}

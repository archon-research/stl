package allocation_tracker

import (
	"context"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// newRecordingTelemetry returns a Telemetry wired to an in-memory metric reader
// so tests can record calls and inspect the resulting metrics.
func newRecordingTelemetry(t *testing.T) (*Telemetry, sdkmetric.Reader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	tel, err := NewTelemetryWithProvider(mp, "mainnet")
	if err != nil {
		t.Fatalf("NewTelemetryWithProvider() error: %v", err)
	}
	return tel, reader
}

func collectMetric(t *testing.T, reader sdkmetric.Reader, name string) metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collecting metrics: %v", err)
	}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name == name {
				return m
			}
		}
	}
	t.Fatalf("metric %q not found", name)
	return metricdata.Metrics{}
}

func TestTelemetry_RecordUnderlyingValueFailure(t *testing.T) {
	tel, reader := newRecordingTelemetry(t)
	ctx := context.Background()
	addr := common.HexToAddress("0x38464507e02c983f20428a6e8566693fe9e422a9")

	tel.RecordUnderlyingValueFailure(ctx, "erc4626", addr, "convert_failed")

	m := collectMetric(t, reader, "allocation.underlying_value.failures.total")
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("allocation.underlying_value.failures.total is %T, want Sum[int64]", m.Data)
	}
	if len(sum.DataPoints) != 1 {
		t.Fatalf("got %d data points, want 1", len(sum.DataPoints))
	}

	dp := sum.DataPoints[0]
	if dp.Value != 1 {
		t.Errorf("datapoint value = %d, want 1", dp.Value)
	}

	// Verify all expected attributes are present with correct values.
	chain, ok := dp.Attributes.Value("chain")
	if !ok || chain.AsString() != "mainnet" {
		t.Errorf("missing or incorrect chain attribute: got %v", chain)
	}

	tokenType, ok := dp.Attributes.Value("token_type")
	if !ok || tokenType.AsString() != "erc4626" {
		t.Errorf("missing or incorrect token_type attribute: got %v", tokenType)
	}

	token, ok := dp.Attributes.Value("token")
	if !ok || token.AsString() != addr.Hex() {
		t.Errorf("missing or incorrect token attribute: got %v, want %s", token, addr.Hex())
	}

	reason, ok := dp.Attributes.Value("reason")
	if !ok || reason.AsString() != "convert_failed" {
		t.Errorf("missing or incorrect reason attribute: got %v", reason)
	}
}

func TestNewTelemetry(t *testing.T) {
	tel, err := NewTelemetry("mainnet")
	if err != nil {
		t.Fatalf("NewTelemetry() error: %v", err)
	}
	if tel == nil {
		t.Fatal("NewTelemetry() returned nil")
	}
}

func TestTelemetry_NilReceiverIsSafe(t *testing.T) {
	var tel *Telemetry
	tel.RecordUnderlyingValueFailure(context.Background(), "erc4626", common.Address{}, "convert_failed") // must not panic
}

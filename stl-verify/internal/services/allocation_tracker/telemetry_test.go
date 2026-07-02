package allocation_tracker

import (
	"context"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestTelemetry_RecordUnderlyingValueFailure(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	tel, err := NewTelemetryWithProvider(mp, "mainnet")
	if err != nil {
		t.Fatalf("NewTelemetryWithProvider: %v", err)
	}

	tel.RecordUnderlyingValueFailure(context.Background(), "erc4626",
		common.HexToAddress("0x38464507e02c983f20428a6e8566693fe9e422a9"), "convert_failed")

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	found := false
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "allocation.underlying_value.failures.total" {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("failures counter not recorded")
	}
}

func TestTelemetry_NilReceiverIsSafe(t *testing.T) {
	var tel *Telemetry
	tel.RecordUnderlyingValueFailure(context.Background(), "erc4626", common.Address{}, "convert_failed") // must not panic
}

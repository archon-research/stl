package uniswap_v3_dex

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	metricsdk "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/archon-research/stl/stl-verify/internal/pkg/dextelemetry"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// N7-4: every invocation of processBlockEvent must emit exactly one
// uniswap_v3.blocks.processed datapoint. Without this the
// VectorUniswapV3IndexerStalled alert in alerts/vector-indexers.yaml cannot
// fire and prod has no pager on a stalled worker.
func TestProcessBlockEvent_EmitsBlockProcessedMetric(t *testing.T) {
	reader := metricsdk.NewManualReader()
	mp := metricsdk.NewMeterProvider(metricsdk.WithReader(reader))
	otel.SetMeterProvider(mp)
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	tel, err := dextelemetry.NewTelemetry("uniswap_v3", 1)
	if err != nil {
		t.Fatalf("dextelemetry.NewTelemetry: %v", err)
	}
	h := newHarness(t)
	h.svc.telemetry = tel

	// Happy path: no receipts of interest = nil. Drive a valid event.
	body := []byte(`[]`)
	h.cache.SetReceipts(1, 100, 0, body)
	if err := h.svc.processBlockEvent(context.Background(), outbound.BlockEvent{
		ChainID:        1,
		BlockNumber:    100,
		Version:        0,
		BlockTimestamp: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Unix(),
	}); err != nil {
		t.Fatalf("happy path processBlockEvent: %v", err)
	}

	// Error path: receipt missing from cache.
	if err := h.svc.processBlockEvent(context.Background(), outbound.BlockEvent{
		ChainID:        1,
		BlockNumber:    9999,
		Version:        0,
		BlockTimestamp: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Unix(),
	}); err == nil {
		t.Fatal("expected processBlockEvent to fail when receipts not in cache")
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	success, errCount := readBlockCountersByStatus(t, &rm, "uniswap_v3.blocks.processed")
	if success != 1 {
		t.Errorf("uniswap_v3.blocks.processed{status=success} = %d, want 1", success)
	}
	if errCount != 1 {
		t.Errorf("uniswap_v3.blocks.processed{status=error} = %d, want 1", errCount)
	}
}

func readBlockCountersByStatus(t *testing.T, rm *metricdata.ResourceMetrics, name string) (success, errCount int64) {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s: unexpected metric type %T", name, m.Data)
			}
			for _, dp := range sum.DataPoints {
				status, _ := dp.Attributes.Value("status")
				switch status.AsString() {
				case "success":
					success += dp.Value
				case "error":
					errCount += dp.Value
				}
			}
			return success, errCount
		}
	}
	t.Fatalf("metric %s not found", name)
	return 0, 0
}


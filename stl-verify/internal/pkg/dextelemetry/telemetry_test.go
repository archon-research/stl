package dextelemetry

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	metricsdk "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// Review-11: every datapoint must carry a `chain="<id>"` attribute so the
// alert rules can `sum by (chain)` like the morpho/oracle alerts do, and
// query-time triage can tell which chain produced the signal without
// inferring from k8s pod labels alone.
func TestRecordBlockProcessed_AttachesChainLabel(t *testing.T) {
	reader := metricsdk.NewManualReader()
	mp := metricsdk.NewMeterProvider(metricsdk.WithReader(reader))
	otel.SetMeterProvider(mp)
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	tel, err := NewTelemetry("curve", 8453) // base
	if err != nil {
		t.Fatalf("NewTelemetry: %v", err)
	}
	ctx := context.Background()
	tel.RecordBlockProcessed(ctx, 10*time.Millisecond, nil)
	tel.RecordError(ctx, "processBlockEvent", errors.New("boom"))

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	if got := readChainAttr(t, &rm, "curve.blocks.processed"); got != "8453" {
		t.Errorf("curve.blocks.processed chain attr = %q, want %q", got, "8453")
	}
	if got := readChainAttr(t, &rm, "curve.errors.total"); got != "8453" {
		t.Errorf("curve.errors.total chain attr = %q, want %q", got, "8453")
	}
	if got := readChainAttr(t, &rm, "curve.block.duration_seconds"); got != "8453" {
		t.Errorf("curve.block.duration_seconds chain attr = %q, want %q", got, "8453")
	}
}

func readChainAttr(t *testing.T, rm *metricdata.ResourceMetrics, name string) string {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range d.DataPoints {
					if v, ok := dp.Attributes.Value("chain"); ok {
						return v.AsString()
					}
				}
			case metricdata.Histogram[float64]:
				for _, dp := range d.DataPoints {
					if v, ok := dp.Attributes.Value("chain"); ok {
						return v.AsString()
					}
				}
			}
			t.Fatalf("metric %s has no chain attribute on any datapoint", name)
		}
	}
	t.Fatalf("metric %s not found", name)
	return ""
}

// N8-4: RecordBlockProcessed previously accepted but dropped the duration
// parameter. Wire it as `<prefix>.block.duration_seconds` histogram so the
// RPCLatencyHigh-style alert class becomes possible for the DEX workers
// (the alerts file already has a TODO comment about the missing histogram).
func TestRecordBlockProcessed_EmitsDurationHistogram(t *testing.T) {
	reader := metricsdk.NewManualReader()
	mp := metricsdk.NewMeterProvider(metricsdk.WithReader(reader))
	otel.SetMeterProvider(mp)
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	tel, err := NewTelemetry("curve", 1)
	if err != nil {
		t.Fatalf("NewTelemetry: %v", err)
	}

	ctx := context.Background()
	tel.RecordBlockProcessed(ctx, 25*time.Millisecond, nil)
	tel.RecordBlockProcessed(ctx, 50*time.Millisecond, nil)
	tel.RecordBlockProcessed(ctx, 100*time.Millisecond, errors.New("boom"))

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	count, sum := readHistogram(t, &rm, "curve.block.duration_seconds")
	if count != 3 {
		t.Errorf("curve.block.duration_seconds count = %d, want 3", count)
	}
	// 0.025 + 0.050 + 0.100 = 0.175s
	const want = 0.175
	if sum < want-1e-6 || sum > want+1e-6 {
		t.Errorf("curve.block.duration_seconds sum = %v, want ~%v", sum, want)
	}
}

func readHistogram(t *testing.T, rm *metricdata.ResourceMetrics, name string) (count uint64, sum float64) {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			hist, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				t.Fatalf("%s: unexpected metric type %T", name, m.Data)
			}
			for _, dp := range hist.DataPoints {
				count += dp.Count
				sum += dp.Sum
			}
			return count, sum
		}
	}
	t.Fatalf("metric %s not found", name)
	return 0, 0
}

func TestNewTelemetry_RejectsEmptyPrefix(t *testing.T) {
	_, err := NewTelemetry("", 1)
	if err == nil {
		t.Fatal("NewTelemetry(\"\", 1) returned nil error; want a validation error")
	}
}

func TestNewTelemetry_RejectsNonPositiveChainID(t *testing.T) {
	for _, chainID := range []int64{0, -1, -8453} {
		if _, err := NewTelemetry("curve", chainID); err == nil {
			t.Errorf("NewTelemetry(\"curve\", %d) returned nil error; chainID must be positive", chainID)
		}
	}
}

func TestRecordBlockProcessed_LabelsStatusByError(t *testing.T) {
	reader := metricsdk.NewManualReader()
	mp := metricsdk.NewMeterProvider(metricsdk.WithReader(reader))
	otel.SetMeterProvider(mp)
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	tel, err := NewTelemetry("curve", 1)
	if err != nil {
		t.Fatalf("NewTelemetry: %v", err)
	}

	ctx := context.Background()
	tel.RecordBlockProcessed(ctx, time.Millisecond, nil)
	tel.RecordBlockProcessed(ctx, time.Millisecond, nil)
	tel.RecordBlockProcessed(ctx, time.Millisecond, errors.New("boom"))

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	successCount, errorCount := readBlockCounters(t, &rm, "curve.blocks.processed")
	if successCount != 2 {
		t.Errorf("status=success count = %d, want 2", successCount)
	}
	if errorCount != 1 {
		t.Errorf("status=error count = %d, want 1", errorCount)
	}
}

func TestRecordError_NoOpOnNilError(t *testing.T) {
	reader := metricsdk.NewManualReader()
	mp := metricsdk.NewMeterProvider(metricsdk.WithReader(reader))
	otel.SetMeterProvider(mp)
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	tel, err := NewTelemetry("balancer", 1)
	if err != nil {
		t.Fatalf("NewTelemetry: %v", err)
	}

	ctx := context.Background()
	tel.RecordError(ctx, "processBlockEvent", nil) // must not increment
	tel.RecordError(ctx, "processBlockEvent", errors.New("boom"))

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	count := readSingleSumCount(t, &rm, "balancer.errors.total")
	if count != 1 {
		t.Errorf("balancer.errors.total = %d, want 1 (nil error must be a no-op)", count)
	}
}

func TestTelemetry_NilSafe(t *testing.T) {
	var tel *Telemetry
	ctx := context.Background()
	tel.RecordBlockProcessed(ctx, time.Second, nil)
	tel.RecordBlockProcessed(ctx, time.Second, errors.New("e"))
	tel.RecordError(ctx, "op", errors.New("e"))
	tel.RecordError(ctx, "op", nil)
}

// readBlockCounters returns the counter values for status=success and
// status=error attributes on the named metric.
func readBlockCounters(t *testing.T, rm *metricdata.ResourceMetrics, name string) (success, errCount int64) {
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

func readSingleSumCount(t *testing.T, rm *metricdata.ResourceMetrics, name string) int64 {
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
			var total int64
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
			return total
		}
	}
	t.Fatalf("metric %s not found", name)
	return 0
}

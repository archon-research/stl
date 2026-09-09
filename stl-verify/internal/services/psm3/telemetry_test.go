package psm3

import (
	"context"
	"errors"
	"math/big"
	"slices"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
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

	tel, err := NewTelemetryWithProvider(mp, "base")
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

func bigFromStr(s string) *big.Int {
	v, _ := new(big.Int).SetString(s, 10)
	return v
}

func sweepState() entity.PSM3State {
	return entity.PSM3State{
		USDSBalance:    big.NewInt(1_000_000),
		SUSDSBalance:   big.NewInt(2_000_000),
		USDCBalance:    big.NewInt(3_000_000),
		TotalAssets:    new(big.Int).Mul(big.NewInt(6), big.NewInt(1_000_000_000_000_000_000)), // 6e18
		ConversionRate: bigFromStr("1100000000000000000000000000"),                             // 1.1 * 1e27
	}
}

// TestRecordSweep_CountsStatus asserts the sweep counter carries chain + status.
func TestRecordSweep_CountsStatus(t *testing.T) {
	tel, reader := newRecordingTelemetry(t)
	ctx := context.Background()

	tel.RecordSweep(ctx, time.Second, nil)
	tel.RecordSweep(ctx, time.Second, errors.New("boom"))

	m := collectMetric(t, reader, "psm3.sweeps.total")
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("psm3.sweeps.total is %T, want Sum[int64]", m.Data)
	}
	statuses := map[string]int64{}
	for _, dp := range sum.DataPoints {
		status, _ := dp.Attributes.Value("status")
		chain, ok := dp.Attributes.Value("chain")
		if !ok || chain.AsString() != "base" {
			t.Errorf("data point missing chain=base attribute: %v", dp.Attributes)
		}
		statuses[status.AsString()] += dp.Value
	}
	if statuses["success"] != 1 || statuses["error"] != 1 {
		t.Errorf("status counts = %v, want success:1 error:1", statuses)
	}
}

// TestSweepDuration_UsesSecondsBuckets guards the latency-alert bucket bug: a
// seconds histogram must use telemetry.SecondsDurationBuckets, not the SDK's
// default millisecond buckets (see morpho telemetry_test).
func TestSweepDuration_UsesSecondsBuckets(t *testing.T) {
	tel, reader := newRecordingTelemetry(t)
	tel.RecordSweep(context.Background(), 30*time.Millisecond, nil)

	m := collectMetric(t, reader, "psm3.sweep.duration_seconds")
	hist, ok := m.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("psm3.sweep.duration_seconds is %T, want Histogram[float64]", m.Data)
	}
	if len(hist.DataPoints) != 1 {
		t.Fatalf("got %d data points, want 1", len(hist.DataPoints))
	}
	if bounds := hist.DataPoints[0].Bounds; !slices.Equal(bounds, telemetry.SecondsDurationBuckets) {
		t.Errorf("bounds = %v, want %v", bounds, telemetry.SecondsDurationBuckets)
	}
}

// TestRecordSnapshot_SetsGauges asserts the snapshot gauges report block height
// and the scaled reserve values for dashboards.
func TestRecordSnapshot_SetsGauges(t *testing.T) {
	tel, reader := newRecordingTelemetry(t)
	st := sweepState()

	tel.RecordSnapshot(context.Background(), 31_000_000, st)

	cases := []struct {
		metric string
		want   float64
	}{
		{"psm3.last_snapshot_block", 31_000_000},
		{"psm3.total_assets", 6},      // 6e18 / 1e18
		{"psm3.conversion_rate", 1.1}, // 1.1e27 / 1e27
	}
	for _, c := range cases {
		m := collectMetric(t, reader, c.metric)
		got, chain := gaugeValue(t, m)
		if chain != "base" {
			t.Errorf("%s chain = %q, want base", c.metric, chain)
		}
		if got != c.want {
			t.Errorf("%s = %v, want %v", c.metric, got, c.want)
		}
	}
}

// gaugeValue extracts the single data point's value (as float64) and chain label
// from an int64 or float64 gauge.
func gaugeValue(t *testing.T, m metricdata.Metrics) (float64, string) {
	t.Helper()
	switch d := m.Data.(type) {
	case metricdata.Gauge[int64]:
		dp := d.DataPoints[0]
		chain, _ := dp.Attributes.Value("chain")
		return float64(dp.Value), chain.AsString()
	case metricdata.Gauge[float64]:
		dp := d.DataPoints[0]
		chain, _ := dp.Attributes.Value("chain")
		return dp.Value, chain.AsString()
	default:
		t.Fatalf("%q is %T, want a Gauge", m.Name, m.Data)
		return 0, ""
	}
}

func TestNewTelemetry(t *testing.T) {
	tel, err := NewTelemetry("base")
	if err != nil {
		t.Fatalf("NewTelemetry() error: %v", err)
	}
	if tel == nil {
		t.Fatal("NewTelemetry() returned nil")
	}
	exerciseAll(t, tel)
}

func TestNewTelemetryWithProvider(t *testing.T) {
	tel, err := NewTelemetryWithProvider(metricnoop.NewMeterProvider(), "base")
	if err != nil {
		t.Fatalf("NewTelemetryWithProvider() error: %v", err)
	}
	exerciseAll(t, tel)
}

func TestTelemetry_NilSafe(t *testing.T) {
	var tel *Telemetry
	exerciseAll(t, tel) // must not panic
}

func exerciseAll(t *testing.T, tel *Telemetry) {
	t.Helper()
	ctx := context.Background()
	tel.RecordSweep(ctx, time.Second, nil)
	tel.RecordSweep(ctx, time.Second, errors.New("e"))
	tel.RecordSnapshot(ctx, 1, sweepState())
}

// Guards the startup seed: VectorPSM3IndexerStalled reads
// psm3_sweeps_total{status="success"} with rate()==0 and must be computable
// from process start. See telemetry.SeedCounter.
func TestNewTelemetry_SeedsSweepStatusSeriesAtZero(t *testing.T) {
	_, reader := newRecordingTelemetry(t)

	dps := testutil.CollectSumDataPoints(t, reader, "psm3.sweeps.total")
	got := map[string]int64{}
	for _, dp := range dps {
		if chain := testutil.AttrValue(dp, "chain"); chain != "base" {
			t.Errorf("psm3.sweeps.total chain attr = %q, want %q", chain, "base")
		}
		got[testutil.AttrValue(dp, "status")] = dp.Value
	}
	for _, status := range []string{"success", "error"} {
		v, ok := got[status]
		if !ok {
			t.Errorf("psm3.sweeps.total missing status=%q series before any sweep", status)
			continue
		}
		if v != 0 {
			t.Errorf("psm3.sweeps.total{status=%q} = %d, want 0", status, v)
		}
	}
}

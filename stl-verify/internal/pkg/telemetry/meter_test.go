package telemetry

import (
	"context"
	"slices"
	"testing"

	otelmetric "go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// TestSecondsHistogramView verifies the defense-in-depth view: a seconds-unit
// histogram created without explicit boundaries falls back to the SDK's
// millisecond-scale defaults, but the view forces it onto SecondsDurationBuckets.
// Non-seconds histograms are left untouched.
func TestSecondsHistogramView(t *testing.T) {
	ctx := context.Background()

	// Baseline: without the view, a boundary-less seconds histogram gets the
	// SDK's default (broken-for-seconds) buckets.
	readerNoView := sdkmetric.NewManualReader()
	mpNoView := sdkmetric.NewMeterProvider(sdkmetric.WithReader(readerNoView))
	t.Cleanup(func() { _ = mpNoView.Shutdown(ctx) })

	histNoView, err := mpNoView.Meter("t").Float64Histogram("x.duration_seconds", otelmetric.WithUnit("s"))
	if err != nil {
		t.Fatalf("creating histogram: %v", err)
	}
	histNoView.Record(ctx, 0.03)
	if slices.Equal(collectHistogramBounds(t, readerNoView, "x.duration_seconds"), SecondsDurationBuckets) {
		t.Fatal("precondition failed: a boundary-less histogram already has SecondsDurationBuckets without the view")
	}

	// With the view, the same boundary-less seconds histogram is forced onto
	// SecondsDurationBuckets.
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader), sdkmetric.WithView(secondsHistogramView()))
	t.Cleanup(func() { _ = mp.Shutdown(ctx) })

	hist, err := mp.Meter("t").Float64Histogram("x.duration_seconds", otelmetric.WithUnit("s"))
	if err != nil {
		t.Fatalf("creating histogram: %v", err)
	}
	hist.Record(ctx, 0.03)
	if got := collectHistogramBounds(t, reader, "x.duration_seconds"); !slices.Equal(got, SecondsDurationBuckets) {
		t.Errorf("with view, bounds = %v, want %v", got, SecondsDurationBuckets)
	}

	// The view must not touch histograms that are not in seconds.
	histPlain, err := mp.Meter("t").Float64Histogram("y.items")
	if err != nil {
		t.Fatalf("creating histogram: %v", err)
	}
	histPlain.Record(ctx, 0.03)
	if got := collectHistogramBounds(t, reader, "y.items"); slices.Equal(got, SecondsDurationBuckets) {
		t.Errorf("view should not apply to non-seconds histogram y.items, but bounds = %v", got)
	}
}

// TestSecondsHistogramView_OverridesInstrumentBoundaries pins the override
// precedence: a seconds histogram that declares its own different boundaries is
// still coerced onto SecondsDurationBuckets by the view. This documents that the
// view is authoritative in production, so a per-instrument boundary that
// diverges from the canonical set silently will not take effect.
func TestSecondsHistogramView_OverridesInstrumentBoundaries(t *testing.T) {
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader), sdkmetric.WithView(secondsHistogramView()))
	t.Cleanup(func() { _ = mp.Shutdown(ctx) })

	hist, err := mp.Meter("t").Float64Histogram(
		"diverging.duration_seconds",
		otelmetric.WithUnit("s"),
		otelmetric.WithExplicitBucketBoundaries(0.5, 1, 2), // intentionally different from the canonical set
	)
	if err != nil {
		t.Fatalf("creating histogram: %v", err)
	}
	hist.Record(ctx, 0.03)

	if got := collectHistogramBounds(t, reader, "diverging.duration_seconds"); !slices.Equal(got, SecondsDurationBuckets) {
		t.Errorf("view should override the instrument's own boundaries; bounds = %v, want %v", got, SecondsDurationBuckets)
	}
}

// TestSecondsHistogramView_IgnoresNonHistograms verifies the view's Kind filter:
// a non-histogram instrument that happens to carry Unit:"s" must not be reshaped
// into a histogram.
func TestSecondsHistogramView_IgnoresNonHistograms(t *testing.T) {
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader), sdkmetric.WithView(secondsHistogramView()))
	t.Cleanup(func() { _ = mp.Shutdown(ctx) })

	counter, err := mp.Meter("t").Float64Counter("z.elapsed_seconds", otelmetric.WithUnit("s"))
	if err != nil {
		t.Fatalf("creating counter: %v", err)
	}
	counter.Add(ctx, 1.5)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("collecting metrics: %v", err)
	}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "z.elapsed_seconds" {
				continue
			}
			if _, isHist := m.Data.(metricdata.Histogram[float64]); isHist {
				t.Errorf("view reshaped a non-histogram Unit:s instrument into a histogram")
			}
			return
		}
	}
	t.Fatal("metric z.elapsed_seconds not found")
}

// TestSecondsDurationBuckets pins the properties the latency alerts depend on:
// fine sub-second resolution (so tens-of-ms RPC calls don't collapse into one
// bucket), coverage straddling the 3s RPC alert threshold, and a top boundary
// past the 30s backup-worker threshold (otherwise p99 clamps to 30 and the >30s
// alert can never fire).
func TestSecondsDurationBuckets(t *testing.T) {
	b := SecondsDurationBuckets
	if len(b) == 0 {
		t.Fatal("SecondsDurationBuckets is empty")
	}

	for i, v := range b {
		if v <= 0 {
			t.Errorf("boundary[%d] = %v, want > 0", i, v)
		}
		if i > 0 && v <= b[i-1] {
			t.Errorf("boundaries not strictly increasing at %d: %v <= %v", i, v, b[i-1])
		}
	}

	if b[0] > 0.005 {
		t.Errorf("smallest boundary = %v, want <= 0.005s for sub-second resolution", b[0])
	}

	// A boundary at/below 3s gives resolution there, and one strictly above 3s
	// lets p99 interpolate past the 3s RPC alert threshold instead of clamping.
	hasResolutionBelow3 := slices.ContainsFunc(b, func(v float64) bool { return v <= 3 })
	hasBoundaryAbove3 := slices.ContainsFunc(b, func(v float64) bool { return v > 3 })
	if !hasResolutionBelow3 || !hasBoundaryAbove3 {
		t.Errorf("boundaries %v must straddle the 3s RPC alert threshold (need a value <=3 and a value >3)", b)
	}

	// A finite boundary strictly above 30s lets p99 resolve past the >30s
	// backup-worker alert threshold instead of clamping at the top bucket.
	if !slices.ContainsFunc(b, func(v float64) bool { return v > 30 }) {
		t.Errorf("boundaries %v need a value > 30s so the >30s backup-worker alert can fire", b)
	}
}

package temporal

import (
	"context"
	"errors"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// Guards the startup seed in seedStatusSeries: every terminal-status series
// must exist at 0 before the first run, so increase() can see the first real
// increment. See seedStatusSeries for the full rationale.
func TestNewCronjobMetrics_SeedsAllStatusSeriesAtZero(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	if _, err := newCronjobMetricsWithProvider(mp); err != nil {
		t.Fatalf("newCronjobMetricsWithProvider() error: %v", err)
	}

	got := collectCounterByStatus(t, reader, "cronjob.runs.total")
	for _, status := range []string{"success", "error", "canceled"} {
		v, ok := got[status]
		if !ok {
			t.Errorf("cronjob.runs.total is missing the status=%q series before any run", status)
			continue
		}
		if v != 0 {
			t.Errorf("cronjob.runs.total{status=%q} = %d, want 0", status, v)
		}
	}
}

// After seeding, the first successful run must land on the seeded success
// series as 0->1 (not orphan it into a parallel series), which is the increment
// increase() needs to see. This holds only while RecordRun (runStatusAttr) and
// seedStatusSeries (runStatusValues) agree on the label values; this guards that.
func TestRecordRun_LandsSuccessOnSeededSeriesAsOne(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	m, err := newCronjobMetricsWithProvider(mp)
	if err != nil {
		t.Fatalf("newCronjobMetricsWithProvider() error: %v", err)
	}

	m.RecordRun(context.Background(), time.Second, nil)

	got := collectCounterByStatus(t, reader, "cronjob.runs.total")
	if got["success"] != 1 {
		t.Errorf("cronjob.runs.total{status=\"success\"} = %d, want 1", got["success"])
	}
	if got["error"] != 0 {
		t.Errorf("cronjob.runs.total{status=\"error\"} = %d, want 0", got["error"])
	}
}

// A failed run whose activity context was canceled (worker shutdown during a
// deploy rollout, or a schedule cancel) must land as "canceled" so
// VectorCronjobRunFailing (keyed on status="error") does not fire on every
// deploy; a genuine failure or a deadline-exceeded run must stay "error".
func TestRecordRun_ClassifiesRunStatus(t *testing.T) {
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	expiredCtx, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	t.Cleanup(cancelExpired)

	tests := []struct {
		name       string
		ctx        context.Context
		runErr     error
		wantStatus string
	}{
		{
			name:       "nil error is success",
			ctx:        context.Background(),
			wantStatus: "success",
		},
		{
			name:       "error with live context is error",
			ctx:        context.Background(),
			runErr:     errors.New("upstream failed"),
			wantStatus: "error",
		},
		{
			name:       "error with canceled context is canceled",
			ctx:        canceledCtx,
			runErr:     errors.New("validation failed: 0 failures, 147 errors"),
			wantStatus: "canceled",
		},
		{
			name:       "error with exceeded deadline is error",
			ctx:        expiredCtx,
			runErr:     errors.New("context deadline exceeded"),
			wantStatus: "error",
		},
		{
			name:       "nil error with canceled context is success",
			ctx:        canceledCtx,
			wantStatus: "success",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

			m, err := newCronjobMetricsWithProvider(mp)
			if err != nil {
				t.Fatalf("newCronjobMetricsWithProvider() error: %v", err)
			}

			m.RecordRun(tt.ctx, time.Second, tt.runErr)

			got := collectCounterByStatus(t, reader, "cronjob.runs.total")
			for _, status := range []string{"success", "error", "canceled"} {
				want := int64(0)
				if status == tt.wantStatus {
					want = 1
				}
				if got[status] != want {
					t.Errorf("cronjob.runs.total{status=%q} = %d, want %d", status, got[status], want)
				}
			}
		})
	}
}

func collectCounterByStatus(t *testing.T, reader sdkmetric.Reader, name string) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collecting metrics: %v", err)
	}
	out := make(map[string]int64)
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
				status, _ := dp.Attributes.Value("status")
				out[status.AsString()] = dp.Value
			}
		}
	}
	return out
}

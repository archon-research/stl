package sqsutil

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// collectReleaseCounter reads the release counter back as status -> value.
func collectReleaseCounter(t *testing.T, reader sdkmetric.Reader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collecting metrics: %v", err)
	}
	out := make(map[string]int64)
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != releaseCounterName {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric %q is %T, want metricdata.Sum[int64]", releaseCounterName, m.Data)
			}
			for _, dp := range sum.DataPoints {
				status, _ := dp.Attributes.Value("status")
				out[status.AsString()] = dp.Value
			}
		}
	}
	return out
}

// installManualMeterProvider points the global provider at a manual reader,
// where ReleaseMessages resolves its counter.
func installManualMeterProvider(t *testing.T) sdkmetric.Reader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(previous)
		_ = mp.Shutdown(context.Background())
	})
	return reader
}

// TestReleaseMessages_CountsEveryReleaseByOutcome pins the metric behind the
// rollout-blackout fix: without it a broken release path shows up only as a
// multi-minute per-chain data gap, with nothing to alert on.
func TestReleaseMessages_CountsEveryReleaseByOutcome(t *testing.T) {
	tests := []struct {
		name          string
		visibilityErr error
		want          map[string]int64
	}{
		{
			name: "successful releases",
			want: map[string]int64{releaseStatusReleased: 2},
		},
		{
			name:          "refused releases",
			visibilityErr: errors.New("AccessDenied: ChangeMessageVisibility"),
			want:          map[string]int64{releaseStatusFailed: 2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := installManualMeterProvider(t)
			consumer := &mockConsumer{visibilityErr: tt.visibilityErr}
			messages := []outbound.SQSMessage{
				makeMsg("1", "h1", blockEvent(100)),
				makeMsg("2", "h2", blockEvent(101)),
			}

			ReleaseMessages(context.Background(), consumer, slog.Default(), messages)

			if got := collectReleaseCounter(t, reader); !maps.Equal(got, tt.want) {
				t.Errorf("%s = %v, want %v", releaseCounterName, got, tt.want)
			}
		})
	}
}

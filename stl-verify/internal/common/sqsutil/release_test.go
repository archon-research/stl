package sqsutil

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

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
		refusals      map[string]error
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
		{
			name:     "one entry of the batch refused",
			refusals: map[string]error{"h2": errors.New("ReceiptHandleIsInvalid")},
			want:     map[string]int64{releaseStatusReleased: 1, releaseStatusFailed: 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := installManualMeterProvider(t)
			consumer := &mockConsumer{visibilityErr: tt.visibilityErr, visibilityRefusals: tt.refusals}
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

// TestReleaseMessages_OneSlowReleaseCannotStrandTheRest pins the batching. The
// release call keeps the SDK's default retryer, so one throttled release can
// burn the whole shared cleanup budget in its own retry chain; per-message
// calls would then leave the rest of the held set hidden for the queue's full
// visibility timeout — the FIFO blackout the release exists to prevent.
func TestReleaseMessages_OneSlowReleaseCannotStrandTheRest(t *testing.T) {
	const held = 10
	messages := heldMessages(held)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	var slowOnce sync.Once
	consumer := &mockConsumer{onRelease: func() {
		slowOnce.Do(func() { time.Sleep(80 * time.Millisecond) })
	}}

	ReleaseMessages(ctx, consumer, slog.Default(), messages)

	if got := len(consumer.released()); got != held {
		t.Fatalf("expected all %d held messages released, got %d", held, got)
	}
}

// TestReleaseMessages_ChunksTheHeldSetToTheSQSBatchCeiling covers the held set
// that outgrows one batch: the backup worker holds an undispatched tail plus
// one message per worker, so 10 is not the ceiling on what a shutdown hands
// back.
func TestReleaseMessages_ChunksTheHeldSetToTheSQSBatchCeiling(t *testing.T) {
	consumer := &mockConsumer{}

	ReleaseMessages(context.Background(), consumer, slog.Default(), heldMessages(14))

	calls := consumer.releaseCalls()
	if len(calls) != 2 {
		t.Fatalf("expected 14 held messages to need 2 calls, got %d", len(calls))
	}
	if got := []int{len(calls[0].handles), len(calls[1].handles)}; !slices.Equal(got, []int{10, 4}) {
		t.Errorf("expected chunks of 10 and 4, got %v", got)
	}
}

func heldMessages(count int) []outbound.SQSMessage {
	messages := make([]outbound.SQSMessage, 0, count)
	for i := range count {
		messages = append(messages, makeMsg(strconv.Itoa(i), fmt.Sprintf("h%d", i), blockEvent(int64(100+i))))
	}
	return messages
}

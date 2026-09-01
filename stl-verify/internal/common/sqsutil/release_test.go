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

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

func TestReleaseMessages_CountsEveryReleaseByOutcome(t *testing.T) {
	tests := []struct {
		name          string
		visibilityErr error
		refusals      map[string]error
		want          map[settleKey]int64
	}{
		{
			name: "successful releases",
			want: map[settleKey]int64{{op: "release", status: "ok"}: 2},
		},
		{
			name:          "refused releases",
			visibilityErr: errors.New("AccessDenied: ChangeMessageVisibility"),
			want:          map[settleKey]int64{{op: "release", status: "failed"}: 2},
		},
		{
			name:     "one entry of the batch refused",
			refusals: map[string]error{"h2": errors.New("ReceiptHandleIsInvalid")},
			want:     map[settleKey]int64{{op: "release", status: "ok"}: 1, {op: "release", status: "failed"}: 1},
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

			ReleaseMessages(context.Background(), consumer, slog.Default(), 1, messages)

			if got := collectSettleCounter(t, reader); !maps.Equal(got, tt.want) {
				t.Errorf("settles = %v, want %v", got, tt.want)
			}
		})
	}
}

// The release call keeps the SDK's default retryer, so one throttled call can
// burn a whole cleanup budget in its own retry chain.
func TestReleaseMessages_OneSlowReleaseCannotStrandTheRest(t *testing.T) {
	const held = 14
	messages := heldMessages(held)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	var slowOnce sync.Once
	consumer := &mockConsumer{onRelease: func() {
		slowOnce.Do(func() { time.Sleep(80 * time.Millisecond) })
	}}

	ReleaseMessages(ctx, consumer, slog.Default(), 1, messages)

	if got := len(consumer.released()); got != held {
		t.Fatalf("expected all %d held messages released, got %d", held, got)
	}
}

// Nothing else stops this call: the release runs detached from the cancelled
// caller, and the backup worker has no lifecycle.Run window around it.
func TestReleaseMessages_BoundsTheDetachedReleaseByTheCleanupBudget(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	consumer := &mockConsumer{}

	ReleaseMessages(ctx, consumer, slog.Default(), 1, heldMessages(1))
	latest := time.Now().Add(SettleTimeout)

	calls := consumer.releaseCalls()
	if len(calls) != 1 {
		t.Fatalf("expected the cancelled caller's release to still go out, got %d calls", len(calls))
	}
	if calls[0].deadline.IsZero() {
		t.Fatal("expected the detached release bounded by a deadline, got none")
	}
	if calls[0].deadline.After(latest) {
		t.Errorf("expected the release deadline within %s, got %s past it",
			SettleTimeout, calls[0].deadline.Sub(latest))
	}
}

// The backup worker's held set is the tail it never dispatched plus its
// Workers*2 buffer plus what the workers pulled, so it outgrows one batch call.
func TestReleaseMessages_ChunksTheHeldSetToTheSQSBatchCeiling(t *testing.T) {
	consumer := &mockConsumer{}

	ReleaseMessages(context.Background(), consumer, slog.Default(), 1, heldMessages(14))

	calls := consumer.releaseCalls()
	if len(calls) != 2 {
		t.Fatalf("expected 14 held messages to need 2 calls, got %d", len(calls))
	}
	if got := []int{len(calls[0].handles), len(calls[1].handles)}; !slices.Equal(got, []int{10, 4}) {
		t.Errorf("expected chunks of 10 and 4, got %v", got)
	}
}

// Every per-chain backup worker shares one service_name, and the backup-worker
// alerts group by (chain, cluster).
func TestReleaseMessages_CountsAgainstTheChainName(t *testing.T) {
	reader := installManualMeterProvider(t)
	consumer := &mockConsumer{}

	ReleaseMessages(context.Background(), consumer, slog.Default(), 42161, heldMessages(1))

	if got := collectSettleChains(t, reader); !slices.Equal(got, []string{"arbitrum"}) {
		t.Errorf("expected the release counted against the chain name, got %v", got)
	}
}

func heldMessages(count int) []outbound.SQSMessage {
	messages := make([]outbound.SQSMessage, 0, count)
	for i := range count {
		messages = append(messages, makeMsg(strconv.Itoa(i), fmt.Sprintf("h%d", i), blockEvent(int64(100+i))))
	}
	return messages
}

// A refusal map alongside the error means SQS applied the batch, so the handles
// it did not name were genuinely released.
func TestReleaseMessages_DoesNotFailTheWholeChunkForOneUnattributableRefusal(t *testing.T) {
	reader := installManualMeterProvider(t)
	consumer := &mockConsumer{
		visibilityErr:     errors.New("SQS refused entries [99], which match no handle in the request"),
		visibilityPartial: map[string]error{"h1": errors.New("ReceiptHandleIsInvalid")},
	}

	ReleaseMessages(context.Background(), consumer, slog.Default(), 1, heldMessages(3))

	want := map[settleKey]int64{{op: "release", status: "ok"}: 2, {op: "release", status: "failed"}: 1}
	if got := collectSettleCounter(t, reader); !maps.Equal(got, want) {
		t.Errorf("settles = %v, want %v", got, want)
	}
}

package sqsutil

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// A delete SQS refuses on a live context leaves the message hidden for the whole
// visibility timeout, blocking that chain's FIFO group — the common shape of a
// blackout, and deletes run on every message where releases run only at shutdown.
func TestProcessMessages_CountsARefusedDelete(t *testing.T) {
	reader := installManualMeterProvider(t)
	consumer := &mockConsumer{
		batches:      [][]outbound.SQSMessage{{makeMsg("1", "h1", blockEvent(100))}},
		deleteErrFor: map[string]error{"h1": errors.New("AccessDenied: DeleteMessage")},
	}

	if _, err := ProcessMessages(context.Background(), testConfig(consumer), noopHandler); err == nil {
		t.Fatal("expected the refused delete returned")
	}

	want := map[settleKey]int64{{op: "delete", status: "failed"}: 1}
	if got := collectSettleCounter(t, reader); !maps.Equal(got, want) {
		t.Errorf("settles = %v, want %v", got, want)
	}
}

func TestProcessMessages_CountsASettledDelete(t *testing.T) {
	reader := installManualMeterProvider(t)
	consumer := &mockConsumer{
		batches: [][]outbound.SQSMessage{{makeMsg("1", "h1", blockEvent(100))}},
	}

	if _, err := ProcessMessages(context.Background(), testConfig(consumer), noopHandler); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := map[settleKey]int64{{op: "delete", status: "ok"}: 1}
	if got := collectSettleCounter(t, reader); !maps.Equal(got, want) {
		t.Errorf("settles = %v, want %v", got, want)
	}
}

// The backoff runs on the live path for every failed message, so a refused
// one must be countable the way a refused delete is.
func TestProcessMessages_CountsABackoffByOutcome(t *testing.T) {
	tests := []struct {
		name      string
		refusals  map[string]error
		want      map[settleKey]int64
		wantWarns int
	}{
		{
			name: "an accepted backoff",
			want: map[settleKey]int64{{op: "backoff", status: "ok"}: 1},
		},
		{
			name:      "a refused backoff",
			refusals:  map[string]error{"h1": errors.New("ReceiptHandleIsInvalid")},
			want:      map[settleKey]int64{{op: "backoff", status: "failed"}: 1},
			wantWarns: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := installManualMeterProvider(t)
			consumer := &mockConsumer{
				batches:            [][]outbound.SQSMessage{{makeMsg("1", "h1", blockEvent(100))}},
				visibilityRefusals: tt.refusals,
			}
			cfg, recorder := recordingConfig(consumer)

			if _, err := ProcessMessages(context.Background(), cfg, failingHandler); err == nil {
				t.Fatal("expected the handler failure to be returned")
			}

			if got := collectSettleCounter(t, reader); !maps.Equal(got, tt.want) {
				t.Errorf("settles = %v, want %v", got, tt.want)
			}
			if got := recorder.CountWarn("retry backoff"); got != tt.wantWarns {
				t.Errorf("refusal warnings = %d, want %d (records: %v)", got, tt.wantWarns, recorder.MessagesAt(slog.LevelWarn))
			}
		})
	}
}

// The SQS client carries no read timeout, so an unbounded settle against a
// silent connection parks the single-goroutine poll loop for good.
func TestCleanupContext_BoundsTheSettleCall(t *testing.T) {
	ctx, cancel := CleanupContext(context.Background())
	defer cancel()

	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("expected a settle context on a live parent to carry a deadline, got none")
	}
	if remaining := time.Until(deadline); remaining > SettleTimeout {
		t.Errorf("expected a deadline within %s, got %s remaining", SettleTimeout, remaining)
	}
}

func TestCleanupContext_SurvivesAParentCancelledMidCall(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	ctx, cancel := CleanupContext(parent)
	defer cancel()

	cancelParent()

	if err := ctx.Err(); err != nil {
		t.Fatalf("expected the settle context to outlive its cancelled parent, got %v", err)
	}
	if _, ok := ctx.Deadline(); !ok {
		t.Error("expected the detached settle context to keep its deadline")
	}
}

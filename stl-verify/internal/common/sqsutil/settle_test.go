package sqsutil

import (
	"context"
	"errors"
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

// The SQS client carries no read timeout, so an unbounded settle against a
// silent connection parks the single-goroutine poll loop for good.
func TestCleanupContext_BoundsTheSettleCallOnALiveParent(t *testing.T) {
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

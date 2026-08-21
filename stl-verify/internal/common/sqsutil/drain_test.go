package sqsutil

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestRunDrainable_KeepsWorkAliveAcrossCallerCancellation pins the reason the
// primitive exists: only work that finishes lets its message be deleted
// instead of stalling the FIFO group for the visibility timeout.
func TestRunDrainable_KeepsWorkAliveAcrossCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	running := make(chan struct{}, 1)
	shutdownRequested := make(chan struct{})

	var workCtxErr error
	go func() {
		<-running
		cancel()
		close(shutdownRequested)
	}()
	outcome := RunDrainable(ctx, DrainBudget{Work: time.Minute, Drain: 2 * time.Second}, func(wctx context.Context) error {
		signalOnce(running)
		<-shutdownRequested
		workCtxErr = wctx.Err()
		return nil
	})

	if outcome.Err != nil || outcome.Abandoned {
		t.Fatalf("expected the drained work to finish cleanly, got %+v", outcome)
	}
	if workCtxErr != nil {
		t.Errorf("expected work to keep a live context across cancellation, got %v", workCtxErr)
	}
}

// TestRunDrainable_AbandonsWorkPastTheDrainBudget covers the other side:
// abandonment is the caller's signal to hand the message back, not to report a
// processing failure.
func TestRunDrainable_AbandonsWorkPastTheDrainBudget(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	running := make(chan struct{}, 1)
	workCancelled := make(chan struct{})

	go func() {
		<-running
		cancel()
	}()
	outcome := RunDrainable(ctx, DrainBudget{Work: time.Minute, Drain: 20 * time.Millisecond}, func(wctx context.Context) error {
		signalOnce(running)
		<-wctx.Done()
		close(workCancelled)
		return wctx.Err()
	})

	if !outcome.Abandoned {
		t.Errorf("expected the outcome to report abandonment, got %+v", outcome)
	}
	if !errors.Is(outcome.Err, ErrDrainAbandoned) {
		t.Errorf("expected ErrDrainAbandoned, got %v", outcome.Err)
	}
	awaitSignal(t, workCancelled, "the abandoned work to observe its cancellation")
}

// TestRunDrainable_ReportsWorkThatIgnoredItsBudget guards the case a bare error
// cannot express: success returned only after the work's own budget expired.
func TestRunDrainable_ReportsWorkThatIgnoredItsBudget(t *testing.T) {
	outcome := RunDrainable(context.Background(), DrainBudget{Work: 10 * time.Millisecond}, func(wctx context.Context) error {
		<-wctx.Done()
		return nil
	})

	if outcome.Err != nil {
		t.Fatalf("expected the work's own nil result, got %v", outcome.Err)
	}
	if !outcome.BudgetExceeded {
		t.Error("expected work that outran its budget to be reported")
	}
}

// TestRunDrainable_ZeroWorkBudgetLeavesWorkUnbounded covers the worker with no
// per-message timeout of its own: the drain stays the only bound added.
func TestRunDrainable_ZeroWorkBudgetLeavesWorkUnbounded(t *testing.T) {
	var hadDeadline bool
	outcome := RunDrainable(context.Background(), DrainBudget{}, func(wctx context.Context) error {
		_, hadDeadline = wctx.Deadline()
		return nil
	})

	if outcome.Err != nil {
		t.Fatalf("unexpected error: %v", outcome.Err)
	}
	if hadDeadline {
		t.Error("expected no deadline on the work context when the work budget is zero")
	}
}

// TestRunDrainableValue_AbandonedWorkYieldsTheZeroValue pins the reason the
// value travels with the outcome: abandoned work keeps running while its caller
// settles the message, so reading its result would race.
func TestRunDrainableValue_AbandonedWorkYieldsTheZeroValue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	running := make(chan struct{}, 1)

	go func() {
		<-running
		cancel()
	}()
	value, outcome := RunDrainableValue(ctx, DrainBudget{Drain: 20 * time.Millisecond}, func(wctx context.Context) (string, error) {
		signalOnce(running)
		<-wctx.Done()
		return "produced-after-abandonment", nil
	})

	if !outcome.Abandoned {
		t.Fatalf("expected the outcome to report abandonment, got %+v", outcome)
	}
	if value != "" {
		t.Errorf("expected the zero value from abandoned work, got %q", value)
	}
}

// TestRunDrainableValue_ReturnsTheWorkValue is the counterpart.
func TestRunDrainableValue_ReturnsTheWorkValue(t *testing.T) {
	value, outcome := RunDrainableValue(context.Background(), DrainBudget{}, func(context.Context) (string, error) {
		return "rpc_fallback", nil
	})

	if outcome.Err != nil {
		t.Fatalf("unexpected error: %v", outcome.Err)
	}
	if value != "rpc_fallback" {
		t.Errorf("expected the work's value, got %q", value)
	}
}

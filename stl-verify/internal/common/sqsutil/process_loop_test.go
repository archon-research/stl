package sqsutil

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

func TestProcessMessages_Success(t *testing.T) {
	event := outbound.BlockEvent{ChainID: 1, BlockNumber: 100, Version: 0, BlockHash: "0xabc"}
	consumer := &mockConsumer{
		batches: [][]outbound.SQSMessage{{makeMsg("1", "h1", event)}},
	}

	var processed []outbound.BlockEvent
	handler := func(_ context.Context, e outbound.BlockEvent) error {
		processed = append(processed, e)
		return nil
	}

	err := ProcessMessages(context.Background(), testConfig(consumer), handler)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(processed) != 1 {
		t.Fatalf("expected 1 processed event, got %d", len(processed))
	}
	if processed[0].BlockNumber != 100 {
		t.Errorf("expected block 100, got %d", processed[0].BlockNumber)
	}
	if len(consumer.deletedHandles) != 1 || consumer.deletedHandles[0] != "h1" {
		t.Errorf("expected handle h1 deleted, got %v", consumer.deletedHandles)
	}
}

// TestProcessMessages_DoesNotDeleteOnHandlerError verifies the SQS half of
// the VEC-188 transient-error invariant: a handler returning any error must
// leave the message undeleted so SQS re-delivers it.
func TestProcessMessages_DoesNotDeleteOnHandlerError(t *testing.T) {
	event := outbound.BlockEvent{ChainID: 1, BlockNumber: 100, Version: 0, BlockHash: "0xabc"}
	consumer := &mockConsumer{
		batches: [][]outbound.SQSMessage{{makeMsg("1", "h1", event)}},
	}

	handler := func(_ context.Context, e outbound.BlockEvent) error {
		return errors.New("handler failed")
	}

	err := ProcessMessages(context.Background(), testConfig(consumer), handler)
	if err == nil {
		t.Fatal("expected error")
	}
	if len(consumer.deletedHandles) != 0 {
		t.Errorf("expected no deletes on handler error, got %v", consumer.deletedHandles)
	}
}

func TestProcessMessages_EmptyBatch(t *testing.T) {
	consumer := &mockConsumer{}

	called := false
	handler := func(_ context.Context, e outbound.BlockEvent) error {
		called = true
		return nil
	}

	err := ProcessMessages(context.Background(), testConfig(consumer), handler)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if called {
		t.Error("handler should not be called for empty batch")
	}
}

func TestProcessMessages_InvalidJSON(t *testing.T) {
	consumer := &mockConsumer{
		batches: [][]outbound.SQSMessage{{
			{MessageID: "1", ReceiptHandle: "h1", Body: "not json"},
		}},
	}

	handler := func(_ context.Context, e outbound.BlockEvent) error {
		t.Fatal("handler should not be called for invalid JSON")
		return nil
	}

	err := ProcessMessages(context.Background(), testConfig(consumer), handler)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestProcessMessages_PartialFailure(t *testing.T) {
	event1 := outbound.BlockEvent{ChainID: 1, BlockNumber: 100, Version: 0, BlockHash: "0xabc"}
	event2 := outbound.BlockEvent{ChainID: 1, BlockNumber: 101, Version: 0, BlockHash: "0xdef"}
	consumer := &mockConsumer{
		batches: [][]outbound.SQSMessage{{
			makeMsg("1", "h1", event1),
			makeMsg("2", "h2", event2),
		}},
	}

	handler := func(_ context.Context, e outbound.BlockEvent) error {
		if e.BlockNumber == 100 {
			return errors.New("fail block 100")
		}
		return nil
	}

	err := ProcessMessages(context.Background(), testConfig(consumer), handler)
	if err == nil {
		t.Fatal("expected error for partial failure")
	}
	// Block 101 should still be deleted
	if len(consumer.deletedHandles) != 1 || consumer.deletedHandles[0] != "h2" {
		t.Errorf("expected h2 deleted, got %v", consumer.deletedHandles)
	}
}

func TestProcessMessages_ChainIDMismatch_SkipsHandler(t *testing.T) {
	event := outbound.BlockEvent{ChainID: 42, BlockNumber: 200, Version: 0, BlockHash: "0xfoo"}
	consumer := &mockConsumer{
		batches: [][]outbound.SQSMessage{{makeMsg("1", "h1", event)}},
	}

	called := false
	handler := func(_ context.Context, e outbound.BlockEvent) error {
		called = true
		return nil
	}

	cfg := testConfig(consumer)
	cfg.ChainID = 1 // expect chain 1, event has chain 42

	err := ProcessMessages(context.Background(), cfg, handler)
	// Chain ID mismatch is deterministic — message is deleted, no error returned
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if called {
		t.Error("handler should not be called when chain ID mismatches")
	}
	if len(consumer.deletedHandles) != 1 || consumer.deletedHandles[0] != "h1" {
		t.Errorf("expected mismatched message to be deleted, got deletes: %v", consumer.deletedHandles)
	}
}

// TestProcessMessages_ReleasesMismatchedMessageWhenShutdownKillsItsDelete
// covers the one settle path that had no shutdown handling: the discard's
// delete rides the caller's context until shutdown cancels it, and a message
// whose delete then fails is left in flight — the loop's guard only releases
// the messages after it.
func TestProcessMessages_ReleasesMismatchedMessageWhenShutdownKillsItsDelete(t *testing.T) {
	foreign := outbound.BlockEvent{ChainID: 42, BlockNumber: 200, Version: 0, BlockHash: "0xfoo"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumer := &mockConsumer{
		receive: func(context.Context, int) ([]outbound.SQSMessage, error) {
			return []outbound.SQSMessage{makeMsg("1", "h1", foreign)}, nil
		},
		beforeDelete: cancel, // SIGTERM lands with the discard's delete in flight
	}
	cfg, _ := recordingConfig(consumer)

	if err := ProcessMessages(ctx, cfg, noopHandler); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := consumer.deleted(); len(got) != 0 {
		t.Errorf("expected the cancelled delete to fail, got deletes %v", got)
	}
	want := []visibilityChange{{handle: "h1", visibility: 0}}
	if got := consumer.released(); !slices.Equal(got, want) {
		t.Errorf("expected the undeletable message released for the successor, got %v", got)
	}
}

func TestProcessMessages_ChainIDMatch_Proceeds(t *testing.T) {
	event := outbound.BlockEvent{ChainID: 1, BlockNumber: 200, Version: 0, BlockHash: "0xfoo"}
	consumer := &mockConsumer{
		batches: [][]outbound.SQSMessage{{makeMsg("1", "h1", event)}},
	}

	var processed []outbound.BlockEvent
	handler := func(_ context.Context, e outbound.BlockEvent) error {
		processed = append(processed, e)
		return nil
	}

	cfg := testConfig(consumer)
	cfg.ChainID = 1 // matches event

	err := ProcessMessages(context.Background(), cfg, handler)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(processed) != 1 {
		t.Fatalf("expected 1 processed event, got %d", len(processed))
	}
	if len(consumer.deletedHandles) != 1 || consumer.deletedHandles[0] != "h1" {
		t.Errorf("expected handle h1 deleted, got %v", consumer.deletedHandles)
	}
}

func TestConfig_Validate(t *testing.T) {
	t.Run("zero chain ID returns error", func(t *testing.T) {
		cfg := Config{ChainID: 0}
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected error for zero ChainID")
		}
	})
	t.Run("non-zero chain ID succeeds", func(t *testing.T) {
		cfg := Config{ChainID: 1}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestRunLoop_StopsOnCancel(t *testing.T) {
	consumer := &mockConsumer{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	handler := func(_ context.Context, e outbound.BlockEvent) error { return nil }

	// Should return quickly without blocking.
	RunLoop(ctx, Config{
		Consumer:     consumer,
		MaxMessages:  10,
		PollInterval: 50 * time.Millisecond,
		Logger:       slog.Default(),
		ChainID:      1,
	}, handler)
	// If we get here without hanging, test passes.
}

// receiveRacingShutdown returns a mock receive that models the real consumer's
// only cancellation-shaped failure: SIGTERM lands after the loop decided to
// poll but before the poll starts, so the consumer refuses to start it instead
// of abandoning one in flight (an aborted poll strands the message SQS handed
// to the dead request). The returned channel must be closed by the test after
// it cancels, which is what sequences the race.
func receiveRacingShutdown(entered chan<- struct{}, shutdownLanded <-chan struct{}, failure error) func(context.Context, int) ([]outbound.SQSMessage, error) {
	return func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
		signalOnce(entered)
		<-shutdownLanded
		if ctx.Err() != nil {
			return nil, failure
		}
		return nil, nil
	}
}

// TestRunLoop_ShutdownCancellationIsNotAnError covers the SIGTERM path: the
// consumer refuses to start a poll on the cancelled context, which is a clean
// shutdown, not a failure. Logging it at ERROR made every rollout of every SQS
// worker feed the error-rate alerts.
func TestRunLoop_ShutdownCancellationIsNotAnError(t *testing.T) {
	receiving := make(chan struct{}, 1)
	shutdownLanded := make(chan struct{})
	consumer := &mockConsumer{
		receive: receiveRacingShutdown(receiving, shutdownLanded,
			fmt.Errorf("not starting a poll: %w", context.Canceled)),
	}
	recorder := &levelRecorder{}

	cancel, done := startRunLoop(consumer, slog.New(recorder), noopHandler)
	awaitSignal(t, receiving, "the receive to start")
	cancel()
	close(shutdownLanded)
	awaitLoopExit(t, done)

	if logged := recorder.messagesAt(slog.LevelError); len(logged) > 0 {
		t.Fatalf("expected no ERROR records for a cancelled shutdown, got %v", logged)
	}
}

// TestRunLoop_LogsGenuineReceiveFailureAtError is the counterpart guard: the
// shutdown-quiet path must not swallow a real receive failure.
func TestRunLoop_LogsGenuineReceiveFailureAtError(t *testing.T) {
	receiving := make(chan struct{}, 1)
	consumer := &mockConsumer{
		receive: func(context.Context, int) ([]outbound.SQSMessage, error) {
			signalOnce(receiving)
			return nil, errors.New("SQS unavailable")
		},
	}
	recorder := &levelRecorder{}

	cancel, done := startRunLoop(consumer, slog.New(recorder), noopHandler)
	<-receiving
	cancel()
	awaitLoopExit(t, done)

	logged := recorder.messagesAt(slog.LevelError)
	if !slices.Contains(logged, "error processing messages") {
		t.Fatalf("expected a genuine receive failure at ERROR, got %v", logged)
	}
}

// TestRunLoop_LogsNestedReceiveDeadlineRacingShutdownAtError guards the receive
// path's silent case: the poll budget expiring as SIGTERM lands means the poll
// was abandoned mid-flight and its message is stranded for the visibility
// timeout, so it is a real failure and nothing below the loop logs it.
func TestRunLoop_LogsNestedReceiveDeadlineRacingShutdownAtError(t *testing.T) {
	receiving := make(chan struct{}, 1)
	shutdownLanded := make(chan struct{})
	consumer := &mockConsumer{
		receive: receiveRacingShutdown(receiving, shutdownLanded,
			fmt.Errorf("failed to receive messages: %w", context.DeadlineExceeded)),
	}
	recorder := &levelRecorder{}

	cancel, done := startRunLoop(consumer, slog.New(recorder), noopHandler)
	awaitSignal(t, receiving, "the receive to start")
	cancel()
	close(shutdownLanded)
	awaitLoopExit(t, done)

	logged := recorder.messagesAt(slog.LevelError)
	if !slices.Contains(logged, "error processing messages") {
		t.Fatalf("expected a nested receive deadline racing shutdown at ERROR, got %v", logged)
	}
}

func TestRunLoop_LogsBadVisibilityTimeout(t *testing.T) {
	// Visibility (60s) below the default handler budget (120s) is logged as an
	// error at startup but does not stop the loop.
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError}))
	consumer := &mockConsumer{visibilityTimeout: 60 * time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // exit the loop immediately after the startup check

	RunLoop(ctx, Config{
		Consumer:     consumer,
		MaxMessages:  10,
		PollInterval: 50 * time.Millisecond,
		Logger:       logger,
		ChainID:      1,
	}, func(_ context.Context, _ outbound.BlockEvent) error { return nil })

	if !strings.Contains(buf.String(), "visibility") {
		t.Errorf("expected a visibility-misconfiguration error to be logged, got: %q", buf.String())
	}
}

func TestValidateVisibilityTimeout(t *testing.T) {
	tests := []struct {
		name       string
		visibility time.Duration
		handler    time.Duration
		wantErr    bool
	}{
		{"above default budget", 180 * time.Second, 0, false},
		{"equal to default budget rejected", DefaultHandlerTimeout, 0, true},
		{"below default budget rejected", 60 * time.Second, 0, true},
		{"above explicit budget", 90 * time.Second, 60 * time.Second, false},
		{"below explicit budget rejected", 30 * time.Second, 60 * time.Second, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateVisibilityTimeout(tt.visibility, tt.handler)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateVisibilityTimeout(%v, %v) error = %v, wantErr %v",
					tt.visibility, tt.handler, err, tt.wantErr)
			}
		})
	}
}

func TestProcessMessages_AppliesHandlerDeadline(t *testing.T) {
	event := outbound.BlockEvent{ChainID: 1, BlockNumber: 100, Version: 0, BlockHash: "0xabc"}
	consumer := &mockConsumer{
		batches: [][]outbound.SQSMessage{{makeMsg("1", "h1", event)}},
	}

	var deadline time.Time
	var hadDeadline bool
	handler := func(ctx context.Context, _ outbound.BlockEvent) error {
		deadline, hadDeadline = ctx.Deadline()
		return nil
	}

	cfg := testConfig(consumer)
	cfg.HandlerTimeout = time.Second

	if err := ProcessMessages(context.Background(), cfg, handler); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !hadDeadline {
		t.Fatal("expected handler context to carry a deadline when HandlerTimeout is set")
	}
	// The deadline must reflect the configured HandlerTimeout, not the default
	// or a leaked parent deadline.
	if remaining := time.Until(deadline); remaining <= 0 || remaining > time.Second {
		t.Fatalf("expected deadline within ~1s (HandlerTimeout), got %v remaining", remaining)
	}
}

func TestProcessMessages_DefaultHandlerDeadlineApplied(t *testing.T) {
	event := outbound.BlockEvent{ChainID: 1, BlockNumber: 100, Version: 0, BlockHash: "0xabc"}
	consumer := &mockConsumer{
		batches: [][]outbound.SQSMessage{{makeMsg("1", "h1", event)}},
	}

	var hadDeadline bool
	handler := func(ctx context.Context, _ outbound.BlockEvent) error {
		_, hadDeadline = ctx.Deadline()
		return nil
	}

	// testConfig leaves HandlerTimeout unset (0): the loop must still bound the
	// handler with the package default, never run it unbounded.
	if err := ProcessMessages(context.Background(), testConfig(consumer), handler); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !hadDeadline {
		t.Fatal("expected default handler deadline to be applied when HandlerTimeout is 0")
	}
}

// TestProcessMessages_CancelsSlowHandlerAndKeepsMessage is the core resilience
// invariant from the 2026-06-18 lock-convoy incident: a handler that exceeds
// its budget is cancelled and its message is left undeleted so SQS redelivers
// it, instead of the goroutine parking forever and silently stalling the queue.
func TestProcessMessages_CancelsSlowHandlerAndKeepsMessage(t *testing.T) {
	event := outbound.BlockEvent{ChainID: 1, BlockNumber: 100, Version: 0, BlockHash: "0xabc"}
	consumer := &mockConsumer{
		batches: [][]outbound.SQSMessage{{makeMsg("1", "h1", event)}},
	}

	handler := func(ctx context.Context, _ outbound.BlockEvent) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
			return nil
		}
	}

	cfg := testConfig(consumer)
	cfg.HandlerTimeout = 20 * time.Millisecond

	err := ProcessMessages(context.Background(), cfg, handler)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded when handler exceeds HandlerTimeout, got %v", err)
	}
	if len(consumer.deletedHandles) != 0 {
		t.Errorf("expected message kept (not deleted) on handler timeout, got deletes: %v", consumer.deletedHandles)
	}
}

// TestProcessMessages_LogsWhenHandlerIgnoresDeadline covers a handler that does
// not honour its context: it runs past the budget but returns nil. The work
// completed, so the message is still deleted, but the budget breach is logged
// so the misbehaving handler is visible.
func TestProcessMessages_LogsWhenHandlerIgnoresDeadline(t *testing.T) {
	event := outbound.BlockEvent{ChainID: 1, BlockNumber: 100, Version: 0, BlockHash: "0xabc"}
	consumer := &mockConsumer{
		batches: [][]outbound.SQSMessage{{makeMsg("1", "h1", event)}},
	}

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	handler := func(_ context.Context, _ outbound.BlockEvent) error {
		time.Sleep(40 * time.Millisecond) // ignores ctx, runs past the budget
		return nil
	}

	cfg := testConfig(consumer)
	cfg.Logger = logger
	cfg.HandlerTimeout = 10 * time.Millisecond

	if err := ProcessMessages(context.Background(), cfg, handler); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(consumer.deletedHandles) != 1 {
		t.Errorf("expected message deleted when handler returns nil, got deletes: %v", consumer.deletedHandles)
	}
	if !strings.Contains(buf.String(), "exceeding its timeout budget") {
		t.Errorf("expected budget-exceeded warning, got logs: %q", buf.String())
	}
}

// TestProcessMessages_DrainsInFlightHandlerOnShutdown pins that SIGTERM
// mid-message leaves the handler a live context and still deletes its message:
// cancelling it would hold the message for the queue's visibility timeout and
// stall the FIFO group behind it.
func TestProcessMessages_DrainsInFlightHandlerOnShutdown(t *testing.T) {
	consumer := &mockConsumer{
		batches: [][]outbound.SQSMessage{{makeMsg("1", "h1", blockEvent(100))}},
	}
	cfg, recorder := recordingConfig(consumer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handling := make(chan struct{}, 1)
	shutdownRequested := make(chan struct{})
	var handlerCtxErr error
	handler := func(hctx context.Context, _ outbound.BlockEvent) error {
		signalOnce(handling)
		<-shutdownRequested
		handlerCtxErr = hctx.Err()
		return nil
	}
	go func() {
		<-handling
		cancel()
		close(shutdownRequested)
	}()

	if err := ProcessMessages(ctx, cfg, handler); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if handlerCtxErr != nil {
		t.Errorf("expected the in-flight handler to keep a live context across shutdown, got %v", handlerCtxErr)
	}
	if got := consumer.deleted(); !slices.Equal(got, []string{"h1"}) {
		t.Errorf("expected the drained message to be deleted, got deletes %v", got)
	}
	if got := consumer.released(); len(got) != 0 {
		t.Errorf("expected no visibility change for a completed message, got %v", got)
	}
	assertNoErrorLogs(t, recorder)
}

// TestProcessMessages_ReleasesMessageWhenDrainBudgetExpires covers the handler
// that cannot finish inside the shutdown window: the message must go back to
// the queue immediately instead of staying hidden until the visibility timeout.
func TestProcessMessages_ReleasesMessageWhenDrainBudgetExpires(t *testing.T) {
	consumer := &mockConsumer{
		batches: [][]outbound.SQSMessage{{makeMsg("1", "h1", blockEvent(100))}},
	}
	cfg, recorder := recordingConfig(consumer)
	cfg.DrainTimeout = 20 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handling := make(chan struct{}, 1)
	handlerReturned := make(chan struct{})
	handler := func(hctx context.Context, _ outbound.BlockEvent) error {
		defer close(handlerReturned)
		signalOnce(handling)
		<-hctx.Done()
		return hctx.Err()
	}
	go func() {
		<-handling
		cancel()
	}()

	err := ProcessMessages(ctx, cfg, handler)
	if !isShutdownCancellation(ctx, err) {
		t.Fatalf("expected an expired drain to read as shutdown cancellation, got %v", err)
	}
	awaitSignal(t, handlerReturned, "the abandoned handler to observe cancellation")

	if got := consumer.deleted(); len(got) != 0 {
		t.Errorf("expected no delete for an unfinished handler, got deletes %v", got)
	}
	want := []visibilityChange{{handle: "h1", visibility: 0}}
	if got := consumer.released(); !slices.Equal(got, want) {
		t.Errorf("expected the message released for the successor, got %v", got)
	}
	assertNoErrorLogs(t, recorder)
}

// TestProcessMessages_ReleasesUnstartedBatchRemainderOnShutdown covers messages
// the batch never started: they were received (hidden) but will not run, so
// they must be handed straight back rather than held for the visibility
// timeout.
func TestProcessMessages_ReleasesUnstartedBatchRemainderOnShutdown(t *testing.T) {
	consumer := &mockConsumer{
		batches: [][]outbound.SQSMessage{{
			makeMsg("1", "h1", blockEvent(100)),
			makeMsg("2", "h2", blockEvent(101)),
		}},
	}
	cfg, recorder := recordingConfig(consumer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var handled []int64
	handler := func(_ context.Context, event outbound.BlockEvent) error {
		handled = append(handled, event.BlockNumber)
		cancel()
		return nil
	}

	if err := ProcessMessages(ctx, cfg, handler); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !slices.Equal(handled, []int64{100}) {
		t.Errorf("expected shutdown to stop the batch after block 100, handled %v", handled)
	}
	if got := consumer.deleted(); !slices.Equal(got, []string{"h1"}) {
		t.Errorf("expected only the finished message deleted, got deletes %v", got)
	}
	want := []visibilityChange{{handle: "h2", visibility: 0}}
	if got := consumer.released(); !slices.Equal(got, want) {
		t.Errorf("expected the unstarted message released for the successor, got %v", got)
	}
	assertNoErrorLogs(t, recorder)
}

// TestProcessMessages_ReleasesBatchThatArrivesDuringShutdown covers the poll
// that completes after SIGTERM: the consumer never abandons an in-flight
// receive, so a batch can land with the loop already shutting down. None of it
// may be processed, and every message must go straight back to the queue or the
// successor's FIFO group stalls for the visibility timeout. This is the i == 0
// case of the loop's shutdown guard, the single owner of that rule.
func TestProcessMessages_ReleasesBatchThatArrivesDuringShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumer := &mockConsumer{
		receive: func(context.Context, int) ([]outbound.SQSMessage, error) {
			cancel() // the poll outlives the shutdown signal, then delivers
			return []outbound.SQSMessage{
				makeMsg("1", "h1", blockEvent(100)),
				makeMsg("2", "h2", blockEvent(101)),
			}, nil
		},
	}
	cfg, recorder := recordingConfig(consumer)

	var handled []int64
	handler := func(_ context.Context, event outbound.BlockEvent) error {
		handled = append(handled, event.BlockNumber)
		return nil
	}

	if err := ProcessMessages(ctx, cfg, handler); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(handled) != 0 {
		t.Errorf("expected no message processed after shutdown began, handled %v", handled)
	}
	if got := consumer.deleted(); len(got) != 0 {
		t.Errorf("expected no deletes, got %v", got)
	}
	want := []visibilityChange{{handle: "h1", visibility: 0}, {handle: "h2", visibility: 0}}
	if got := consumer.released(); !slices.Equal(got, want) {
		t.Errorf("expected the whole batch released for the successor, got %v", got)
	}
	assertNoErrorLogs(t, recorder)
}

// TestProcessMessages_ReleasesWholeBatchInOneQueueCall pins the shutdown
// budget: a queue call per message lets a full batch of 10 share one
// ShutdownCleanupTimeout between 10 retryable calls, so one throttled release
// starves the rest and strands them for the visibility timeout.
func TestProcessMessages_ReleasesWholeBatchInOneQueueCall(t *testing.T) {
	const batchSize = 10
	batch := make([]outbound.SQSMessage, 0, batchSize)
	for i := range batchSize {
		batch = append(batch, makeMsg(strconv.Itoa(i), fmt.Sprintf("h%d", i), blockEvent(int64(100+i))))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumer := &mockConsumer{
		receive: func(context.Context, int) ([]outbound.SQSMessage, error) {
			cancel()
			return batch, nil
		},
	}
	cfg, _ := recordingConfig(consumer)

	if err := ProcessMessages(ctx, cfg, noopHandler); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	calls := consumer.releaseCalls()
	if len(calls) != 1 {
		t.Fatalf("expected the batch released in one queue call, got %d", len(calls))
	}
	if got := len(calls[0].handles); got != batchSize {
		t.Errorf("expected all %d messages in that call, got %d", batchSize, got)
	}
}

// TestProcessMessages_HandlerFailureLeavesVisibilityUntouched pins the
// non-shutdown failure path: the queue's visibility timeout is the deliberate
// retry pacing for a poison pill, so a failing handler must not shorten it.
func TestProcessMessages_HandlerFailureLeavesVisibilityUntouched(t *testing.T) {
	consumer := &mockConsumer{
		batches: [][]outbound.SQSMessage{{makeMsg("1", "h1", blockEvent(100))}},
	}
	cfg, recorder := recordingConfig(consumer)

	handler := func(context.Context, outbound.BlockEvent) error {
		return errors.New("persisting block: connection reset")
	}

	if err := ProcessMessages(context.Background(), cfg, handler); err == nil {
		t.Fatal("expected the handler failure to be returned")
	}
	if got := consumer.deleted(); len(got) != 0 {
		t.Errorf("expected no delete on handler failure, got deletes %v", got)
	}
	if got := consumer.released(); len(got) != 0 {
		t.Errorf("expected the retry backoff left intact, got visibility changes %v", got)
	}
	if logged := recorder.messagesAt(slog.LevelError); !slices.Contains(logged, "failed to process message") {
		t.Errorf("expected a genuine handler failure at ERROR, got %v", logged)
	}
}

// TestProcessMessages_GenuineFailureAtShutdownStillLogsError is the counterpart
// to the quiet drain paths: a real failure racing SIGTERM must still surface at
// ERROR, while its message is released like any other unfinished one.
func TestProcessMessages_GenuineFailureAtShutdownStillLogsError(t *testing.T) {
	consumer := &mockConsumer{
		batches: [][]outbound.SQSMessage{{makeMsg("1", "h1", blockEvent(100))}},
	}
	cfg, recorder := recordingConfig(consumer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handling := make(chan struct{}, 1)
	shutdownRequested := make(chan struct{})
	handler := func(context.Context, outbound.BlockEvent) error {
		signalOnce(handling)
		<-shutdownRequested
		return errors.New("persisting block: connection reset")
	}
	go func() {
		<-handling
		cancel()
		close(shutdownRequested)
	}()

	if err := ProcessMessages(ctx, cfg, handler); err == nil {
		t.Fatal("expected the handler failure to be returned")
	}
	if got := consumer.deleted(); len(got) != 0 {
		t.Errorf("expected no delete on handler failure, got deletes %v", got)
	}
	want := []visibilityChange{{handle: "h1", visibility: 0}}
	if got := consumer.released(); !slices.Equal(got, want) {
		t.Errorf("expected the failed message released for the successor, got %v", got)
	}
	if logged := recorder.messagesAt(slog.LevelError); !slices.Contains(logged, "failed to process message") {
		t.Errorf("expected a genuine handler failure at ERROR, got %v", logged)
	}
}

// TestRunLoop_DrainsInFlightMessageOnShutdown is the end-to-end shape of the
// rollout: cancel the loop while a handler is running, and the loop finishes
// that message, deletes it and exits without an ERROR record.
func TestRunLoop_DrainsInFlightMessageOnShutdown(t *testing.T) {
	consumer := &mockConsumer{
		batches: [][]outbound.SQSMessage{{makeMsg("1", "h1", blockEvent(100))}},
	}
	recorder := &levelRecorder{}

	handling := make(chan struct{}, 1)
	shutdownRequested := make(chan struct{})
	handler := func(context.Context, outbound.BlockEvent) error {
		signalOnce(handling)
		<-shutdownRequested
		return nil
	}

	cancel, done := startRunLoop(consumer, slog.New(recorder), handler)
	awaitSignal(t, handling, "the handler to start")
	cancel()
	close(shutdownRequested)
	awaitLoopExit(t, done)

	if got := consumer.deleted(); !slices.Equal(got, []string{"h1"}) {
		t.Errorf("expected the drained message to be deleted, got deletes %v", got)
	}
	assertNoErrorLogs(t, recorder)
}

// TestRunLoop_LogsNestedDeadlineRacingShutdownAtError pins the one deadline the
// shutdown path must not swallow. The loop's context is a signal.NotifyContext
// and never carries a deadline, so a DeadlineExceeded arriving with SIGTERM came
// from a nested budget (handler timeout, RPC or DB per-attempt deadline) and is
// a real failure.
func TestRunLoop_LogsNestedDeadlineRacingShutdownAtError(t *testing.T) {
	consumer := &mockConsumer{
		batches: [][]outbound.SQSMessage{{makeMsg("1", "h1", blockEvent(100))}},
	}
	recorder := &levelRecorder{}

	handling := make(chan struct{}, 1)
	shutdownRequested := make(chan struct{})
	handler := func(context.Context, outbound.BlockEvent) error {
		signalOnce(handling)
		<-shutdownRequested
		return fmt.Errorf("reading pool state: %w", context.DeadlineExceeded)
	}

	cancel, done := startRunLoop(consumer, slog.New(recorder), handler)
	awaitSignal(t, handling, "the handler to start")
	cancel()
	close(shutdownRequested)
	awaitLoopExit(t, done)

	if logged := recorder.messagesAt(slog.LevelError); !slices.Contains(logged, "error processing messages") {
		t.Fatalf("expected a nested deadline racing shutdown at ERROR, got %v", logged)
	}
}

// TestProcessMessages_ReleasesAnUnsettledMessageWhenShutdownLandsLaterInTheBatch
// covers the message the shutdown guard cannot see: one left in flight while
// the context was still live, with SIGTERM landing during a later message's
// handler. Only the messages the loop never reached were handed back, so the
// earlier one stalled its FIFO group for the whole visibility timeout.
func TestProcessMessages_ReleasesAnUnsettledMessageWhenShutdownLandsLaterInTheBatch(t *testing.T) {
	malformed := outbound.SQSMessage{MessageID: "1", ReceiptHandle: "h1", Body: "{not json"}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumer := &mockConsumer{
		batches: [][]outbound.SQSMessage{{malformed, makeMsg("2", "h2", blockEvent(101))}},
	}
	cfg, _ := recordingConfig(consumer)

	handler := func(context.Context, outbound.BlockEvent) error {
		cancel()
		return nil
	}

	if err := ProcessMessages(ctx, cfg, handler); err == nil {
		t.Fatal("expected the parse failure returned")
	}
	if got := consumer.deleted(); !slices.Equal(got, []string{"h2"}) {
		t.Errorf("expected only the parsed message deleted, got %v", got)
	}
	want := []visibilityChange{{handle: "h1", visibility: 0}}
	if got := consumer.released(); !slices.Equal(got, want) {
		t.Errorf("expected the unparseable message released for the successor, got %v", got)
	}
}

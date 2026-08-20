package sqsutil

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// visibilityChange records one ChangeMessageVisibility call.
type visibilityChange struct {
	handle     string
	visibility time.Duration
}

// mockConsumer implements outbound.SQSConsumer for testing.
type mockConsumer struct {
	mu                  sync.Mutex
	batches             [][]outbound.SQSMessage // each call to ReceiveMessages pops one batch
	deletedHandles      []string
	visibilityChanges   []visibilityChange
	visibilityDeadlines []time.Time
	deleteErr           error
	visibilityTimeout   time.Duration // 0 -> a safe default well above the handler budget

	// receive, when set, replaces the batch-popping behaviour so a test can
	// drive ReceiveMessages failures.
	receive func(ctx context.Context, maxMessages int) ([]outbound.SQSMessage, error)
}

func (m *mockConsumer) VisibilityTimeout() time.Duration {
	if m.visibilityTimeout > 0 {
		return m.visibilityTimeout
	}
	return 300 * time.Second
}

func (m *mockConsumer) ReceiveMessages(ctx context.Context, maxMessages int) ([]outbound.SQSMessage, error) {
	if m.receive != nil {
		return m.receive(ctx, maxMessages)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.batches) == 0 {
		return nil, nil
	}
	batch := m.batches[0]
	m.batches = m.batches[1:]
	return batch, nil
}

// DeleteMessage refuses a cancelled context the way the AWS SDK does, so a
// test catches cleanup that is still riding the shutdown-cancelled context.
func (m *mockConsumer) DeleteMessage(ctx context.Context, handle string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deletedHandles = append(m.deletedHandles, handle)
	return m.deleteErr
}

func (m *mockConsumer) ChangeMessageVisibility(ctx context.Context, handle string, visibility time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	deadline, _ := ctx.Deadline()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.visibilityChanges = append(m.visibilityChanges, visibilityChange{handle: handle, visibility: visibility})
	m.visibilityDeadlines = append(m.visibilityDeadlines, deadline)
	return nil
}

func (m *mockConsumer) Close() error { return nil }

func (m *mockConsumer) deleted() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.deletedHandles)
}

func (m *mockConsumer) released() []visibilityChange {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.visibilityChanges)
}

func (m *mockConsumer) releaseDeadlines() []time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.visibilityDeadlines)
}

func makeMsg(id, handle string, event outbound.BlockEvent) outbound.SQSMessage {
	body, _ := json.Marshal(event)
	return outbound.SQSMessage{MessageID: id, ReceiptHandle: handle, Body: string(body)}
}

func testConfig(consumer *mockConsumer) Config {
	return Config{
		Consumer:    consumer,
		MaxMessages: 10,
		Logger:      slog.Default(),
		ChainID:     1,
	}
}

// levelRecorder is a slog.Handler that keeps every record so a test can assert
// on the level a message was logged at, not just on the text.
type levelRecorder struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *levelRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (h *levelRecorder) Handle(_ context.Context, record slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, record.Clone())
	return nil
}

func (h *levelRecorder) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *levelRecorder) WithGroup(string) slog.Handler { return h }

func (h *levelRecorder) messagesAt(level slog.Level) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var messages []string
	for _, record := range h.records {
		if record.Level == level {
			messages = append(messages, record.Message)
		}
	}
	return messages
}

// startRunLoop runs RunLoop on its own goroutine, returning the shutdown
// trigger and a channel closed once the loop has exited.
func startRunLoop(consumer *mockConsumer, logger *slog.Logger, handler BlockEventHandler) (context.CancelFunc, <-chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunLoop(ctx, Config{
			Consumer:     consumer,
			MaxMessages:  10,
			PollInterval: 10 * time.Millisecond,
			Logger:       logger,
			ChainID:      1,
		}, handler)
	}()
	return cancel, done
}

func noopHandler(context.Context, outbound.BlockEvent) error { return nil }

func awaitLoopExit(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunLoop did not return after cancellation")
	}
}

// signalOnce reports entry into a mock receive without blocking or panicking on
// repeated polls.
func signalOnce(ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func blockEvent(number int64) outbound.BlockEvent {
	return outbound.BlockEvent{ChainID: 1, BlockNumber: number, Version: 0, BlockHash: "0xabc"}
}

// recordingConfig pairs a test Config with the recorder behind its logger, so a
// test can assert on the level every shutdown-path record was logged at.
func recordingConfig(consumer *mockConsumer) (Config, *levelRecorder) {
	recorder := &levelRecorder{}
	cfg := testConfig(consumer)
	cfg.Logger = slog.New(recorder)
	return cfg, recorder
}

func awaitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func assertNoErrorLogs(t *testing.T, recorder *levelRecorder) {
	t.Helper()
	if logged := recorder.messagesAt(slog.LevelError); len(logged) > 0 {
		t.Errorf("expected no ERROR records on the shutdown path, got %v", logged)
	}
}

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

// TestRunLoop_ShutdownCancellationIsNotAnError covers the SIGTERM path: the
// in-flight ReceiveMessages is cancelled with the loop's context, which is a
// clean shutdown, not a failure. Logging it at ERROR made every rollout of
// every SQS worker feed the error-rate alerts.
func TestRunLoop_ShutdownCancellationIsNotAnError(t *testing.T) {
	receiving := make(chan struct{}, 1)
	consumer := &mockConsumer{
		receive: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
			signalOnce(receiving)
			<-ctx.Done()
			return nil, fmt.Errorf("receiving from SQS: %w", ctx.Err())
		},
	}
	recorder := &levelRecorder{}

	cancel, done := startRunLoop(consumer, slog.New(recorder), noopHandler)
	<-receiving
	cancel()
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
// path's silent case: an SDK per-attempt deadline firing as SIGTERM lands is a
// real failure, and nothing below the loop logs it.
func TestRunLoop_LogsNestedReceiveDeadlineRacingShutdownAtError(t *testing.T) {
	receiving := make(chan struct{}, 1)
	consumer := &mockConsumer{
		receive: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
			signalOnce(receiving)
			<-ctx.Done()
			return nil, fmt.Errorf("receiving from SQS: %w", context.DeadlineExceeded)
		},
	}
	recorder := &levelRecorder{}

	cancel, done := startRunLoop(consumer, slog.New(recorder), noopHandler)
	awaitSignal(t, receiving, "the receive to start")
	cancel()
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

// TestProcessMessages_DrainsInFlightHandlerOnShutdown covers the rollout
// blackout measured in kind: SIGTERM mid-message used to cancel the handler,
// leaving the message in flight for the queue's visibility timeout (300s) and
// starving the successor pod, which on a FIFO queue is the chain's whole block
// stream. The handler must instead keep a live context and its message must be
// deleted.
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
// successor's FIFO group stalls for the visibility timeout.
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

// TestProcessMessages_ReleasesWholeBatchWithinOneCleanupBudget pins the
// shutdown budget: a cleanup context per message lets a full batch of 10 need
// 10x ShutdownCleanupTimeout, which no longer fits lifecycle.ShutdownTimeout.
func TestProcessMessages_ReleasesWholeBatchWithinOneCleanupBudget(t *testing.T) {
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

	deadlines := consumer.releaseDeadlines()
	if len(deadlines) != batchSize {
		t.Fatalf("expected %d releases, got %d", batchSize, len(deadlines))
	}
	for i, deadline := range deadlines {
		if !deadline.Equal(deadlines[0]) {
			t.Errorf("release %d got its own cleanup budget (deadline %s, first %s); the batch must share one",
				i, deadline, deadlines[0])
		}
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

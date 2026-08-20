package sqsutil

import (
	"context"
	"encoding/json"
	"log/slog"
	"slices"
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
	visibilityErr       error
	visibilityTimeout   time.Duration // 0 -> a safe default well above the handler budget

	// receive, when set, replaces the batch-popping behaviour so a test can
	// drive ReceiveMessages failures.
	receive func(ctx context.Context, maxMessages int) ([]outbound.SQSMessage, error)

	// beforeDelete, when set, runs at the start of DeleteMessage so a test can
	// land a shutdown with the delete in flight.
	beforeDelete func()
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
	if m.beforeDelete != nil {
		m.beforeDelete()
	}
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
	return m.visibilityErr
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

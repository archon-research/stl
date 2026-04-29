package sqsutil

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// mockConsumer implements outbound.SQSConsumer for testing.
type mockConsumer struct {
	mu             sync.Mutex
	batches        [][]outbound.SQSMessage // each call to ReceiveMessages pops one batch
	deletedHandles []string
	deleteErr      error
}

func (m *mockConsumer) ReceiveMessages(_ context.Context, _ int) ([]outbound.SQSMessage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.batches) == 0 {
		return nil, nil
	}
	batch := m.batches[0]
	m.batches = m.batches[1:]
	return batch, nil
}

func (m *mockConsumer) DeleteMessage(_ context.Context, handle string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deletedHandles = append(m.deletedHandles, handle)
	return m.deleteErr
}

func (m *mockConsumer) Close() error { return nil }

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

	// Should return quickly without blocking
	RunLoop(ctx, Config{
		Consumer:     consumer,
		MaxMessages:  10,
		PollInterval: 50 * time.Millisecond,
		Logger:       slog.Default(),
	}, handler)
	// If we get here without hanging, test passes
}

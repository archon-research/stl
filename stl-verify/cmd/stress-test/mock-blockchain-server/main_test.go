package main

import (
	"context"
	"testing"
	"time"
)

// TestRun_StartStop verifies that run starts successfully and shuts down cleanly when the context is cancelled.
func TestRun_StartStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() {
		errCh <- run(ctx, ":0")
	}()

	// Give the server a moment to start before cancelling.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for run to return after cancel")
	}
}

// TestRun_BadAddr verifies that run returns an error when given an invalid address.
func TestRun_BadAddr(t *testing.T) {
	ctx := context.Background()
	err := run(ctx, "invalid:addr:extra")
	if err == nil {
		t.Fatal("expected error for invalid address, got nil")
	}
}

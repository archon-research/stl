package main

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// TestRun_StartStop verifies that run starts successfully and shuts down cleanly when the context is cancelled.
func TestRun_StartStop(t *testing.T) {
	// Reserve a free port, release it, then pass it to run so we can poll for readiness.
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	addr := fmt.Sprintf(":%d", port)
	dialAddr := fmt.Sprintf("localhost:%d", port)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() {
		errCh <- run(ctx, addr)
	}()

	testutil.WaitForCondition(t, 2*time.Second, func() bool {
		conn, err := net.DialTimeout("tcp", dialAddr, 10*time.Millisecond)
		if err != nil {
			return false
		}
		conn.Close()
		return true
	}, "server ready")

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

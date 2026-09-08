package testutil

import (
	"context"
	"encoding/hex"
	"io"
	"log/slog"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// E18 returns n * 10^18 as a *big.Int.
func E18(n int64) *big.Int {
	return new(big.Int).Mul(big.NewInt(n), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
}

// DiscardLogger returns an slog.Logger that writes to io.Discard.
func DiscardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// WaitForCondition polls condition every 10ms until it returns true or timeout expires.
func WaitForCondition(t *testing.T, timeout time.Duration, condition func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", description)
}

// WorkerStartupTimeout bounds a worker binary's boot in an integration test.
const WorkerStartupTimeout = 30 * time.Second

// WaitForFirstPoll blocks until the worker's first SQS receive lands. Reading
// errCh is what makes a boot refusal visible: a binary that returns from run()
// before polling otherwise reports a bare timeout with none of its own error.
func WaitForFirstPoll(t *testing.T, errCh <-chan error, firstCall <-chan struct{}) {
	t.Helper()
	select {
	case <-firstCall:
	case err := <-errCh:
		t.Fatalf("run returned before polling SQS: %v", err)
	case <-time.After(WorkerStartupTimeout):
		t.Fatal("timed out waiting for the worker to start polling SQS")
	}
}

// WaitForWorkerCondition is WaitForCondition for a worker binary under test: it
// fails with the binary's own error when run() returns before the condition
// holds, rather than waiting out the deadline with nothing to report.
func WaitForWorkerCondition(t *testing.T, errCh <-chan error, timeout time.Duration, condition func() bool, description string) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		if condition() {
			return
		}
		select {
		case err := <-errCh:
			t.Fatalf("run returned before %s: %v", description, err)
		case <-deadline:
			t.Fatalf("timeout waiting for: %s", description)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// WaitFor polls condition with a ticker until it returns true or the timeout expires.
// Returns true if the condition was met, false on timeout.
func WaitFor(t *testing.T, timeout time.Duration, interval time.Duration, condition func() bool) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Check immediately before first tick.
	if condition() {
		return true
	}

	for {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
			if condition() {
				return true
			}
		}
	}
}

// DisableAllOracles disables all migration-seeded oracles for test isolation.
func DisableAllOracles(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `UPDATE oracle SET enabled = false`); err != nil {
		t.Fatalf("disable seeded oracles: %v", err)
	}
}

// HexToBytes converts a hex string (with or without 0x prefix) to bytes.
func HexToBytes(s string) ([]byte, error) {
	s = strings.TrimPrefix(s, "0x")
	s = strings.TrimPrefix(s, "0X")
	return hex.DecodeString(s)
}

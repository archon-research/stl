package testutil

import (
	"context"
	"encoding/hex"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

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
// Call this after SetupTimescaleDB when creating custom test oracles.
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

package testutil

import (
	"context"
	"testing"
	"time"
)

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

package testutil

import (
	"encoding/hex"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
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

// HexToBytes converts a hex string (with or without 0x prefix) to bytes.
func HexToBytes(s string) ([]byte, error) {
	s = strings.TrimPrefix(s, "0x")
	s = strings.TrimPrefix(s, "0X")
	return hex.DecodeString(s)
}

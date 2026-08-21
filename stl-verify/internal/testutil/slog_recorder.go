package testutil

import (
	"context"
	"log/slog"
	"strings"
	"sync"
)

// SlogRecorder is a slog.Handler that captures records so tests can assert
// on emitted logs. It locks, so a test may drive it from several goroutines
// (e.g. a worker pool logging its shutdown path).
type SlogRecorder struct {
	mu      sync.Mutex
	Records []slog.Record
}

func (h *SlogRecorder) Enabled(context.Context, slog.Level) bool { return true }
func (h *SlogRecorder) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.Records = append(h.Records, r.Clone())
	return nil
}
func (h *SlogRecorder) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *SlogRecorder) WithGroup(string) slog.Handler      { return h }

// CountWarn returns how many captured warn-level records contain substr in
// their message.
func (h *SlogRecorder) CountWarn(substr string) int {
	return h.count(slog.LevelWarn, substr)
}

// MessagesAt returns the messages of every captured record at the given level.
func (h *SlogRecorder) MessagesAt(level slog.Level) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var messages []string
	for _, r := range h.Records {
		if r.Level == level {
			messages = append(messages, r.Message)
		}
	}
	return messages
}

func (h *SlogRecorder) count(level slog.Level, substr string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, r := range h.Records {
		if r.Level == level && strings.Contains(r.Message, substr) {
			n++
		}
	}
	return n
}

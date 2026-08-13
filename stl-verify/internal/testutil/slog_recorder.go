package testutil

import (
	"context"
	"log/slog"
	"strings"
)

// SlogRecorder is a slog.Handler that captures records so tests can assert
// on emitted logs. Tests using it run their logging sequentially, so it
// needs no locking.
type SlogRecorder struct{ Records []slog.Record }

func (h *SlogRecorder) Enabled(context.Context, slog.Level) bool { return true }
func (h *SlogRecorder) Handle(_ context.Context, r slog.Record) error {
	h.Records = append(h.Records, r)
	return nil
}
func (h *SlogRecorder) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *SlogRecorder) WithGroup(string) slog.Handler      { return h }

// CountWarn returns how many captured warn-level records contain substr in
// their message.
func (h *SlogRecorder) CountWarn(substr string) int {
	n := 0
	for _, r := range h.Records {
		if r.Level == slog.LevelWarn && strings.Contains(r.Message, substr) {
			n++
		}
	}
	return n
}

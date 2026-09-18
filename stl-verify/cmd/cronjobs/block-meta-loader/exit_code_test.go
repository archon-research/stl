package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// A drain reaches this process as a cancelled context. Treated as a failure it consumes a restart
// budget, so two drains in a long run would leave one real attempt.
func TestExitCode(t *testing.T) {
	for _, c := range []struct {
		name string
		err  error
		want int
	}{
		{"success", nil, 0},
		{"cancelled by a signal", context.Canceled, 0},
		{"cancelled, wrapped", fmt.Errorf("loading pending blocks: %w", context.Canceled), 0},
		{"a real failure", errors.New("connection reset"), 1},
		{"deadline exceeded is not a clean stop", context.DeadlineExceeded, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := exitCode(c.err); got != c.want {
				t.Errorf("exitCode(%v) = %d, want %d", c.err, got, c.want)
			}
		})
	}
}

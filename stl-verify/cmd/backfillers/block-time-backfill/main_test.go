package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun_RejectsArguments pins the env-driven contract: the tool takes no positional
// arguments, so any are rejected rather than silently ignored.
func TestRun_RejectsArguments(t *testing.T) {
	err := run(context.Background(), []string{"unexpected"})
	if err == nil || !strings.Contains(err.Error(), "takes no arguments") {
		t.Fatalf("run(args) = %v, want an error rejecting arguments", err)
	}
}

// TestRun_RequiresDatabaseURL pins that a missing DATABASE_URL fails loudly (a backfill
// that silently ran against no/empty database would report success having done nothing).
func TestRun_RequiresDatabaseURL(t *testing.T) {
	t.Setenv("DATABASE_URL", "") // env.Require treats empty as unset
	err := run(context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), "DATABASE_URL") {
		t.Fatalf("run without DATABASE_URL = %v, want a DATABASE_URL error", err)
	}
}

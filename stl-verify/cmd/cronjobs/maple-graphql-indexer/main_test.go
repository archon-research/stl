package main

import (
	"context"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
)

// Env-validation paths that fail before any database access run as plain
// unit tests; the full wiring is covered by the integration test.

func TestSetupRunner_MissingChainID(t *testing.T) {
	t.Setenv("CHAIN_ID", "")

	_, err := setupRunner(context.Background(), temporal.Dependencies{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "CHAIN_ID") {
		t.Errorf("error %q should mention CHAIN_ID", err.Error())
	}
}

func TestSetupRunner_InvalidChainID(t *testing.T) {
	t.Setenv("CHAIN_ID", "not-a-number")

	_, err := setupRunner(context.Background(), temporal.Dependencies{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "CHAIN_ID must be a valid integer") {
		t.Errorf("error = %q", err.Error())
	}
}

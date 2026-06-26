package main

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
)

// Env-validation paths that resolve before any database access run as plain
// unit tests; the service-wiring path is covered by the integration test
// (main_integration_test.go).

func discardDeps() temporal.Dependencies {
	return temporal.Dependencies{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func TestSetupRunner_RequiresEtherscanKey(t *testing.T) {
	t.Setenv("CHAIN_ID", "1")
	t.Setenv("ETHERSCAN_API_KEY", "")

	_, err := setupRunner(context.Background(), discardDeps())
	if err == nil {
		t.Fatal("missing Etherscan key should error, got nil")
	}
	if !strings.Contains(err.Error(), "ETHERSCAN_API_KEY") {
		t.Errorf("error %q should mention ETHERSCAN_API_KEY", err.Error())
	}
}

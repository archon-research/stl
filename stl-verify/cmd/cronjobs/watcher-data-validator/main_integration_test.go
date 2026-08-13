//go:build integration

package main

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// TestSetupRunner_WiresService covers setupRunner end to end: chain ID resolved,
// Etherscan key required, verifier + block-state repository + data validator
// service all constructed against a real Postgres. It asserts construction only;
// runner.Run is exercised against a mocked canonical source in the data_validator
// service integration tests, since setupRunner builds the verifier against the
// real Etherscan endpoint.
func TestSetupRunner_WiresService(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTimescaleDB(t)
	defer cleanup()

	t.Setenv("CHAIN_ID", "1")
	t.Setenv("ETHERSCAN_API_KEY", "test-key")

	deps := temporal.Dependencies{
		Pool:   pool,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	runner, err := setupRunner(ctx, deps)
	if err != nil {
		t.Fatalf("enabled setupRunner should wire the service, got: %v", err)
	}
	if runner == nil {
		t.Fatal("expected a non-nil runner for an enabled chain")
	}
}

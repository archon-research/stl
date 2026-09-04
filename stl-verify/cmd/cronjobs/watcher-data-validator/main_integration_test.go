//go:build integration

package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var sharedDSN string

func TestMain(m *testing.M) {
	os.Exit(testutil.RunShared(m, testutil.Shared{TimescaleDSN: &sharedDSN}))
}

// TestSetupRunner_WiresService covers setupRunner end to end: chain ID resolved,
// Etherscan key required, verifier + block-state repository + data validator
// service all constructed against a real Postgres. It asserts construction only;
// runner.Run is exercised against a mocked canonical source in the data_validator
// service integration tests, since setupRunner builds the verifier against the
// real Etherscan endpoint.
func TestSetupRunner_WiresService(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	t.Setenv("CHAIN_ID", "1")
	t.Setenv("ETHERSCAN_API_KEY", "test-key")
	t.Setenv("BUILD_GIT_HASH", "test")
	testutil.SetDevIdentity(t)

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

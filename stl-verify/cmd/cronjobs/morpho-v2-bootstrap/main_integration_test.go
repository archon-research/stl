//go:build integration

package main

import (
	"context"
	"log/slog"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// TestSetupRunner_WiresAgainstAMigratedDatabase migrates a fresh DB and builds
// the runner exactly as main() does. It covers the composition root: the build
// registry, the four repositories, the multicall client, the morpho-indexer
// replay service, and the bootstrap service all construct against real schema.
// A missing migration or a repository constructor that rejects the wiring shows
// up here rather than on the first Trigger in the Temporal UI.
func TestSetupRunner_WiresAgainstAMigratedDatabase(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestSchema(t, sharedDSN)
	defer cleanup()

	t.Setenv("BUILD_GIT_HASH", "test")
	t.Setenv("CHAIN_ID", "1")
	t.Setenv("ALCHEMY_API_KEY", "test-key")
	// Dialing an HTTP RPC URL does not open a connection, so no node is needed
	// to prove the wiring; the run itself is exercised by the service tests.
	t.Setenv("ALCHEMY_HTTP_URL", "http://127.0.0.1:1/v2")

	runner, err := setupRunner(context.Background(), temporal.Dependencies{Pool: pool, Logger: slog.Default()})
	if err != nil {
		t.Fatalf("setupRunner: %v", err)
	}
	if runner == nil {
		t.Fatal("setupRunner returned a nil runner")
	}
}

// TestSetupRunner_RejectsAnUnsupportedChain: the bootstrap needs a known VaultV2
// factory deploy block to bound its sweep. Starting on a chain without one would
// otherwise mean sweeping from block 0 or silently doing nothing.
func TestSetupRunner_RejectsAnUnsupportedChain(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestSchema(t, sharedDSN)
	defer cleanup()

	t.Setenv("BUILD_GIT_HASH", "test")
	t.Setenv("CHAIN_ID", "8453")
	t.Setenv("ALCHEMY_API_KEY", "test-key")
	t.Setenv("ALCHEMY_HTTP_URL", "http://127.0.0.1:1/v2")

	if _, err := setupRunner(context.Background(), temporal.Dependencies{Pool: pool, Logger: slog.Default()}); err == nil {
		t.Fatal("expected setupRunner to reject a chain with no known VaultV2 factory deploy block")
	}
}

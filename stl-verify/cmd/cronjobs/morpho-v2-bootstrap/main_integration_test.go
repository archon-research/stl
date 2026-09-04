//go:build integration

package main

import (
	"context"
	"log/slog"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/testsuite"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/services/morpho_v2_bootstrap"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// setWorkerEnv installs the environment a deployed pod would have, with the RPC
// fixture answering nodeChain to CHAIN_ID=configuredChain.
func setWorkerEnv(t *testing.T, configuredChain, nodeChain int64) {
	t.Helper()

	t.Setenv("BUILD_GIT_HASH", "test")
	t.Setenv("CHAIN_ID", strconv.FormatInt(configuredChain, 10))
	t.Setenv("ALCHEMY_API_KEY", "test-key")
	t.Setenv("ALCHEMY_HTTP_URL", testutil.StartChainIDRPC(t, nodeChain).URL)
}

// The type name is spelled out rather than read from workflowTypeName: the
// constant would rename both sides together and pin nothing. The activity is
// mocked because a real run sweeps mainnet.
func TestIntegration_Register_RunsTheDocumentedWorkflowTypeWithNoInput(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	t.Cleanup(cleanup)
	setWorkerEnv(t, 1, 1)

	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	deps := temporal.Dependencies{Pool: pool, Logger: slog.Default()}
	if err := register(context.Background(), deps, env); err != nil {
		t.Fatalf("running the production registration: %v", err)
	}
	env.OnActivity("Execute", mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow("MorphoV2Bootstrap")

	if !env.IsWorkflowCompleted() {
		t.Fatal("expected the workflow to complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("running the workflow by its documented type name: %v", err)
	}
	env.AssertExpectations(t)
}

// TestSetupRunner_WiresAgainstAMigratedDatabase migrates a fresh DB and builds
// the runner exactly as the worker does. It covers the composition root: the
// build registry, the four repositories, the multicall client, the morpho-indexer
// replay service, and the bootstrap service all construct against real schema.
// A missing migration or a repository constructor that rejects the wiring shows
// up here rather than on the first hand-started run.
func TestSetupRunner_WiresAgainstAMigratedDatabase(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	setWorkerEnv(t, 1, 1)

	runner, err := setupRunner(context.Background(), temporal.Dependencies{Pool: pool, Logger: slog.Default()}, temporal.NewActivityProgress[morpho_v2_bootstrap.SweepProgress]())
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
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	setWorkerEnv(t, 8453, 8453)

	_, err := setupRunner(context.Background(), temporal.Dependencies{Pool: pool, Logger: slog.Default()}, temporal.NewActivityProgress[morpho_v2_bootstrap.SweepProgress]())
	if err == nil || !strings.Contains(err.Error(), "no known factory deploy block") {
		t.Fatalf("err = %v, want the rejection of a chain with no known VaultV2 factory deploy block", err)
	}
}

// TestSetupRunner_RefusesAChainIDMismatch: a Base pod handed a mainnet URL must
// stop here, before the replay service can write mainnet state under chain 8453.
func TestSetupRunner_RefusesAChainIDMismatch(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	setWorkerEnv(t, 8453, 1)

	_, err := setupRunner(context.Background(), temporal.Dependencies{Pool: pool, Logger: slog.Default()}, temporal.NewActivityProgress[morpho_v2_bootstrap.SweepProgress]())
	if err == nil || !strings.Contains(err.Error(), "RPC chain ID mismatch: RPC reports 1, config says 8453") {
		t.Fatalf("err = %v, want the chain-id mismatch", err)
	}
}

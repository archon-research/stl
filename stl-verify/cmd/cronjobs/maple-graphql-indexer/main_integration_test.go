//go:build integration

package main

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var sharedDSN string

func TestMain(m *testing.M) {
	dsn, cleanup := testutil.StartTimescaleDBForMain()
	sharedDSN = dsn

	code := m.Run()

	cleanup()
	code = testutil.CheckGoroutineLeaks(code)
	os.Exit(code)
}

func TestSetupRunner_WiresService(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestSchema(t, sharedDSN)
	defer cleanup()

	t.Setenv("CHAIN_ID", "1")
	t.Setenv("BUILD_GIT_HASH", "test-hash")

	runner, err := setupRunner(ctx, temporal.Dependencies{Pool: pool})
	if err != nil {
		t.Fatalf("setupRunner: %v", err)
	}
	if runner == nil {
		t.Fatal("runner is nil")
	}
}

func TestSetupRunner_RejectsNonMainnetChain(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestSchema(t, sharedDSN)
	defer cleanup()

	t.Setenv("CHAIN_ID", "137")
	t.Setenv("BUILD_GIT_HASH", "test-hash")

	_, err := setupRunner(ctx, temporal.Dependencies{Pool: pool})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "chainID must be 1") {
		t.Errorf("error = %q", err.Error())
	}
}

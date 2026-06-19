package main

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// Env-validation paths that resolve before any database access run as plain
// unit tests; the enabled path's service wiring is covered by the integration
// test (main_integration_test.go).

func discardDeps() temporal.Dependencies {
	return temporal.Dependencies{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func countInfoMsg(rec *testutil.SlogRecorder, substr string) int {
	n := 0
	for _, r := range rec.Records {
		if r.Level == slog.LevelInfo && strings.Contains(r.Message, substr) {
			n++
		}
	}
	return n
}

func TestSetupRunner_DisabledChainSkipsWithoutEtherscanKey(t *testing.T) {
	t.Setenv("CHAIN_ID", "10")
	t.Setenv("DATA_VALIDATION_ENABLED", "false")
	// ETHERSCAN_API_KEY intentionally unset: a disabled chain must not require it.
	t.Setenv("ETHERSCAN_API_KEY", "")

	runner, err := setupRunner(context.Background(), discardDeps())
	if err != nil {
		t.Fatalf("disabled chain should not error, got: %v", err)
	}
	if runner == nil {
		t.Fatal("expected a no-op runner, got nil")
	}
	if err := runner.Run(context.Background()); err != nil {
		t.Errorf("disabled runner should succeed (no-op), got: %v", err)
	}
}

func TestSetupRunner_DisabledRunnerLogsSkipPerRun(t *testing.T) {
	t.Setenv("CHAIN_ID", "10")
	t.Setenv("DATA_VALIDATION_ENABLED", "false")

	rec := &testutil.SlogRecorder{}
	runner, err := setupRunner(context.Background(), temporal.Dependencies{Logger: slog.New(rec)})
	if err != nil {
		t.Fatalf("setupRunner: %v", err)
	}

	before := countInfoMsg(rec, "skipped")
	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("runner.Run: %v", err)
	}
	if got := countInfoMsg(rec, "skipped") - before; got != 1 {
		t.Errorf("expected 1 skip log emitted during the run, got %d", got)
	}
}

func TestSetupRunner_EnabledByDefaultRequiresEtherscanKey(t *testing.T) {
	t.Setenv("CHAIN_ID", "1")
	// DATA_VALIDATION_ENABLED intentionally unset: defaults to enabled.
	t.Setenv("ETHERSCAN_API_KEY", "")

	_, err := setupRunner(context.Background(), discardDeps())
	if err == nil {
		t.Fatal("enabled chain with no Etherscan key should error, got nil")
	}
	if !strings.Contains(err.Error(), "ETHERSCAN_API_KEY") {
		t.Errorf("error %q should mention ETHERSCAN_API_KEY", err.Error())
	}
}

func TestSetupRunner_InvalidEnabledFlagErrors(t *testing.T) {
	t.Setenv("CHAIN_ID", "1")
	t.Setenv("DATA_VALIDATION_ENABLED", "not-a-bool")

	_, err := setupRunner(context.Background(), discardDeps())
	if err == nil {
		t.Fatal("unparseable DATA_VALIDATION_ENABLED should error, got nil")
	}
	if !strings.Contains(err.Error(), "DATA_VALIDATION_ENABLED") {
		t.Errorf("error %q should mention DATA_VALIDATION_ENABLED", err.Error())
	}
}

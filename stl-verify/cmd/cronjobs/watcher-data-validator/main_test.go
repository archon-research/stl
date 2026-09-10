package main

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/services/data_validator"
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

// TestValidationError_NamesFailingChecks: the runner's error is what the
// Temporal UI shows and what the alert quotes, so it must carry the messages the
// runbook tells the operator to read, not just how many there were.
func TestValidationError_NamesFailingChecks(t *testing.T) {
	report := data_validator.NewReport(1, 100)
	report.AddCheck(data_validator.CheckResult{Name: "Chain Integrity", Status: data_validator.StatusPassed})
	report.AddCheck(data_validator.CheckResult{
		Name:    "Orphan-only heights",
		Status:  data_validator.StatusFailed,
		Message: "14 height(s) have only an orphaned block: 25087888",
	})
	report.Finalize()

	err := validationError(report)
	if err == nil {
		t.Fatal("validationError() = nil, want an error")
	}
	for _, want := range []string{"1 failure", "0 errors", "Orphan-only heights", "25087888"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q should contain %q", err, want)
		}
	}
}

func TestValidationError_NilOnSuccess(t *testing.T) {
	report := data_validator.NewReport(1, 100)
	report.AddCheck(data_validator.CheckResult{Name: "Chain Integrity", Status: data_validator.StatusPassed})
	report.Finalize()

	if err := validationError(report); err != nil {
		t.Errorf("validationError() = %v, want nil", err)
	}
}

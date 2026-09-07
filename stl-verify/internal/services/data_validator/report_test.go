package data_validator

import "testing"

func TestReport_SkippedDoesNotFailRun(t *testing.T) {
	r := NewReport(0, 10)
	r.AddCheck(CheckResult{Name: "spot", Status: StatusPassed})
	r.AddCheck(CheckResult{Name: "spot2", Status: StatusSkipped})
	r.Finalize()

	if r.Skipped != 1 {
		t.Fatalf("Skipped = %d, want 1", r.Skipped)
	}
	if !r.Success() {
		t.Fatalf("Success() = false, want true (skipped must not fail the run)")
	}
}

func TestReport_FailedFailsRun(t *testing.T) {
	r := NewReport(0, 10)
	r.AddCheck(CheckResult{Name: "spot", Status: StatusFailed})
	r.Finalize()

	if r.Success() {
		t.Fatal("Success() = true, want false (a hash mismatch must fail the run)")
	}
}

// TestReport_FailureSummary: the runner turns the report into one error string,
// which is all an operator sees in the Temporal UI and the alert. Counts alone
// name nothing to act on.
func TestReport_FailureSummary(t *testing.T) {
	r := NewReport(0, 10)
	r.AddCheck(CheckResult{Name: "Chain Integrity", Status: StatusPassed, Message: "valid through 10"})
	r.AddCheck(CheckResult{Name: "Orphan-only heights", Status: StatusFailed, Message: "2 height(s) have only an orphaned block: 4, 7"})
	r.AddCheck(CheckResult{Name: "Reorg Validation", Status: StatusError, Message: "etherscan timeout"})
	r.AddCheck(CheckResult{Name: "Spot check 9", Status: StatusSkipped, Message: "no verifier"})
	r.Finalize()

	got := r.FailureSummary()
	want := "Orphan-only heights: 2 height(s) have only an orphaned block: 4, 7; Reorg Validation: etherscan timeout"
	if got != want {
		t.Errorf("FailureSummary() = %q, want %q", got, want)
	}
}

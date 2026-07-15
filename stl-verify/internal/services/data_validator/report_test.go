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

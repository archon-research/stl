package data_validator

import (
	"time"
)

// CheckResult represents the result of a single validation check.
type CheckResult struct {
	// Name is the check name (e.g., "Chain Integrity", "Reorg 142").
	Name string `json:"name"`

	// Status is the check result: "passed", "failed", "error", or "skipped".
	Status string `json:"status"`

	// Message is the status message (empty for passed checks).
	Message string `json:"message,omitempty"`

	// Duration is how long the check took.
	Duration time.Duration `json:"duration"`

	// Details contains additional check-specific information.
	Details map[string]any `json:"details,omitempty"`
}

// Report represents the complete validation report.
type Report struct {
	// FromBlock is the start of the validated range.
	FromBlock int64 `json:"from_block"`

	// ToBlock is the end of the validated range.
	ToBlock int64 `json:"to_block"`

	// StartTime is when validation started.
	StartTime time.Time `json:"start_time"`

	// EndTime is when validation completed.
	EndTime time.Time `json:"end_time"`

	// Duration is the total validation duration.
	Duration time.Duration `json:"duration"`

	// Checks contains all validation check results.
	Checks []CheckResult `json:"checks"`

	// Summary statistics.
	Passed  int `json:"passed"`
	Failed  int `json:"failed"`
	Errors  int `json:"errors"`
	Skipped int `json:"skipped"`
}

// NewReport creates a new empty report.
func NewReport(fromBlock, toBlock int64) *Report {
	return &Report{
		FromBlock: fromBlock,
		ToBlock:   toBlock,
		StartTime: time.Now(),
		Checks:    make([]CheckResult, 0),
	}
}

// AddCheck adds a check result to the report.
func (r *Report) AddCheck(result CheckResult) {
	r.Checks = append(r.Checks, result)
	switch result.Status {
	case StatusPassed:
		r.Passed++
	case StatusFailed:
		r.Failed++
	case StatusError:
		r.Errors++
	case StatusSkipped:
		r.Skipped++
	}
}

// Finalize completes the report with end time and duration.
func (r *Report) Finalize() {
	r.EndTime = time.Now()
	r.Duration = r.EndTime.Sub(r.StartTime)
}

// Success returns true if all checks passed.
func (r *Report) Success() bool {
	return r.Failed == 0 && r.Errors == 0
}

// Status constants for check results.
const (
	StatusPassed  = "passed"
	StatusFailed  = "failed"
	StatusError   = "error"
	StatusSkipped = "skipped"
)

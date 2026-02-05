package data_validator

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// CheckResult represents the result of a single validation check.
type CheckResult struct {
	// Name is the check name (e.g., "Chain Integrity", "Reorg 142").
	Name string `json:"name"`

	// Status is the check result: "passed", "failed", or "error".
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
	Passed int `json:"passed"`
	Failed int `json:"failed"`
	Errors int `json:"errors"`
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
	StatusPassed = "passed"
	StatusFailed = "failed"
	StatusError  = "error"
)

// FormatText returns the report as human-readable text.
func (r *Report) FormatText() string {
	var sb strings.Builder

	// Header
	sb.WriteString("================================================================================\n")
	sb.WriteString("                        DATA VALIDATION REPORT\n")
	sb.WriteString("================================================================================\n")
	sb.WriteString(fmt.Sprintf("Validation Range: Block %s to %s\n",
		formatNumber(r.FromBlock), formatNumber(r.ToBlock)))
	sb.WriteString(fmt.Sprintf("Duration:         %s\n\n", formatDuration(r.Duration)))

	// Check results
	for _, check := range r.Checks {
		statusIcon := "[PASSED]"
		switch check.Status {
		case StatusFailed:
			statusIcon = "[FAILED]"
		case StatusError:
			statusIcon = "[ERROR]"
		}

		sb.WriteString(fmt.Sprintf("%s %s", statusIcon, check.Name))
		if check.Duration > 0 {
			sb.WriteString(fmt.Sprintf(" (%s)", formatDuration(check.Duration)))
		}
		sb.WriteString("\n")

		if check.Message != "" {
			// Indent multi-line messages
			lines := strings.Split(check.Message, "\n")
			for _, line := range lines {
				sb.WriteString(fmt.Sprintf("         %s\n", line))
			}
		}
	}

	// Summary
	sb.WriteString(fmt.Sprintf("\nSUMMARY: %d passed, %d failed, %d errors\n",
		r.Passed, r.Failed, r.Errors))

	if r.Success() {
		sb.WriteString("RESULT: PASSED (exit code 0)\n")
	} else {
		sb.WriteString("RESULT: FAILED (exit code 1)\n")
	}

	return sb.String()
}

// FormatJSON returns the report as JSON.
func (r *Report) FormatJSON() (string, error) {
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshaling report: %w", err)
	}
	return string(data), nil
}

// formatNumber adds thousands separators to a number.
func formatNumber(n int64) string {
	str := fmt.Sprintf("%d", n)
	if n < 0 {
		return str
	}

	// Add commas
	var result strings.Builder
	for i, c := range str {
		if i > 0 && (len(str)-i)%3 == 0 {
			result.WriteRune(',')
		}
		result.WriteRune(c)
	}
	return result.String()
}

// formatDuration formats a duration for human readability.
func formatDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%.1fms", float64(d.Milliseconds()))
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	minutes := int(d.Minutes())
	seconds := int(d.Seconds()) % 60
	return fmt.Sprintf("%dm%ds", minutes, seconds)
}

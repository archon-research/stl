package temporal

import (
	"context"
	"errors"
	"strings"
	"testing"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func TestDataValidationWorkflow(t *testing.T) {
	tests := []struct {
		name           string
		activityResult *ValidateDataOutput
		activityErr    error
		wantErr        bool
		errContains    string
	}{
		{
			name: "successful validation",
			activityResult: &ValidateDataOutput{
				Success:    true,
				Passed:     5,
				Failed:     0,
				Errors:     0,
				FromBlock:  100,
				ToBlock:    200,
				DurationMs: 5000,
				ReportText: "All checks passed.",
			},
		},
		{
			name: "validation with failures",
			activityResult: &ValidateDataOutput{
				Success:    false,
				Passed:     3,
				Failed:     2,
				Errors:     0,
				FromBlock:  100,
				ToBlock:    200,
				DurationMs: 8000,
				ReportText: "2 checks failed.",
				FailedChecks: []FailedCheck{
					{Name: "Check A", Status: "failed", Message: "mismatch"},
					{Name: "Check B", Status: "failed", Message: "missing"},
				},
			},
		},
		{
			name:        "activity returns error",
			activityErr: errors.New("activity failed"),
			wantErr:     true,
			errContains: "executing ValidateData activity",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			suite := &testsuite.WorkflowTestSuite{}
			env := suite.NewTestWorkflowEnvironment()

			result := tt.activityResult
			activityErr := tt.activityErr
			env.RegisterActivityWithOptions(
				func(_ context.Context) (*ValidateDataOutput, error) {
					return result, activityErr
				},
				activity.RegisterOptions{Name: "ValidateData"},
			)

			env.ExecuteWorkflow(DataValidationWorkflow)

			if !env.IsWorkflowCompleted() {
				t.Fatal("expected workflow to be completed")
			}

			err := env.GetWorkflowError()
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q should contain %q", err.Error(), tt.errContains)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected workflow error: %v", err)
			}
			var wfResult DataValidationWorkflowOutput
			if err := env.GetWorkflowResult(&wfResult); err != nil {
				t.Fatalf("failed to get workflow result: %v", err)
			}
			if wfResult.Success != tt.activityResult.Success {
				t.Errorf("Success: got %v, want %v", wfResult.Success, tt.activityResult.Success)
			}
			if wfResult.Passed != tt.activityResult.Passed {
				t.Errorf("Passed: got %d, want %d", wfResult.Passed, tt.activityResult.Passed)
			}
			if wfResult.Failed != tt.activityResult.Failed {
				t.Errorf("Failed: got %d, want %d", wfResult.Failed, tt.activityResult.Failed)
			}
			if wfResult.ReportText != tt.activityResult.ReportText {
				t.Errorf("ReportText: got %q, want %q", wfResult.ReportText, tt.activityResult.ReportText)
			}
			if len(wfResult.FailedChecks) != len(tt.activityResult.FailedChecks) {
				t.Errorf("FailedChecks length: got %d, want %d", len(wfResult.FailedChecks), len(tt.activityResult.FailedChecks))
			}
		})
	}
}

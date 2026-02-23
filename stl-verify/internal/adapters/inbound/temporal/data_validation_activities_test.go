package temporal

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"

	"github.com/archon-research/stl/stl-verify/internal/services/data_validator"
)

type mockDataValidator struct {
	validateFn func(ctx context.Context) (*data_validator.Report, error)
}

func (m *mockDataValidator) Validate(ctx context.Context) (*data_validator.Report, error) {
	return m.validateFn(ctx)
}

func TestNewDataValidationActivities(t *testing.T) {
	tests := []struct {
		name        string
		validator   DataValidator
		wantErr     bool
		errContains string
	}{
		{
			name:      "valid validator",
			validator: &mockDataValidator{},
		},
		{
			name:        "nil validator",
			wantErr:     true,
			errContains: "validator cannot be nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			activities, err := NewDataValidationActivities(tt.validator)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q should contain %q", err.Error(), tt.errContains)
				}
				if activities != nil {
					t.Errorf("expected nil activities, got %v", activities)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if activities == nil {
				t.Fatal("expected non-nil activities")
			}
		})
	}
}

func TestValidateData(t *testing.T) {
	tests := []struct {
		name             string
		report           *data_validator.Report
		validateErr      error
		wantErr          bool
		errContains      string
		wantSuccess      bool
		wantFailedChecks int
	}{
		{
			name: "all checks passed",
			report: &data_validator.Report{
				FromBlock: 100,
				ToBlock:   200,
				StartTime: time.Now(),
				Checks: []data_validator.CheckResult{
					{Name: "Chain Integrity", Status: data_validator.StatusPassed},
				},
				Passed: 1,
			},
			wantSuccess:      true,
			wantFailedChecks: 0,
		},
		{
			name: "some checks failed",
			report: &data_validator.Report{
				FromBlock: 100,
				ToBlock:   200,
				StartTime: time.Now(),
				Checks: []data_validator.CheckResult{
					{Name: "Chain Integrity", Status: data_validator.StatusPassed},
					{Name: "Spot Check Block 150", Status: data_validator.StatusFailed, Message: "hash mismatch"},
					{Name: "Spot Check Block 175", Status: data_validator.StatusError, Message: "timeout"},
				},
				Passed: 1,
				Failed: 1,
				Errors: 1,
			},
			wantSuccess:      false,
			wantFailedChecks: 2,
		},
		{
			name:        "validation returns error",
			validateErr: errors.New("database connection lost"),
			wantErr:     true,
			errContains: "running data validation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			suite := &testsuite.WorkflowTestSuite{}
			activityEnv := suite.NewTestActivityEnvironment()

			activities, err := NewDataValidationActivities(&mockDataValidator{
				validateFn: func(_ context.Context) (*data_validator.Report, error) {
					return tt.report, tt.validateErr
				},
			})
			if err != nil {
				t.Fatalf("unexpected error creating activities: %v", err)
			}

			activityEnv.RegisterActivity(activities.ValidateData)
			result, err := activityEnv.ExecuteActivity(activities.ValidateData)

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
				t.Fatalf("unexpected error: %v", err)
			}
			var output ValidateDataOutput
			if err := result.Get(&output); err != nil {
				t.Fatalf("failed to get output: %v", err)
			}
			if output.Success != tt.wantSuccess {
				t.Errorf("Success: got %v, want %v", output.Success, tt.wantSuccess)
			}
			if output.FromBlock != tt.report.FromBlock {
				t.Errorf("FromBlock: got %d, want %d", output.FromBlock, tt.report.FromBlock)
			}
			if output.ToBlock != tt.report.ToBlock {
				t.Errorf("ToBlock: got %d, want %d", output.ToBlock, tt.report.ToBlock)
			}
			if output.Passed != tt.report.Passed {
				t.Errorf("Passed: got %d, want %d", output.Passed, tt.report.Passed)
			}
			if output.Failed != tt.report.Failed {
				t.Errorf("Failed: got %d, want %d", output.Failed, tt.report.Failed)
			}
			if len(output.FailedChecks) != tt.wantFailedChecks {
				t.Errorf("FailedChecks length: got %d, want %d", len(output.FailedChecks), tt.wantFailedChecks)
			}
		})
	}
}

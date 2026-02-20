package temporal

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
				assert.Nil(t, activities)
				return
			}
			require.NoError(t, err)
			assert.NotNil(t, activities)
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
			require.NoError(t, err)

			activityEnv.RegisterActivity(activities.ValidateData)
			result, err := activityEnv.ExecuteActivity(activities.ValidateData)

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
				return
			}

			require.NoError(t, err)
			var output ValidateDataOutput
			require.NoError(t, result.Get(&output))
			assert.Equal(t, tt.wantSuccess, output.Success)
			assert.Equal(t, tt.report.FromBlock, output.FromBlock)
			assert.Equal(t, tt.report.ToBlock, output.ToBlock)
			assert.Equal(t, tt.report.Passed, output.Passed)
			assert.Equal(t, tt.report.Failed, output.Failed)
			assert.Len(t, output.FailedChecks, tt.wantFailedChecks)
		})
	}
}

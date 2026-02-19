package temporal

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
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
			},
		},
		{
			name:        "activity returns error",
			activityErr: assert.AnError,
			wantErr:     true,
			errContains: "executing ValidateData activity",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			suite := &testsuite.WorkflowTestSuite{}
			env := suite.NewTestWorkflowEnvironment()

			var a *DataValidationActivities
			env.OnActivity(a.ValidateData, mock.Anything).
				Return(tt.activityResult, tt.activityErr)

			env.ExecuteWorkflow(DataValidationWorkflow)

			require.True(t, env.IsWorkflowCompleted())

			err := env.GetWorkflowError()
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
				return
			}

			require.NoError(t, err)
			var result DataValidationWorkflowOutput
			require.NoError(t, env.GetWorkflowResult(&result))
			assert.Equal(t, tt.activityResult.Success, result.Success)
			assert.Equal(t, tt.activityResult.Passed, result.Passed)
			assert.Equal(t, tt.activityResult.Failed, result.Failed)
		})
	}
}

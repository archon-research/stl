package temporal

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
)

func TestPriceFetchWorkflow(t *testing.T) {
	tests := []struct {
		name           string
		input          PriceFetchWorkflowInput
		activityResult *FetchCurrentPricesOutput
		activityErr    error
		wantErr        bool
		errContains    string
	}{
		{
			name:           "successful workflow with asset IDs",
			input:          PriceFetchWorkflowInput{AssetIDs: []string{"bitcoin", "ethereum"}},
			activityResult: &FetchCurrentPricesOutput{Success: true},
		},
		{
			name:           "successful workflow with empty asset IDs",
			input:          PriceFetchWorkflowInput{},
			activityResult: &FetchCurrentPricesOutput{Success: true},
		},
		{
			name:        "activity returns error",
			input:       PriceFetchWorkflowInput{AssetIDs: []string{"bitcoin"}},
			activityErr: assert.AnError,
			wantErr:     true,
			errContains: "executing FetchCurrentPrices activity",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			suite := &testsuite.WorkflowTestSuite{}
			env := suite.NewTestWorkflowEnvironment()

			var a *PriceFetchActivities
			env.OnActivity(a.FetchCurrentPrices, mock.Anything, mock.Anything).
				Return(tt.activityResult, tt.activityErr)

			env.ExecuteWorkflow(PriceFetchWorkflow, tt.input)

			require.True(t, env.IsWorkflowCompleted())

			err := env.GetWorkflowError()
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
				return
			}

			require.NoError(t, err)
			var result PriceFetchWorkflowOutput
			require.NoError(t, env.GetWorkflowResult(&result))
			assert.True(t, result.Success)
		})
	}
}

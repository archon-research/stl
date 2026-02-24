package temporal

import (
	"context"
	"errors"
	"strings"
	"testing"

	"go.temporal.io/sdk/activity"
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
			activityErr: errors.New("activity failed"),
			wantErr:     true,
			errContains: "executing FetchCurrentPrices activity",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			suite := &testsuite.WorkflowTestSuite{}
			env := suite.NewTestWorkflowEnvironment()

			result := tt.activityResult
			activityErr := tt.activityErr
			env.RegisterActivityWithOptions(
				func(_ context.Context, _ FetchCurrentPricesInput) (*FetchCurrentPricesOutput, error) {
					return result, activityErr
				},
				activity.RegisterOptions{Name: "FetchCurrentPrices"},
			)

			env.ExecuteWorkflow(PriceFetchWorkflow, tt.input)

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
			var wfResult PriceFetchWorkflowOutput
			if err := env.GetWorkflowResult(&wfResult); err != nil {
				t.Fatalf("failed to get workflow result: %v", err)
			}
			if !wfResult.Success {
				t.Error("expected result.Success to be true")
			}
		})
	}
}

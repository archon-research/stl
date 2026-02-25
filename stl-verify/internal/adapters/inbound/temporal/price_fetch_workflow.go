package temporal

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// PriceFetchWorkflowInput is the input for the PriceFetchWorkflow.
type PriceFetchWorkflowInput struct {
	AssetIDs []string
}

// PriceFetchWorkflowOutput is the output of the PriceFetchWorkflow.
type PriceFetchWorkflowOutput struct {
	Success bool
}

// PriceFetchWorkflow orchestrates fetching current prices.
// It delegates actual work to the FetchCurrentPrices activity.
func PriceFetchWorkflow(ctx workflow.Context, input PriceFetchWorkflowInput) (*PriceFetchWorkflowOutput, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("starting price fetch workflow")

	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout:    2 * time.Minute,
		ScheduleToCloseTimeout: 4 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    5,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	var activities *PriceFetchActivities
	var result FetchCurrentPricesOutput
	err := workflow.ExecuteActivity(ctx, activities.FetchCurrentPrices, FetchCurrentPricesInput(input)).Get(ctx, &result)
	if err != nil {
		return nil, fmt.Errorf("executing FetchCurrentPrices activity: %w", err)
	}

	logger.Info("price fetch workflow completed successfully")
	return &PriceFetchWorkflowOutput{Success: result.Success}, nil
}

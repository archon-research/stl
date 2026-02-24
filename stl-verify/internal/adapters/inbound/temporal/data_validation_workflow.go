package temporal

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// DataValidationWorkflowOutput is the output of the DataValidationWorkflow.
type DataValidationWorkflowOutput struct {
	Success      bool          `json:"success"`
	Passed       int           `json:"passed"`
	Failed       int           `json:"failed"`
	Errors       int           `json:"errors"`
	DurationMs   int64         `json:"duration_ms"`
	ReportText   string        `json:"report_text,omitempty"`
	FailedChecks []FailedCheck `json:"failed_checks,omitempty"`
}

// DataValidationWorkflow orchestrates chain data validation.
// It delegates actual work to the ValidateData activity.
func DataValidationWorkflow(ctx workflow.Context) (*DataValidationWorkflowOutput, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("starting data validation workflow")

	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout:    10 * time.Minute,
		ScheduleToCloseTimeout: 30 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    2 * time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    5,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	var activities *DataValidationActivities
	var result ValidateDataOutput
	err := workflow.ExecuteActivity(ctx, activities.ValidateData).Get(ctx, &result)
	if err != nil {
		return nil, fmt.Errorf("executing ValidateData activity: %w", err)
	}

	logger.Info("data validation workflow completed",
		"success", result.Success,
		"passed", result.Passed,
		"failed", result.Failed,
		"errors", result.Errors,
	)

	return &DataValidationWorkflowOutput{
		Success:      result.Success,
		Passed:       result.Passed,
		Failed:       result.Failed,
		Errors:       result.Errors,
		DurationMs:   result.DurationMs,
		ReportText:   result.ReportText,
		FailedChecks: result.FailedChecks,
	}, nil
}

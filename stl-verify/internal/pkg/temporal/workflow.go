package temporal

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
	temporalsdk "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// Runner is the interface a cronjob service must implement.
type Runner interface {
	Run(ctx context.Context) error
}

// RunnerFunc adapts a plain function to the Runner interface.
type RunnerFunc func(ctx context.Context) error

func (f RunnerFunc) Run(ctx context.Context) error { return f(ctx) }

// cronjobActivities wraps a Runner for Temporal activity execution.
type cronjobActivities struct {
	runner Runner
}

// newCronjobActivities creates a new cronjobActivities instance.
func newCronjobActivities(runner Runner) (*cronjobActivities, error) {
	if runner == nil {
		return nil, fmt.Errorf("runner cannot be nil")
	}
	return &cronjobActivities{runner: runner}, nil
}

// CronjobOutput is the output of the cronjob workflow and activity.
type CronjobOutput struct {
	Success bool `json:"success"`
}

// Execute runs the cronjob.
func (a *cronjobActivities) Execute(ctx context.Context) (*CronjobOutput, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("starting cronjob execution")

	if err := a.runner.Run(ctx); err != nil {
		return nil, fmt.Errorf("running cronjob: %w", err)
	}

	logger.Info("cronjob execution completed")
	return &CronjobOutput{Success: true}, nil
}

// cronjobWorkflow orchestrates a single cronjob activity execution.
func cronjobWorkflow(ctx workflow.Context) (*CronjobOutput, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("starting cronjob workflow")

	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout:    10 * time.Minute,
		ScheduleToCloseTimeout: 30 * time.Minute,
		RetryPolicy: &temporalsdk.RetryPolicy{
			InitialInterval:    2 * time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    5,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	var activities *cronjobActivities
	var result CronjobOutput
	err := workflow.ExecuteActivity(ctx, activities.Execute).Get(ctx, &result)
	if err != nil {
		return nil, fmt.Errorf("executing cronjob activity: %w", err)
	}

	logger.Info("cronjob workflow completed")
	return &CronjobOutput{Success: result.Success}, nil
}

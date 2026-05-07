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

func newCronjobActivities(runner Runner) (*cronjobActivities, error) {
	if runner == nil {
		return nil, fmt.Errorf("runner cannot be nil")
	}
	return &cronjobActivities{runner: runner}, nil
}

// Execute runs the cronjob.
func (a *cronjobActivities) Execute(ctx context.Context) error {
	logger := activity.GetLogger(ctx)
	logger.Info("starting cronjob execution")

	if err := a.runner.Run(ctx); err != nil {
		return fmt.Errorf("running cronjob: %w", err)
	}

	logger.Info("cronjob execution completed")
	return nil
}

// cronjobWorkflow orchestrates a single cronjob activity execution.
func cronjobWorkflow(ctx workflow.Context) error {
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
	if err := workflow.ExecuteActivity(ctx, activities.Execute).Get(ctx, nil); err != nil {
		return fmt.Errorf("executing cronjob activity: %w", err)
	}

	logger.Info("cronjob workflow completed")
	return nil
}

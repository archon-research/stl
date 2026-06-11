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

// scheduledAtKey carries the workflow's schedule-stable timestamp through
// the activity context.
type scheduledAtKey struct{}

// ScheduledAtFromContext returns the timestamp the cronjob workflow stamped
// on this run. It is identical across activity retries of the same run, so
// runners that snapshot data keyed by time (e.g. the Maple GraphQL indexer's
// synced_at) can make retries idempotent instead of multiplying snapshots.
func ScheduledAtFromContext(ctx context.Context) (time.Time, bool) {
	t, ok := ctx.Value(scheduledAtKey{}).(time.Time)
	return t, ok && !t.IsZero()
}

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

// Execute runs the cronjob. scheduledAt is the workflow-recorded timestamp
// (stable across activity retries) exposed to the runner via
// ScheduledAtFromContext.
func (a *cronjobActivities) Execute(ctx context.Context, scheduledAt time.Time) error {
	logger := activity.GetLogger(ctx)
	logger.Info("starting cronjob execution", "scheduledAt", scheduledAt)

	ctx = context.WithValue(ctx, scheduledAtKey{}, scheduledAt)
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

	// workflow.Now is recorded once in the workflow history, so server-side
	// activity retries (the RetryPolicy above) all observe the same value.
	scheduledAt := workflow.Now(ctx).UTC()

	var activities *cronjobActivities
	if err := workflow.ExecuteActivity(ctx, activities.Execute, scheduledAt).Get(ctx, nil); err != nil {
		return fmt.Errorf("executing cronjob activity: %w", err)
	}

	logger.Info("cronjob workflow completed")
	return nil
}

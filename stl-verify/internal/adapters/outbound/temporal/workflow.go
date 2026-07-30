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

// ContextWithScheduledAt returns ctx carrying the schedule-stable timestamp
// the activity normally stamps. Exported so composition-root tests can
// exercise the same path their runner takes in production.
func ContextWithScheduledAt(ctx context.Context, scheduledAt time.Time) context.Context {
	return context.WithValue(ctx, scheduledAtKey{}, scheduledAt)
}

// cronjobActivities wraps a Runner for Temporal activity execution.
type cronjobActivities struct {
	runner  Runner
	metrics *cronjobMetrics
}

// newCronjobActivities wraps runner for activity execution. metrics may be nil
// (it is nil-receiver-safe), e.g. in unit tests that don't wire telemetry.
func newCronjobActivities(runner Runner, metrics *cronjobMetrics) (*cronjobActivities, error) {
	if runner == nil {
		return nil, fmt.Errorf("runner cannot be nil")
	}
	return &cronjobActivities{runner: runner, metrics: metrics}, nil
}

// Execute runs the cronjob. scheduledAt is the workflow-recorded timestamp
// (stable across activity retries) exposed to the runner via
// ScheduledAtFromContext.
func (a *cronjobActivities) Execute(ctx context.Context, scheduledAt time.Time) error {
	logger := activity.GetLogger(ctx)
	logger.Info("starting cronjob execution", "scheduledAt", scheduledAt)

	ctx = ContextWithScheduledAt(ctx, scheduledAt)
	start := time.Now()
	err := a.runner.Run(ctx)
	// Recorded per activity execution (so a retried run that ultimately
	// succeeds emits both an error and a success); the vector-cronjobs alerts
	// account for this by treating a warning as "any error" and a page as
	// "errors with no success over the window". A run interrupted by activity
	// cancellation lands as "canceled", not "error" — see runStatusAttr.
	a.metrics.RecordRun(ctx, time.Since(start), err)
	if err != nil {
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

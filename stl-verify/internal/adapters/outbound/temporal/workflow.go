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

	// Heartbeat on a ticker so that, on a long-running activity with a
	// HeartbeatTimeout configured (see workflowParams), a crashed worker is
	// detected in minutes rather than at StartToCloseTimeout. It is harmless
	// when no HeartbeatTimeout is set (the server just ignores the beats). Kept
	// here in the wrapper, not the Runner, so runners stay Temporal-free.
	hbDone := make(chan struct{})
	defer close(hbDone)
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-hbDone:
				return
			case <-t.C:
				activity.RecordHeartbeat(ctx)
			}
		}
	}()

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

// workflowParams carries the per-job activity timing/retry into the workflow as
// input, so it is recorded in workflow history and observed identically on every
// replay (rather than closing over mutable config). Every field is optional: a
// zero value falls back to the shared default in cronjobWorkflow, so a schedule
// that passes no args — including any schedule created before this parameter
// existed — runs exactly as before. Long-running jobs (e.g. a multi-hour history
// backfill) raise StartToCloseTimeout and ScheduleToCloseTimeout, usually set
// MaximumAttempts=1, and set a HeartbeatTimeout so a dead worker is caught fast.
type workflowParams struct {
	StartToCloseTimeout    time.Duration
	ScheduleToCloseTimeout time.Duration
	HeartbeatTimeout       time.Duration
	MaximumAttempts        int32
}

// cronjobWorkflow orchestrates a single cronjob activity execution. The params
// argument is decoded from schedule input; missing (older/interval schedules
// that pass no args) decodes to the zero value, and each zero field below falls
// back to the original default — so this signature change is backward compatible.
func cronjobWorkflow(ctx workflow.Context, params workflowParams) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("starting cronjob workflow")

	startToClose := params.StartToCloseTimeout
	if startToClose == 0 {
		startToClose = 10 * time.Minute
	}
	scheduleToClose := params.ScheduleToCloseTimeout
	if scheduleToClose == 0 {
		scheduleToClose = 30 * time.Minute
	}
	maxAttempts := params.MaximumAttempts
	if maxAttempts == 0 {
		maxAttempts = 5
	}

	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout:    startToClose,
		ScheduleToCloseTimeout: scheduleToClose,
		// Zero HeartbeatTimeout means "no heartbeat requirement" (the default for
		// short jobs); a long job sets it so a crashed worker is detected quickly.
		HeartbeatTimeout: params.HeartbeatTimeout,
		RetryPolicy: &temporalsdk.RetryPolicy{
			InitialInterval:    2 * time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    maxAttempts,
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

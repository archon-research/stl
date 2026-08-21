package temporal

import (
	"context"
	"fmt"
	"sync"
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

// ProgressHeartbeater owns the activity's heartbeat on behalf of a runner that
// records progress in the heartbeat details. *ActivityProgress implements it.
type ProgressHeartbeater interface {
	// Beat sends one heartbeat carrying the progress recorded so far.
	Beat(ctx context.Context)
	// Reset drops the progress carried so far, so one execution's record cannot
	// ride the next execution's beats.
	Reset()
}

// cronjobActivities wraps a Runner for Temporal activity execution.
type cronjobActivities struct {
	runner    Runner
	metrics   *cronjobMetrics
	heartbeat time.Duration
	progress  ProgressHeartbeater
}

// newCronjobActivities wraps runner for activity execution. metrics may be nil
// (it is nil-receiver-safe), e.g. in unit tests that don't wire telemetry. A
// zero heartbeat disables liveness reporting; a nil progress store makes the
// heartbeat a bare liveness ping.
func newCronjobActivities(runner Runner, metrics *cronjobMetrics, heartbeat time.Duration, progress ProgressHeartbeater) (*cronjobActivities, error) {
	if runner == nil {
		return nil, fmt.Errorf("runner cannot be nil")
	}
	return &cronjobActivities{runner: runner, metrics: metrics, heartbeat: heartbeat, progress: progress}, nil
}

// Execute runs the cronjob. scheduledAt is the workflow-recorded timestamp
// (stable across activity retries) exposed to the runner via
// ScheduledAtFromContext.
func (a *cronjobActivities) Execute(ctx context.Context, scheduledAt time.Time) error {
	logger := activity.GetLogger(ctx)
	logger.Info("starting cronjob execution", "scheduledAt", scheduledAt)

	ctx = ContextWithScheduledAt(ctx, scheduledAt)
	resetProgress(a.progress)
	stopHeartbeat := StartHeartbeat(ctx, a.heartbeat, a.progress)
	start := time.Now()
	err := a.runner.Run(ctx)
	stopHeartbeat()
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

// StartHeartbeat reports activity liveness every interval until the returned
// stop function is called, so Temporal notices a worker that died mid-run
// instead of waiting out StartToCloseTimeout. A zero interval disables it, which
// is why the returned stop is always safe to call.
//
// stop JOINS the reporter rather than merely signalling it, so no ping can land
// after the activity has reported its result, and it is idempotent, so a caller
// may both defer it and call it inline.
//
// The Runner interface carries no progress signal, so without a progress store
// the heartbeat is a plain liveness ping with no details: it answers "is this
// worker still alive", not "how far has it got". A runner that DOES record
// progress supplies one, and the ping goes through it — Temporal keeps only the
// last heartbeat's details, so a bare ping in between would erase them. Such a
// store also swallows the ping entirely while it holds no record, which is a
// liveness gap taken on purpose: see ActivityProgress.Beat.
//
// Exported because activities outside this package (the morpho-vault-backfill's,
// which register their own workflows through RunWorker rather than running as a
// Runner) need the same reporter.
func StartHeartbeat(ctx context.Context, interval time.Duration, progress ProgressHeartbeater) (stop func()) {
	if interval <= 0 {
		return func() {}
	}
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				beat(ctx, progress)
			}
		}
	}()
	return sync.OnceFunc(func() {
		close(done)
		<-finished
	})
}

// resetProgress clears whatever an earlier execution left in the store, before
// the runner's own LoadProgress reseeds it from THIS execution's details.
func resetProgress(progress ProgressHeartbeater) {
	if progress == nil {
		return
	}
	progress.Reset()
}

// beat sends one liveness heartbeat, through the progress store when there is
// one so the details it recorded survive the ping.
func beat(ctx context.Context, progress ProgressHeartbeater) {
	if progress == nil {
		activity.RecordHeartbeat(ctx)
		return
	}
	progress.Beat(ctx)
}

// Defaults a cronjob gets when it leaves ActivityTimeouts zero.
const (
	defaultStartToCloseTimeout    = 10 * time.Minute
	defaultScheduleToCloseTimeout = 30 * time.Minute
	defaultMaximumAttempts        = int32(5)
)

// heartbeatTimeoutFactor is the grace Temporal allows between heartbeats. The
// activity pings on ActivityTimeouts.Heartbeat; the server must tolerate a
// missed ping (GC pause, a slow chunk) before declaring the worker dead, so the
// timeout is a multiple of the interval rather than equal to it.
const heartbeatTimeoutFactor = 3

// ActivityTimeouts bounds one activity execution. A job whose single run can
// legitimately take hours must raise StartToClose, or Temporal kills it mid-run
// at the 10m default.
//
// On the scheduled path it travels as the cronjobWorkflow argument, because
// ensureSchedule bakes it into the schedule's action at creation. Any zero field
// falls back to the default above, which is also what a schedule created before
// this argument existed decodes to. reconcileScheduleSpec deliberately touches
// only the timing spec, so changing the values later requires deleting the
// schedule in Temporal and restarting the worker — the same caveat that already
// applies to a changed interval.
//
// An on-demand job has no action to carry them and binds them at registration
// instead (RegisterRunner), so a redeploy is enough to change them.
type ActivityTimeouts struct {
	StartToClose    time.Duration
	ScheduleToClose time.Duration
	MaximumAttempts int32
	// Heartbeat, when non-zero, makes the activity report liveness on this
	// interval. Without it a worker that dies mid-run is only noticed when
	// StartToClose expires — hours later for a long-running job. Leave zero for
	// a short tick, where the timeout is already the detection window.
	Heartbeat time.Duration
}

func (t ActivityTimeouts) resolve() ActivityTimeouts {
	if t.StartToClose <= 0 {
		t.StartToClose = defaultStartToCloseTimeout
	}
	if t.ScheduleToClose <= 0 {
		t.ScheduleToClose = defaultScheduleToCloseTimeout
	}
	if t.MaximumAttempts <= 0 {
		t.MaximumAttempts = defaultMaximumAttempts
	}
	return t
}

// cronjobWorkflow orchestrates a single cronjob activity execution.
func cronjobWorkflow(ctx workflow.Context, timeouts ActivityTimeouts) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("starting cronjob workflow")

	timeouts = timeouts.resolve()
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout:    timeouts.StartToClose,
		ScheduleToCloseTimeout: timeouts.ScheduleToClose,
		HeartbeatTimeout:       timeouts.Heartbeat * heartbeatTimeoutFactor,
		RetryPolicy: &temporalsdk.RetryPolicy{
			InitialInterval:    2 * time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    timeouts.MaximumAttempts,
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

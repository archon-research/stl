package temporal

import (
	"context"
	"time"

	"go.temporal.io/sdk/interceptor"
)

// runMetricsInterceptor records the outcome of every activity execution on an
// on-demand worker, so RunWorker jobs emit the same cronjob.runs.total and
// cronjob.run.duration_seconds series the scheduled path does and inherit the
// alerts keyed on them. Without it a hand-triggered job is observable only to
// whoever has the Temporal UI open.
//
// Deliberately at the ACTIVITY level, not the workflow level. Activity
// interceptors run once per attempt in the worker process, outside the workflow
// sandbox; a workflow-level counter would re-emit on every history replay and
// silently inflate. It also matches what the counter already means on the
// scheduled path — one record per activity execution, so a retried run that
// eventually succeeds emits both an error and a success (see metrics.go).
type runMetricsInterceptor struct {
	interceptor.WorkerInterceptorBase
	metrics *cronjobMetrics
}

func newRunMetricsInterceptor(metrics *cronjobMetrics) *runMetricsInterceptor {
	return &runMetricsInterceptor{metrics: metrics}
}

func (i *runMetricsInterceptor) InterceptActivity(
	ctx context.Context,
	next interceptor.ActivityInboundInterceptor,
) interceptor.ActivityInboundInterceptor {
	return &activityRunRecorder{
		ActivityInboundInterceptorBase: interceptor.ActivityInboundInterceptorBase{Next: next},
		metrics:                        i.metrics,
	}
}

type activityRunRecorder struct {
	interceptor.ActivityInboundInterceptorBase
	metrics *cronjobMetrics
}

func (r *activityRunRecorder) ExecuteActivity(
	ctx context.Context,
	in *interceptor.ExecuteActivityInput,
) (any, error) {
	start := time.Now()
	out, err := r.ActivityInboundInterceptorBase.ExecuteActivity(ctx, in)
	// RecordRun classifies a cancelled context as "canceled" rather than "error",
	// which is what keeps a deploy landing mid-run from looking like a failure.
	r.metrics.RecordRun(ctx, time.Since(start), err)
	return out, err
}

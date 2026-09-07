package temporal

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// runCounts reads cronjob.runs.total out of an in-memory reader, keyed by status.
// Only non-zero values are returned, so the construction-time zero-seeding does
// not mask a counter that never actually incremented.
func runCounts(t *testing.T, reader sdkmetric.Reader) map[string]int64 {
	t.Helper()

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collecting metrics: %v", err)
	}

	counts := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "cronjob.runs.total" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("cronjob.runs.total has data type %T, want Sum[int64]", m.Data)
			}
			for _, dp := range sum.DataPoints {
				status, found := dp.Attributes.Value("status")
				if !found {
					t.Fatal("a cronjob.runs.total data point carries no status attribute")
				}
				if dp.Value != 0 {
					counts[status.String()] = dp.Value
				}
			}
		}
	}
	return counts
}

// newInterceptedActivityEnv wires the real interceptor over a test activity
// environment, returning the reader its metrics land in.
func newInterceptedActivityEnv(t *testing.T, fn any) (*testsuite.TestActivityEnvironment, sdkmetric.Reader) {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	metrics, err := newCronjobMetricsWithProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	if err != nil {
		t.Fatalf("creating metrics: %v", err)
	}

	env := (&testsuite.WorkflowTestSuite{}).NewTestActivityEnvironment()
	env.SetWorkerOptions(workerOptions(metrics))
	env.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: "Probe"})
	return env, reader
}

// An on-demand job must land on the same counter the scheduled path uses, or the
// alerts keyed on it cannot see the job at all.
func TestRunMetricsInterceptor_RecordsActivityOutcome(t *testing.T) {
	tests := []struct {
		name       string
		activityFn any
		wantStatus string
	}{
		{
			name:       "successful activity",
			activityFn: func(context.Context) error { return nil },
			wantStatus: "success",
		},
		{
			name:       "failing activity",
			activityFn: func(context.Context) error { return errors.New("boom") },
			wantStatus: "error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env, reader := newInterceptedActivityEnv(t, tc.activityFn)

			_, _ = env.ExecuteActivity("Probe")

			counts := runCounts(t, reader)
			if counts[tc.wantStatus] != 1 {
				t.Errorf("cronjob.runs.total{status=%q} = %d, want 1 (got %v)",
					tc.wantStatus, counts[tc.wantStatus], counts)
			}
			if len(counts) != 1 {
				t.Errorf("expected exactly one non-zero status series, got %v", counts)
			}
		})
	}
}

// Each attempt is its own record, matching the scheduled path's semantics: a
// retried run that eventually succeeds emits both an error and a success.
func TestRunMetricsInterceptor_RecordsEveryAttemptSeparately(t *testing.T) {
	env, reader := newInterceptedActivityEnv(t, func(context.Context) error { return nil })

	for range 3 {
		if _, err := env.ExecuteActivity("Probe"); err != nil {
			t.Fatalf("executing activity: %v", err)
		}
	}

	if counts := runCounts(t, reader); counts["success"] != 3 {
		t.Errorf("cronjob.runs.total{status=success} = %d, want 3", counts["success"])
	}
}

// The interceptor must not swallow or alter what the activity returned.
func TestRunMetricsInterceptor_PassesResultAndErrorThrough(t *testing.T) {
	sentinel := errors.New("sentinel")
	env, _ := newInterceptedActivityEnv(t, func(context.Context) (int, error) { return 0, sentinel })

	_, err := env.ExecuteActivity("Probe")

	if err == nil || !errorContains(err, "sentinel") {
		t.Fatalf("error = %v, want it to carry the activity's own error", err)
	}
}

// Instrumenting the activity rather than the workflow is what keeps the counter
// honest under replay. This pins the half that is observable in-process: when
// the executions are driven by a workflow, the count still tracks activity
// executions and nothing else.
//
// It does NOT exercise a real replay. TestWorkflowEnvironment exposes no event
// history, so a worker.WorkflowReplayer pass needs a history captured from a
// live server; and because replay never invokes activities, such a test asserts
// that the counter does not move at all rather than that it stays put.
func TestRunMetricsInterceptor_RecordsOncePerActivityExecutionInAWorkflow(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	metrics, err := newCronjobMetricsWithProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	if err != nil {
		t.Fatalf("creating metrics: %v", err)
	}

	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	env.SetWorkerOptions(workerOptions(metrics))
	env.RegisterActivityWithOptions(
		func(context.Context) error { return nil },
		activity.RegisterOptions{Name: "Probe"},
	)

	// Two calls rather than one, so a counter keyed on the workflow rather than
	// the activity would land on a visibly different number.
	wf := func(ctx workflow.Context) error {
		ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: time.Minute,
		})
		for range 2 {
			if err := workflow.ExecuteActivity(ctx, "Probe").Get(ctx, nil); err != nil {
				return err
			}
		}
		return nil
	}
	env.RegisterWorkflow(wf)

	env.ExecuteWorkflow(wf)

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("unexpected workflow error: %v", err)
	}
	if counts := runCounts(t, reader); counts["success"] != 2 {
		t.Errorf("cronjob.runs.total{status=success} = %d, want exactly 2 (one per activity "+
			"execution, not per workflow); got %v", counts["success"], counts)
	}
}

func errorContains(err error, substr string) bool {
	return err != nil && strings.Contains(err.Error(), substr)
}

// The metrics interceptor is what makes on-demand jobs visible to the alerts, and
// nothing else in the suite notices if RunWorker stops installing it: every other
// test builds its own worker options. This pins the production wiring itself.
func TestWorkerOptions_InstallsTheRunMetricsInterceptor(t *testing.T) {
	metrics, err := newCronjobMetricsWithProvider(sdkmetric.NewMeterProvider())
	if err != nil {
		t.Fatalf("creating metrics: %v", err)
	}

	opts := workerOptions(metrics)

	if len(opts.Interceptors) != 1 {
		t.Fatalf("worker options carry %d interceptors, want exactly 1", len(opts.Interceptors))
	}
	if _, ok := opts.Interceptors[0].(*runMetricsInterceptor); !ok {
		t.Errorf("interceptor is %T, want *runMetricsInterceptor — without it an on-demand "+
			"job emits no cronjob.runs.total and the alerts cannot see it", opts.Interceptors[0])
	}
}

package temporal

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	mnoop "go.opentelemetry.io/otel/metric/noop"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
)

func validWorkerConfig() WorkerConfig {
	return WorkerConfig{
		Name:         "test-on-demand",
		OpenDatabase: func(context.Context) (*pgxpool.Pool, error) { return nil, nil },
		Register:     func(context.Context, Dependencies, worker.Registry) error { return nil },
	}
}

func TestWorkerConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*WorkerConfig)
		wantErr string
	}{
		{name: "complete config", mutate: func(*WorkerConfig) {}},
		{
			name:    "missing name",
			mutate:  func(c *WorkerConfig) { c.Name = "" },
			wantErr: "Name is required",
		},
		{
			name:    "missing database opener",
			mutate:  func(c *WorkerConfig) { c.OpenDatabase = nil },
			wantErr: "OpenDatabase is required",
		},
		{
			name:    "missing register hook",
			mutate:  func(c *WorkerConfig) { c.Register = nil },
			wantErr: "Register is required",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validWorkerConfig()
			tc.mutate(&cfg)

			err := cfg.validate()

			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

// RunWorker must install the global OTel providers before anything else is built,
// for the same reason RunCronjob must: service telemetry creates its instruments
// from the global providers at construction time, so a later init leaves them
// bound to no-ops for the process lifetime. The database opener returns a sentinel
// so the run stops before dialing Temporal, by which point the providers must
// already be set.
func TestRunWorker_InitializesOTELBeforeOpeningTheDatabase(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
	t.Setenv("JAEGER_ENDPOINT", "localhost:4317")

	prevTP := otel.GetTracerProvider()
	prevMP := otel.GetMeterProvider()
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetMeterProvider(prevMP)
	})

	sentinel := errors.New("sentinel: stop before temporal dial")
	cfg := validWorkerConfig()
	cfg.OpenDatabase = func(context.Context) (*pgxpool.Pool, error) { return nil, sentinel }

	err := RunWorker(context.Background(), BuildMeta{Commit: "test"}, cfg)

	if err == nil || !strings.Contains(err.Error(), "sentinel") {
		t.Fatalf("expected the sentinel database error, got %v", err)
	}
	if _, ok := otel.GetTracerProvider().(*sdktrace.TracerProvider); !ok {
		t.Errorf("global tracer provider = %T, want *sdktrace.TracerProvider", otel.GetTracerProvider())
	}
	if _, ok := otel.GetMeterProvider().(mnoop.MeterProvider); ok {
		t.Error("global meter provider is the no-op implementation; metrics would record nothing")
	}
}

// TestRegisterRunner_RunsTheRunnerFromAnInputlessStart is the whole point of the
// helper: a job with nothing to parameterise must be startable with no input
// payload, and the run must still reach the Runner. A workflow declaring a
// parameter could not be started that way without the operator inventing a
// value for it.
func TestRegisterRunner_RunsTheRunnerFromAnInputlessStart(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	ran := false

	err := RegisterRunner(env, RunnerJob{
		WorkflowType: "OneShotRepair",
		Runner: RunnerFunc(func(context.Context) error {
			ran = true
			return nil
		}),
		Timeouts: ActivityTimeouts{StartToClose: time.Minute, ScheduleToClose: 2 * time.Minute, MaximumAttempts: 3},
	})
	if err != nil {
		t.Fatalf("RegisterRunner: %v", err)
	}

	env.ExecuteWorkflow("OneShotRepair")

	if !env.IsWorkflowCompleted() {
		t.Fatal("expected the workflow to complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("running the registered workflow type with no input: %v", err)
	}
	if !ran {
		t.Error("the runner never ran; the registered workflow reached no activity")
	}
}

// The bounds must reach the activity from the registration rather than from
// workflow input, because an operator supplies no input: a MaximumAttempts of 1
// would leave a heartbeat-resuming job with no later attempt to resume into.
func TestRegisterRunner_AppliesTheRegisteredRetryBudget(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	attempts := 0

	err := RegisterRunner(env, RunnerJob{
		WorkflowType: "OneShotRepair",
		Runner: RunnerFunc(func(context.Context) error {
			attempts++
			return errors.New("chain read failed")
		}),
		Timeouts: ActivityTimeouts{StartToClose: time.Minute, ScheduleToClose: 2 * time.Minute, MaximumAttempts: 3},
	})
	if err != nil {
		t.Fatalf("RegisterRunner: %v", err)
	}

	env.ExecuteWorkflow("OneShotRepair")

	if env.GetWorkflowError() == nil {
		t.Fatal("expected the workflow to fail after exhausting its attempts")
	}
	if attempts != 3 {
		t.Errorf("runner attempts = %d, want 3 — the registered MaximumAttempts is what a hand-started run gets", attempts)
	}
}

func TestRegisterRunner_RequiresAWorkflowType(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()

	err := RegisterRunner(env, RunnerJob{Runner: RunnerFunc(func(context.Context) error { return nil })})

	if err == nil || !strings.Contains(err.Error(), "WorkflowType") {
		t.Fatalf("error = %v, want one naming the missing WorkflowType", err)
	}
}

func TestRunWorker_RejectsInvalidConfigBeforeAnySetup(t *testing.T) {
	opened := false
	cfg := validWorkerConfig()
	cfg.Name = ""
	cfg.OpenDatabase = func(context.Context) (*pgxpool.Pool, error) {
		opened = true
		return nil, nil
	}

	err := RunWorker(context.Background(), BuildMeta{}, cfg)

	if err == nil || !strings.Contains(err.Error(), "validating worker config") {
		t.Fatalf("error = %v, want a config validation error", err)
	}
	if opened {
		t.Error("database was opened despite an invalid config")
	}
}

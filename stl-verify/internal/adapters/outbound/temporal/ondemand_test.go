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
			name:    "no database opener and no declaration that none is wanted",
			mutate:  func(c *WorkerConfig) { c.OpenDatabase = nil },
			wantErr: "OpenDatabase is required (or set NoDatabase)",
		},
		{
			name:   "a job that declares it wants no database",
			mutate: func(c *WorkerConfig) { c.OpenDatabase, c.NoDatabase = nil, true },
		},
		{
			name:    "an opener alongside the declaration that none is wanted",
			mutate:  func(c *WorkerConfig) { c.NoDatabase = true },
			wantErr: "mutually exclusive",
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

// A zero Heartbeat leaves the activity with no heartbeat timeout, so a worker
// that dies holding a resume point is only noticed when StartToClose expires.
func TestRegisterRunner_RefusesAResumableJobWithNoLivenessTicker(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()

	err := RegisterRunner(env, RunnerJob{
		WorkflowType: "OneShotRepair",
		Runner:       RunnerFunc(func(context.Context) error { return nil }),
		Timeouts:     ActivityTimeouts{StartToClose: time.Minute},
		Progress:     &fakeProgressHeartbeater{onBeat: func() {}},
	})

	if err == nil || !strings.Contains(err.Error(), "Heartbeat") {
		t.Fatalf("error = %v, want one naming the missing Timeouts.Heartbeat", err)
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

// A pod told to stop before the worker was even built has done nothing wrong:
// surfacing the cancelled context makes the main exit 1 and the rollout look
// like a crash. RunWorker's clean stop after w.Run returns nil, and so does this.
func TestRunWorker_TreatsACancelledStartupAsAShutdown(t *testing.T) {
	t.Setenv("TEMPORAL_HOST_PORT", "127.0.0.1:1")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := RunWorker(ctx, BuildMeta{Commit: "test"}, validWorkerConfig())

	if err != nil {
		t.Fatalf("RunWorker = %v, want a cancelled startup reported as a clean stop", err)
	}
}

func TestNewBootstrap_SurfacesACancelledContextWithoutDialingTemporal(t *testing.T) {
	t.Setenv("TEMPORAL_HOST_PORT", "127.0.0.1:1")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := newBootstrap(ctx, BuildMeta{Commit: "test"}, "cancelled", nil)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("newBootstrap = %v, want the cancelled context, not a dial error", err)
	}
}

// Two jobs on one worker is what a worker that owns more than one backfill of the
// same data needs (the Uniswap V4 bootstrap owns two). Both register a method
// called Execute, so the second one names its activity; without that a real
// worker panics, and the test environment — which disables that check — silently
// routes BOTH workflow types to whichever Runner registered last. Each case
// therefore starts one of the two and demands its own Runner.
func TestRegisterRunner_HostsTwoJobsOnOneWorkerWithoutCrossingThem(t *testing.T) {
	for _, started := range []string{"FirstBackfill", "SecondBackfill"} {
		t.Run(started, func(t *testing.T) {
			env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
			var ran []string

			// The first job keeps the default name, as a deployed one must.
			for _, job := range []struct{ workflowType, activityName string }{
				{"FirstBackfill", ""},
				{"SecondBackfill", "SecondBackfillExecute"},
			} {
				name := job.workflowType
				err := RegisterRunner(env, RunnerJob{
					WorkflowType: name,
					ActivityName: job.activityName,
					Runner: RunnerFunc(func(context.Context) error {
						ran = append(ran, name)
						return nil
					}),
					Timeouts: ActivityTimeouts{StartToClose: time.Minute, ScheduleToClose: 2 * time.Minute, MaximumAttempts: 1},
				})
				if err != nil {
					t.Fatalf("RegisterRunner(%s): %v", name, err)
				}
			}

			env.ExecuteWorkflow(started)

			if err := env.GetWorkflowError(); err != nil {
				t.Fatalf("running %s: %v", started, err)
			}
			if len(ran) != 1 || ran[0] != started {
				t.Fatalf("runners that ran = %v, want only %s: the workflow reached another job's Runner", ran, started)
			}
		})
	}
}

// Every job deployed before ActivityName existed records "Execute" in its
// workflow histories, so a run in flight across a rollout replays that name. An
// unnamed job must keep it, and the scheduled cronjob path must keep it too.
func TestRunnerJobActivityName_DefaultsToTheAlreadyDeployedName(t *testing.T) {
	if cronjobActivityMethod != "Execute" {
		t.Fatalf("cronjobActivityMethod = %q, want Execute: an in-flight run replays against that name", cronjobActivityMethod)
	}
	unnamed := RunnerJob{WorkflowType: "UniswapV4PositionBootstrap"}
	if got := unnamed.activityName(); got != "Execute" {
		t.Errorf("an unnamed job's activity is %q, want Execute", got)
	}
	// Empty prefix leaves the SDK's own derivation from the method name alone.
	if got := unnamed.activityPrefix(); got != "" {
		t.Errorf("an unnamed job's registration prefix is %q, want empty", got)
	}

	named := RunnerJob{WorkflowType: "X", ActivityName: "UniswapV4PosmTransferBackfillExecute"}
	if got := named.activityName(); got != "UniswapV4PosmTransferBackfillExecute" {
		t.Errorf("a named job's activity is %q", got)
	}
	if got := named.activityPrefix(); got != "UniswapV4PosmTransferBackfill" {
		t.Errorf("a named job's registration prefix is %q, want the name minus the method", got)
	}
}

// The SDK builds the name as prefix+method, so a name that does not end in the
// method could never be registered under it — caught here rather than at boot.
func TestRegisterRunner_RefusesAnActivityNameThatCannotBeRegistered(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()

	err := RegisterRunner(env, RunnerJob{
		WorkflowType: "SomeBackfill",
		ActivityName: "SomeBackfillRun",
		Runner:       RunnerFunc(func(context.Context) error { return nil }),
		Timeouts:     ActivityTimeouts{StartToClose: time.Minute, MaximumAttempts: 1},
	})
	if err == nil || !strings.Contains(err.Error(), "must end in") {
		t.Fatalf("RegisterRunner error = %v, want it to reject a name that does not end in the method name", err)
	}
}

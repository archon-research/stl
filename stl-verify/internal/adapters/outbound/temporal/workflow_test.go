package temporal

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
)

type mockRunner struct {
	runFn func(ctx context.Context) error
}

func (m *mockRunner) Run(ctx context.Context) error { return m.runFn(ctx) }

func TestNewCronjobActivities(t *testing.T) {
	tests := []struct {
		name        string
		runner      Runner
		wantErr     bool
		errContains string
	}{
		{
			name:   "valid runner",
			runner: &mockRunner{},
		},
		{
			name:        "nil runner",
			wantErr:     true,
			errContains: "runner cannot be nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			activities, err := newCronjobActivities(tt.runner, nil, 0, nil)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q should contain %q", err.Error(), tt.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if activities == nil {
				t.Fatal("expected non-nil activities")
			}
		})
	}
}

func TestRunnerFunc(t *testing.T) {
	called := false
	fn := RunnerFunc(func(_ context.Context) error {
		called = true
		return nil
	})

	if err := fn.Run(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("expected function to be called")
	}
}

func TestCronjobActivities_Execute(t *testing.T) {
	tests := []struct {
		name        string
		runErr      error
		wantErr     bool
		errContains string
	}{
		{
			name: "successful execution",
		},
		{
			name:        "runner returns error",
			runErr:      errors.New("service failed"),
			wantErr:     true,
			errContains: "running cronjob",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			suite := &testsuite.WorkflowTestSuite{}
			activityEnv := suite.NewTestActivityEnvironment()

			scheduledAt := time.Date(2026, 6, 10, 10, 0, 0, 0, time.UTC)
			var called bool
			var gotScheduledAt time.Time
			var gotOK bool
			activities, err := newCronjobActivities(&mockRunner{
				runFn: func(ctx context.Context) error {
					called = true
					gotScheduledAt, gotOK = ScheduledAtFromContext(ctx)
					return tt.runErr
				},
			}, nil, 0, nil)
			if err != nil {
				t.Fatalf("unexpected error creating activities: %v", err)
			}

			activityEnv.RegisterActivity(activities.Execute)
			_, err = activityEnv.ExecuteActivity(activities.Execute, scheduledAt)

			if !called {
				t.Fatal("expected runner.Run to be called")
			}
			if !gotOK || !gotScheduledAt.Equal(scheduledAt) {
				t.Errorf("ScheduledAtFromContext = %v/%v, want %v/true", gotScheduledAt, gotOK, scheduledAt)
			}

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q should contain %q", err.Error(), tt.errContains)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// TestStartHeartbeat_ZeroIntervalIsANoOp: every cronjob that does not opt in
// keeps today's behavior — no goroutine, no heartbeat — and the returned stop is
// still safe to call.
func TestStartHeartbeat_ZeroIntervalIsANoOp(t *testing.T) {
	stop := startHeartbeat(context.Background(), 0, nil)
	if stop == nil {
		t.Fatal("startHeartbeat returned a nil stop function")
	}
	stop()
}

// TestCronjobActivities_Execute_HeartbeatsWithoutLeakingTheReporter runs a
// heartbeat-enabled activity whose runner outlives many intervals. The activity
// must still succeed, and Execute must join the reporting goroutine before
// returning — an unjoined reporter would keep heartbeating into a finished
// activity's context and leak one goroutine per run.
//
// It asserts on the absence of startHeartbeat's own frame rather than a
// goroutine count: the SDK spawns its own short-lived heartbeat batcher, so a
// count comparison would be measuring Temporal's internals, not this code's.
// The number of heartbeats is likewise not asserted — the SDK batches them
// before they reach any listener, so an exact count would flake.
func TestCronjobActivities_Execute_HeartbeatsWithoutLeakingTheReporter(t *testing.T) {
	const interval = 2 * time.Millisecond
	suite := &testsuite.WorkflowTestSuite{}
	activityEnv := suite.NewTestActivityEnvironment()

	activities, err := newCronjobActivities(&mockRunner{
		runFn: func(context.Context) error {
			time.Sleep(20 * interval)
			return nil
		},
	}, nil, interval, nil)
	if err != nil {
		t.Fatalf("newCronjobActivities: %v", err)
	}

	activityEnv.RegisterActivity(activities.Execute)
	if _, err := activityEnv.ExecuteActivity(activities.Execute, time.Now().UTC()); err != nil {
		t.Fatalf("ExecuteActivity: %v", err)
	}

	// Execute joins the reporter synchronously, so its frame must already be
	// gone — no sleep-and-hope.
	if stacks := goroutineStacks(t); strings.Contains(stacks, "startHeartbeat") {
		t.Errorf("a heartbeat reporter goroutine survived the activity; Execute did not join it:\n%s", stacks)
	}
}

// TestStartHeartbeat_BeatsThroughTheProgressStore: with a progress store the
// liveness ticker must not ping Temporal directly. A bare ping carries no
// details, and Temporal keeps only the last heartbeat's, so it would erase the
// resume point the runner recorded.
//
// The plain context is part of the assertion: activity.RecordHeartbeat panics
// outside an activity, so a ticker that bypassed the store would fail here
// rather than pass quietly.
func TestStartHeartbeat_BeatsThroughTheProgressStore(t *testing.T) {
	beats := make(chan struct{}, 1)
	store := &fakeProgressHeartbeater{onBeat: func() {
		select {
		case beats <- struct{}{}:
		default:
		}
	}}

	stop := startHeartbeat(context.Background(), time.Millisecond, store)
	defer stop()

	select {
	case <-beats:
	case <-time.After(2 * time.Second):
		t.Fatal("the liveness ticker never beat through the progress store")
	}
}

type fakeProgressHeartbeater struct {
	onBeat func()
}

func (f *fakeProgressHeartbeater) Beat(context.Context) { f.onBeat() }

func goroutineStacks(t *testing.T) string {
	t.Helper()
	buf := make([]byte, 1<<16)
	return string(buf[:runtime.Stack(buf, true)])
}

func TestActivityTimeouts_Resolve(t *testing.T) {
	tests := []struct {
		name string
		in   ActivityTimeouts
		want ActivityTimeouts
	}{
		{
			name: "zero value keeps the defaults every existing cronjob runs on",
			in:   ActivityTimeouts{},
			want: ActivityTimeouts{
				StartToClose:    defaultStartToCloseTimeout,
				ScheduleToClose: defaultScheduleToCloseTimeout,
				MaximumAttempts: defaultMaximumAttempts,
			},
		},
		{
			name: "each field is overridable independently",
			in:   ActivityTimeouts{StartToClose: 6 * time.Hour},
			want: ActivityTimeouts{
				StartToClose:    6 * time.Hour,
				ScheduleToClose: defaultScheduleToCloseTimeout,
				MaximumAttempts: defaultMaximumAttempts,
			},
		},
		{
			name: "fully specified value passes through",
			in:   ActivityTimeouts{StartToClose: 6 * time.Hour, ScheduleToClose: 12 * time.Hour, MaximumAttempts: 1},
			want: ActivityTimeouts{StartToClose: 6 * time.Hour, ScheduleToClose: 12 * time.Hour, MaximumAttempts: 1},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.in.resolve(); got != tc.want {
				t.Fatalf("resolve() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// TestCronjobWorkflow_MissingArgDecodesToDefaults pins backward compatibility
// with the schedules already stored in Temporal: they were created before
// cronjobWorkflow took a timeouts argument, so their action carries no payload.
// This exercises the exact decode Temporal performs for such a run
// (decodeArgsToRawValues → DataConverter.FromPayloads): an absent payload leaves
// the argument at its zero value, which resolve() maps back to today's defaults.
// If the SDK ever started rejecting a missing argument instead, every existing
// cronjob would break on redeploy — this test is the tripwire.
func TestCronjobWorkflow_MissingArgDecodesToDefaults(t *testing.T) {
	var decoded ActivityTimeouts
	if err := converter.GetDefaultDataConverter().FromPayloads(nil, &decoded); err != nil {
		t.Fatalf("decoding an absent workflow argument: %v", err)
	}
	want := ActivityTimeouts{
		StartToClose:    defaultStartToCloseTimeout,
		ScheduleToClose: defaultScheduleToCloseTimeout,
		MaximumAttempts: defaultMaximumAttempts,
	}
	if got := decoded.resolve(); got != want {
		t.Fatalf("resolved timeouts for a pre-existing schedule = %+v, want %+v", got, want)
	}
}

func TestCronjobWorkflow(t *testing.T) {
	tests := []struct {
		name        string
		activityErr error
		wantErr     bool
		errContains string
	}{
		{
			name: "successful workflow",
		},
		{
			name:        "activity returns error",
			activityErr: errors.New("activity failed"),
			wantErr:     true,
			errContains: "executing cronjob activity",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			suite := &testsuite.WorkflowTestSuite{}
			env := suite.NewTestWorkflowEnvironment()

			activityErr := tt.activityErr
			var gotScheduledAt time.Time
			env.RegisterActivityWithOptions(
				func(_ context.Context, scheduledAt time.Time) error {
					gotScheduledAt = scheduledAt
					return activityErr
				},
				activity.RegisterOptions{Name: "Execute"},
			)

			env.ExecuteWorkflow(cronjobWorkflow, ActivityTimeouts{})

			if gotScheduledAt.IsZero() {
				t.Error("workflow did not pass a scheduledAt to the activity")
			}

			if !env.IsWorkflowCompleted() {
				t.Fatal("expected workflow to be completed")
			}

			err := env.GetWorkflowError()
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q should contain %q", err.Error(), tt.errContains)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected workflow error: %v", err)
			}
		})
	}
}

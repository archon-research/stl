package temporal

import (
	"context"
	"errors"
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
			activities, err := newCronjobActivities(tt.runner, nil)
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
			}, nil)
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

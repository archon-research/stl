package temporalutil

import (
	"context"
	"errors"
	"strings"
	"testing"

	"go.temporal.io/sdk/activity"
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
			activities, err := newCronjobActivities(tt.runner)
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

			var called bool
			activities, err := newCronjobActivities(&mockRunner{
				runFn: func(_ context.Context) error {
					called = true
					return tt.runErr
				},
			})
			if err != nil {
				t.Fatalf("unexpected error creating activities: %v", err)
			}

			activityEnv.RegisterActivity(activities.Execute)
			result, err := activityEnv.ExecuteActivity(activities.Execute)

			if !called {
				t.Fatal("expected runner.Run to be called")
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
			var output CronjobOutput
			if err := result.Get(&output); err != nil {
				t.Fatalf("failed to get output: %v", err)
			}
			if !output.Success {
				t.Error("expected output.Success to be true")
			}
		})
	}
}

func TestCronjobWorkflow(t *testing.T) {
	tests := []struct {
		name           string
		activityResult *CronjobOutput
		activityErr    error
		wantErr        bool
		errContains    string
	}{
		{
			name:           "successful workflow",
			activityResult: &CronjobOutput{Success: true},
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

			result := tt.activityResult
			activityErr := tt.activityErr
			env.RegisterActivityWithOptions(
				func(_ context.Context) (*CronjobOutput, error) {
					return result, activityErr
				},
				activity.RegisterOptions{Name: "Execute"},
			)

			env.ExecuteWorkflow(cronjobWorkflow)

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
			var wfResult CronjobOutput
			if err := env.GetWorkflowResult(&wfResult); err != nil {
				t.Fatalf("failed to get workflow result: %v", err)
			}
			if !wfResult.Success {
				t.Error("expected result.Success to be true")
			}
		})
	}
}

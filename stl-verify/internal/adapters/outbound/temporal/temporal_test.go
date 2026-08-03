package temporal

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/mock"
	"go.opentelemetry.io/otel"
	mnoop "go.opentelemetry.io/otel/metric/noop"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/mocks"
)

// TestRunCronjob_InitializesOTEL pins the OTel bootstrap in RunCronjob:
// service telemetry (e.g. the maple indexer's) creates instruments from the
// GLOBAL providers, so if RunCronjob stopped initializing them every cronjob
// metric would silently become a no-op again. The database opener returns a
// sentinel error so the run stops before dialing Temporal — by then the
// providers must already be set.
func TestRunCronjob_InitializesOTEL(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
	t.Setenv("JAEGER_ENDPOINT", "localhost:4317")

	prevTP := otel.GetTracerProvider()
	prevMP := otel.GetMeterProvider()
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetMeterProvider(prevMP)
	})

	sentinel := errors.New("sentinel: stop before temporal dial")
	err := RunCronjob(context.Background(), BuildMeta{Commit: "test"}, CronjobConfig{
		Name:            "otel-test-cronjob",
		IntervalDefault: "10m",
		OpenDatabase: func(context.Context) (*pgxpool.Pool, error) {
			return nil, sentinel
		},
		Setup: func(context.Context, Dependencies) (Runner, error) {
			return RunnerFunc(func(context.Context) error { return nil }), nil
		},
	})
	if err == nil || !strings.Contains(err.Error(), "sentinel") {
		t.Fatalf("expected sentinel database error, got %v", err)
	}

	if _, ok := otel.GetTracerProvider().(*sdktrace.TracerProvider); !ok {
		t.Errorf("global tracer provider = %T, want *sdktrace.TracerProvider", otel.GetTracerProvider())
	}
	if _, ok := otel.GetMeterProvider().(mnoop.MeterProvider); ok {
		t.Error("global meter provider is the no-op implementation; cronjob metrics would record nothing")
	}
}

// TestCronjobConfig_Validate covers the Manual-mode relaxation (VEC-490): a
// trigger-only job needs no IntervalDefault, while every other job still does.
func TestCronjobConfig_Validate(t *testing.T) {
	openDB := func(context.Context) (*pgxpool.Pool, error) { return nil, nil }
	setup := func(context.Context, Dependencies) (Runner, error) { return nil, nil }

	tests := []struct {
		name    string
		cfg     CronjobConfig
		wantErr string
	}{
		{
			name: "manual job needs no interval",
			cfg:  CronjobConfig{Name: "backfill", Manual: true, OpenDatabase: openDB, Setup: setup},
		},
		{
			name:    "non-manual job requires interval",
			cfg:     CronjobConfig{Name: "ticker", OpenDatabase: openDB, Setup: setup},
			wantErr: "IntervalDefault is required",
		},
		{
			name: "interval job with interval is valid",
			cfg:  CronjobConfig{Name: "ticker", IntervalDefault: "5m", OpenDatabase: openDB, Setup: setup},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("validate() = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("validate() = %v, want error containing %q", err, tt.wantErr)
			}
		})
	}
}

// TestCronjobConfig_WorkflowArgs maps the activity overrides into the workflow
// input verbatim; zero values are preserved (cronjobWorkflow resolves them to
// defaults), so an interval job with no overrides passes a zero workflowParams.
func TestCronjobConfig_WorkflowArgs(t *testing.T) {
	if got := (CronjobConfig{}).workflowArgs(); got != (workflowParams{}) {
		t.Errorf("workflowArgs() with no overrides = %+v, want zero value", got)
	}
	cfg := CronjobConfig{
		ActivityStartToCloseTimeout:    24 * time.Hour,
		ActivityScheduleToCloseTimeout: 24 * time.Hour,
		ActivityHeartbeatTimeout:       2 * time.Minute,
		ActivityMaxAttempts:            1,
	}
	want := workflowParams{
		StartToCloseTimeout:    24 * time.Hour,
		ScheduleToCloseTimeout: 24 * time.Hour,
		HeartbeatTimeout:       2 * time.Minute,
		MaximumAttempts:        1,
	}
	if got := cfg.workflowArgs(); got != want {
		t.Errorf("workflowArgs() = %+v, want %+v", got, want)
	}
}

func TestBuildScheduleSpec_Offset(t *testing.T) {
	tests := []struct {
		name       string
		cfg        CronjobConfig
		env        map[string]string
		wantEvery  time.Duration
		wantOffset time.Duration
		wantErr    bool
	}{
		{
			name:       "no offset env configured",
			cfg:        CronjobConfig{IntervalDefault: "1h"},
			wantEvery:  time.Hour,
			wantOffset: 0,
		},
		{
			name:       "offset env set",
			cfg:        CronjobConfig{IntervalDefault: "1h", IntervalOffsetEnv: "OFFSET"},
			env:        map[string]string{"OFFSET": "5m"},
			wantEvery:  time.Hour,
			wantOffset: 5 * time.Minute,
		},
		{
			name:       "offset env empty falls back to zero",
			cfg:        CronjobConfig{IntervalDefault: "1h", IntervalOffsetEnv: "OFFSET"},
			env:        map[string]string{},
			wantEvery:  time.Hour,
			wantOffset: 0,
		},
		{
			name:       "interval env overrides default",
			cfg:        CronjobConfig{IntervalEnv: "INTERVAL", IntervalDefault: "1h"},
			env:        map[string]string{"INTERVAL": "30m"},
			wantEvery:  30 * time.Minute,
			wantOffset: 0,
		},
		{
			name:    "invalid offset errors",
			cfg:     CronjobConfig{IntervalDefault: "1h", IntervalOffsetEnv: "OFFSET"},
			env:     map[string]string{"OFFSET": "not-a-duration"},
			wantErr: true,
		},
		{
			name:    "invalid interval errors",
			cfg:     CronjobConfig{IntervalEnv: "INTERVAL", IntervalDefault: "1h"},
			env:     map[string]string{"INTERVAL": "not-a-duration"},
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			getenv := func(k string) string { return tc.env[k] }
			spec, err := buildScheduleSpec(tc.cfg, getenv)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			got := spec.Intervals[0]
			if got.Every != tc.wantEvery || got.Offset != tc.wantOffset {
				t.Fatalf("got {Every:%s Offset:%s}, want {Every:%s Offset:%s}",
					got.Every, got.Offset, tc.wantEvery, tc.wantOffset)
			}
		})
	}
}

func TestApplyScheduleSpecUpdate_PreservesActionReplacesSpec(t *testing.T) {
	action := &client.ScheduleWorkflowAction{ID: "scheduled-x", TaskQueue: "x"}
	in := client.ScheduleUpdateInput{
		Description: client.ScheduleDescription{
			Schedule: client.Schedule{
				Action: action,
				Spec:   &client.ScheduleSpec{Intervals: []client.ScheduleIntervalSpec{{Every: time.Hour}}},
			},
		},
	}
	want := client.ScheduleSpec{Intervals: []client.ScheduleIntervalSpec{{Every: time.Hour, Offset: 5 * time.Minute}}}

	upd := applyScheduleSpecUpdate(in, want)

	if upd.Schedule.Spec.Intervals[0].Offset != 5*time.Minute {
		t.Fatalf("Offset = %s, want 5m (spec must be replaced)", upd.Schedule.Spec.Intervals[0].Offset)
	}
	gotAction, ok := upd.Schedule.Action.(*client.ScheduleWorkflowAction)
	if !ok {
		t.Fatalf("Action type = %T, want *client.ScheduleWorkflowAction (action must be preserved)", upd.Schedule.Action)
	}
	if gotAction.ID != "scheduled-x" || gotAction.TaskQueue != "x" {
		t.Fatalf("Action = %+v, want ID=scheduled-x TaskQueue=x (action must be untouched)", gotAction)
	}
}

// TestApplyScheduleActionUpdate_PreservesSpecAndStateReplacesAction pins the
// manual (trigger-only) reconcile: the action's Args are patched while the timing
// spec and paused state are left untouched, so a redeploy never unpauses the
// schedule or re-adds an interval.
func TestApplyScheduleActionUpdate_PreservesSpecAndStateReplacesAction(t *testing.T) {
	paused := true
	in := client.ScheduleUpdateInput{
		Description: client.ScheduleDescription{
			Schedule: client.Schedule{
				Action: &client.ScheduleWorkflowAction{ID: "scheduled-backfill", TaskQueue: "backfill", Args: []any{"old"}},
				Spec:   &client.ScheduleSpec{}, // manual: no intervals
				State:  &client.ScheduleState{Paused: paused},
			},
		},
	}
	want := &client.ScheduleWorkflowAction{ID: "scheduled-backfill", TaskQueue: "backfill", Args: []any{"new"}}

	upd := applyScheduleActionUpdate(in, want)

	gotAction, ok := upd.Schedule.Action.(*client.ScheduleWorkflowAction)
	if !ok {
		t.Fatalf("Action type = %T, want *client.ScheduleWorkflowAction", upd.Schedule.Action)
	}
	if len(gotAction.Args) != 1 || gotAction.Args[0] != "new" {
		t.Fatalf("Args = %v, want [new] (action must be replaced)", gotAction.Args)
	}
	if len(upd.Schedule.Spec.Intervals) != 0 {
		t.Fatalf("Spec.Intervals = %v, want empty (no interval must be added)", upd.Schedule.Spec.Intervals)
	}
	if upd.Schedule.State == nil || !upd.Schedule.State.Paused {
		t.Fatalf("State.Paused = %v, want true (must stay paused)", upd.Schedule.State)
	}
}

// TestEnsureSchedule_ReconcileFailureIsNonFatal pins that a failed reconcile of
// an already-existing schedule does not abort worker startup. ensureSchedule is
// shared by every cronjob worker; the schedule already exists with a valid spec,
// so a transient Temporal error while re-applying the (best-effort) offset must
// not crashloop the worker.
func TestEnsureSchedule_ReconcileFailureIsNonFatal(t *testing.T) {
	handle := &mocks.ScheduleHandle{}
	handle.On("Update", mock.Anything, mock.Anything).Return(errors.New("temporal unavailable"))

	scheduleClient := &mocks.ScheduleClient{}
	scheduleClient.On("Create", mock.Anything, mock.Anything).
		Return(nil, errors.New("schedule already registered"))
	scheduleClient.On("GetHandle", mock.Anything, mock.Anything).Return(handle)

	c := &mocks.Client{}
	c.On("ScheduleClient").Return(scheduleClient)

	cfg := CronjobConfig{Name: "test-job", IntervalDefault: "1h"}
	err := ensureSchedule(context.Background(), c, slog.Default(), "test-job", cfg)
	if err != nil {
		t.Fatalf("ensureSchedule returned %v, want nil (reconcile failure must be non-fatal)", err)
	}
}

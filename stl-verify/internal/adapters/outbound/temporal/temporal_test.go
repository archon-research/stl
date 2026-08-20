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

func TestCronjobConfigValidate(t *testing.T) {
	base := func(mutate func(*CronjobConfig)) CronjobConfig {
		cfg := CronjobConfig{
			Name:            "job",
			IntervalDefault: "5m",
			OpenDatabase:    func(context.Context) (*pgxpool.Pool, error) { return nil, nil },
			Setup:           func(context.Context, Dependencies) (Runner, error) { return nil, nil },
		}
		mutate(&cfg)
		return cfg
	}
	tests := []struct {
		name    string
		cfg     CronjobConfig
		wantErr string
	}{
		{
			name: "scheduled cronjob with an interval is valid",
			cfg:  base(func(*CronjobConfig) {}),
		},
		{
			name:    "scheduled cronjob without an interval is rejected",
			cfg:     base(func(c *CronjobConfig) { c.IntervalDefault = "" }),
			wantErr: "IntervalDefault",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("validate() = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("validate() = %v, want an error mentioning %q", err, tc.wantErr)
			}
		})
	}
}

// A cronjob's schedule must reach Temporal unpaused and on its interval: a
// paused one would leave the job producing nothing while the worker looks
// perfectly healthy.
func TestEnsureSchedule_ScheduledJobStaysUnpaused(t *testing.T) {
	opts, err := captureScheduleCreate(t, CronjobConfig{Name: "interval-job", IntervalDefault: "1h"})
	if err != nil {
		t.Fatalf("ensureSchedule: %v", err)
	}
	if opts.Paused {
		t.Error("Paused = true for an interval-driven cronjob; a cronjob must keep running on its schedule")
	}
	if len(opts.Spec.Intervals) != 1 || opts.Spec.Intervals[0].Every != time.Hour {
		t.Errorf("Spec.Intervals = %v, want a single 1h interval", opts.Spec.Intervals)
	}
}

// TestEnsureSchedule_PassesActivityTimeoutsToWorkflow pins that the configured
// timeouts reach the schedule's workflow action as an argument. Without this a
// multi-hour bootstrap would be killed by the 10m default StartToCloseTimeout.
func TestEnsureSchedule_PassesActivityTimeoutsToWorkflow(t *testing.T) {
	want := ActivityTimeouts{StartToClose: 6 * time.Hour, ScheduleToClose: 12 * time.Hour, MaximumAttempts: 2}
	opts, err := captureScheduleCreate(t, CronjobConfig{Name: "long-job", IntervalDefault: "24h", ActivityTimeouts: want})
	if err != nil {
		t.Fatalf("ensureSchedule: %v", err)
	}
	action, ok := opts.Action.(*client.ScheduleWorkflowAction)
	if !ok {
		t.Fatalf("Action type = %T, want *client.ScheduleWorkflowAction", opts.Action)
	}
	if len(action.Args) != 1 {
		t.Fatalf("Args = %v, want exactly the activity timeouts", action.Args)
	}
	got, ok := action.Args[0].(ActivityTimeouts)
	if !ok {
		t.Fatalf("Args[0] type = %T, want ActivityTimeouts", action.Args[0])
	}
	if got != want {
		t.Fatalf("Args[0] = %+v, want %+v", got, want)
	}
}

// captureScheduleCreate runs ensureSchedule against a mock ScheduleClient and
// returns the ScheduleOptions it tried to create.
func captureScheduleCreate(t *testing.T, cfg CronjobConfig) (client.ScheduleOptions, error) {
	t.Helper()
	var got client.ScheduleOptions
	scheduleClient := &mocks.ScheduleClient{}
	scheduleClient.On("Create", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { got = args.Get(1).(client.ScheduleOptions) }).
		Return(nil, nil)

	c := &mocks.Client{}
	c.On("ScheduleClient").Return(scheduleClient)

	err := ensureSchedule(context.Background(), c, slog.Default(), cfg.Name, cfg)
	return got, err
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

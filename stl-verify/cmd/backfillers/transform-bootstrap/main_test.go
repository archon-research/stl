package main

import (
	"os"
	"strings"
	"testing"
	"time"
)

// TestParamsFromEnv covers the window resolution that runs at worker STARTUP, so
// a malformed value fails the pod rather than a run an operator has already
// started. Each rejection case matters because the alternative is a run that
// quietly copies a different window than the one asked for.
func TestParamsFromEnv(t *testing.T) {
	tests := []struct {
		name       string
		env        map[string]string
		wantStep   time.Duration
		wantFrom   time.Time
		wantSource string
		wantErr    string
	}{
		{
			name:     "all unset uses the default step and the derive-per-source sentinel",
			env:      nil,
			wantStep: defaultBootstrapStep,
		},
		{
			name:     "step override",
			env:      map[string]string{"BOOTSTRAP_STEP": "720h"},
			wantStep: 720 * time.Hour,
		},
		{
			name:     "from as a date",
			env:      map[string]string{"BOOTSTRAP_FROM": "2025-01-01"},
			wantStep: defaultBootstrapStep,
			wantFrom: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "from as rfc3339 with an offset is normalised to utc",
			env:      map[string]string{"BOOTSTRAP_FROM": "2025-01-01T02:00:00+02:00"},
			wantStep: defaultBootstrapStep,
			wantFrom: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:       "source scopes the run",
			env:        map[string]string{"BOOTSTRAP_SOURCE": "morpho_market_state"},
			wantStep:   defaultBootstrapStep,
			wantSource: "morpho_market_state",
		},
		{
			// An empty BOOTSTRAP_FROM must stay the sentinel rather than reach
			// ParseTime, which rejects "".
			name:     "empty from stays the sentinel",
			env:      map[string]string{"BOOTSTRAP_FROM": ""},
			wantStep: defaultBootstrapStep,
		},
		{
			name:    "unparseable from is rejected",
			env:     map[string]string{"BOOTSTRAP_FROM": "last tuesday"},
			wantErr: "BOOTSTRAP_FROM",
		},
		{
			// Set-but-empty is misconfiguration, not "unset": a ConfigMap key
			// present with an empty value must fail loudly rather than silently
			// fall back to the 30-day default.
			name:    "empty step is rejected rather than defaulted",
			env:     map[string]string{"BOOTSTRAP_STEP": ""},
			wantErr: "BOOTSTRAP_STEP",
		},
		{
			name:    "unparseable step is rejected",
			env:     map[string]string{"BOOTSTRAP_STEP": "a fortnight"},
			wantErr: "BOOTSTRAP_STEP",
		},
		{
			// A non-positive step never advances the per-window loop, so it must
			// not reach the service as a valid-looking param.
			name:    "zero step is rejected",
			env:     map[string]string{"BOOTSTRAP_STEP": "0s"},
			wantErr: "BOOTSTRAP_STEP",
		},
		{
			name:    "negative step is rejected",
			env:     map[string]string{"BOOTSTRAP_STEP": "-1h"},
			wantErr: "BOOTSTRAP_STEP",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setenv-then-Unsetenv: t.Setenv registers the restore for after the
			// test, os.Unsetenv makes the key genuinely absent. It matters that it
			// is absent rather than empty — GetPositiveDuration uses LookupEnv and
			// rejects a set-but-empty value instead of reading it as unset, which
			// the "empty step" case below pins.
			for _, k := range []string{"BOOTSTRAP_STEP", "BOOTSTRAP_FROM", "BOOTSTRAP_SOURCE"} {
				t.Setenv(k, "placeholder")
				if err := os.Unsetenv(k); err != nil {
					t.Fatalf("unsetting %s: %v", k, err)
				}
			}
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			got, err := paramsFromEnv()

			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("paramsFromEnv() = %+v, want an error naming %s", got, tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("paramsFromEnv() error = %q, want it to name %s", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("paramsFromEnv(): %v", err)
			}
			if got.Step != tt.wantStep {
				t.Errorf("Step = %v, want %v", got.Step, tt.wantStep)
			}
			if !got.From.Equal(tt.wantFrom) {
				t.Errorf("From = %v, want %v", got.From, tt.wantFrom)
			}
			if got.Source != tt.wantSource {
				t.Errorf("Source = %q, want %q", got.Source, tt.wantSource)
			}
		})
	}
}

// TestBootstrapActivityTimeouts pins the values a run depends on: the shared 10m
// default would kill a full-history walk, and a zero Heartbeat would leave a
// worker that died mid-run undetected until StartToClose (24h away).
func TestBootstrapActivityTimeouts(t *testing.T) {
	got := bootstrapActivityTimeouts

	if got.StartToClose != 24*time.Hour {
		t.Errorf("StartToClose = %v, want 24h", got.StartToClose)
	}
	if got.ScheduleToClose < got.StartToClose {
		t.Errorf("ScheduleToClose %v is below StartToClose %v, so it caps the run early",
			got.ScheduleToClose, got.StartToClose)
	}
	if got.Heartbeat <= 0 {
		t.Error("Heartbeat is zero, so a worker that dies mid-run is only noticed at StartToClose")
	}
	if got.Heartbeat >= got.StartToClose {
		t.Errorf("Heartbeat %v is not meaningfully shorter than StartToClose %v",
			got.Heartbeat, got.StartToClose)
	}
	if got.MaximumAttempts != 1 {
		t.Errorf("MaximumAttempts = %d, want 1: the walk records no resumable progress, "+
			"so a retry repeats hours of work from the first window", got.MaximumAttempts)
	}
}

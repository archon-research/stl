package main

import (
	"slices"
	"testing"
	"time"
)

func TestParseProjections(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		want    []string
		wantErr bool
	}{
		{"single", "materialize_morpho_market", []string{"materialize_morpho_market"}, false},
		{"several with spaces", " materialize_a , materialize_b ,materialize_c", []string{"materialize_a", "materialize_b", "materialize_c"}, false},
		{"trailing comma is not a blank entry", "materialize_a,materialize_b,", []string{"materialize_a", "materialize_b"}, false},
		{"empty segments dropped", ",,materialize_a,,", []string{"materialize_a"}, false},
		{"all empty", " , ,", nil, false},
		{"a view name is not a materializer", "position_morpho_market", nil, true},
		{"the shared function is not a projection", "materialize_position_projection", nil, true},
		{"quoting or SQL is rejected", `materialize_a"; drop table x; --`, nil, true},
		{"upper case is rejected", "Materialize_A", nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseProjections(tc.raw)
			if (err != nil) != tc.wantErr {
				t.Fatalf("parseProjections(%q) error = %v; wantErr %v", tc.raw, err, tc.wantErr)
			}
			if !slices.Equal(got, tc.want) {
				t.Fatalf("parseProjections(%q) = %v; want %v", tc.raw, got, tc.want)
			}
		})
	}
}

// The first tick is the full-history bootstrap, one statement per projection. At the shared 10m
// StartToClose Temporal would start a second attempt while the first query kept running, and the
// schedule keeps the timeouts it was created with, so the default could not be raised by a redeploy.
func TestMaterializeActivityTimeouts_AccommodateTheBootstrap(t *testing.T) {
	if materializeActivityTimeouts.StartToClose < 6*time.Hour {
		t.Errorf("StartToClose = %s, want at least 6h for the full-history first run", materializeActivityTimeouts.StartToClose)
	}
	if materializeActivityTimeouts.ScheduleToClose < materializeActivityTimeouts.StartToClose {
		t.Errorf("ScheduleToClose (%s) is below StartToClose (%s)", materializeActivityTimeouts.ScheduleToClose, materializeActivityTimeouts.StartToClose)
	}
	// Without a heartbeat the activity's context is never cancelled when Temporal gives up on it,
	// so the database query keeps running beside the next attempt.
	if materializeActivityTimeouts.Heartbeat <= 0 || materializeActivityTimeouts.Heartbeat >= materializeActivityTimeouts.StartToClose {
		t.Errorf("Heartbeat = %s, want positive and shorter than StartToClose", materializeActivityTimeouts.Heartbeat)
	}
	if materializeActivityTimeouts.MaximumAttempts <= 0 || materializeActivityTimeouts.MaximumAttempts > 5 {
		t.Errorf("MaximumAttempts = %d, want 1..5", materializeActivityTimeouts.MaximumAttempts)
	}
}

// The sizing above only matters if the worker is built with it.
func TestCronjobConfig_UsesTheMaterializeTimeouts(t *testing.T) {
	cfg := cronjobConfig("position-materializer", "postgres://u:p@localhost:5432/d", []string{"materialize_a"})
	if cfg.ActivityTimeouts != materializeActivityTimeouts {
		t.Errorf("ActivityTimeouts = %+v, want materializeActivityTimeouts %+v", cfg.ActivityTimeouts, materializeActivityTimeouts)
	}
}

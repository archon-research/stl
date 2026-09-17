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

// The first tick is the whole-history bootstrap; at the shared 10m StartToClose it could never finish,
// and the schedule keeps the timeouts it was created with, so a redeploy could not raise them.
func TestMaterializeActivityTimeouts_AccommodateTheBootstrap(t *testing.T) {
	if materializeActivityTimeouts.StartToClose < 6*time.Hour {
		t.Errorf("StartToClose = %s, want at least 6h for the full-history first run", materializeActivityTimeouts.StartToClose)
	}
	// Every attempt must be able to run to StartToClose, or the last one is cut short or never starts.
	if full := time.Duration(materializeActivityTimeouts.MaximumAttempts) * materializeActivityTimeouts.StartToClose; materializeActivityTimeouts.ScheduleToClose < full {
		t.Errorf("ScheduleToClose (%s) is below %d attempts of StartToClose (%s)", materializeActivityTimeouts.ScheduleToClose, materializeActivityTimeouts.MaximumAttempts, full)
	}
	// Without a heartbeat a dead worker is noticed only at StartToClose, and Temporal's cancellations
	// never reach the running query.
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

// A pod that dies mid-run must not leave its statement running beside the retry.
func TestMaterializerDBConfig_ChecksTheClientWhileAStatementRuns(t *testing.T) {
	cfg := materializerDBConfig("postgres://u:p@localhost:5432/d")
	if cfg.ClientConnectionCheckInterval <= 0 || cfg.ClientConnectionCheckInterval > time.Minute {
		t.Errorf("ClientConnectionCheckInterval = %s, want a positive interval of at most 1m", cfg.ClientConnectionCheckInterval)
	}
}

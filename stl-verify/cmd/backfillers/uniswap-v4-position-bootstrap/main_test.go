package main

import "testing"

// Both names are spelled out rather than compared to their constants, which
// would rename together and pin nothing. The alert regexes in
// alerts/vector-cronjobs.yaml, the Deployment and the runbook carry the same
// two strings.
func TestDeployedNames_MatchTheAlertsAndTheRunbook(t *testing.T) {
	if taskQueueName != "uniswap-v4-position-bootstrap" {
		t.Errorf("taskQueueName = %q, want %q", taskQueueName, "uniswap-v4-position-bootstrap")
	}
	if workflowTypeName != "UniswapV4PositionBootstrap" {
		t.Errorf("workflowTypeName = %q, want %q", workflowTypeName, "UniswapV4PositionBootstrap")
	}
}

// Heartbeat details are readable only by a later attempt of the same activity,
// so a single attempt would have nothing to resume into, and without a
// heartbeat a killed worker goes unnoticed until StartToClose expires.
func TestBootstrapActivityTimeouts_LeaveRoomToResume(t *testing.T) {
	if bootstrapActivityTimeouts.MaximumAttempts < 2 {
		t.Errorf("MaximumAttempts = %d, want at least 2", bootstrapActivityTimeouts.MaximumAttempts)
	}
	if bootstrapActivityTimeouts.Heartbeat <= 0 {
		t.Error("Heartbeat is zero: a dead worker would only be noticed at StartToClose")
	}
	if bootstrapActivityTimeouts.ScheduleToClose < bootstrapActivityTimeouts.StartToClose {
		t.Errorf("ScheduleToClose %s is below StartToClose %s: no attempt could use its full ceiling",
			bootstrapActivityTimeouts.ScheduleToClose, bootstrapActivityTimeouts.StartToClose)
	}
}

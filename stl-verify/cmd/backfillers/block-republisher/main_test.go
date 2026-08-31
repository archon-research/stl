package main

import "testing"

// Both names are spelled out rather than compared to their constants, which
// would rename together and pin nothing. The alert regex in
// alerts/vector-cronjobs.yaml and the runbook carry the same two strings.
func TestDeployedNames_MatchTheAlertsAndTheRunbook(t *testing.T) {
	if taskQueueName != "block-republisher" {
		t.Errorf("taskQueueName = %q, want %q", taskQueueName, "block-republisher")
	}
	if workflowTypeName != "BlockRepublish" {
		t.Errorf("workflowTypeName = %q, want %q", workflowTypeName, "BlockRepublish")
	}
}

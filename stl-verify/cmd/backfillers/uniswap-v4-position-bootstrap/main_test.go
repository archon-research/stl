package main

import (
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
)

// Every name is spelled out rather than compared to its constant, which would
// rename together and pin nothing. The alert regexes in
// alerts/vector-cronjobs.yaml, the Deployment and the runbook carry the same
// strings, and an operator types the workflow types by hand.
func TestDeployedNames_MatchTheAlertsAndTheRunbook(t *testing.T) {
	names := map[string]string{
		taskQueueName:            "uniswap-v4-position-bootstrap",
		positionWorkflowTypeName: "UniswapV4PositionBootstrap",
		transferWorkflowTypeName: "UniswapV4PosmTransferBackfill",
		metricPrefix:             "uniswap_v4",
	}
	for got, want := range names {
		if got != want {
			t.Errorf("deployed name %q, want %q", got, want)
		}
	}
}

// The transfer backfill's rows must land on the counter
// VectorUniswapV4NFTTransferGrowthHigh reads, which is the live indexer's, so the
// prefix has to be the one uniswapV4Factory.MetricPrefix() declares.
func TestMetricPrefix_IsTheLiveIndexers(t *testing.T) {
	if metricPrefix != "uniswap_v4" {
		t.Errorf("metricPrefix = %q, want uniswap_v4: the backfill would grow the table on a counter no rule reads", metricPrefix)
	}
}

// Heartbeat details are readable only by a later attempt of the same activity,
// so a single attempt would have nothing to resume into, and without a
// heartbeat a killed worker goes unnoticed until StartToClose expires.
func TestActivityTimeouts_LeaveRoomToResume(t *testing.T) {
	for name, timeouts := range map[string]temporal.ActivityTimeouts{
		positionWorkflowTypeName: bootstrapActivityTimeouts,
		transferWorkflowTypeName: transferActivityTimeouts,
	} {
		t.Run(name, func(t *testing.T) {
			if timeouts.MaximumAttempts < 2 {
				t.Errorf("MaximumAttempts = %d, want at least 2", timeouts.MaximumAttempts)
			}
			if timeouts.Heartbeat <= 0 {
				t.Error("Heartbeat is zero: a dead worker would only be noticed at StartToClose")
			}
			if timeouts.ScheduleToClose < timeouts.StartToClose {
				t.Errorf("ScheduleToClose %s is below StartToClose %s: no attempt could use its full ceiling",
					timeouts.ScheduleToClose, timeouts.StartToClose)
			}
		})
	}
}

package main

import (
	"regexp"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/pkg/chainutil"
)

// Every name is spelled out rather than compared to its constant, which would
// rename together and pin nothing. The alert regexes in
// alerts/vector-cronjobs.yaml, the Deployment and the runbook carry the same
// strings, and an operator types the workflow types by hand. The queue goes
// through the helper that builds it, as block-republisher's equivalent does;
// every chain's queue name is pinned with chainutil.TaskQueueName.
func TestDeployedNames_MatchTheAlertsAndTheRunbook(t *testing.T) {
	t.Setenv("CHAIN_ID", "1")

	queue, err := chainutil.TaskQueueName(ethereumQueueName)
	if err != nil {
		t.Fatalf("TaskQueueName error = %v", err)
	}

	names := map[string]string{
		queue:                    "uniswap-v4-position-bootstrap",
		positionWorkflowTypeName: "UniswapV4PositionBootstrap",
		transferWorkflowTypeName: "UniswapV4PosmTransferBackfill",
		// uniswapV4Factory.MetricPrefix()'s value: the backfill's rows have to land
		// on the counter VectorUniswapV4NFTTransferGrowthHigh reads, the live
		// indexer's, or they grow the table on a series no rule looks at.
		metricPrefix: "uniswap_v4",
	}
	for got, want := range names {
		if got != want {
			t.Errorf("deployed name %q, want %q", got, want)
		}
	}
}

// RunWorker hands WorkerConfig.Name to InitOTEL as the OTel service name, so the
// nft-transfer selectors in alerts/vector-indexers.yaml have to match that name.
func TestWorkerConfig_NamesTheServiceTheAlertSelectorsMatch(t *testing.T) {
	// Spelled out as the rules spell it, and anchored both ends the way Prometheus
	// anchors =~ and !~, so the chain-prefix form is what is being checked.
	const selector = `(^|.*-)uniswap-v4-position-bootstrap`
	matcher := regexp.MustCompile(`^(?:` + selector + `)$`)

	// Both forms of the derived queue, since it is the name RunWorker exports.
	for _, chainID := range []string{"1", "8453"} {
		t.Run(chainID, func(t *testing.T) {
			t.Setenv("CHAIN_ID", chainID)
			taskQueue, err := chainutil.TaskQueueName(ethereumQueueName)
			if err != nil {
				t.Fatalf("TaskQueueName: %v", err)
			}

			cfg := (&bootstrapWorker{}).workerConfig(taskQueue, "postgres://unused/unused")

			if !matcher.MatchString(cfg.Name) {
				t.Errorf("WorkerConfig.Name = %q: the nft-transfer rules select this worker with service_name=~%q, which does not match it",
					cfg.Name, selector)
			}
		})
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

// A full mainnet posm history plus its archive version reads is a multi-hour run,
// so the ceiling has to clear one end to end and the total has to admit the
// attempts the retry budget promises. Temporal kills an activity at StartToClose
// mid-run, and a resumed attempt would then restart the same wall clock.
func TestTransferActivityTimeouts_ClearAMultiHourRun(t *testing.T) {
	const longestExpectedRun = 7*time.Hour + 30*time.Minute

	if transferActivityTimeouts.StartToClose < longestExpectedRun {
		t.Errorf("StartToClose = %s, want at least %s: a run measured at that length would be killed mid-flight",
			transferActivityTimeouts.StartToClose, longestExpectedRun)
	}
	if want := time.Duration(transferActivityTimeouts.MaximumAttempts) * longestExpectedRun; transferActivityTimeouts.ScheduleToClose < want {
		t.Errorf("ScheduleToClose = %s, want at least %s so %d attempts of a %s run each fit",
			transferActivityTimeouts.ScheduleToClose, want, transferActivityTimeouts.MaximumAttempts, longestExpectedRun)
	}
}

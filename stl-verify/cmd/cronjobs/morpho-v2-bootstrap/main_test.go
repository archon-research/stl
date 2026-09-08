package main

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/services/morpho_v2_bootstrap"
)

// Env-validation paths that resolve before any database or RPC access run as
// plain unit tests; the service-wiring path is covered by the integration test
// (main_integration_test.go).

func discardDeps() temporal.Dependencies {
	return temporal.Dependencies{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func TestRun_RequiresDatabaseURL(t *testing.T) {
	t.Setenv("DATABASE_URL", "")

	err := run(context.Background())
	if err == nil {
		t.Fatal("missing DATABASE_URL should error, got nil")
	}
	if !strings.Contains(err.Error(), "DATABASE_URL") {
		t.Errorf("error %q should mention DATABASE_URL", err.Error())
	}
}

// The task queue doubles as the OTel service name, which is the label the
// vector-cronjobs alerts select this job by. Spelled out rather than compared to
// the constant, which would rename both sides together and pin nothing.
func TestTaskQueueName_MatchesTheAlertServiceName(t *testing.T) {
	if taskQueueName != "morpho-v2-bootstrap" {
		t.Errorf("taskQueueName = %q, want %q", taskQueueName, "morpho-v2-bootstrap")
	}
}

func TestSetupRunner_RequiresChainID(t *testing.T) {
	t.Setenv("CHAIN_ID", "")
	t.Setenv("ALCHEMY_API_KEY", "key")

	_, _, err := setupRunner(context.Background(), discardDeps(), temporal.NewActivityProgress[morpho_v2_bootstrap.SweepProgress]())
	if err == nil {
		t.Fatal("missing CHAIN_ID should error, got nil")
	}
	if !strings.Contains(err.Error(), "CHAIN_ID") {
		t.Errorf("error %q should mention CHAIN_ID", err.Error())
	}
	if !strings.Contains(err.Error(), "requiring chain ID") {
		t.Errorf("error %q should identify the failed operation", err.Error())
	}
}

func TestSetupRunner_RequiresAlchemyKey(t *testing.T) {
	t.Setenv("CHAIN_ID", "1")
	t.Setenv("ALCHEMY_API_KEY", "")

	_, _, err := setupRunner(context.Background(), discardDeps(), temporal.NewActivityProgress[morpho_v2_bootstrap.SweepProgress]())
	if err == nil {
		t.Fatal("missing ALCHEMY_API_KEY should error, got nil")
	}
	if !strings.Contains(err.Error(), "ALCHEMY_API_KEY") {
		t.Errorf("error %q should mention ALCHEMY_API_KEY", err.Error())
	}
	if !strings.Contains(err.Error(), "resolving RPC URL") {
		t.Errorf("error %q should identify the failed operation", err.Error())
	}
}

func TestSetupRunner_IdentifiesSweepConfigFailure(t *testing.T) {
	t.Setenv("CHAIN_ID", "1")
	t.Setenv("BOOTSTRAP_BLOCK_CHUNK_SIZE", "lots")

	_, _, err := setupRunner(context.Background(), discardDeps(), temporal.NewActivityProgress[morpho_v2_bootstrap.SweepProgress]())
	if err == nil || !strings.Contains(err.Error(), "parsing sweep config") {
		t.Fatalf("err = %v, want sweep-config operation context", err)
	}
}

func TestRegister_IdentifiesRunnerSetupFailure(t *testing.T) {
	t.Setenv("CHAIN_ID", "")

	err := (&bootstrapWorker{}).register(context.Background(), discardDeps(), nil)
	if err == nil || !strings.Contains(err.Error(), "setting up bootstrap runner") {
		t.Fatalf("err = %v, want runner-setup operation context", err)
	}
}

func TestParseSweepConfig(t *testing.T) {
	defaults := morpho_v2_bootstrap.ConfigDefaults()
	tests := []struct {
		name          string
		env           map[string]string
		wantChunk     int64
		wantBatch     int
		wantErrSubstr string
	}{
		{
			name:      "unset env keeps the defaults",
			env:       map[string]string{},
			wantChunk: defaults.BlockChunkSize,
			wantBatch: defaults.AddressBatchSize,
		},
		{
			name:      "both tunables overridable",
			env:       map[string]string{"BOOTSTRAP_BLOCK_CHUNK_SIZE": "2000", "BOOTSTRAP_ADDRESS_BATCH_SIZE": "25"},
			wantChunk: 2000,
			wantBatch: 25,
		},
		{
			name:          "a malformed chunk size is a startup error, not a silent default",
			env:           map[string]string{"BOOTSTRAP_BLOCK_CHUNK_SIZE": "lots"},
			wantErrSubstr: "BOOTSTRAP_BLOCK_CHUNK_SIZE",
		},
		{
			name:          "a malformed batch size is a startup error",
			env:           map[string]string{"BOOTSTRAP_ADDRESS_BATCH_SIZE": "many"},
			wantErrSubstr: "BOOTSTRAP_ADDRESS_BATCH_SIZE",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseSweepConfig(func(k string) string { return tc.env[k] })
			if tc.wantErrSubstr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSubstr) {
					t.Fatalf("err = %v, want one mentioning %q", err, tc.wantErrSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseSweepConfig: %v", err)
			}
			if got.BlockChunkSize != tc.wantChunk || got.AddressBatchSize != tc.wantBatch {
				t.Fatalf("config = {chunk %d, batch %d}, want {%d, %d}",
					got.BlockChunkSize, got.AddressBatchSize, tc.wantChunk, tc.wantBatch)
			}
		})
	}
}

// TestBootstrapActivityTimeouts_AllowARetryToResumeButStayBounded: heartbeat
// details are readable only by a LATER ATTEMPT of the same activity, so a single
// attempt cannot resume — an interrupted run would restart at the deploy block.
// The count stays small so a run that keeps failing still goes red for an
// operator instead of retrying all day.
func TestBootstrapActivityTimeouts_AllowARetryToResumeButStayBounded(t *testing.T) {
	if bootstrapActivityTimeouts.MaximumAttempts < 2 {
		t.Errorf("MaximumAttempts = %d, want at least 2 — with one attempt there is no attempt to resume into",
			bootstrapActivityTimeouts.MaximumAttempts)
	}
	if bootstrapActivityTimeouts.MaximumAttempts > 5 {
		t.Errorf("MaximumAttempts = %d, want at most 5 — a run that keeps failing must reach an operator",
			bootstrapActivityTimeouts.MaximumAttempts)
	}
}

// TestBootstrapActivityTimeouts_AccommodateAMultiHourRun guards the reason this
// cronjob needed the shared runner extended at all: a full mainnet sweep runs for
// hours, and the shared 10m StartToClose default would kill it mid-run, leaving
// the repair permanently half-done from the operator's point of view.
func TestBootstrapActivityTimeouts_AccommodateAMultiHourRun(t *testing.T) {
	if bootstrapActivityTimeouts.StartToClose < 6*time.Hour {
		t.Errorf("StartToClose = %s, want at least 6h for a full mainnet sweep", bootstrapActivityTimeouts.StartToClose)
	}
	if bootstrapActivityTimeouts.ScheduleToClose < bootstrapActivityTimeouts.StartToClose {
		t.Errorf("ScheduleToClose (%s) is below StartToClose (%s); the run would be cut short",
			bootstrapActivityTimeouts.ScheduleToClose, bootstrapActivityTimeouts.StartToClose)
	}
	// Without a heartbeat, a worker killed mid-run (any deploy rolls this
	// Deployment) holds the activity open until StartToClose expires — hours of
	// a job appearing to run with nothing behind it.
	if bootstrapActivityTimeouts.Heartbeat <= 0 {
		t.Error("Heartbeat is unset; a killed worker would go undetected until StartToClose expires")
	}
	if bootstrapActivityTimeouts.Heartbeat >= bootstrapActivityTimeouts.StartToClose {
		t.Errorf("Heartbeat (%s) is not shorter than StartToClose (%s), so it detects nothing sooner",
			bootstrapActivityTimeouts.Heartbeat, bootstrapActivityTimeouts.StartToClose)
	}
}

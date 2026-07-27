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

func TestSetupRunner_RequiresChainID(t *testing.T) {
	t.Setenv("CHAIN_ID", "")
	t.Setenv("ALCHEMY_API_KEY", "key")

	_, err := setupRunner(context.Background(), discardDeps())
	if err == nil {
		t.Fatal("missing CHAIN_ID should error, got nil")
	}
	if !strings.Contains(err.Error(), "CHAIN_ID") {
		t.Errorf("error %q should mention CHAIN_ID", err.Error())
	}
}

func TestSetupRunner_RequiresAlchemyKey(t *testing.T) {
	t.Setenv("CHAIN_ID", "1")
	t.Setenv("ALCHEMY_API_KEY", "")

	_, err := setupRunner(context.Background(), discardDeps())
	if err == nil {
		t.Fatal("missing ALCHEMY_API_KEY should error, got nil")
	}
	if !strings.Contains(err.Error(), "ALCHEMY_API_KEY") {
		t.Errorf("error %q should mention ALCHEMY_API_KEY", err.Error())
	}
}

func TestResolveRPCURL(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		want    string
		wantErr string
	}{
		{
			name: "defaults to eth mainnet",
			env:  map[string]string{"ALCHEMY_API_KEY": "secret"},
			want: "https://eth-mainnet.g.alchemy.com/v2/secret",
		},
		{
			name: "honours an explicit base url",
			env:  map[string]string{"ALCHEMY_API_KEY": "secret", "ALCHEMY_HTTP_URL": "https://base-mainnet.g.alchemy.com/v2"},
			want: "https://base-mainnet.g.alchemy.com/v2/secret",
		},
		{
			name:    "an absent key is a hard error, never an unauthenticated URL",
			env:     map[string]string{},
			wantErr: "ALCHEMY_API_KEY",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveRPCURL(func(k string) string { return tc.env[k] })
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want one mentioning %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveRPCURL: %v", err)
			}
			if got != tc.want {
				t.Fatalf("url = %q, want %q", got, tc.want)
			}
		})
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
	if bootstrapActivityTimeouts.MaximumAttempts != 1 {
		t.Errorf("MaximumAttempts = %d, want 1 — a failed multi-hour run needs an operator, not an automatic retry",
			bootstrapActivityTimeouts.MaximumAttempts)
	}
}

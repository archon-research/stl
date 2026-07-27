package main

import (
	"strings"
	"testing"
)

func TestParseConfig(t *testing.T) {
	// Neutralise any ambient env fallbacks so the flag-only cases are
	// deterministic across machines/CI.
	t.Setenv("S3_BUCKET", "")
	t.Setenv("DATABASE_URL", "")
	t.Setenv("RPC_URL", "")

	const (
		bucket = "test-bucket"
		db     = "postgres://localhost/test"
		rpc    = "http://localhost:8545"
	)
	// withConns returns a fresh args slice with the three required connection
	// flags plus the given extras, avoiding shared-backing-array aliasing.
	withConns := func(extra ...string) []string {
		return append([]string{"-bucket", bucket, "-db", db, "-rpc-url", rpc}, extra...)
	}

	// VaultV2 factory deploy block on Ethereum mainnet (chain 1).
	const v2DeployBlockMainnet = 23_375_073

	tests := []struct {
		name     string
		args     []string
		wantFrom int64
		wantTo   int64
		wantErr  bool
		errSub   string
	}{
		{
			name:     "explicit from and to",
			args:     withConns("-from", "100", "-to", "200"),
			wantFrom: 100,
			wantTo:   200,
		},
		{
			name:     "from-v2-deploy defaults from on chain 1",
			args:     withConns("-from-v2-deploy", "-to", "23500000"),
			wantFrom: v2DeployBlockMainnet,
			wantTo:   23500000,
		},
		{
			name:     "explicit from wins over from-v2-deploy",
			args:     withConns("-from-v2-deploy", "-from", "23400000", "-to", "23500000"),
			wantFrom: 23400000,
			wantTo:   23500000,
		},
		{
			name:    "from-v2-deploy on unsupported chain errors",
			args:    withConns("-from-v2-deploy", "-to", "200", "-chain-id", "8453"),
			wantErr: true,
			errSub:  "from-v2-deploy",
		},
		{
			name:    "missing from without from-v2-deploy errors",
			args:    withConns("-to", "200"),
			wantErr: true,
			errSub:  "-from",
		},
		{
			name:    "missing bucket errors",
			args:    []string{"-db", db, "-rpc-url", rpc, "-from", "1", "-to", "2"},
			wantErr: true,
			errSub:  "bucket",
		},
		{
			name:    "from greater than to errors",
			args:    withConns("-from", "300", "-to", "200"),
			wantErr: true,
			errSub:  "-from",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := parseConfig(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (cfg=%+v)", cfg)
				}
				if tt.errSub != "" && !strings.Contains(err.Error(), tt.errSub) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.errSub)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cfg.from != tt.wantFrom {
				t.Errorf("from = %d, want %d", cfg.from, tt.wantFrom)
			}
			if cfg.to != tt.wantTo {
				t.Errorf("to = %d, want %d", cfg.to, tt.wantTo)
			}
		})
	}
}

func TestParseConfig_ReplayProgressFileDefaultsEmpty(t *testing.T) {
	t.Setenv("S3_BUCKET", "")
	t.Setenv("DATABASE_URL", "")
	t.Setenv("RPC_URL", "")

	cfg, err := parseConfig([]string{
		"-bucket", "b", "-db", "d", "-rpc-url", "r", "-from", "1", "-to", "2",
	})
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.replayProgressFile != "" {
		t.Errorf("replayProgressFile = %q, want empty (no checkpointing by default)", cfg.replayProgressFile)
	}

	cfg, err = parseConfig([]string{
		"-bucket", "b", "-db", "d", "-rpc-url", "r", "-from", "1", "-to", "2",
		"-replay-progress-file", "/tmp/progress.jsonl",
	})
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.replayProgressFile != "/tmp/progress.jsonl" {
		t.Errorf("replayProgressFile = %q, want /tmp/progress.jsonl", cfg.replayProgressFile)
	}
}

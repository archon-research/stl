package main

import (
	"os"
	"testing"
)

func TestParseFlags(t *testing.T) {
	// Unset DATABASE_URL before each test so env doesn't bleed across cases.
	t.Cleanup(func() { os.Unsetenv("DATABASE_URL") })

	tests := []struct {
		name    string
		args    []string
		env     map[string]string
		wantErr bool
		wantCfg *cliConfig // nil means don't check fields
	}{
		{
			name:    "missing --from defaults to -1 and is rejected",
			args:    []string{"--to", "100", "--bucket", "b", "--rpc-url", "http://x", "--db", "postgres://x"},
			wantErr: true,
		},
		{
			name:    "missing --to defaults to -1 and is rejected",
			args:    []string{"--from", "0", "--bucket", "b", "--rpc-url", "http://x", "--db", "postgres://x"},
			wantErr: true,
		},
		{
			name:    "from 0 to 0 is valid (block zero)",
			args:    []string{"--from", "0", "--to", "0", "--bucket", "b", "--rpc-url", "http://x", "--db", "postgres://x"},
			wantErr: false,
			wantCfg: &cliConfig{
				fromBlock:   0,
				toBlock:     0,
				bucket:      "b",
				rpcURL:      "http://x",
				dbURL:       "postgres://x",
				chainID:     1,
				concurrency: 10,
				awsRegion:   "us-east-1",
			},
		},
		{
			name:    "--to < --from is rejected",
			args:    []string{"--from", "100", "--to", "50", "--bucket", "b", "--rpc-url", "http://x", "--db", "postgres://x"},
			wantErr: true,
		},
		{
			name:    "missing --bucket is rejected",
			args:    []string{"--from", "0", "--to", "10", "--rpc-url", "http://x", "--db", "postgres://x"},
			wantErr: true,
		},
		{
			name:    "missing --rpc-url is rejected",
			args:    []string{"--from", "0", "--to", "10", "--bucket", "b", "--db", "postgres://x"},
			wantErr: true,
		},
		{
			name:    "missing --db with no DATABASE_URL env var is rejected",
			args:    []string{"--from", "0", "--to", "10", "--bucket", "b", "--rpc-url", "http://x"},
			wantErr: true,
		},
		{
			name:    "missing --db falls back to DATABASE_URL env var",
			args:    []string{"--from", "0", "--to", "10", "--bucket", "b", "--rpc-url", "http://x"},
			env:     map[string]string{"DATABASE_URL": "postgres://from-env"},
			wantErr: false,
			wantCfg: &cliConfig{
				fromBlock:   0,
				toBlock:     10,
				bucket:      "b",
				rpcURL:      "http://x",
				dbURL:       "postgres://from-env",
				chainID:     1,
				concurrency: 10,
				awsRegion:   "us-east-1",
			},
		},
		{
			name: "valid full set of flags",
			args: []string{
				"--from", "1000",
				"--to", "2000",
				"--bucket", "my-bucket",
				"--rpc-url", "http://erigon:8545",
				"--db", "postgres://localhost/stl",
				"--chain-id", "1",
				"--concurrency", "4",
				"--aws-region", "eu-west-1",
			},
			wantErr: false,
			wantCfg: &cliConfig{
				fromBlock:   1000,
				toBlock:     2000,
				bucket:      "my-bucket",
				rpcURL:      "http://erigon:8545",
				dbURL:       "postgres://localhost/stl",
				chainID:     1,
				concurrency: 4,
				awsRegion:   "eu-west-1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.env {
				os.Setenv(k, v)
			}
			t.Cleanup(func() {
				for k := range tt.env {
					os.Unsetenv(k)
				}
			})

			cfg, err := parseFlags(tt.args)
			if tt.wantErr && err == nil {
				t.Errorf("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if tt.wantCfg != nil && err == nil {
				if cfg.fromBlock != tt.wantCfg.fromBlock {
					t.Errorf("fromBlock: got %d, want %d", cfg.fromBlock, tt.wantCfg.fromBlock)
				}
				if cfg.toBlock != tt.wantCfg.toBlock {
					t.Errorf("toBlock: got %d, want %d", cfg.toBlock, tt.wantCfg.toBlock)
				}
				if cfg.bucket != tt.wantCfg.bucket {
					t.Errorf("bucket: got %q, want %q", cfg.bucket, tt.wantCfg.bucket)
				}
				if cfg.rpcURL != tt.wantCfg.rpcURL {
					t.Errorf("rpcURL: got %q, want %q", cfg.rpcURL, tt.wantCfg.rpcURL)
				}
				if cfg.dbURL != tt.wantCfg.dbURL {
					t.Errorf("dbURL: got %q, want %q", cfg.dbURL, tt.wantCfg.dbURL)
				}
				if cfg.chainID != tt.wantCfg.chainID {
					t.Errorf("chainID: got %d, want %d", cfg.chainID, tt.wantCfg.chainID)
				}
				if cfg.concurrency != tt.wantCfg.concurrency {
					t.Errorf("concurrency: got %d, want %d", cfg.concurrency, tt.wantCfg.concurrency)
				}
				if cfg.awsRegion != tt.wantCfg.awsRegion {
					t.Errorf("awsRegion: got %q, want %q", cfg.awsRegion, tt.wantCfg.awsRegion)
				}
			}
		})
	}
}

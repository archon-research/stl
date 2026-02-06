package main

import (
	"strings"
	"testing"
)

func TestParseFlags(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		envVars   map[string]string
		wantCfg   cliConfig
		wantError string
	}{
		{
			name: "all flags provided via CLI",
			args: []string{"-rpc-url", "http://erigon:8545", "-from", "1000", "-to", "2000", "-db", "postgres://localhost:5432/testdb"},
			wantCfg: cliConfig{
				rpcURL:      "http://erigon:8545",
				fromBlock:   1000,
				toBlock:     2000,
				concurrency: 100,
				batchSize:   1000,
				dbURL:       "postgres://localhost:5432/testdb",
				verbose:     false,
			},
		},
		{
			name: "custom concurrency and batch-size",
			args: []string{"-rpc-url", "http://erigon:8545", "-from", "100", "-to", "200", "-db", "postgres://localhost/db", "-concurrency", "50", "-batch-size", "500"},
			wantCfg: cliConfig{
				rpcURL:      "http://erigon:8545",
				fromBlock:   100,
				toBlock:     200,
				concurrency: 50,
				batchSize:   500,
				dbURL:       "postgres://localhost/db",
				verbose:     false,
			},
		},
		{
			name:    "db from env var",
			args:    []string{"-rpc-url", "http://erigon:8545", "-from", "100", "-to", "200"},
			envVars: map[string]string{"DATABASE_URL": "postgres://localhost:5432/envdb"},
			wantCfg: cliConfig{
				rpcURL:      "http://erigon:8545",
				fromBlock:   100,
				toBlock:     200,
				concurrency: 100,
				batchSize:   1000,
				dbURL:       "postgres://localhost:5432/envdb",
				verbose:     false,
			},
		},
		{
			name: "verbose flag",
			args: []string{"-rpc-url", "http://erigon:8545", "-from", "100", "-to", "200", "-db", "postgres://localhost/db", "-verbose"},
			wantCfg: cliConfig{
				rpcURL:      "http://erigon:8545",
				fromBlock:   100,
				toBlock:     200,
				concurrency: 100,
				batchSize:   1000,
				dbURL:       "postgres://localhost/db",
				verbose:     true,
			},
		},
		{
			name: "default concurrency and batch-size",
			args: []string{"-rpc-url", "http://erigon:8545", "-from", "500", "-to", "600", "-db", "postgres://localhost/db"},
			wantCfg: cliConfig{
				rpcURL:      "http://erigon:8545",
				fromBlock:   500,
				toBlock:     600,
				concurrency: 100,
				batchSize:   1000,
				dbURL:       "postgres://localhost/db",
				verbose:     false,
			},
		},
		{
			name:      "missing rpc-url",
			args:      []string{"-from", "100", "-to", "200", "-db", "postgres://localhost/db"},
			wantError: "--rpc-url is required",
		},
		{
			name:      "missing from",
			args:      []string{"-rpc-url", "http://erigon:8545", "-to", "200", "-db", "postgres://localhost/db"},
			wantError: "--from is required",
		},
		{
			name:      "missing to",
			args:      []string{"-rpc-url", "http://erigon:8545", "-from", "100", "-db", "postgres://localhost/db"},
			wantError: "--to is required",
		},
		{
			name:      "to less than from",
			args:      []string{"-rpc-url", "http://erigon:8545", "-from", "200", "-to", "100", "-db", "postgres://localhost/db"},
			wantError: "--to must be >= --from",
		},
		{
			name:      "missing database URL - no flag no env",
			args:      []string{"-rpc-url", "http://erigon:8545", "-from", "100", "-to", "200"},
			wantError: "database URL not provided",
		},
		{
			name:      "invalid flag",
			args:      []string{"--nonexistent"},
			wantError: "flag provided but not defined",
		},
		{
			name: "CLI db flag takes precedence over env var",
			args: []string{"-rpc-url", "http://erigon:8545", "-from", "100", "-to", "200", "-db", "postgres://localhost/cli-db"},
			envVars: map[string]string{
				"DATABASE_URL": "postgres://localhost/env-db",
			},
			wantCfg: cliConfig{
				rpcURL:      "http://erigon:8545",
				fromBlock:   100,
				toBlock:     200,
				concurrency: 100,
				batchSize:   1000,
				dbURL:       "postgres://localhost/cli-db",
				verbose:     false,
			},
		},
		{
			name: "from equals to is valid",
			args: []string{"-rpc-url", "http://erigon:8545", "-from", "100", "-to", "100", "-db", "postgres://localhost/db"},
			wantCfg: cliConfig{
				rpcURL:      "http://erigon:8545",
				fromBlock:   100,
				toBlock:     100,
				concurrency: 100,
				batchSize:   1000,
				dbURL:       "postgres://localhost/db",
				verbose:     false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.envVars {
				t.Setenv(k, v)
			}

			cfg, err := parseFlags(tt.args)

			if tt.wantError != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.wantError)
				}
				if !strings.Contains(err.Error(), tt.wantError) {
					t.Fatalf("expected error containing %q, got %q", tt.wantError, err.Error())
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if cfg.rpcURL != tt.wantCfg.rpcURL {
				t.Errorf("rpcURL: expected %q, got %q", tt.wantCfg.rpcURL, cfg.rpcURL)
			}
			if cfg.fromBlock != tt.wantCfg.fromBlock {
				t.Errorf("fromBlock: expected %d, got %d", tt.wantCfg.fromBlock, cfg.fromBlock)
			}
			if cfg.toBlock != tt.wantCfg.toBlock {
				t.Errorf("toBlock: expected %d, got %d", tt.wantCfg.toBlock, cfg.toBlock)
			}
			if cfg.concurrency != tt.wantCfg.concurrency {
				t.Errorf("concurrency: expected %d, got %d", tt.wantCfg.concurrency, cfg.concurrency)
			}
			if cfg.batchSize != tt.wantCfg.batchSize {
				t.Errorf("batchSize: expected %d, got %d", tt.wantCfg.batchSize, cfg.batchSize)
			}
			if cfg.dbURL != tt.wantCfg.dbURL {
				t.Errorf("dbURL: expected %q, got %q", tt.wantCfg.dbURL, cfg.dbURL)
			}
			if cfg.verbose != tt.wantCfg.verbose {
				t.Errorf("verbose: expected %v, got %v", tt.wantCfg.verbose, cfg.verbose)
			}
		})
	}
}

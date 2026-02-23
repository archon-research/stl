package main

import (
	"strings"
	"testing"
)

func TestParseConfig(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		envVars   map[string]string
		wantCfg   cliConfig
		wantError string
	}{
		{
			name: "all flags provided via CLI",
			args: []string{"-queue", "https://sqs.us-east-1.amazonaws.com/123/my-queue", "-db", "postgres://localhost:5432/testdb"},
			envVars: map[string]string{
				"ETH_RPC_API_KEY":  "test-key",
				"ETH_RPC_HTTP_URL": "https://eth.example.com",
			},
			wantCfg: cliConfig{
				queueURL:       "https://sqs.us-east-1.amazonaws.com/123/my-queue",
				dbURL:          "postgres://localhost:5432/testdb",
				rpcHTTPBaseURL: "https://eth.example.com",
				rpcURL:         "https://eth.example.com/test-key",
			},
		},
		{
			name: "queue from env var",
			args: []string{"-db", "postgres://localhost:5432/testdb"},
			envVars: map[string]string{
				"AWS_SQS_QUEUE_URL": "https://sqs.us-east-1.amazonaws.com/123/env-queue",
				"ETH_RPC_API_KEY":   "test-key",
				"ETH_RPC_HTTP_URL":  "https://eth.example.com",
			},
			wantCfg: cliConfig{
				queueURL:       "https://sqs.us-east-1.amazonaws.com/123/env-queue",
				dbURL:          "postgres://localhost:5432/testdb",
				rpcHTTPBaseURL: "https://eth.example.com",
				rpcURL:         "https://eth.example.com/test-key",
			},
		},
		{
			name: "db from env var",
			args: []string{"-queue", "https://sqs.us-east-1.amazonaws.com/123/my-queue"},
			envVars: map[string]string{
				"DATABASE_URL":     "postgres://localhost:5432/envdb",
				"ETH_RPC_API_KEY":  "test-key",
				"ETH_RPC_HTTP_URL": "https://eth.example.com",
			},
			wantCfg: cliConfig{
				queueURL:       "https://sqs.us-east-1.amazonaws.com/123/my-queue",
				dbURL:          "postgres://localhost:5432/envdb",
				rpcHTTPBaseURL: "https://eth.example.com",
				rpcURL:         "https://eth.example.com/test-key",
			},
		},
		{
			name:      "missing queue URL - no flag no env",
			args:      []string{"-db", "postgres://localhost:5432/testdb"},
			envVars:   map[string]string{"ETH_RPC_API_KEY": "test-key"},
			wantError: "queue URL not provided",
		},
		{
			name:      "missing database URL - no flag no env",
			args:      []string{"-queue", "https://sqs.us-east-1.amazonaws.com/123/my-queue"},
			envVars:   map[string]string{"ETH_RPC_API_KEY": "test-key"},
			wantError: "database URL not provided",
		},
		{
			name:      "invalid flag",
			args:      []string{"--nonexistent"},
			wantError: "flag provided but not defined",
		},
		{
			name: "CLI flag takes precedence over env var",
			args: []string{"-queue", "https://sqs.us-east-1.amazonaws.com/123/cli-queue", "-db", "postgres://localhost/cli-db"},
			envVars: map[string]string{
				"AWS_SQS_QUEUE_URL": "https://sqs.us-east-1.amazonaws.com/123/env-queue",
				"DATABASE_URL":      "postgres://localhost/env-db",
				"ETH_RPC_API_KEY":   "test-key",
				"ETH_RPC_HTTP_URL":  "https://eth.example.com",
			},
			wantCfg: cliConfig{
				queueURL:       "https://sqs.us-east-1.amazonaws.com/123/cli-queue",
				dbURL:          "postgres://localhost/cli-db",
				rpcHTTPBaseURL: "https://eth.example.com",
				rpcURL:         "https://eth.example.com/test-key",
			},
		},
		{
			name: "both from env vars",
			args: []string{},
			envVars: map[string]string{
				"AWS_SQS_QUEUE_URL": "https://sqs.us-east-1.amazonaws.com/123/env-queue",
				"DATABASE_URL":      "postgres://localhost/env-db",
				"ETH_RPC_API_KEY":   "test-key",
				"ETH_RPC_HTTP_URL":  "https://eth.example.com",
			},
			wantCfg: cliConfig{
				queueURL:       "https://sqs.us-east-1.amazonaws.com/123/env-queue",
				dbURL:          "postgres://localhost/env-db",
				rpcHTTPBaseURL: "https://eth.example.com",
				rpcURL:         "https://eth.example.com/test-key",
			},
		},
		{
			name: "missing ETH_RPC_API_KEY",
			args: []string{"-queue", "https://sqs.us-east-1.amazonaws.com/123/my-queue", "-db", "postgres://localhost/db"},
			// No ETH_RPC_API_KEY set
			wantError: "ETH_RPC_API_KEY",
		},
		{
			name: "missing ETH_RPC_HTTP_URL",
			args: []string{"-queue", "https://sqs.us-east-1.amazonaws.com/123/my-queue", "-db", "postgres://localhost/db"},
			envVars: map[string]string{
				"ETH_RPC_API_KEY": "test-key",
			},
			wantError: "ETH_RPC_HTTP_URL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Ensure RPC env vars are empty unless provided in envVars.
			if _, has := tt.envVars["ETH_RPC_API_KEY"]; !has {
				t.Setenv("ETH_RPC_API_KEY", "")
			}
			if _, has := tt.envVars["ETH_RPC_HTTP_URL"]; !has {
				t.Setenv("ETH_RPC_HTTP_URL", "")
			}

			for k, v := range tt.envVars {
				t.Setenv(k, v)
			}

			cfg, err := parseConfig(tt.args)

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

			if cfg.queueURL != tt.wantCfg.queueURL {
				t.Errorf("queueURL: expected %q, got %q", tt.wantCfg.queueURL, cfg.queueURL)
			}
			if cfg.dbURL != tt.wantCfg.dbURL {
				t.Errorf("dbURL: expected %q, got %q", tt.wantCfg.dbURL, cfg.dbURL)
			}
			if cfg.rpcURL != tt.wantCfg.rpcURL {
				t.Errorf("rpcURL: expected %q, got %q", tt.wantCfg.rpcURL, cfg.rpcURL)
			}
			if cfg.rpcHTTPBaseURL != tt.wantCfg.rpcHTTPBaseURL {
				t.Errorf("rpcHTTPBaseURL: expected %q, got %q", tt.wantCfg.rpcHTTPBaseURL, cfg.rpcHTTPBaseURL)
			}
		})
	}
}

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
			args: []string{"-queue", "https://sqs.us-east-1.amazonaws.com/123/my-queue", "-db", "postgres://localhost:5432/testdb"},
			wantCfg: cliConfig{
				queueURL: "https://sqs.us-east-1.amazonaws.com/123/my-queue",
				dbURL:    "postgres://localhost:5432/testdb",
				verbose:  false,
			},
		},
		{
			name:    "queue from env var",
			args:    []string{"-db", "postgres://localhost:5432/testdb"},
			envVars: map[string]string{"AWS_SQS_QUEUE_URL": "https://sqs.us-east-1.amazonaws.com/123/env-queue"},
			wantCfg: cliConfig{
				queueURL: "https://sqs.us-east-1.amazonaws.com/123/env-queue",
				dbURL:    "postgres://localhost:5432/testdb",
				verbose:  false,
			},
		},
		{
			name:    "db from env var",
			args:    []string{"-queue", "https://sqs.us-east-1.amazonaws.com/123/my-queue"},
			envVars: map[string]string{"DATABASE_URL": "postgres://localhost:5432/envdb"},
			wantCfg: cliConfig{
				queueURL: "https://sqs.us-east-1.amazonaws.com/123/my-queue",
				dbURL:    "postgres://localhost:5432/envdb",
				verbose:  false,
			},
		},
		{
			name: "verbose flag",
			args: []string{"-queue", "https://sqs.us-east-1.amazonaws.com/123/my-queue", "-db", "postgres://localhost:5432/testdb", "-verbose"},
			wantCfg: cliConfig{
				queueURL: "https://sqs.us-east-1.amazonaws.com/123/my-queue",
				dbURL:    "postgres://localhost:5432/testdb",
				verbose:  true,
			},
		},
		{
			name:      "missing queue URL - no flag no env",
			args:      []string{"-db", "postgres://localhost:5432/testdb"},
			wantError: "queue URL not provided",
		},
		{
			name:      "missing database URL - no flag no env",
			args:      []string{"-queue", "https://sqs.us-east-1.amazonaws.com/123/my-queue"},
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
			},
			wantCfg: cliConfig{
				queueURL: "https://sqs.us-east-1.amazonaws.com/123/cli-queue",
				dbURL:    "postgres://localhost/cli-db",
				verbose:  false,
			},
		},
		{
			name: "both from env vars",
			args: []string{},
			envVars: map[string]string{
				"AWS_SQS_QUEUE_URL": "https://sqs.us-east-1.amazonaws.com/123/env-queue",
				"DATABASE_URL":      "postgres://localhost/env-db",
			},
			wantCfg: cliConfig{
				queueURL: "https://sqs.us-east-1.amazonaws.com/123/env-queue",
				dbURL:    "postgres://localhost/env-db",
				verbose:  false,
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

			if cfg.queueURL != tt.wantCfg.queueURL {
				t.Errorf("queueURL: expected %q, got %q", tt.wantCfg.queueURL, cfg.queueURL)
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

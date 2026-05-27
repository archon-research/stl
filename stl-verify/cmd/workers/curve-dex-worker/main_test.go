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
			args: []string{
				"-queue", "https://sqs.us-east-1.amazonaws.com/123/my-queue",
				"-db", "postgres://localhost:5432/testdb",
				"-redis", "localhost:6379",
			},
			envVars: map[string]string{
				"ALCHEMY_API_KEY":  "test-key",
				"ALCHEMY_HTTP_URL": "https://eth.example.com",
				"S3_BUCKET":        "my-bucket",
				"DEPLOY_ENV":       "sentinelstaging",
				"CHAIN_ID":         "1",
			},
			wantCfg: cliConfig{
				queueURL:          "https://sqs.us-east-1.amazonaws.com/123/my-queue",
				dbURL:             "postgres://localhost:5432/testdb",
				redisAddr:         "localhost:6379",
				alchemyURL:        "https://eth.example.com/test-key",
				s3Bucket:          "my-bucket",
				deployEnv:         "sentinelstaging",
				maxMessages:       10,
				waitTime:          20,
				visibilityTimeout: 300,
				chainID:           1,
			},
		},
		{
			name: "env vars only",
			args: []string{},
			envVars: map[string]string{
				"AWS_SQS_QUEUE_URL": "https://sqs.us-east-1.amazonaws.com/123/env-queue",
				"DATABASE_URL":      "postgres://localhost/envdb",
				"REDIS_ADDR":        "envredis:6379",
				"ALCHEMY_API_KEY":   "test-key",
				"S3_BUCKET":         "my-bucket",
				"DEPLOY_ENV":        "sentinelstaging",
				"CHAIN_ID":          "1",
			},
			wantCfg: cliConfig{
				queueURL:          "https://sqs.us-east-1.amazonaws.com/123/env-queue",
				dbURL:             "postgres://localhost/envdb",
				redisAddr:         "envredis:6379",
				alchemyURL:        "https://eth-mainnet.g.alchemy.com/v2/test-key",
				s3Bucket:          "my-bucket",
				deployEnv:         "sentinelstaging",
				maxMessages:       10,
				waitTime:          20,
				visibilityTimeout: 300,
				chainID:           1,
			},
		},
		{
			name:      "missing queue URL",
			args:      []string{"-db", "postgres://localhost/db", "-redis", "localhost:6379"},
			envVars:   map[string]string{"ALCHEMY_API_KEY": "test-key", "CHAIN_ID": "1"},
			wantError: "queue URL not provided",
		},
		{
			name:      "missing database URL",
			args:      []string{"-queue", "https://sqs.us-east-1.amazonaws.com/123/q", "-redis", "localhost:6379"},
			envVars:   map[string]string{"ALCHEMY_API_KEY": "test-key", "CHAIN_ID": "1"},
			wantError: "database URL not provided",
		},
		{
			name:      "missing ALCHEMY_API_KEY",
			args:      []string{"-queue", "https://sqs.us-east-1.amazonaws.com/123/q", "-db", "postgres://localhost/db", "-redis", "localhost:6379"},
			wantError: "ALCHEMY_API_KEY",
		},
		{
			name:      "missing redis address",
			args:      []string{"-queue", "https://sqs.us-east-1.amazonaws.com/123/q", "-db", "postgres://localhost/db"},
			envVars:   map[string]string{"ALCHEMY_API_KEY": "test-key", "CHAIN_ID": "1"},
			wantError: "redis address not provided",
		},
		{
			name:      "missing S3_BUCKET",
			args:      []string{"-queue", "https://sqs.us-east-1.amazonaws.com/123/q", "-db", "postgres://localhost/db", "-redis", "localhost:6379"},
			envVars:   map[string]string{"ALCHEMY_API_KEY": "test-key", "CHAIN_ID": "1"},
			wantError: "S3_BUCKET environment variable is required",
		},
		{
			name: "missing DEPLOY_ENV",
			args: []string{"-queue", "https://sqs.us-east-1.amazonaws.com/123/q", "-db", "postgres://localhost/db", "-redis", "localhost:6379"},
			envVars: map[string]string{
				"ALCHEMY_API_KEY": "test-key",
				"S3_BUCKET":       "my-bucket",
				"CHAIN_ID":        "1",
			},
			wantError: "DEPLOY_ENV environment variable is required",
		},
		{
			// Fail-fast on missing CHAIN_ID instead of silently defaulting to
			// mainnet. A prod pod misconfigured for a sidechain would otherwise
			// indexed mainnet events under the wrong chain_id.
			name: "missing CHAIN_ID",
			args: []string{"-queue", "https://sqs.us-east-1.amazonaws.com/123/q", "-db", "postgres://localhost/db", "-redis", "localhost:6379"},
			envVars: map[string]string{
				"ALCHEMY_API_KEY": "test-key",
				"S3_BUCKET":       "my-bucket",
				"DEPLOY_ENV":      "sentinelstaging",
			},
			wantError: "CHAIN_ID environment variable is required",
		},
		{
			name:      "invalid flag",
			args:      []string{"--nonexistent"},
			wantError: "flag provided but not defined",
		},
		{
			name: "invalid CHAIN_ID",
			args: []string{"-queue", "https://sqs.us-east-1.amazonaws.com/123/q", "-db", "postgres://localhost/db", "-redis", "localhost:6379"},
			envVars: map[string]string{
				"ALCHEMY_API_KEY": "test-key",
				"CHAIN_ID":        "not-a-number",
			},
			wantError: "parsing CHAIN_ID",
		},
		{
			name: "invalid SQS_WAIT_TIME",
			args: []string{"-queue", "https://sqs.us-east-1.amazonaws.com/123/q", "-db", "postgres://localhost/db", "-redis", "localhost:6379"},
			envVars: map[string]string{
				"ALCHEMY_API_KEY": "test-key",
				"SQS_WAIT_TIME":   "abc",
				"CHAIN_ID":        "1",
			},
			wantError: "parsing SQS_WAIT_TIME",
		},
		{
			name: "invalid SQS_VISIBILITY_TIMEOUT",
			args: []string{"-queue", "https://sqs.us-east-1.amazonaws.com/123/q", "-db", "postgres://localhost/db", "-redis", "localhost:6379"},
			envVars: map[string]string{
				"ALCHEMY_API_KEY":        "test-key",
				"SQS_VISIBILITY_TIMEOUT": "abc",
				"CHAIN_ID":               "1",
			},
			wantError: "parsing SQS_VISIBILITY_TIMEOUT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, key := range []string{
				"ALCHEMY_API_KEY", "ALCHEMY_HTTP_URL", "AWS_SQS_QUEUE_URL",
				"DATABASE_URL", "REDIS_ADDR", "CHAIN_ID",
				"SQS_WAIT_TIME", "SQS_VISIBILITY_TIMEOUT",
				"S3_BUCKET", "DEPLOY_ENV",
			} {
				if _, has := tt.envVars[key]; !has {
					t.Setenv(key, "")
				}
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
				t.Errorf("queueURL = %q, want %q", cfg.queueURL, tt.wantCfg.queueURL)
			}
			if cfg.dbURL != tt.wantCfg.dbURL {
				t.Errorf("dbURL = %q, want %q", cfg.dbURL, tt.wantCfg.dbURL)
			}
			if cfg.redisAddr != tt.wantCfg.redisAddr {
				t.Errorf("redisAddr = %q, want %q", cfg.redisAddr, tt.wantCfg.redisAddr)
			}
			if cfg.alchemyURL != tt.wantCfg.alchemyURL {
				t.Errorf("alchemyURL = %q, want %q", cfg.alchemyURL, tt.wantCfg.alchemyURL)
			}
			if cfg.s3Bucket != tt.wantCfg.s3Bucket {
				t.Errorf("s3Bucket = %q, want %q", cfg.s3Bucket, tt.wantCfg.s3Bucket)
			}
			if cfg.deployEnv != tt.wantCfg.deployEnv {
				t.Errorf("deployEnv = %q, want %q", cfg.deployEnv, tt.wantCfg.deployEnv)
			}
			if cfg.maxMessages != tt.wantCfg.maxMessages {
				t.Errorf("maxMessages = %d, want %d", cfg.maxMessages, tt.wantCfg.maxMessages)
			}
			if cfg.chainID != tt.wantCfg.chainID {
				t.Errorf("chainID = %d, want %d", cfg.chainID, tt.wantCfg.chainID)
			}
		})
	}
}

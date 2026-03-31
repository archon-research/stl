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
				"S3_BUCKET":        "stl-sentinelstaging-ethereum-raw",
				"DEPLOY_ENV":       "staging",
			},
			wantCfg: cliConfig{
				queueURL:          "https://sqs.us-east-1.amazonaws.com/123/my-queue",
				dbURL:             "postgres://localhost:5432/testdb",
				redisAddr:         "localhost:6379",
				alchemyURL:        "https://eth.example.com/test-key",
				s3Bucket:          "stl-sentinelstaging-ethereum-raw",
				deployEnv:         "staging",
				maxMessages:       10,
				waitTime:          20,
				visibilityTimeout: 300,
				sweepBlocks:       75,
				chainID:           1,
			},
		},
		{
			name: "queue from env var",
			args: []string{"-db", "postgres://localhost/testdb", "-redis", "localhost:6379"},
			envVars: map[string]string{
				"AWS_SQS_QUEUE_URL": "https://sqs.us-east-1.amazonaws.com/123/env-queue",
				"ALCHEMY_API_KEY":   "test-key",
				"S3_BUCKET":         "stl-sentinelstaging-ethereum-raw",
				"DEPLOY_ENV":        "staging",
			},
			wantCfg: cliConfig{
				queueURL:          "https://sqs.us-east-1.amazonaws.com/123/env-queue",
				dbURL:             "postgres://localhost/testdb",
				redisAddr:         "localhost:6379",
				alchemyURL:        "https://eth-mainnet.g.alchemy.com/v2/test-key",
				s3Bucket:          "stl-sentinelstaging-ethereum-raw",
				deployEnv:         "staging",
				maxMessages:       10,
				waitTime:          20,
				visibilityTimeout: 300,
				sweepBlocks:       75,
				chainID:           1,
			},
		},
		{
			name: "db from env var",
			args: []string{"-queue", "https://sqs.us-east-1.amazonaws.com/123/q", "-redis", "localhost:6379"},
			envVars: map[string]string{
				"DATABASE_URL":    "postgres://localhost:5432/envdb",
				"ALCHEMY_API_KEY": "test-key",
				"S3_BUCKET":       "stl-sentinelstaging-ethereum-raw",
				"DEPLOY_ENV":      "staging",
			},
			wantCfg: cliConfig{
				queueURL:          "https://sqs.us-east-1.amazonaws.com/123/q",
				dbURL:             "postgres://localhost:5432/envdb",
				redisAddr:         "localhost:6379",
				alchemyURL:        "https://eth-mainnet.g.alchemy.com/v2/test-key",
				s3Bucket:          "stl-sentinelstaging-ethereum-raw",
				deployEnv:         "staging",
				maxMessages:       10,
				waitTime:          20,
				visibilityTimeout: 300,
				sweepBlocks:       75,
				chainID:           1,
			},
		},
		{
			name: "redis from env var",
			args: []string{"-queue", "https://sqs.us-east-1.amazonaws.com/123/q", "-db", "postgres://localhost/db"},
			envVars: map[string]string{
				"REDIS_ADDR":      "redis.example.com:6379",
				"ALCHEMY_API_KEY": "test-key",
				"S3_BUCKET":       "stl-sentinelstaging-ethereum-raw",
				"DEPLOY_ENV":      "staging",
			},
			wantCfg: cliConfig{
				queueURL:          "https://sqs.us-east-1.amazonaws.com/123/q",
				dbURL:             "postgres://localhost/db",
				redisAddr:         "redis.example.com:6379",
				alchemyURL:        "https://eth-mainnet.g.alchemy.com/v2/test-key",
				s3Bucket:          "stl-sentinelstaging-ethereum-raw",
				deployEnv:         "staging",
				maxMessages:       10,
				waitTime:          20,
				visibilityTimeout: 300,
				sweepBlocks:       75,
				chainID:           1,
			},
		},
		{
			name:      "missing queue URL",
			args:      []string{"-db", "postgres://localhost/db", "-redis", "localhost:6379"},
			envVars:   map[string]string{"ALCHEMY_API_KEY": "test-key"},
			wantError: "queue URL not provided",
		},
		{
			name:      "missing database URL",
			args:      []string{"-queue", "https://sqs.us-east-1.amazonaws.com/123/q", "-redis", "localhost:6379"},
			envVars:   map[string]string{"ALCHEMY_API_KEY": "test-key"},
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
			envVars:   map[string]string{"ALCHEMY_API_KEY": "test-key"},
			wantError: "redis address not provided",
		},
		{
			name:      "missing S3_BUCKET",
			args:      []string{"-queue", "https://sqs.us-east-1.amazonaws.com/123/q", "-db", "postgres://localhost/db", "-redis", "localhost:6379"},
			envVars:   map[string]string{"ALCHEMY_API_KEY": "test-key"},
			wantError: "S3_BUCKET environment variable is required",
		},
		{
			name: "missing DEPLOY_ENV",
			args: []string{"-queue", "https://sqs.us-east-1.amazonaws.com/123/q", "-db", "postgres://localhost/db", "-redis", "localhost:6379"},
			envVars: map[string]string{
				"ALCHEMY_API_KEY": "test-key",
				"S3_BUCKET":       "stl-sentinelstaging-ethereum-raw",
			},
			wantError: "DEPLOY_ENV environment variable is required",
		},
		{
			name:      "invalid flag",
			args:      []string{"--nonexistent"},
			wantError: "flag provided but not defined",
		},
		{
			name: "CLI flag takes precedence over env var",
			args: []string{
				"-queue", "https://sqs.us-east-1.amazonaws.com/123/cli-queue",
				"-db", "postgres://localhost/cli-db",
				"-redis", "cli-redis:6379",
			},
			envVars: map[string]string{
				"AWS_SQS_QUEUE_URL": "https://sqs.us-east-1.amazonaws.com/123/env-queue",
				"DATABASE_URL":      "postgres://localhost/env-db",
				"REDIS_ADDR":        "env-redis:6379",
				"ALCHEMY_API_KEY":   "test-key",
				"S3_BUCKET":         "stl-sentinelstaging-ethereum-raw",
				"DEPLOY_ENV":        "staging",
			},
			wantCfg: cliConfig{
				queueURL:          "https://sqs.us-east-1.amazonaws.com/123/cli-queue",
				dbURL:             "postgres://localhost/cli-db",
				redisAddr:         "cli-redis:6379",
				alchemyURL:        "https://eth-mainnet.g.alchemy.com/v2/test-key",
				s3Bucket:          "stl-sentinelstaging-ethereum-raw",
				deployEnv:         "staging",
				maxMessages:       10,
				waitTime:          20,
				visibilityTimeout: 300,
				sweepBlocks:       75,
				chainID:           1,
			},
		},
		{
			name: "custom chain ID from env",
			args: []string{"-queue", "https://sqs.us-east-1.amazonaws.com/123/q", "-db", "postgres://localhost/db", "-redis", "localhost:6379"},
			envVars: map[string]string{
				"ALCHEMY_API_KEY": "test-key",
				"CHAIN_ID":        "8453",
				"S3_BUCKET":       "stl-sentinelstaging-ethereum-raw",
				"DEPLOY_ENV":      "staging",
			},
			wantCfg: cliConfig{
				queueURL:          "https://sqs.us-east-1.amazonaws.com/123/q",
				dbURL:             "postgres://localhost/db",
				redisAddr:         "localhost:6379",
				alchemyURL:        "https://eth-mainnet.g.alchemy.com/v2/test-key",
				s3Bucket:          "stl-sentinelstaging-ethereum-raw",
				deployEnv:         "staging",
				maxMessages:       10,
				waitTime:          20,
				visibilityTimeout: 300,
				sweepBlocks:       75,
				chainID:           8453,
			},
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
			name: "SQS_WAIT_TIME env override",
			args: []string{"-queue", "https://sqs.us-east-1.amazonaws.com/123/q", "-db", "postgres://localhost/db", "-redis", "localhost:6379"},
			envVars: map[string]string{
				"ALCHEMY_API_KEY": "test-key",
				"SQS_WAIT_TIME":   "5",
				"S3_BUCKET":       "stl-sentinelstaging-ethereum-raw",
				"DEPLOY_ENV":      "staging",
			},
			wantCfg: cliConfig{
				queueURL:          "https://sqs.us-east-1.amazonaws.com/123/q",
				dbURL:             "postgres://localhost/db",
				redisAddr:         "localhost:6379",
				alchemyURL:        "https://eth-mainnet.g.alchemy.com/v2/test-key",
				s3Bucket:          "stl-sentinelstaging-ethereum-raw",
				deployEnv:         "staging",
				maxMessages:       10,
				waitTime:          5,
				visibilityTimeout: 300,
				sweepBlocks:       75,
				chainID:           1,
			},
		},
		{
			name: "invalid SQS_WAIT_TIME",
			args: []string{"-queue", "https://sqs.us-east-1.amazonaws.com/123/q", "-db", "postgres://localhost/db", "-redis", "localhost:6379"},
			envVars: map[string]string{
				"ALCHEMY_API_KEY": "test-key",
				"SQS_WAIT_TIME":   "abc",
			},
			wantError: "parsing SQS_WAIT_TIME",
		},
		{
			name: "SQS_VISIBILITY_TIMEOUT env override",
			args: []string{"-queue", "https://sqs.us-east-1.amazonaws.com/123/q", "-db", "postgres://localhost/db", "-redis", "localhost:6379"},
			envVars: map[string]string{
				"ALCHEMY_API_KEY":        "test-key",
				"SQS_VISIBILITY_TIMEOUT": "60",
				"S3_BUCKET":              "stl-sentinelstaging-ethereum-raw",
				"DEPLOY_ENV":             "staging",
			},
			wantCfg: cliConfig{
				queueURL:          "https://sqs.us-east-1.amazonaws.com/123/q",
				dbURL:             "postgres://localhost/db",
				redisAddr:         "localhost:6379",
				alchemyURL:        "https://eth-mainnet.g.alchemy.com/v2/test-key",
				s3Bucket:          "stl-sentinelstaging-ethereum-raw",
				deployEnv:         "staging",
				maxMessages:       10,
				waitTime:          20,
				visibilityTimeout: 60,
				sweepBlocks:       75,
				chainID:           1,
			},
		},
		{
			name: "invalid SQS_VISIBILITY_TIMEOUT",
			args: []string{"-queue", "https://sqs.us-east-1.amazonaws.com/123/q", "-db", "postgres://localhost/db", "-redis", "localhost:6379"},
			envVars: map[string]string{
				"ALCHEMY_API_KEY":        "test-key",
				"SQS_VISIBILITY_TIMEOUT": "abc",
			},
			wantError: "parsing SQS_VISIBILITY_TIMEOUT",
		},
		{
			name: "custom max messages flag",
			args: []string{
				"-queue", "https://sqs.us-east-1.amazonaws.com/123/q",
				"-db", "postgres://localhost/db",
				"-redis", "localhost:6379",
				"-max", "5",
			},
			envVars: map[string]string{
				"ALCHEMY_API_KEY": "test-key",
				"S3_BUCKET":       "stl-sentinelstaging-ethereum-raw",
				"DEPLOY_ENV":      "staging",
			},
			wantCfg: cliConfig{
				queueURL:          "https://sqs.us-east-1.amazonaws.com/123/q",
				dbURL:             "postgres://localhost/db",
				redisAddr:         "localhost:6379",
				alchemyURL:        "https://eth-mainnet.g.alchemy.com/v2/test-key",
				s3Bucket:          "stl-sentinelstaging-ethereum-raw",
				deployEnv:         "staging",
				maxMessages:       5,
				waitTime:          20,
				visibilityTimeout: 300,
				sweepBlocks:       75,
				chainID:           1,
			},
		},
		{
			name: "custom sweep-blocks flag",
			args: []string{
				"-queue", "https://sqs.us-east-1.amazonaws.com/123/q",
				"-db", "postgres://localhost/db",
				"-redis", "localhost:6379",
				"-sweep-blocks", "100",
			},
			envVars: map[string]string{
				"ALCHEMY_API_KEY": "test-key",
				"S3_BUCKET":       "stl-sentinelstaging-ethereum-raw",
				"DEPLOY_ENV":      "staging",
			},
			wantCfg: cliConfig{
				queueURL:          "https://sqs.us-east-1.amazonaws.com/123/q",
				dbURL:             "postgres://localhost/db",
				redisAddr:         "localhost:6379",
				alchemyURL:        "https://eth-mainnet.g.alchemy.com/v2/test-key",
				s3Bucket:          "stl-sentinelstaging-ethereum-raw",
				deployEnv:         "staging",
				maxMessages:       10,
				waitTime:          20,
				visibilityTimeout: 300,
				sweepBlocks:       100,
				chainID:           1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Ensure env vars are empty unless provided in envVars
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
				t.Errorf("queueURL: expected %q, got %q", tt.wantCfg.queueURL, cfg.queueURL)
			}
			if cfg.dbURL != tt.wantCfg.dbURL {
				t.Errorf("dbURL: expected %q, got %q", tt.wantCfg.dbURL, cfg.dbURL)
			}
			if cfg.redisAddr != tt.wantCfg.redisAddr {
				t.Errorf("redisAddr: expected %q, got %q", tt.wantCfg.redisAddr, cfg.redisAddr)
			}
			if cfg.alchemyURL != tt.wantCfg.alchemyURL {
				t.Errorf("alchemyURL: expected %q, got %q", tt.wantCfg.alchemyURL, cfg.alchemyURL)
			}
			if cfg.s3Bucket != tt.wantCfg.s3Bucket {
				t.Errorf("s3Bucket: expected %q, got %q", tt.wantCfg.s3Bucket, cfg.s3Bucket)
			}
			if cfg.deployEnv != tt.wantCfg.deployEnv {
				t.Errorf("deployEnv: expected %q, got %q", tt.wantCfg.deployEnv, cfg.deployEnv)
			}
			if cfg.maxMessages != tt.wantCfg.maxMessages {
				t.Errorf("maxMessages: expected %d, got %d", tt.wantCfg.maxMessages, cfg.maxMessages)
			}
			if cfg.waitTime != tt.wantCfg.waitTime {
				t.Errorf("waitTime: expected %d, got %d", tt.wantCfg.waitTime, cfg.waitTime)
			}
			if cfg.visibilityTimeout != tt.wantCfg.visibilityTimeout {
				t.Errorf("visibilityTimeout: expected %d, got %d", tt.wantCfg.visibilityTimeout, cfg.visibilityTimeout)
			}
			if cfg.sweepBlocks != tt.wantCfg.sweepBlocks {
				t.Errorf("sweepBlocks: expected %d, got %d", tt.wantCfg.sweepBlocks, cfg.sweepBlocks)
			}
			if cfg.chainID != tt.wantCfg.chainID {
				t.Errorf("chainID: expected %d, got %d", tt.wantCfg.chainID, cfg.chainID)
			}
		})
	}
}

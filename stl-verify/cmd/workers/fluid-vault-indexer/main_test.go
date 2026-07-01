package main

import (
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/services/fluid_vault_indexer"
)

func TestParseConfig(t *testing.T) {
	customDebtToken := "0x1234567890123456789012345678901234567890"

	tests := []struct {
		name      string
		args      []string
		envVars   map[string]string
		wantCfg   cliConfig
		wantError string
	}{
		{
			name: "all flags provided via CLI defaults debt token to sUSDS",
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
				chainName:         "mainnet",
				targetDebtToken:   fluid_vault_indexer.SUSDSAddress,
			},
		},
		{
			name: "queue redis db S3 deploy-env from env vars",
			args: []string{},
			envVars: map[string]string{
				"AWS_SQS_QUEUE_URL": "https://sqs.us-east-1.amazonaws.com/123/env-queue",
				"DATABASE_URL":      "postgres://localhost/envdb",
				"REDIS_ADDR":        "redis.example.com:6379",
				"ALCHEMY_API_KEY":   "test-key",
				"S3_BUCKET":         "my-bucket",
				"DEPLOY_ENV":        "sentinelstaging",
			},
			wantCfg: cliConfig{
				queueURL:          "https://sqs.us-east-1.amazonaws.com/123/env-queue",
				dbURL:             "postgres://localhost/envdb",
				redisAddr:         "redis.example.com:6379",
				alchemyURL:        "https://eth-mainnet.g.alchemy.com/v2/test-key",
				s3Bucket:          "my-bucket",
				deployEnv:         "sentinelstaging",
				maxMessages:       10,
				waitTime:          20,
				visibilityTimeout: 300,
				chainID:           1,
				chainName:         "mainnet",
				targetDebtToken:   fluid_vault_indexer.SUSDSAddress,
			},
		},
		{
			name: "TARGET_DEBT_TOKEN env overrides the default",
			args: []string{"-queue", "https://sqs.us-east-1.amazonaws.com/123/q", "-db", "postgres://localhost/db", "-redis", "localhost:6379"},
			envVars: map[string]string{
				"ALCHEMY_API_KEY":   "test-key",
				"S3_BUCKET":         "my-bucket",
				"DEPLOY_ENV":        "sentinelstaging",
				"TARGET_DEBT_TOKEN": customDebtToken,
			},
			wantCfg: cliConfig{
				queueURL:          "https://sqs.us-east-1.amazonaws.com/123/q",
				dbURL:             "postgres://localhost/db",
				redisAddr:         "localhost:6379",
				alchemyURL:        "https://eth-mainnet.g.alchemy.com/v2/test-key",
				s3Bucket:          "my-bucket",
				deployEnv:         "sentinelstaging",
				maxMessages:       10,
				waitTime:          20,
				visibilityTimeout: 300,
				chainID:           1,
				chainName:         "mainnet",
				targetDebtToken:   common.HexToAddress(customDebtToken),
			},
		},
		{
			name: "invalid TARGET_DEBT_TOKEN",
			args: []string{"-queue", "https://sqs.us-east-1.amazonaws.com/123/q", "-db", "postgres://localhost/db", "-redis", "localhost:6379"},
			envVars: map[string]string{
				"ALCHEMY_API_KEY":   "test-key",
				"S3_BUCKET":         "my-bucket",
				"DEPLOY_ENV":        "sentinelstaging",
				"TARGET_DEBT_TOKEN": "not-an-address",
			},
			wantError: "invalid TARGET_DEBT_TOKEN",
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
				"S3_BUCKET":       "my-bucket",
			},
			wantError: "DEPLOY_ENV environment variable is required",
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
			name: "SQS_WAIT_TIME and SQS_VISIBILITY_TIMEOUT env overrides",
			args: []string{"-queue", "https://sqs.us-east-1.amazonaws.com/123/q", "-db", "postgres://localhost/db", "-redis", "localhost:6379"},
			envVars: map[string]string{
				"ALCHEMY_API_KEY":        "test-key",
				"SQS_WAIT_TIME":          "5",
				"SQS_VISIBILITY_TIMEOUT": "60",
				"S3_BUCKET":              "my-bucket",
				"DEPLOY_ENV":             "sentinelstaging",
			},
			wantCfg: cliConfig{
				queueURL:          "https://sqs.us-east-1.amazonaws.com/123/q",
				dbURL:             "postgres://localhost/db",
				redisAddr:         "localhost:6379",
				alchemyURL:        "https://eth-mainnet.g.alchemy.com/v2/test-key",
				s3Bucket:          "my-bucket",
				deployEnv:         "sentinelstaging",
				maxMessages:       10,
				waitTime:          5,
				visibilityTimeout: 60,
				chainID:           1,
				chainName:         "mainnet",
				targetDebtToken:   fluid_vault_indexer.SUSDSAddress,
			},
		},
		{
			name: "explicit wait and visibility-timeout flags win over env vars",
			args: []string{
				"-queue", "https://sqs.us-east-1.amazonaws.com/123/q",
				"-db", "postgres://localhost/db",
				"-redis", "localhost:6379",
				"-wait", "11",
				"-visibility-timeout", "222",
			},
			envVars: map[string]string{
				"ALCHEMY_API_KEY":        "test-key",
				"SQS_WAIT_TIME":          "5",
				"SQS_VISIBILITY_TIMEOUT": "60",
				"S3_BUCKET":              "my-bucket",
				"DEPLOY_ENV":             "sentinelstaging",
			},
			wantCfg: cliConfig{
				queueURL:          "https://sqs.us-east-1.amazonaws.com/123/q",
				dbURL:             "postgres://localhost/db",
				redisAddr:         "localhost:6379",
				alchemyURL:        "https://eth-mainnet.g.alchemy.com/v2/test-key",
				s3Bucket:          "my-bucket",
				deployEnv:         "sentinelstaging",
				maxMessages:       10,
				waitTime:          11,
				visibilityTimeout: 222,
				chainID:           1,
				chainName:         "mainnet",
				targetDebtToken:   fluid_vault_indexer.SUSDSAddress,
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, key := range []string{
				"ALCHEMY_API_KEY", "ALCHEMY_HTTP_URL", "AWS_SQS_QUEUE_URL",
				"DATABASE_URL", "REDIS_ADDR", "CHAIN_ID",
				"SQS_WAIT_TIME", "SQS_VISIBILITY_TIMEOUT",
				"S3_BUCKET", "DEPLOY_ENV", "TARGET_DEBT_TOKEN",
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

			if cfg != tt.wantCfg {
				t.Errorf("config mismatch:\n got  %+v\n want %+v", cfg, tt.wantCfg)
			}
		})
	}
}

package main

import (
	"maps"
	"strings"
	"testing"

	rawdatabackup "github.com/archon-research/stl/stl-verify/internal/services/raw_data_backup"
)

// requiredEnv is a valid baseline env that lets parseConfig succeed. Tests
// remove or override individual keys to exercise the validation branches.
func requiredEnv() map[string]string {
	return map[string]string{
		"SQS_QUEUE_URL": "https://sqs.eu-west-1.amazonaws.com/123/main.fifo",
		"DLQ_QUEUE_URL": "https://sqs.eu-west-1.amazonaws.com/123/dlq.fifo",
		"S3_BUCKET":     "stl-sentinelstaging-avalanche-raw",
		"REDIS_ADDR":    "redis.example.com:6379",
		"CHAIN_ID":      "43114",
		"DEPLOY_ENV":    "staging",
	}
}

func TestParseConfig(t *testing.T) {
	// Keys that may bleed in from the host environment; cleared per-case unless set.
	allKeys := []string{
		"SQS_QUEUE_URL", "DLQ_QUEUE_URL", "S3_BUCKET", "REDIS_ADDR", "CHAIN_ID",
		"DEPLOY_ENV", "AWS_REGION", "CACHE_MISS_MAX_RETRIES", "WORKERS",
	}

	tests := []struct {
		name      string
		env       map[string]string
		workers   int
		wantError string
		check     func(t *testing.T, cfg workerConfig)
	}{
		{
			name:    "all required present",
			env:     requiredEnv(),
			workers: 2,
			check: func(t *testing.T, cfg workerConfig) {
				if cfg.queueURL != "https://sqs.eu-west-1.amazonaws.com/123/main.fifo" {
					t.Errorf("unexpected queueURL: %q", cfg.queueURL)
				}
				if cfg.dlqQueueURL != "https://sqs.eu-west-1.amazonaws.com/123/dlq.fifo" {
					t.Errorf("unexpected dlqQueueURL: %q", cfg.dlqQueueURL)
				}
				if cfg.chainID != 43114 {
					t.Errorf("unexpected chainID: %d", cfg.chainID)
				}
				if cfg.awsRegion != "eu-west-1" {
					t.Errorf("expected default region eu-west-1, got %q", cfg.awsRegion)
				}
				if cfg.cacheMissMaxRetries != rawdatabackup.ConfigDefaults().CacheMissMaxRetries {
					t.Errorf("expected default cache miss retries, got %d", cfg.cacheMissMaxRetries)
				}
				if cfg.workers != 2 {
					t.Errorf("expected workers 2, got %d", cfg.workers)
				}
			},
		},
		{
			name:      "missing SQS_QUEUE_URL",
			env:       without(requiredEnv(), "SQS_QUEUE_URL"),
			workers:   2,
			wantError: "SQS_QUEUE_URL",
		},
		{
			name:    "DLQ_QUEUE_URL derived from SQS_QUEUE_URL when unset",
			env:     without(requiredEnv(), "DLQ_QUEUE_URL"),
			workers: 2,
			check: func(t *testing.T, cfg workerConfig) {
				want := "https://sqs.eu-west-1.amazonaws.com/123/main-dlq.fifo"
				if cfg.dlqQueueURL != want {
					t.Errorf("expected derived DLQ URL %q, got %q", want, cfg.dlqQueueURL)
				}
			},
		},
		{
			name:      "DLQ derivation fails for non-fifo queue",
			env:       with(without(requiredEnv(), "DLQ_QUEUE_URL"), "SQS_QUEUE_URL", "https://sqs.eu-west-1.amazonaws.com/123/main"),
			workers:   2,
			wantError: "cannot derive DLQ URL",
		},
		{
			name:      "missing S3_BUCKET",
			env:       without(requiredEnv(), "S3_BUCKET"),
			workers:   2,
			wantError: "S3_BUCKET",
		},
		{
			name:      "missing REDIS_ADDR",
			env:       without(requiredEnv(), "REDIS_ADDR"),
			workers:   2,
			wantError: "REDIS_ADDR",
		},
		{
			name:      "missing CHAIN_ID",
			env:       without(requiredEnv(), "CHAIN_ID"),
			workers:   2,
			wantError: "CHAIN_ID environment variable is required",
		},
		{
			name:      "invalid CHAIN_ID",
			env:       with(requiredEnv(), "CHAIN_ID", "not-a-number"),
			workers:   2,
			wantError: "invalid CHAIN_ID",
		},
		{
			name:      "missing DEPLOY_ENV",
			env:       without(requiredEnv(), "DEPLOY_ENV"),
			workers:   2,
			wantError: "DEPLOY_ENV",
		},
		{
			name:      "bucket validation failure",
			env:       with(requiredEnv(), "S3_BUCKET", "wrong-bucket-name"),
			workers:   2,
			wantError: "S3 bucket validation failed",
		},
		{
			name:      "invalid CACHE_MISS_MAX_RETRIES",
			env:       with(requiredEnv(), "CACHE_MISS_MAX_RETRIES", "abc"),
			workers:   2,
			wantError: "CACHE_MISS_MAX_RETRIES",
		},
		{
			name:      "negative CACHE_MISS_MAX_RETRIES",
			env:       with(requiredEnv(), "CACHE_MISS_MAX_RETRIES", "-1"),
			workers:   2,
			wantError: "CACHE_MISS_MAX_RETRIES must be >= 0",
		},
		{
			name:    "CACHE_MISS_MAX_RETRIES override",
			env:     with(requiredEnv(), "CACHE_MISS_MAX_RETRIES", "0"),
			workers: 2,
			check: func(t *testing.T, cfg workerConfig) {
				if cfg.cacheMissMaxRetries != 0 {
					t.Errorf("expected cache miss retries 0, got %d", cfg.cacheMissMaxRetries)
				}
			},
		},
		{
			name:    "AWS_REGION override",
			env:     with(requiredEnv(), "AWS_REGION", "us-east-1"),
			workers: 2,
			check: func(t *testing.T, cfg workerConfig) {
				if cfg.awsRegion != "us-east-1" {
					t.Errorf("expected region us-east-1, got %q", cfg.awsRegion)
				}
			},
		},
		{
			name:    "WORKERS env override",
			env:     with(requiredEnv(), "WORKERS", "8"),
			workers: 2,
			check: func(t *testing.T, cfg workerConfig) {
				if cfg.workers != 8 {
					t.Errorf("expected workers 8 from env, got %d", cfg.workers)
				}
			},
		},
		{
			name:    "non-positive workers falls back to default",
			env:     requiredEnv(),
			workers: 0,
			check: func(t *testing.T, cfg workerConfig) {
				if cfg.workers != 2 {
					t.Errorf("expected default workers 2, got %d", cfg.workers)
				}
			},
		},
		{
			name:    "invalid WORKERS env is ignored",
			env:     with(requiredEnv(), "WORKERS", "nope"),
			workers: 3,
			check: func(t *testing.T, cfg workerConfig) {
				if cfg.workers != 3 {
					t.Errorf("expected workers to keep flag value 3, got %d", cfg.workers)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, key := range allKeys {
				if _, has := tt.env[key]; !has {
					t.Setenv(key, "")
				}
			}
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			cfg, err := parseConfig(tt.workers)

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
			if tt.check != nil {
				tt.check(t, cfg)
			}
		})
	}
}

func with(m map[string]string, key, value string) map[string]string {
	out := make(map[string]string, len(m))
	maps.Copy(out, m)
	out[key] = value
	return out
}

func without(m map[string]string, key string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		if k == key {
			continue
		}
		out[k] = v
	}
	return out
}

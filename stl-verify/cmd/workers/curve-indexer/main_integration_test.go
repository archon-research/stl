//go:build integration

package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun_MissingQueueURL verifies that run returns a clear error when the
// SQS queue URL is absent. No infra is touched: ParseConfig fails before any
// connection is attempted.
func TestRun_MissingQueueURL(t *testing.T) {
	t.Setenv("AWS_SQS_QUEUE_URL", "")
	t.Setenv("DATABASE_URL", "postgres://localhost/test")
	t.Setenv("ALCHEMY_API_KEY", "testkey")
	t.Setenv("REDIS_ADDR", "localhost:6379")
	t.Setenv("CHAIN_ID", "1")
	t.Setenv("S3_BUCKET", "stl-raw-data-mainnet-staging")
	t.Setenv("DEPLOY_ENV", "staging")

	err := run(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error for missing queue URL, got nil")
	}
	if !strings.Contains(err.Error(), "queue") {
		t.Errorf("expected error to mention 'queue', got: %v", err)
	}
}

// TestRun_MissingDatabaseURL verifies that run returns a clear error when the
// database URL is absent.
func TestRun_MissingDatabaseURL(t *testing.T) {
	t.Setenv("AWS_SQS_QUEUE_URL", "https://sqs.eu-west-1.amazonaws.com/123/test-queue")
	t.Setenv("DATABASE_URL", "")
	t.Setenv("ALCHEMY_API_KEY", "testkey")
	t.Setenv("REDIS_ADDR", "localhost:6379")
	t.Setenv("CHAIN_ID", "1")
	t.Setenv("S3_BUCKET", "stl-raw-data-mainnet-staging")
	t.Setenv("DEPLOY_ENV", "staging")

	err := run(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error for missing database URL, got nil")
	}
	if !strings.Contains(err.Error(), "database") {
		t.Errorf("expected error to mention 'database', got: %v", err)
	}
}

// TestRun_MissingAlchemyAPIKey verifies that run returns a clear error when
// ALCHEMY_API_KEY is not set.
func TestRun_MissingAlchemyAPIKey(t *testing.T) {
	t.Setenv("AWS_SQS_QUEUE_URL", "https://sqs.eu-west-1.amazonaws.com/123/test-queue")
	t.Setenv("DATABASE_URL", "postgres://localhost/test")
	t.Setenv("ALCHEMY_API_KEY", "")
	t.Setenv("REDIS_ADDR", "localhost:6379")
	t.Setenv("CHAIN_ID", "1")
	t.Setenv("S3_BUCKET", "stl-raw-data-mainnet-staging")
	t.Setenv("DEPLOY_ENV", "staging")

	err := run(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error for missing ALCHEMY_API_KEY, got nil")
	}
	if !strings.Contains(err.Error(), "ALCHEMY_API_KEY") {
		t.Errorf("expected error to mention 'ALCHEMY_API_KEY', got: %v", err)
	}
}

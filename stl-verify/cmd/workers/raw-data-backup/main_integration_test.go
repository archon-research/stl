//go:build integration

package main

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var (
	sharedRedisAddr     string
	sharedLocalStackCfg testutil.LocalStackConfig
)

// rawBucketPrefix satisfies chainutil.ValidateS3BucketForChain, a prefix check
// rather than an equality one, so a per-test suffix is allowed.
const rawBucketPrefix = "stl-sentineltest-ethereum-raw-"

func TestMain(m *testing.M) {
	os.Exit(testutil.RunShared(m, testutil.Shared{
		RedisAddr:          &sharedRedisAddr,
		LocalStack:         &sharedLocalStackCfg,
		LocalStackServices: "sqs,s3",
	}))
}

// TestRunIntegration_DLQURLDerivationFails confirms run() fails fast when the DLQ
// URL is neither set nor derivable (a main queue URL without a .fifo suffix).
func TestRunIntegration_DLQURLDerivationFails(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("DLQ_QUEUE_URL", "")
	t.Setenv("SQS_QUEUE_URL", "http://localhost/main-queue") // no .fifo: cannot derive

	err := run(context.Background(), slog.Default(), 1)
	if err == nil {
		t.Fatal("expected error when the DLQ URL cannot be derived")
	}
	if !strings.Contains(err.Error(), "derive DLQ URL") {
		t.Errorf("expected DLQ derivation error, got: %v", err)
	}
}

// TestRunIntegration_RedisUnreachable confirms run() surfaces a Redis connection
// error rather than starting the service.
func TestRunIntegration_RedisUnreachable(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("REDIS_ADDR", "127.0.0.1:1") // nothing listening
	t.Setenv("AWS_SQS_ENDPOINT", sharedLocalStackCfg.Endpoint)

	err := run(context.Background(), slog.Default(), 1)
	if err == nil {
		t.Fatal("expected error for unreachable Redis")
	}
	if !strings.Contains(err.Error(), "Redis") {
		t.Errorf("expected Redis connection error, got: %v", err)
	}
}

// TestRunIntegration_StartupAndShutdown wires real Redis + LocalStack and a mock
// SQS endpoint, runs the worker, and confirms it polls SQS and shuts down
// cleanly when the context is cancelled.
func TestRunIntegration_StartupAndShutdown(t *testing.T) {
	ctx := context.Background()

	s3Client := testutil.NewS3Client(t, ctx, sharedLocalStackCfg)
	bucket := testutil.S3TestBucketName(t, rawBucketPrefix)
	testutil.EnsureBucket(t, ctx, s3Client, bucket)

	sqsServer, sqsState := testutil.StartMockSQS(t)
	defer sqsServer.Close()

	setBaseEnv(t)
	t.Setenv("S3_BUCKET", bucket)
	t.Setenv("DEPLOY_ENV", "test")
	t.Setenv("CHAIN_ID", "1")
	t.Setenv("REDIS_ADDR", sharedRedisAddr)
	t.Setenv("AWS_SQS_ENDPOINT", sqsServer.URL)
	t.Setenv("AWS_S3_ENDPOINT", sharedLocalStackCfg.Endpoint)
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(runCtx, slog.Default(), 1)
	}()

	testutil.WaitForFirstPoll(t, errCh, sqsState.FirstCallReceived)

	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run() returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run() did not return after context cancellation")
	}

	if receives := sqsState.Receives(); receives < 1 {
		t.Errorf("expected at least 1 ReceiveMessage call, got %d", receives)
	}
}

// setBaseEnv populates a valid baseline environment for run()/parseConfig.
func setBaseEnv(t *testing.T) {
	t.Helper()
	t.Setenv("SQS_QUEUE_URL", "http://localhost/main-queue")
	t.Setenv("DLQ_QUEUE_URL", "http://localhost/dlq-queue.fifo")
	t.Setenv("S3_BUCKET", testutil.S3TestBucketName(t, rawBucketPrefix))
	t.Setenv("REDIS_ADDR", "127.0.0.1:6379")
	t.Setenv("CHAIN_ID", "1")
	t.Setenv("DEPLOY_ENV", "test")
	// The RPC fallback only needs a non-empty URL to build the Alchemy client; no
	// network call is made at startup, so a placeholder is sufficient here.
	t.Setenv("ALCHEMY_API_KEY", "test-api-key")
	t.Setenv("ALCHEMY_HTTP_URL", "https://eth-mainnet.g.alchemy.com/v2")
}

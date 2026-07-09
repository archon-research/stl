//go:build integration

package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var (
	sharedDSN           string
	sharedRedisAddr     string
	sharedLocalStackCfg testutil.LocalStackConfig
)

func TestMain(m *testing.M) {
	dsn, dbCleanup := testutil.StartTimescaleDBForMain()
	sharedDSN = dsn
	redisAddr, redisCleanup := testutil.StartRedisForMain()
	sharedRedisAddr = redisAddr
	lsCfg, lsCleanup := testutil.StartLocalStackForMain("s3")
	sharedLocalStackCfg = lsCfg

	code := m.Run()

	lsCleanup()
	redisCleanup()
	dbCleanup()
	code = testutil.CheckGoroutineLeaks(code)
	os.Exit(code)
}

// ---------------------------------------------------------------------------
// Integration tests for run()
// ---------------------------------------------------------------------------

func TestRunIntegration_BadConnectionConfig(t *testing.T) {
	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer rpcServer.Close()

	// CHAIN_ID=1 lets ParseConfig accept the default mainnet Alchemy endpoint
	// convention and pass the S3-bucket / chain cross-check for "ethereum".
	t.Setenv("BUILD_GIT_HASH", "test")
	t.Setenv("ALCHEMY_API_KEY", "test-api-key")
	t.Setenv("ALCHEMY_HTTP_URL", rpcServer.URL)
	t.Setenv("S3_BUCKET", "stl-sentineltest-ethereum-raw")
	t.Setenv("DEPLOY_ENV", "test")
	t.Setenv("CHAIN_ID", "1")
	t.Setenv("DEX", "curve")

	err := run(context.Background(), []string{
		"-queue", "http://localhost/test-queue",
		"-redis", "localhost:6379",
		"-db", "postgres://invalid:invalid@localhost:1/nonexistent?connect_timeout=1",
	})
	if err == nil {
		t.Fatal("expected error for bad database URL")
	}
	// Could fail at Redis connection or database connection.
	if !strings.Contains(err.Error(), "database") && !strings.Contains(err.Error(), "connect") && !strings.Contains(err.Error(), "Redis") {
		t.Errorf("expected connection error, got: %v", err)
	}
}

// TestRunIntegration_StartupAndShutdown exercises the full cmd-level boot path
// (Bootstrap -> factory.BuildHandler -> LoadPools over the seeded registry ->
// RunLoop) for BOTH DEX factories. Running only DEX=curve would let a
// uniswap-v3-specific wiring regression (nil dep, chain-ID plumbing, seed drift
// in RegisteredPoolsFromRows) ship green, since the registry-map unit test only
// checks ServiceName/MetricPrefix, not the production build path.
func TestRunIntegration_StartupAndShutdown(t *testing.T) {
	for _, dex := range []string{"curve", "uniswap-v3"} {
		t.Run(dex, func(t *testing.T) {
			runStartupAndShutdown(t, dex)
		})
	}
}

func runStartupAndShutdown(t *testing.T, dex string) {
	ctx := context.Background()

	// SetupTestSchema applies all migrations, which seed the Curve pools and the
	// 18 Uniswap V3 pools on chain_id=1. run() calls LoadPools(chainID) and fails
	// hard on zero pools, so CHAIN_ID must be "1" to match the seeded rows.
	_, dbURL, dbCleanup := testutil.SetupTestSchema(t, sharedDSN)
	defer dbCleanup()

	// After the capability-probe removal, startup makes no chain call, and with no
	// SQS messages RunLoop just polls; a fixed "0x1" reply is enough for the dial.
	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":"0x1"}`)
	}))
	defer rpcServer.Close()

	sqsServer, sqsState := testutil.StartMockSQS(t)
	defer sqsServer.Close()

	s3Client := testutil.NewS3Client(t, ctx, sharedLocalStackCfg)

	// Bucket name must satisfy the stl-sentinel{env}-{chain}-raw prefix convention;
	// chainutil resolves chain 1 to "ethereum", so this is the only valid name.
	const (
		bucket    = "stl-sentineltest-ethereum-raw"
		deployEnv = "test"
	)

	if _, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("create S3 bucket: %v", err)
	}

	t.Setenv("BUILD_GIT_HASH", "test")
	t.Setenv("ALCHEMY_API_KEY", "test-api-key")
	t.Setenv("ALCHEMY_HTTP_URL", rpcServer.URL)
	t.Setenv("AWS_SQS_ENDPOINT", sqsServer.URL)
	t.Setenv("AWS_S3_ENDPOINT", sharedLocalStackCfg.Endpoint)
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("S3_BUCKET", bucket)
	t.Setenv("DEPLOY_ENV", deployEnv)
	t.Setenv("CHAIN_ID", "1")
	t.Setenv("DEX", dex)

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(runCtx, []string{
			"-queue", "http://localhost/test-queue",
			"-db", dbURL,
			"-redis", sharedRedisAddr,
		})
	}()

	// A ReceiveMessage call means Bootstrap -> LoadPools -> the DEX's
	// BuildHandler all succeeded and RunLoop is polling.
	select {
	case <-sqsState.FirstCallReceived:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for service to start")
	}

	// Service is running and polling SQS. Trigger graceful shutdown.
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

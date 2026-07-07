//go:build integration

package main

import (
	"bytes"
	"compress/gzip"
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
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var (
	sharedDSN           string
	sharedRedisAddr     string
	sharedLocalStackCfg testutil.LocalStackConfig
)

// testBucket / testDeployEnv match the chainutil.ValidateS3BucketForChain
// rule: stl-sentinel{env}-{chain}-raw. chainID=1 → "ethereum".
const (
	testBucket    = "stl-sentineltest-ethereum-raw"
	testDeployEnv = "test"
	// archiveBucket receives raw SC call archives when ARCHIVE_SC_CALLS=true.
	archiveBucket = "stl-raw-sc-calls-test"
	// archivePrefix is the chain_id partition rawsckey.Build writes under for chainID=1.
	archivePrefix = "raw-sc-calls/chain_id=1/"
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

// integrationEnv holds the running service and its dependencies for a single
// run() integration test. Built by setupIntegrationTest.
type integrationEnv struct {
	pool       *pgxpool.Pool
	s3Client   *s3.Client
	sqsState   *testutil.MockSQSServer
	tokenCount int
	bgCtx      context.Context
	cancel     context.CancelFunc
	errCh      chan error
}

// setupConfig is configured via setupOption.
type setupConfig struct {
	archiving bool
}

type setupOption func(*setupConfig)

// withArchiving enables ARCHIVE_SC_CALLS for the run and provisions the archive
// bucket.
func withArchiving() setupOption { return func(c *setupConfig) { c.archiving = true } }

// setupIntegrationTest provisions the DB schema, mock RPC/SQS servers, seeded
// S3 block, and environment, then starts run() and blocks until the service is
// polling SQS. It registers all teardown via t.Cleanup; callers drive shutdown
// with (*integrationEnv).waitForShutdown.
func setupIntegrationTest(t *testing.T, opts ...setupOption) *integrationEnv {
	t.Helper()

	var cfg setupConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	pool, dbURL, cleanup := testutil.SetupTestSchema(t, sharedDSN)
	t.Cleanup(cleanup)

	bgCtx := context.Background()

	// Disable all oracles except sparklend — the mock RPC is parameterized for a single oracle.
	if _, err := pool.Exec(bgCtx, `UPDATE oracle SET enabled = false WHERE name != 'sparklend'`); err != nil {
		t.Fatalf("disable non-sparklend oracles: %v", err)
	}

	var tokenCount int
	if err := pool.QueryRow(bgCtx,
		`SELECT COUNT(*) FROM oracle_asset oa
		 JOIN oracle os ON os.id = oa.oracle_id
		 WHERE os.name = 'sparklend' AND oa.enabled = true`).Scan(&tokenCount); err != nil {
		t.Fatalf("count tokens: %v", err)
	}
	if tokenCount == 0 {
		t.Fatal("no seeded oracle assets found")
	}

	rpcServer := testutil.StartMockEthRPC(t, tokenCount)
	t.Cleanup(rpcServer.Close)

	sqsServer, sqsState := testutil.StartMockSQS(t)
	t.Cleanup(sqsServer.Close)

	// Pre-seed S3 with the block JSON for block 18000000 so resolveBlockTimestamp
	// can recover a timestamp via the cache→S3 fallback path.
	s3Client := testutil.NewS3Client(t, bgCtx, sharedLocalStackCfg)
	if _, err := s3Client.CreateBucket(bgCtx, &s3.CreateBucketInput{Bucket: aws.String(testBucket)}); err != nil {
		t.Fatalf("create S3 bucket: %v", err)
	}
	if cfg.archiving {
		if _, err := s3Client.CreateBucket(bgCtx, &s3.CreateBucketInput{Bucket: aws.String(archiveBucket)}); err != nil {
			t.Fatalf("create archive S3 bucket: %v", err)
		}
	}
	seedBlockToS3(t, bgCtx, s3Client, testBucket, 18_000_000, 1, 1_700_000_000)

	// Enqueue one block event message for the service to process.
	sqsState.AddMessage(`{"chainId":1,"blockNumber":18000000,"version":1,"blockHash":"0xabc","blockTimestamp":1700000000}`)

	// Configure environment for run()
	t.Setenv("BUILD_GIT_HASH", "test")
	t.Setenv("ALCHEMY_API_KEY", "test-api-key")
	t.Setenv("ALCHEMY_HTTP_URL", rpcServer.URL)
	t.Setenv("AWS_SQS_ENDPOINT", sqsServer.URL)
	t.Setenv("AWS_S3_ENDPOINT", sharedLocalStackCfg.Endpoint)
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("S3_BUCKET", testBucket)
	t.Setenv("DEPLOY_ENV", testDeployEnv)
	if cfg.archiving {
		t.Setenv("ARCHIVE_SC_CALLS", "true")
		t.Setenv("RAW_SC_BUCKET", archiveBucket)
	}

	// Use a cancellable context instead of SIGINT for clean test shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, []string{
			"-queue", "http://localhost/test-queue",
			"-db", dbURL,
			"-redis", sharedRedisAddr,
		})
	}()

	// Wait for the service to start (SQS ReceiveMessage call indicates it's polling)
	select {
	case <-sqsState.FirstCallReceived:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for service to start")
	}

	return &integrationEnv{
		pool:       pool,
		s3Client:   s3Client,
		sqsState:   sqsState,
		tokenCount: tokenCount,
		bgCtx:      bgCtx,
		cancel:     cancel,
		errCh:      errCh,
	}
}

// waitForPrices blocks until at least tokenCount prices are stored.
func (e *integrationEnv) waitForPrices(t *testing.T) {
	t.Helper()
	testutil.WaitForCondition(t, 30*time.Second, func() bool {
		var count int
		e.pool.QueryRow(e.bgCtx, `SELECT COUNT(*) FROM onchain_token_price`).Scan(&count)
		return count >= e.tokenCount
	}, "prices to be stored in DB")
}

// waitForDelete blocks until the processed message has been deleted from SQS.
// DeleteMessage is the final step of the poll loop, after the DB write that
// waitForPrices observes; without gating shutdown on it, cancelling the context
// can abort the in-flight DeleteMessage (context canceled) and the delete never
// registers.
func (e *integrationEnv) waitForDelete(t *testing.T) {
	t.Helper()
	testutil.WaitForCondition(t, 30*time.Second, func() bool {
		return e.sqsState.Deletes() >= 1
	}, "message to be deleted from SQS")
}

// waitForShutdown cancels run()'s context and asserts it returns cleanly.
func (e *integrationEnv) waitForShutdown(t *testing.T) {
	t.Helper()
	e.cancel()
	select {
	case err := <-e.errCh:
		if err != nil {
			t.Fatalf("run() returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run() did not return after context cancellation")
	}
}

func TestRunIntegration_HappyPath(t *testing.T) {
	env := setupIntegrationTest(t)

	env.waitForPrices(t)
	env.waitForDelete(t)
	env.waitForShutdown(t)

	// Verify prices
	var priceCount int
	env.pool.QueryRow(env.bgCtx, `SELECT COUNT(*) FROM onchain_token_price`).Scan(&priceCount)
	if priceCount < env.tokenCount {
		t.Errorf("expected at least %d prices, got %d", env.tokenCount, priceCount)
	}
}

// TestRunIntegration_ArchivesRawCalls drives run() with ARCHIVE_SC_CALLS=true
// and asserts that processing a block writes raw SC call archives to S3. This
// covers the otherwise-untested enabled wiring branch in run() plus the
// WithBlockVersion plumbing in the service.
func TestRunIntegration_ArchivesRawCalls(t *testing.T) {
	env := setupIntegrationTest(t, withArchiving())

	env.waitForPrices(t)

	// Archives are fire-and-forget; poll the archive bucket until an object lands
	// whose key carries the expected block version and source. rawsckey.Build
	// formats the filename as {block}_{blockVersion}_{source}_{batchHash}, so the
	// segment also pins that the message's version 1 (not the default 0) is keyed.
	const wantSegment = "18000000_1_oracle-price_"
	testutil.WaitForCondition(t, 30*time.Second, func() bool {
		out, err := env.s3Client.ListObjectsV2(env.bgCtx, &s3.ListObjectsV2Input{
			Bucket: aws.String(archiveBucket),
			Prefix: aws.String(archivePrefix),
		})
		if err != nil {
			return false
		}
		for _, obj := range out.Contents {
			if strings.Contains(aws.ToString(obj.Key), wantSegment) {
				return true
			}
		}
		return false
	}, "a raw SC call archive whose key contains "+wantSegment)

	env.waitForShutdown(t)
}

func TestRunIntegration_BadDatabaseURL(t *testing.T) {
	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer rpcServer.Close()

	t.Setenv("BUILD_GIT_HASH", "test")
	t.Setenv("ALCHEMY_API_KEY", "test-api-key")
	t.Setenv("ALCHEMY_HTTP_URL", rpcServer.URL)
	t.Setenv("S3_BUCKET", testBucket)
	t.Setenv("DEPLOY_ENV", testDeployEnv)

	err := run(context.Background(), []string{
		"-queue", "http://localhost/test-queue",
		"-redis", sharedRedisAddr,
		"-db", "postgres://invalid:invalid@localhost:1/nonexistent?connect_timeout=1",
	})
	if err == nil {
		t.Fatal("expected error for bad database URL")
	}
	// Either the postgres connection fails first, or Redis/S3 wiring surfaces an
	// error — anything other than parseConfig succeeding is acceptable.
	if !strings.Contains(err.Error(), "database") && !strings.Contains(err.Error(), "connect") && !strings.Contains(err.Error(), "Redis") {
		t.Errorf("expected database/connect/Redis error, got: %v", err)
	}
}

// seedBlockToS3 uploads gzipped block JSON to the test bucket at the canonical
// s3key path so the worker's cache reader can fall back to S3 on a Redis miss.
func seedBlockToS3(t *testing.T, ctx context.Context, client *s3.Client, bucket string, blockNumber int64, version int, timestamp int64) {
	t.Helper()

	payload := fmt.Sprintf(`{"timestamp":"0x%x"}`, timestamp)

	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	if _, err := gw.Write([]byte(payload)); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := gw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}

	key := s3key.Build(blockNumber, version, s3key.Block)
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(buf.Bytes()),
	}); err != nil {
		t.Fatalf("put block to S3 (%s): %v", key, err)
	}
}

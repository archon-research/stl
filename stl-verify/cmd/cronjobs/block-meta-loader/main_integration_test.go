//go:build integration

package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.temporal.io/sdk/testsuite"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockmetacfg"
	"github.com/archon-research/stl/stl-verify/internal/pkg/chainutil"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var (
	sharedDSN           string
	sharedLocalStackCfg testutil.LocalStackConfig
)

func TestMain(m *testing.M) {
	os.Exit(testutil.RunShared(m, testutil.Shared{
		TimescaleDSN:       &sharedDSN,
		LocalStack:         &sharedLocalStackCfg,
		LocalStackServices: "s3",
	}))
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// The bucket name has to satisfy the chain guard, which keys on DEPLOY_ENV and the chain slug.
// absentBucket satisfies that guard and is never created: LocalStack grants every action, so a
// bucket that is not there is the only way to make the startup probe fail.
const (
	testDeployEnv = "staging"
	testBucket    = "stl-sentinelstaging-ethereum-raw-itest"
	absentBucket  = testBucket + "-absent"
)

// This drives the deployed wiring — blockmetacfg.Load's env parsing and bucket guard, the real S3 reader
// built the way the binary builds it, register's activity wiring, and the workflow — against a real
// database and LocalStack. run() itself only resolves the queue and hands off to RunWorker, which
// needs a Temporal server; everything below that is exercised here.
func TestBlockMetaLoad_FillsReferencedBlocks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	testutil.SeedReferencedBlocks(t, ctx, pool, 500, 501)

	s3Client := testutil.NewS3Client(t, ctx, sharedLocalStackCfg)
	if _, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(testBucket)}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	testutil.UploadBlockHeader(t, ctx, s3Client, testBucket, 500, 0, "0x67c02710")
	testutil.UploadBlockHeader(t, ctx, s3Client, testBucket, 501, 0, "0x67c02720")

	t.Setenv("BUILD_GIT_HASH", "integration-test")
	// Off, so this test is about the fill path. The margin's own behaviour is below.
	t.Setenv("HEAD_MARGIN", "0")
	t.Setenv("CHAIN_ID", "1")
	t.Setenv("DEPLOY_ENV", testDeployEnv)
	t.Setenv("S3_BUCKET", testBucket)
	t.Setenv("DATABASE_URL", "unused-here")
	t.Setenv("AWS_S3_ENDPOINT", sharedLocalStackCfg.Endpoint)
	t.Setenv("AWS_REGION", sharedLocalStackCfg.Region)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	cfg, err := blockmetacfg.Load()
	if err != nil {
		t.Fatalf("blockmetacfg.Load: %v", err)
	}

	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	if err := register(ctx, cfg, temporal.Dependencies{Pool: pool, Logger: discardLogger()}, env); err != nil {
		t.Fatalf("register: %v", err)
	}

	env.ExecuteWorkflow(workflowTypeName, LoadParams{})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow: %v", err)
	}
	var result LoadProgress
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatalf("workflow result: %v", err)
	}
	if result.Loaded != 2 {
		t.Errorf("workflow reported %d rows loaded, want 2", result.Loaded)
	}

	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM block_meta WHERE chain_id = 1`).Scan(&rows); err != nil {
		t.Fatalf("count block_meta: %v", err)
	}
	if rows != 2 {
		t.Errorf("block_meta holds %d rows, want 2", rows)
	}

	// Provenance has to survive the whole path, not just the repository: the activity opens the
	// writer run and hands the build down, and only an end-to-end check sees it drop it. 0 is the
	// column default, which ADR-0006 reads as pre-tracking data.
	var unstamped int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM block_meta
		 WHERE chain_id = 1 AND (build_id IS NULL OR build_id = 0 OR run_id IS NULL)`).Scan(&unstamped); err != nil {
		t.Fatalf("count unstamped rows: %v", err)
	}
	if unstamped != 0 {
		t.Errorf("%d row(s) carry no build or run; the activity is not passing them down", unstamped)
	}
}

// The guard that keeps one chain's archive from being read under another chain's id. It runs at
// registration, so a misconfigured deployment is a worker that will not start.
func TestBlockMetaLoad_RefusesAnotherChainsBucket(t *testing.T) {
	t.Setenv("CHAIN_ID", "1")
	t.Setenv("DEPLOY_ENV", testDeployEnv)
	t.Setenv("S3_BUCKET", "stl-sentinelstaging-base-raw-itest")
	t.Setenv("DATABASE_URL", "unused-here")

	if _, err := blockmetacfg.Load(); err == nil {
		t.Fatal("a bucket belonging to another chain was accepted")
	}
}

// The head margin has to reach the work list, not just exist in config: the archive trails the
// indexers at the head, so a run without it reports normal lag as an absent object on every pass.
// Both blocks here sit inside the default margin, so a run that applied it loads nothing — and a
// run that dropped it on the way through would load them and fail this.
func TestBlockMetaLoad_HeadMarginHoldsBackTheNewestBlocks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	testutil.SeedReferencedBlocks(t, ctx, pool, 500, 501)

	s3Client := testutil.NewS3Client(t, ctx, sharedLocalStackCfg)
	if _, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(testBucket)}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	testutil.UploadBlockHeader(t, ctx, s3Client, testBucket, 500, 0, "0x67c02710")
	testutil.UploadBlockHeader(t, ctx, s3Client, testBucket, 501, 0, "0x67c02720")

	t.Setenv("BUILD_GIT_HASH", "integration-test")
	t.Setenv("CHAIN_ID", "1")
	t.Setenv("DEPLOY_ENV", testDeployEnv)
	t.Setenv("S3_BUCKET", testBucket)
	t.Setenv("DATABASE_URL", "unused-here")
	t.Setenv("AWS_S3_ENDPOINT", sharedLocalStackCfg.Endpoint)
	t.Setenv("AWS_REGION", sharedLocalStackCfg.Region)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	cfg, err := blockmetacfg.Load()
	if err != nil {
		t.Fatalf("blockmetacfg.Load: %v", err)
	}
	if cfg.HeadMargin == 0 {
		t.Fatal("the default head margin is 0; this test would prove nothing")
	}

	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	if err := register(ctx, cfg, temporal.Dependencies{Pool: pool, Logger: discardLogger()}, env); err != nil {
		t.Fatalf("register: %v", err)
	}
	env.ExecuteWorkflow(workflowTypeName, LoadParams{})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow: %v", err)
	}

	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM block_meta WHERE chain_id = 1`).Scan(&rows); err != nil {
		t.Fatalf("count block_meta: %v", err)
	}
	if rows != 0 {
		t.Errorf("block_meta holds %d rows; blocks inside the head margin must be held back", rows)
	}
}

// The archive grants come from an EKS Pod Identity association granted in the infra repo, and
// without them every header read fails. Proven at registration, a missing grant is a pod that will
// not start; discovered on the first read it is a run an operator started and has to come back to.
// A bucket that does not exist stands in for the denial here: LocalStack grants everything, so an
// absent bucket is the only way to make the probe fail without a policy engine.
func TestBlockMetaLoad_RefusesToStartWithoutArchiveAccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	// The probe can only fail on a bucket that is really absent, and the tests above create
	// testBucket in this same LocalStack. Assert the precondition: were absentBucket ever
	// created, this test would pass on a probe that rejected nothing.
	s3Client := testutil.NewS3Client(t, ctx, sharedLocalStackCfg)
	if _, err := s3Client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(absentBucket)}); err == nil {
		t.Fatalf("%s exists, so a passing probe here would prove nothing", absentBucket)
	}

	t.Setenv("BUILD_GIT_HASH", "integration-test")
	t.Setenv("CHAIN_ID", "1")
	t.Setenv("DEPLOY_ENV", testDeployEnv)
	t.Setenv("S3_BUCKET", absentBucket)
	t.Setenv("DATABASE_URL", "unused-here")
	t.Setenv("AWS_S3_ENDPOINT", sharedLocalStackCfg.Endpoint)
	t.Setenv("AWS_REGION", sharedLocalStackCfg.Region)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	cfg, err := blockmetacfg.Load()
	if err != nil {
		t.Fatalf("blockmetacfg.Load: %v", err)
	}
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	err = register(ctx, cfg, temporal.Dependencies{Pool: pool, Logger: discardLogger()}, env)
	if err == nil {
		t.Fatal("registration succeeded against an archive it cannot read; the first run would fail on AccessDenied instead")
	}
	if !strings.Contains(err.Error(), absentBucket) {
		t.Errorf("error %q does not name the bucket it could not use", err)
	}
}

// The Deployment, task queue and OTel service name are one string per chain.
func TestTaskQueueIsPrefixedPerChain(t *testing.T) {
	for chainID, want := range map[string]string{"1": queueBaseName, "8453": "base-" + queueBaseName, "43114": "avalanche-" + queueBaseName} {
		t.Run(chainID, func(t *testing.T) {
			t.Setenv("CHAIN_ID", chainID)
			got, err := chainutil.TaskQueueName(queueBaseName)
			if err != nil || got != want {
				t.Errorf("TaskQueueName(%q) on chain %s = %q, %v; want %q", queueBaseName, chainID, got, err, want)
			}
		})
	}
}

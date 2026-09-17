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
	"github.com/jackc/pgx/v5/pgxpool"

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

// The bucket name has to satisfy the chain guard; absentBucket satisfies it and is never created.
const (
	testDeployEnv = "staging"
	testBucket    = "stl-sentinelstaging-ethereum-raw-itest"
	absentBucket  = testBucket + "-absent"
)

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func setTopupEnv(t *testing.T, bucket string) {
	t.Helper()
	t.Setenv("BUILD_GIT_HASH", "integration-test")
	t.Setenv("HEAD_MARGIN", "0")
	t.Setenv("CHAIN_ID", "1")
	t.Setenv("DEPLOY_ENV", testDeployEnv)
	t.Setenv("S3_BUCKET", bucket)
	t.Setenv("DATABASE_URL", "unused-here")
	t.Setenv("AWS_S3_ENDPOINT", sharedLocalStackCfg.Endpoint)
	t.Setenv("AWS_REGION", sharedLocalStackCfg.Region)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
}

func loadConfig(t *testing.T) blockmetacfg.Config {
	t.Helper()
	cfg, err := blockmetacfg.Load()
	if err != nil {
		t.Fatalf("blockmetacfg.Load: %v", err)
	}
	return cfg
}

func countBlockMeta(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM block_meta WHERE chain_id = 1`).Scan(&n); err != nil {
		t.Fatalf("count block_meta: %v", err)
	}
	return n
}

// Each tick loads at most MAX_BLOCKS and the next tick resumes, through the runner setup builds.
func TestTopup_EachTickIsBoundedAndTheNextResumes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	testutil.SeedReferencedBlocks(t, ctx, pool, 500, 501, 502)

	s3Client := testutil.NewS3Client(t, ctx, sharedLocalStackCfg)
	if _, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(testBucket)}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	for i, b := range []int64{500, 501, 502} {
		testutil.UploadBlockHeader(t, ctx, s3Client, testBucket, b, 0, []string{"0x67c02710", "0x67c02720", "0x67c02730"}[i])
	}
	setTopupEnv(t, testBucket)
	t.Setenv("MAX_BLOCKS", "2")

	runner, err := setup(ctx, loadConfig(t), temporal.Dependencies{Pool: pool, Logger: discardLogger()})
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := runner.Run(ctx); err != nil {
		t.Fatalf("first tick: %v", err)
	}
	if got := countBlockMeta(t, ctx, pool); got != 2 {
		t.Fatalf("first tick left %d blocks in block_meta, want 2 (MAX_BLOCKS)", got)
	}
	if err := runner.Run(ctx); err != nil {
		t.Fatalf("second tick: %v", err)
	}
	if got := countBlockMeta(t, ctx, pool); got != 3 {
		t.Errorf("second tick left %d blocks in block_meta, want 3", got)
	}

	var unstamped, runs int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE build_id IS NULL OR build_id = 0 OR run_id IS NULL), count(DISTINCT run_id)
		  FROM block_meta WHERE chain_id = 1`).Scan(&unstamped, &runs); err != nil {
		t.Fatalf("read provenance: %v", err)
	}
	if unstamped != 0 || runs != 2 {
		t.Errorf("%d unstamped rows across %d runs; want every row stamped and one writer run per tick", unstamped, runs)
	}
}

// An unbounded pass is the on-demand loader's job, so a scheduled tick refuses MAX_BLOCKS=0.
func TestTopup_RefusesAnUnboundedTick(t *testing.T) {
	setTopupEnv(t, testBucket)
	t.Setenv("MAX_BLOCKS", "0")
	_, err := setup(context.Background(), loadConfig(t), temporal.Dependencies{Logger: discardLogger()})
	if err == nil || !strings.Contains(err.Error(), "MAX_BLOCKS") {
		t.Fatalf("setup returned %v; MAX_BLOCKS=0 must be refused", err)
	}
}

func TestTopup_RefusesANegativeOrUnparseableCap(t *testing.T) {
	for _, v := range []string{"-1", "many"} {
		t.Run(v, func(t *testing.T) {
			setTopupEnv(t, testBucket)
			t.Setenv("MAX_BLOCKS", v)
			if _, err := setup(context.Background(), loadConfig(t), temporal.Dependencies{Logger: discardLogger()}); err == nil {
				t.Fatalf("MAX_BLOCKS=%q was accepted", v)
			}
		})
	}
}

// A missing archive grant is a pod that will not start, not a tick that fails every hour.
func TestTopup_RefusesToStartWithoutArchiveAccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	s3Client := testutil.NewS3Client(t, ctx, sharedLocalStackCfg)
	if _, err := s3Client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(absentBucket)}); err == nil {
		t.Fatalf("%s exists, so a passing probe here would prove nothing", absentBucket)
	}
	setTopupEnv(t, absentBucket)
	_, err := setup(ctx, loadConfig(t), temporal.Dependencies{Logger: discardLogger()})
	if err == nil || !strings.Contains(err.Error(), absentBucket) {
		t.Fatalf("setup returned %v; it must refuse an archive it cannot read, naming the bucket", err)
	}
}

// A tick whose run fails returns the error, so Temporal records the attempt as failed.
func TestTopup_ATickThatCannotLoadFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	testutil.SeedReferencedBlocks(t, ctx, pool, 900)

	s3Client := testutil.NewS3Client(t, ctx, sharedLocalStackCfg)
	if _, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(testBucket)}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	setTopupEnv(t, testBucket)
	runner, err := setup(ctx, loadConfig(t), temporal.Dependencies{Pool: pool, Logger: discardLogger()})
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := runner.Run(ctx); err == nil || !strings.Contains(err.Error(), "900/0") {
		t.Fatalf("tick returned %v; a block absent from the archive must fail it", err)
	}
}

// run resolves the task queue before anything else, so an unknown chain fails at startup.
func TestTopup_RunRefusesAnUnknownChain(t *testing.T) {
	t.Setenv("CHAIN_ID", "999999999")
	if err := run(context.Background()); err == nil || !strings.Contains(err.Error(), "task queue") {
		t.Fatalf("run returned %v; an unknown chain must fail resolving the task queue", err)
	}
}

func TestTopup_RunRefusesAnIncompleteConfiguration(t *testing.T) {
	setTopupEnv(t, testBucket)
	t.Setenv("S3_BUCKET", "")
	if err := run(context.Background()); err == nil || !strings.Contains(err.Error(), "loading configuration") {
		t.Fatalf("run returned %v; a deployment with no bucket must fail loading configuration", err)
	}
}

// A tick that cannot open its writer run fails rather than loading rows no run owns.
func TestTopup_ATickWithoutADatabaseFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	s3Client := testutil.NewS3Client(t, ctx, sharedLocalStackCfg)
	if _, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(testBucket)}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	setTopupEnv(t, testBucket)
	runner, err := setup(ctx, loadConfig(t), temporal.Dependencies{Pool: pool, Logger: discardLogger()})
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	pool.Close()
	if err := runner.Run(ctx); err == nil {
		t.Fatal("a tick against a closed pool succeeded")
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

// A tick runs longer than the 10-minute cronjob default, so it gets its own ceiling and a heartbeat; the
// ceiling stays inside the interval so one tick cannot run into the next.
func TestTopup_ATickHasItsOwnCeilingAndAHeartbeat(t *testing.T) {
	c := cronjobConfig("block-meta-topup", blockmetacfg.Config{DSN: "unused"}).ActivityTimeouts
	interval, err := time.ParseDuration(cronjobConfig("block-meta-topup", blockmetacfg.Config{}).IntervalDefault)
	if err != nil {
		t.Fatalf("parse interval: %v", err)
	}
	if c.StartToClose <= 10*time.Minute || c.ScheduleToClose < c.StartToClose || c.ScheduleToClose >= interval {
		t.Errorf("timeouts %+v: want StartToClose above the 10m default and StartToClose <= ScheduleToClose < %s", c, interval)
	}
	if c.Heartbeat <= 0 {
		t.Error("no heartbeat: a worker that dies mid-tick is only noticed when the ceiling expires")
	}
}

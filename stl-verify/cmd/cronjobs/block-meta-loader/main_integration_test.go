//go:build integration

package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/testsuite"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
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
const (
	testDeployEnv = "staging"
	testBucket    = "stl-sentinelstaging-ethereum-raw-itest"
)

func uploadBlock(t *testing.T, ctx context.Context, client *s3.Client, blockNum int64, version int, hexTimestamp string) {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(fmt.Appendf(nil, `{"timestamp":%q}`, hexTimestamp)); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(s3key.Build(blockNum, version, s3key.Block)),
		Body:   bytes.NewReader(buf.Bytes()),
	}); err != nil {
		t.Fatalf("put block %d/%d: %v", blockNum, version, err)
	}
}

// seedReferencedBlocks gives the work list something to enumerate: protocol_event rows referencing
// blocks that block_meta lacks.
func seedReferencedBlocks(t *testing.T, ctx context.Context, pool *pgxpool.Pool, blocks ...int64) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO chain (chain_id, name) VALUES (1, 'ethereum') ON CONFLICT DO NOTHING;
		INSERT INTO protocol (chain_id, address, name) VALUES (1, '\x7001', 'itest') ON CONFLICT DO NOTHING;`); err != nil {
		t.Fatalf("seed chain/protocol: %v", err)
	}
	for _, b := range blocks {
		if _, err := pool.Exec(ctx, `
			INSERT INTO protocol_event
				(chain_id, protocol_id, block_number, block_version, tx_hash, log_index, contract_address, event_name, event_data)
			VALUES (1, (SELECT id FROM protocol WHERE address='\x7001'), $1, 0, '\x09'::bytea, 0, '\x02'::bytea, 'Borrow', '{}'::jsonb)
			ON CONFLICT DO NOTHING`, b); err != nil {
			t.Fatalf("seed referenced block %d: %v", b, err)
		}
	}
}

// This drives the deployed wiring — loadConfig's env parsing and bucket guard, the real S3 reader
// built the way the binary builds it, register's activity wiring, and the workflow — against a real
// database and LocalStack. run() itself only resolves the queue and hands off to RunWorker, which
// needs a Temporal server; everything below that is exercised here.
func TestBlockMetaLoad_FillsReferencedBlocks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	seedReferencedBlocks(t, ctx, pool, 500, 501)

	s3Client := testutil.NewS3Client(t, ctx, sharedLocalStackCfg)
	if _, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(testBucket)}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	uploadBlock(t, ctx, s3Client, 500, 0, "0x67c02710")
	uploadBlock(t, ctx, s3Client, 501, 0, "0x67c02720")

	t.Setenv("BUILD_GIT_HASH", "integration-test")
	t.Setenv("CHAIN_ID", "1")
	t.Setenv("DEPLOY_ENV", testDeployEnv)
	t.Setenv("S3_BUCKET", testBucket)
	t.Setenv("DATABASE_URL", "unused-here")
	t.Setenv("AWS_S3_ENDPOINT", sharedLocalStackCfg.Endpoint)
	t.Setenv("AWS_REGION", sharedLocalStackCfg.Region)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
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
}

// The guard that keeps one chain's archive from being read under another chain's id. It runs at
// registration, so a misconfigured deployment is a worker that will not start.
func TestBlockMetaLoad_RefusesAnotherChainsBucket(t *testing.T) {
	t.Setenv("CHAIN_ID", "1")
	t.Setenv("DEPLOY_ENV", testDeployEnv)
	t.Setenv("S3_BUCKET", "stl-sentinelstaging-base-raw-itest")
	t.Setenv("DATABASE_URL", "unused-here")

	if _, err := loadConfig(); err == nil {
		t.Fatal("a bucket belonging to another chain was accepted")
	}
}

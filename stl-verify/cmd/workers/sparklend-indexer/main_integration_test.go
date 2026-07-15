//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	redisAdapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/redis"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var (
	sharedDSN           string
	sharedRedisAddr     string
	sharedLocalStackCfg testutil.LocalStackConfig
)

// testBucket / testDeployEnv satisfy chainutil.ValidateS3BucketForChain, which the
// cache reader enforces at construction: stl-sentinel{env}-{chain}-raw, chainID=1 → "ethereum".
const (
	testBucket    = "stl-sentineltest-ethereum-raw"
	testDeployEnv = "test"
	// archiveBucket receives raw SC call archives when ARCHIVE_SC_CALLS=true.
	archiveBucket = "test-sparklend-worker-raw-sc-calls"
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

// TestRunIntegration_ArchivesRawCalls drives the SQS worker end to end with
// ARCHIVE_SC_CALLS=true: it seeds a SparkLend Borrow receipt into Redis, enqueues
// one block event, and serves a mock RPC. Processing the event drives real
// Multicall3 calls through the archiving-wrapped multicaller, so the worker must
// write a raw SC call object to S3 keyed under the block's reorg version.
func TestRunIntegration_ArchivesRawCalls(t *testing.T) {
	bgCtx := context.Background()

	pool, dbURL, cleanup := testutil.SetupTestSchema(t, sharedDSN)
	t.Cleanup(cleanup)

	// SparkLend's PoolDataProvider is active from block 16776400; use a block above it.
	const blockNum = int64(16800000)
	const version = 1
	const daiAddress = "0x6B175474E89094C44Da98b954EedeAC495271d0F"
	const sparkLendPool = "0xC13e21B648A5Ee794902342038FF3aDAB66BE987"

	// Seed Redis with a Borrow receipt so the worker's cache read returns it
	// directly (no S3 fallback needed).
	seedBorrowReceipt(t, bgCtx, blockNum, version, sparkLendPool)

	s3Client := testutil.NewS3Client(t, bgCtx, sharedLocalStackCfg)
	for _, b := range []string{testBucket, archiveBucket} {
		if _, err := s3Client.CreateBucket(bgCtx, &s3.CreateBucketInput{Bucket: aws.String(b)}); err != nil {
			t.Fatalf("create bucket %s: %v", b, err)
		}
	}

	rpcServer := testutil.BuildSparkLendBorrowMockRPC(t, daiAddress)
	t.Cleanup(rpcServer.Close)

	sqsServer, sqsState := testutil.StartMockSQS(t)
	t.Cleanup(sqsServer.Close)
	sqsState.AddMessage(fmt.Sprintf(
		`{"chainId":1,"blockNumber":%d,"version":%d,"blockHash":"0x%064x","blockTimestamp":1700000000}`,
		blockNum, version, blockNum,
	))

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
	t.Setenv("CHAIN_ID", "1")
	t.Setenv("ARCHIVE_SC_CALLS", "true")
	t.Setenv("RAW_SC_BUCKET", archiveBucket)

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

	select {
	case <-sqsState.FirstCallReceived:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for worker to start polling SQS")
	}

	// Wait until the Borrow event is fully processed (borrower row written) so the
	// run loop is idle before we shut down, avoiding a context-cancelled mid-write.
	testutil.WaitForCondition(t, 30*time.Second, func() bool {
		var count int
		if err := pool.QueryRow(bgCtx, `SELECT COUNT(*) FROM borrower`).Scan(&count); err != nil {
			return false
		}
		return count >= 1
	}, "the Borrow event to be persisted")

	// Archives are fire-and-forget; poll until the object lands. The key embeds the
	// block version: rawsckey.Build formats {block}_{blockVersion}_{source}_{batchHash},
	// so the worker must archive under version 1 (not the default bv=0) with the
	// "sparklend" source. This pins the reorg-aware keying contract for the live path.
	wantSegment := fmt.Sprintf("%d_%d_sparklend_", blockNum, version)
	testutil.WaitForCondition(t, 30*time.Second, func() bool {
		out, err := s3Client.ListObjectsV2(bgCtx, &s3.ListObjectsV2Input{
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
	}, fmt.Sprintf("a raw SC call archive whose key contains %q", wantSegment))

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run() returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run() did not return after context cancellation")
	}

	// run() has returned, so every fire-and-forget archive write has drained. The
	// positive check above can be satisfied by an unrelated number-pinned Execute
	// batch at the same block, so assert directly that nothing was archived at
	// block 0: a hash-pinned state read reaching the archiver without
	// WithBlockNumber would key its batch there (VEC-471). A real archive's
	// filename starts with the block number, never "0_".
	listOut, listErr := s3Client.ListObjectsV2(bgCtx, &s3.ListObjectsV2Input{
		Bucket: aws.String(archiveBucket),
		Prefix: aws.String(archivePrefix),
	})
	if listErr != nil {
		t.Fatalf("listing archive bucket: %v", listErr)
	}
	for _, obj := range listOut.Contents {
		key := aws.ToString(obj.Key)
		if base := key[strings.LastIndex(key, "/")+1:]; strings.HasPrefix(base, "0_") {
			t.Fatalf("raw SC call archive keyed at block 0 (%s): a hash-pinned state read was archived without WithBlockNumber", key)
		}
	}
}

// seedBorrowReceipt writes a SparkLend Borrow receipt for DAI into the Redis block
// cache at the given block/version, so the worker's cache read returns it.
func seedBorrowReceipt(t *testing.T, ctx context.Context, blockNum int64, version int, poolAddress string) {
	t.Helper()

	// SparkLend Borrow event for DAI by a synthetic borrower (see the backfill test).
	const borrowTopic = "0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0"
	const reserveTopic = "0x0000000000000000000000006b175474e89094c44da98b954eedeac495271d0f"
	const userTopic = "0x000000000000000000000000d8da6bf26964af9d7eed9e03e53415d37aa96045"
	const referralTopic = "0x0000000000000000000000000000000000000000000000000000000000000000"

	borrowAmount := new(big.Int).Mul(big.NewInt(100), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
	borrowData := fmt.Sprintf(""+
		"000000000000000000000000d8da6bf26964af9d7eed9e03e53415d37aa96045"+
		"%064x"+
		"0000000000000000000000000000000000000000000000000000000000000002"+
		"000000000000000000000000000000000000000000000004563918244f400000",
		borrowAmount,
	)

	// A valid 32-byte hex tx hash: saveProtocolEvent runs it through common.FromHex,
	// which yields empty bytes (and a "txHash must not be empty" error) for non-hex input.
	txHash := "0x" + strings.Repeat("ab", 32)
	receipts := []shared.TransactionReceipt{
		{
			TransactionHash: txHash,
			Logs: []shared.Log{
				{
					Address:         poolAddress,
					Topics:          []string{borrowTopic, reserveTopic, userTopic, referralTopic},
					Data:            "0x" + borrowData,
					LogIndex:        "0x0",
					TransactionHash: txHash,
				},
			},
		},
	}

	data, err := json.Marshal(receipts)
	if err != nil {
		t.Fatalf("marshal receipts: %v", err)
	}

	cacheCfg := redisAdapter.ConfigDefaults()
	cacheCfg.Addr = sharedRedisAddr
	blockCache, err := redisAdapter.NewBlockCache(cacheCfg, nil)
	if err != nil {
		t.Fatalf("create block cache: %v", err)
	}
	defer blockCache.Close()

	if err := blockCache.SetReceipts(ctx, 1, blockNum, version, data); err != nil {
		t.Fatalf("seed receipts in Redis: %v", err)
	}
}

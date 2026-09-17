//go:build integration && e2e

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/jackc/pgx/v5/pgxpool"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	redisAdapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/redis"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// poisonBlock holds the Morpho Blue event whose caller traps on every probe
// selector; its message parked in the prod DLQ (VEC-698).
const poisonBlock int64 = 25827558

// e2eBlocks is the default block list: a VaultV2 discovery block the unit
// fixtures use (24481834), the poison block with its neighbours, and the other
// block parked in the same DLQ for exceeding the handler deadline (25809526).
var e2eBlocks = []int64{24481834, 25827557, poisonBlock, 25827559, 25809526}

// TestE2E_RealBlocks drives the worker over real mainnet blocks against the real
// RPC, with the shared Postgres, Redis and LocalStack behind it, and requires
// every block to be acknowledged. Run it with `make e2e-real-blocks` (optionally
// `BLOCKS=a,b,c`); it skips without ALCHEMY_API_KEY.
func TestE2E_RealBlocks(t *testing.T) {
	apiKey := os.Getenv("ALCHEMY_API_KEY")
	if apiKey == "" {
		t.Skip("ALCHEMY_API_KEY not set")
	}
	blocks := e2eBlockList(t)
	ctx := context.Background()
	metrics := testutil.InstallMeterProvider(t)

	pool, dbURL, cleanup := testutil.SetupTestDB(t, sharedDSN)
	t.Cleanup(cleanup)
	keyPrefix := testutil.SanitizeTestName(t.Name())
	t.Setenv("REDIS_KEY_PREFIX", keyPrefix)
	sqsServer, sqsState := testutil.StartMockSQS(t)
	t.Cleanup(sqsServer.Close)

	seedQueue(t, ctx, apiKey, keyPrefix, sqsState, blocks)
	setWorkerEnv(t, ctx, sqsServer.URL)
	stop := startWorker(t, ctx, dbURL, sqsState)

	testutil.WaitForCondition(t, 15*time.Minute, func() bool {
		return sqsState.Deletes() >= len(blocks)
	}, fmt.Sprintf("all %d blocks to be acknowledged (deleted from the queue)", len(blocks)))
	stop()

	logRowCounts(t, ctx, pool, blocks)
	if slices.Contains(blocks, poisonBlock) {
		assertPoisonBlockNarrowed(t, ctx, pool, metrics)
	}
}

func e2eBlockList(t *testing.T) []int64 {
	t.Helper()
	raw := os.Getenv("E2E_BLOCKS")
	if raw == "" {
		return e2eBlocks
	}
	var blocks []int64
	for field := range strings.SplitSeq(raw, ",") {
		n, err := strconv.ParseInt(strings.TrimSpace(field), 10, 64)
		if err != nil {
			t.Fatalf("E2E_BLOCKS: %q is not a block number", field)
		}
		blocks = append(blocks, n)
	}
	return blocks
}

// seedQueue stores each block's real receipts in the Redis block cache the way
// the watcher would and enqueues the block pointer the watcher would publish.
func seedQueue(t *testing.T, ctx context.Context, apiKey, keyPrefix string, queue *testutil.MockSQSServer, blocks []int64) {
	t.Helper()
	rpcURL := strings.TrimRight(os.Getenv("ALCHEMY_HTTP_URL"), "/")
	if rpcURL == "" {
		rpcURL = "https://eth-mainnet.g.alchemy.com/v2"
	}
	chain, err := rpc.DialContext(ctx, rpcURL+"/"+apiKey)
	if err != nil {
		t.Fatalf("dialing RPC: %s", redact(err, apiKey))
	}
	t.Cleanup(chain.Close)
	for _, block := range blocks {
		hash, timestamp := seedRealBlock(t, ctx, chain, apiKey, keyPrefix, block)
		queue.AddMessage(fmt.Sprintf(
			`{"chainId":1,"blockNumber":%d,"version":0,"blockHash":"%s","blockTimestamp":%d}`,
			block, hash, timestamp,
		))
	}
}

func seedRealBlock(t *testing.T, ctx context.Context, chain *rpc.Client, apiKey, keyPrefix string, block int64) (string, uint64) {
	t.Helper()
	hexBlock := hexutil.EncodeBig(big.NewInt(block))

	var header struct {
		Hash      string `json:"hash"`
		Timestamp string `json:"timestamp"`
	}
	if err := chain.CallContext(ctx, &header, "eth_getBlockByNumber", hexBlock, false); err != nil {
		t.Fatalf("eth_getBlockByNumber(%d): %s", block, redact(err, apiKey))
	}
	timestamp, err := hexutil.DecodeUint64(header.Timestamp)
	if err != nil {
		t.Fatalf("decoding timestamp of %d: %v", block, err)
	}
	var receipts json.RawMessage
	if err := chain.CallContext(ctx, &receipts, "eth_getBlockReceipts", hexBlock); err != nil {
		t.Fatalf("eth_getBlockReceipts(%d): %s", block, redact(err, apiKey))
	}

	cacheCfg := redisAdapter.ConfigDefaults()
	cacheCfg.Addr = sharedRedisAddr
	cacheCfg.KeyPrefix = keyPrefix
	blockCache, err := redisAdapter.NewBlockCache(cacheCfg, nil)
	if err != nil {
		t.Fatalf("create block cache: %v", err)
	}
	defer blockCache.Close()
	if err := blockCache.SetReceipts(ctx, 1, block, 0, receipts); err != nil {
		t.Fatalf("seed receipts for %d: %v", block, err)
	}
	t.Logf("block %d: seeded %d bytes of receipts, hash %s", block, len(receipts), header.Hash)
	return header.Hash, timestamp
}

// redact keeps the API key out of test output: go-ethereum's transport errors
// embed the full request URL.
func redact(err error, apiKey string) string {
	return strings.ReplaceAll(err.Error(), apiKey, "<redacted>")
}

func setWorkerEnv(t *testing.T, ctx context.Context, sqsEndpoint string) {
	t.Helper()
	s3Client := testutil.NewS3Client(t, ctx, sharedLocalStackCfg)
	rawBucket := testutil.S3TestBucketName(t, rawBucketPrefix)
	testutil.EnsureBucket(t, ctx, s3Client, rawBucket)

	t.Setenv("BUILD_GIT_HASH", "e2e")
	t.Setenv("AWS_SQS_ENDPOINT", sqsEndpoint)
	t.Setenv("AWS_S3_ENDPOINT", sharedLocalStackCfg.Endpoint)
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("S3_BUCKET", rawBucket)
	t.Setenv("DEPLOY_ENV", testDeployEnv)
	t.Setenv("CHAIN_ID", "1")
}

// startWorker runs the binary's run() against the shared services and returns a
// stop that cancels it and requires a clean exit.
func startWorker(t *testing.T, ctx context.Context, dbURL string, queue *testutil.MockSQSServer) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, []string{
			"-queue", "http://localhost/e2e-queue",
			"-db", dbURL,
			"-redis", sharedRedisAddr,
		}, nil)
	}()
	testutil.WaitForFirstPoll(t, errCh, queue.FirstCallReceived)
	return func() {
		cancel()
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("run() returned error: %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("run() did not return after context cancellation")
		}
	}
}

func logRowCounts(t *testing.T, ctx context.Context, pool *pgxpool.Pool, blocks []int64) {
	t.Helper()
	for _, block := range blocks {
		t.Logf("block %d: acknowledged; market state rows=%d, vaults discovered=%d",
			block, marketStateRows(t, ctx, pool, block), vaultsDiscoveredAt(t, ctx, pool, block))
	}
}

// assertPoisonBlockNarrowed pins what the poison block is in the list for: its
// probe was narrowed rather than retried, and the Supply its trapping caller
// made was persisted.
func assertPoisonBlockNarrowed(t *testing.T, ctx context.Context, pool *pgxpool.Pool, metrics sdkmetric.Reader) {
	t.Helper()
	want := map[string]string{"reason": "gas_exhausted"}
	if got := testutil.CounterValue(t, metrics, "multicall.batches.narrowed", want); got < 1 {
		t.Errorf("multicall.batches.narrowed%v = %d, want at least 1: the poison block's probe must have been narrowed", want, got)
	}
	if got := marketStateRows(t, ctx, pool, poisonBlock); got < 1 {
		t.Errorf("market state rows at %d = %d, want at least 1: the trapping caller's Supply must be persisted", poisonBlock, got)
	}
}

func marketStateRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, block int64) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM morpho_market_state WHERE block_number = $1`, block).Scan(&n); err != nil {
		t.Fatalf("counting market states at %d: %v", block, err)
	}
	return n
}

func vaultsDiscoveredAt(t *testing.T, ctx context.Context, pool *pgxpool.Pool, block int64) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM morpho_vault WHERE created_at_block = $1`, block).Scan(&n); err != nil {
		t.Fatalf("counting vaults discovered at %d: %v", block, err)
	}
	return n
}

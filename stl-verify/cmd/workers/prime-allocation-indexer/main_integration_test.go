//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ethereum/go-ethereum/common"
	gethtypes "github.com/ethereum/go-ethereum/core/types"

	redisAdapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/redis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	at "github.com/archon-research/stl/stl-verify/internal/services/allocation_tracker"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var (
	sharedDSN           string
	sharedRedisAddr     string
	sharedLocalStackCfg testutil.LocalStackConfig
)

// archiveBucket / archivePrefix mirror the sparklend worker's archiving test.
// archiveBucket receives raw SC call archives when ARCHIVE_SC_CALLS=true;
// archivePrefix is the chain_id partition rawsckey.Build writes under for chainID=1.
const (
	archiveBucket = "test-prime-allocation-worker-raw-sc-calls"
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

func TestRunIntegration_BadConnectionConfig(t *testing.T) {
	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer rpcServer.Close()

	t.Setenv("BUILD_GIT_HASH", "test")
	t.Setenv("ALCHEMY_API_KEY", "test-api-key")
	t.Setenv("ALCHEMY_HTTP_URL", rpcServer.URL)
	t.Setenv("S3_BUCKET", "stl-sentineltest-ethereum-raw")
	t.Setenv("DEPLOY_ENV", "test")
	t.Setenv("CHAIN_ID", "1")

	err := run(context.Background(), []string{
		"-queue", "http://localhost/test-queue",
		"-redis", "localhost:6379",
		"-db", "postgres://invalid:invalid@localhost:1/nonexistent?connect_timeout=1",
	})
	if err == nil {
		t.Fatal("expected error for bad connection config")
	}
	// Could fail at Redis connection or database connection
	if !strings.Contains(err.Error(), "database") && !strings.Contains(err.Error(), "connect") && !strings.Contains(err.Error(), "Redis") {
		t.Errorf("expected connection error, got: %v", err)
	}
}

func TestRunIntegration_StartupAndShutdown(t *testing.T) {
	ctx := context.Background()

	_, dbURL, dbCleanup := testutil.SetupTestSchema(t, sharedDSN)
	defer dbCleanup()

	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":"0x1"}`)
	}))
	defer rpcServer.Close()

	sqsServer, sqsState := testutil.StartMockSQS(t)
	defer sqsServer.Close()

	s3Client := testutil.NewS3Client(t, ctx, sharedLocalStackCfg)

	// Bucket name must satisfy the stl-sentinel{env}-{chain}-raw prefix convention.
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

	// Wait for the service to start (SQS ReceiveMessage call indicates it's polling)
	select {
	case <-sqsState.FirstCallReceived:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for service to start")
	}

	// Service is running and polling SQS. Trigger graceful shutdown.
	cancel()

	// Wait for run() to return
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run() returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run() did not return after context cancellation")
	}

	// Verify SQS was polled
	if receives := sqsState.Receives(); receives < 1 {
		t.Errorf("expected at least 1 ReceiveMessage call, got %d", receives)
	}
}

// Real mainnet fixtures from the default axis-synome contract export: USDS is a
// plain erc20 entry held by Grove's proxy, so processing a USDS Transfer into
// that proxy drives a balanceOf + decimals + symbol multicall through the
// archiving-wrapped multicaller. The "grove" prime is seeded by
// db/migrations/20260305_120000_create_prime_debts.sql so the position handler's
// star -> prime_id lookup resolves.
const (
	usdsTokenAddr  = "0xdC035D45d973E3EC169d2276DDab16f1e407384F"
	groveProxyAddr = "0x1369f7b2b38c76B6478c0f0E66D94923421891Ba"
)

// TestRunIntegration_ArchivesRawCalls drives the SQS worker end to end with
// ARCHIVE_SC_CALLS=true: it seeds a USDS Transfer receipt into Redis, enqueues
// one block event, and serves a Multicall3 mock RPC. Processing the event drives
// real aggregate3 calls (balanceOf, then decimals/symbol metadata) through the
// archiving-wrapped multicaller, so the worker must write a raw SC call object to
// S3 keyed under the block's reorg version.
func TestRunIntegration_ArchivesRawCalls(t *testing.T) {
	bgCtx := context.Background()

	pool, dbURL, cleanup := testutil.SetupTestSchema(t, sharedDSN)
	t.Cleanup(cleanup)

	const blockNum = int64(19_000_000)
	const version = 1

	// Seed Redis with a USDS Transfer into the Grove proxy so the worker's cache
	// read returns it directly (no S3 fallback needed) and the entry matches a
	// tracked (token, proxy) pair.
	seedUsdsTransferReceipt(t, bgCtx, blockNum, version)

	s3Client := testutil.NewS3Client(t, bgCtx, sharedLocalStackCfg)
	const testBucket = "stl-sentineltest-ethereum-raw"
	for _, b := range []string{testBucket, archiveBucket} {
		if _, err := s3Client.CreateBucket(bgCtx, &s3.CreateBucketInput{Bucket: aws.String(b)}); err != nil {
			t.Fatalf("create bucket %s: %v", b, err)
		}
	}

	rpcServer := buildErc20MulticallMockRPC(t)
	t.Cleanup(rpcServer.Close)

	sqsServer, sqsState := testutil.StartMockSQS(t)
	t.Cleanup(sqsServer.Close)
	sqsState.AddMessage(fmt.Sprintf(
		`{"chainId":1,"blockNumber":%d,"version":%d,"blockHash":"0xabc","blockTimestamp":1700000000}`,
		blockNum, version,
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
	t.Setenv("DEPLOY_ENV", "test")
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

	// Wait until the transfer is fully processed (allocation_position row written)
	// so the run loop is idle before we shut down, avoiding a context-cancelled
	// mid-write.
	testutil.WaitForCondition(t, 30*time.Second, func() bool {
		var count int
		if err := pool.QueryRow(bgCtx, `SELECT COUNT(*) FROM allocation_position`).Scan(&count); err != nil {
			return false
		}
		return count >= 1
	}, "the USDS transfer to be persisted")

	// Archives are fire-and-forget; poll until the object lands. The key embeds the
	// block version: rawsckey.Build formats {block}_{blockVersion}_{source}_{batchHash},
	// so the worker must archive under version 1 (not the default bv=0) with the
	// "prime-allocation" source. This pins the reorg-aware keying contract for the
	// live path.
	wantSegment := fmt.Sprintf("%d_%d_prime-allocation_", blockNum, version)
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

// seedUsdsTransferReceipt writes a receipt with a single USDS Transfer log (from
// an external sender into the Grove proxy) into the Redis block cache at the
// given block/version, so the worker's cache read returns it and the
// TransferExtractor emits a matching event.
func seedUsdsTransferReceipt(t *testing.T, ctx context.Context, blockNum int64, version int) {
	t.Helper()

	token := common.HexToAddress(usdsTokenAddr)
	proxy := common.HexToAddress(groveProxyAddr)
	externalSender := common.HexToAddress("0x9999999999999999999999999999999999999999")
	transferTopic := common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

	amount := new(big.Int).Mul(big.NewInt(1_000_000), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
	receipts := []at.TransactionReceipt{
		{
			TransactionHash: "0x" + strings.Repeat("ab", 32),
			Logs: []gethtypes.Log{
				{
					Address: token,
					Topics: []common.Hash{
						transferTopic,
						common.BytesToHash(externalSender.Bytes()),
						common.BytesToHash(proxy.Bytes()),
					},
					Data:  common.LeftPadBytes(amount.Bytes(), 32),
					Index: 0,
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

// buildErc20MulticallMockRPC serves a JSON-RPC endpoint that answers the two
// Multicall3 aggregate3 batches the worker issues for a plain erc20 entry:
// balanceOf(proxy) (from BalanceOfSource) and decimals()/symbol() (from the
// position handler's metadata cache). Each inner sub-call is dispatched by
// selector and answered with a packed ERC-20 return; unknown selectors fail the
// test loudly so a silent zero-substitution cannot mask a routing bug.
func buildErc20MulticallMockRPC(t *testing.T) *httptest.Server {
	t.Helper()

	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("load ERC20 ABI: %v", err)
	}
	multicallABI, err := abis.GetMulticall3ABI()
	if err != nil {
		t.Fatalf("load multicall3 ABI: %v", err)
	}

	balanceOfMethod := erc20ABI.Methods["balanceOf"]
	decimalsMethod := erc20ABI.Methods["decimals"]
	symbolMethod := erc20ABI.Methods["symbol"]

	balance := new(big.Int).Mul(big.NewInt(50_000_000), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
	balanceData, err := balanceOfMethod.Outputs.Pack(balance)
	if err != nil {
		t.Fatalf("pack balanceOf: %v", err)
	}
	decimalsData, err := decimalsMethod.Outputs.Pack(uint8(18))
	if err != nil {
		t.Fatalf("pack decimals: %v", err)
	}
	symbolData, err := symbolMethod.Outputs.Pack("USDS")
	if err != nil {
		t.Fatalf("pack symbol: %v", err)
	}

	type mcResult struct {
		Success    bool
		ReturnData []byte
	}

	// dispatch answers a single inner sub-call by its 4-byte selector.
	dispatch := func(callData []byte) mcResult {
		if len(callData) < 4 {
			t.Errorf("sub-call calldata too short: %d bytes", len(callData))
			return mcResult{Success: false}
		}
		sel := callData[:4]
		switch {
		case bytes.Equal(sel, balanceOfMethod.ID):
			return mcResult{Success: true, ReturnData: balanceData}
		case bytes.Equal(sel, decimalsMethod.ID):
			return mcResult{Success: true, ReturnData: decimalsData}
		case bytes.Equal(sel, symbolMethod.ID):
			return mcResult{Success: true, ReturnData: symbolData}
		default:
			t.Errorf("unexpected selector %x in aggregate3 sub-call", sel)
			return mcResult{Success: false}
		}
	}

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string          `json:"method"`
			ID     json.RawMessage `json:"id"`
			Params json.RawMessage `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		switch req.Method {
		case "eth_chainId":
			testutil.WriteRPCResult(w, req.ID, json.RawMessage(`"0x1"`))
			return
		case "eth_blockNumber":
			testutil.WriteRPCResult(w, req.ID, json.RawMessage(`"0x1216af0"`))
			return
		case "eth_call":
			// handled below
		default:
			testutil.WriteRPCResult(w, req.ID, json.RawMessage(`"0x1"`))
			return
		}

		calls := testutil.ExtractAggregate3Calls(req.Params)
		if calls == nil {
			t.Errorf("eth_call was not a decodable aggregate3 payload")
			http.Error(w, "bad aggregate3 payload", http.StatusBadRequest)
			return
		}

		results := make([]mcResult, len(calls))
		for i, c := range calls {
			results[i] = dispatch(c.CallData)
		}

		packed, err := multicallABI.Methods["aggregate3"].Outputs.Pack(results)
		if err != nil {
			t.Errorf("pack aggregate3 outputs: %v", err)
			http.Error(w, "pack error", http.StatusInternalServerError)
			return
		}

		resultJSON, _ := json.Marshal("0x" + hex.EncodeToString(packed))
		testutil.WriteRPCResult(w, req.ID, json.RawMessage(resultJSON))
	}))
}

//go:build integration

package main

import (
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

	redisAdapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/redis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/rpcutil"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var (
	sharedDSN           string
	sharedRedisAddr     string
	sharedLocalStackCfg testutil.LocalStackConfig
)

// testBucket / testDeployEnv satisfy chainutil.ValidateS3BucketForChain, which the
// cache reader enforces at construction: stl-sentinel{env}-{chain}-raw, chainID=1 -> "ethereum".
const (
	testBucket    = "stl-sentineltest-ethereum-raw"
	testDeployEnv = "test"
	// archiveBucket receives raw SC call archives when ARCHIVE_SC_CALLS=true.
	archiveBucket = "test-morpho-worker-raw-sc-calls"
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

func TestRunIntegration_BadConnectionConfig(t *testing.T) {
	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer rpcServer.Close()

	t.Setenv("BUILD_GIT_HASH", "test")
	t.Setenv("ALCHEMY_API_KEY", "test-api-key")
	t.Setenv("ALCHEMY_HTTP_URL", rpcServer.URL)
	t.Setenv("S3_BUCKET", "stl-sentineltest-ethereum-raw")
	t.Setenv("DEPLOY_ENV", "test")

	err := run(context.Background(), []string{
		"-queue", "http://localhost/test-queue",
		"-redis", "localhost:6379",
		"-db", "postgres://invalid:invalid@localhost:1/nonexistent?connect_timeout=1",
	})
	if err == nil {
		t.Fatal("expected error for bad database URL")
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

// TestRunIntegration_ArchivesRawCalls drives the SQS worker end to end with
// ARCHIVE_SC_CALLS=true: it seeds a Morpho Blue AccrueInterest receipt into Redis,
// enqueues one block event, and serves a mock RPC. Processing the event drives real
// Multicall3 calls through the archiving-wrapped multicaller, so the worker must
// write a raw SC call object to S3 keyed under the block's reorg version.
//
// AccrueInterest is the simplest archiving driver for morpho: it emits from the
// Morpho Blue singleton (always relevant, no vault discovery), and its
// MorphoBlueVaultCandidates set is empty, so the V1/V1.1 discovery pre-walk
// probes nothing. The handler issues a market() multicall (the archived batch),
// then materialises the market via idToMarketParams() + token metadata reads.
func TestRunIntegration_ArchivesRawCalls(t *testing.T) {
	bgCtx := context.Background()

	pool, dbURL, cleanup := testutil.SetupTestSchema(t, sharedDSN)
	t.Cleanup(cleanup)

	// Morpho Blue is deployed at block 18883124 on Ethereum; use a block above it.
	const blockNum = int64(19000000)
	const version = 1

	// Seed Redis with an AccrueInterest receipt so the worker's cache read returns
	// it directly (no S3 fallback needed).
	seedAccrueInterestReceipt(t, bgCtx, blockNum, version)

	s3Client := testutil.NewS3Client(t, bgCtx, sharedLocalStackCfg)
	for _, b := range []string{testBucket, archiveBucket} {
		if _, err := s3Client.CreateBucket(bgCtx, &s3.CreateBucketInput{Bucket: aws.String(b)}); err != nil {
			t.Fatalf("create bucket %s: %v", b, err)
		}
	}

	rpcServer := buildMorphoAccrueInterestMockRPC(t)
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

	// Wait until the AccrueInterest event is fully processed (a market state row is
	// written) so the run loop is idle before we shut down, avoiding a
	// context-cancelled mid-write.
	testutil.WaitForCondition(t, 30*time.Second, func() bool {
		var count int
		if err := pool.QueryRow(bgCtx, `SELECT COUNT(*) FROM morpho_market_state`).Scan(&count); err != nil {
			return false
		}
		return count >= 1
	}, "the AccrueInterest event to be persisted")

	// Archives are fire-and-forget; poll until the object lands. The key embeds the
	// block version: rawsckey.Build formats {block}_{blockVersion}_{source}_{batchHash},
	// so the worker must archive under version 1 (not the default bv=0) with the
	// "morpho" source. This pins the reorg-aware keying contract for the live path.
	wantSegment := fmt.Sprintf("%d_%d_morpho_", blockNum, version)
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
}

// morphoBlueAddress is the immutable Morpho Blue singleton, lowercased for
// case-insensitive eth_call target matching.
const morphoBlueAddress = "0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb"

// testMarketID is the indexed market id carried by the seeded AccrueInterest log.
var testMarketID = common.HexToHash("0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc")

// testLoanToken / testCollToken are the market's loan and collateral tokens,
// returned by the mocked idToMarketParams() and resolved via token metadata reads.
var (
	testLoanToken = common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	testCollToken = common.HexToAddress("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
)

// seedAccrueInterestReceipt writes a Morpho Blue AccrueInterest receipt into the
// Redis block cache at the given block/version, so the worker's cache read
// returns it.
func seedAccrueInterestReceipt(t *testing.T, ctx context.Context, blockNum int64, version int) {
	t.Helper()

	eventsABI, err := abis.GetMorphoBlueEventsABI()
	if err != nil {
		t.Fatalf("load Morpho Blue events ABI: %v", err)
	}
	event := eventsABI.Events["AccrueInterest"]
	// AccrueInterest: indexed(id), non-indexed(prevBorrowRate, interest, feeShares).
	data, err := event.Inputs.NonIndexed().Pack(big.NewInt(1000), big.NewInt(2000), big.NewInt(0))
	if err != nil {
		t.Fatalf("pack AccrueInterest data: %v", err)
	}

	// A valid 32-byte hex tx hash: saveProtocolEvent runs it through common.FromHex.
	txHash := "0x" + strings.Repeat("ab", 32)
	receipts := []shared.TransactionReceipt{
		{
			TransactionHash: txHash,
			Logs: []shared.Log{
				{
					Address: common.HexToAddress(morphoBlueAddress).Hex(),
					Topics: []string{
						event.ID.Hex(),
						testMarketID.Hex(),
					},
					Data:            "0x" + hex.EncodeToString(data),
					LogIndex:        "0x0",
					TransactionHash: txHash,
				},
			},
		},
	}

	payload, err := json.Marshal(receipts)
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

	if err := blockCache.SetReceipts(ctx, 1, blockNum, version, payload); err != nil {
		t.Fatalf("seed receipts in Redis: %v", err)
	}
}

// buildMorphoAccrueInterestMockRPC returns a mock JSON-RPC server handling the
// three Multicall3.aggregate3 batches the indexer issues for an AccrueInterest
// event whose market is not yet in the database:
//
//  1. getMarketState: 1 call, market(bytes32) on Morpho Blue. This is the batch
//     whose archive we assert on.
//  2. ensureMarket -> getMarketParams: 1 call, idToMarketParams(bytes32) on
//     Morpho Blue, returning the loan/collateral token addresses.
//  3. ensureMarket -> getTokenPairMetadata: 4 calls, symbol()/decimals() for the
//     loan and collateral tokens.
//
// Batches are distinguished by call count, the first call's target, and (for the
// two single-call Morpho Blue batches) the first call's 4-byte selector.
func buildMorphoAccrueInterestMockRPC(t *testing.T) *httptest.Server {
	t.Helper()

	const multicall3Address = "0xca11bde05977b3631167028862be2a173976ca11"

	readABI, err := abis.GetMorphoBlueReadABI()
	if err != nil {
		t.Fatalf("load Morpho Blue read ABI: %v", err)
	}
	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("load ERC20 ABI: %v", err)
	}
	multicallABI, err := abis.GetMulticall3ABI()
	if err != nil {
		t.Fatalf("load multicall3 ABI: %v", err)
	}

	// Selectors used to discriminate the two single-call Morpho Blue batches.
	marketCall, err := readABI.Pack("market", testMarketID)
	if err != nil {
		t.Fatalf("pack market(): %v", err)
	}
	marketParamsCall, err := readABI.Pack("idToMarketParams", testMarketID)
	if err != nil {
		t.Fatalf("pack idToMarketParams(): %v", err)
	}
	marketSelector := selectorOf(t, marketCall)
	marketParamsSelector := selectorOf(t, marketParamsCall)

	// market() result: arbitrary but valid uint128 fields.
	marketData, err := readABI.Methods["market"].Outputs.Pack(
		big.NewInt(1000000), big.NewInt(900000), big.NewInt(500000),
		big.NewInt(450000), big.NewInt(1700000000), big.NewInt(0),
	)
	if err != nil {
		t.Fatalf("pack market(): %v", err)
	}

	// idToMarketParams() result: loan + collateral tokens plus oracle/irm/lltv.
	marketParamsData, err := readABI.Methods["idToMarketParams"].Outputs.Pack(
		testLoanToken, testCollToken,
		common.HexToAddress("0xcccccccccccccccccccccccccccccccccccccccc"),
		common.HexToAddress("0xdddddddddddddddddddddddddddddddddddddddd"),
		new(big.Int).Mul(big.NewInt(8), new(big.Int).Exp(big.NewInt(10), big.NewInt(17), nil)),
	)
	if err != nil {
		t.Fatalf("pack idToMarketParams(): %v", err)
	}

	// Pre-pack ERC-20 metadata for the token pair batch (symbol + decimals each).
	loanSymbolData, err := erc20ABI.Methods["symbol"].Outputs.Pack("LOAN")
	if err != nil {
		t.Fatalf("pack symbol(LOAN): %v", err)
	}
	collSymbolData, err := erc20ABI.Methods["symbol"].Outputs.Pack("COLL")
	if err != nil {
		t.Fatalf("pack symbol(COLL): %v", err)
	}
	decimalsData, err := erc20ABI.Methods["decimals"].Outputs.Pack(uint8(18))
	if err != nil {
		t.Fatalf("pack decimals(): %v", err)
	}

	type mcResult struct {
		Success    bool
		ReturnData []byte
	}
	packAgg := func(results []mcResult) string {
		out, packErr := multicallABI.Methods["aggregate3"].Outputs.Pack(results)
		if packErr != nil {
			t.Fatalf("pack aggregate3: %v", packErr)
		}
		return "0x" + hex.EncodeToString(out)
	}

	marketAggHex := packAgg([]mcResult{{Success: true, ReturnData: marketData}})
	marketParamsAggHex := packAgg([]mcResult{{Success: true, ReturnData: marketParamsData}})
	tokenPairAggHex := packAgg([]mcResult{
		{Success: true, ReturnData: loanSymbolData},
		{Success: true, ReturnData: decimalsData},
		{Success: true, ReturnData: collSymbolData},
		{Success: true, ReturnData: decimalsData},
	})

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		var req rpcutil.Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			testutil.WriteRPCError(w, json.RawMessage(`1`), -32700, "parse error")
			return
		}
		if req.Method != "eth_call" {
			testutil.WriteRPCError(w, req.ID, -32601, "method not found: "+req.Method)
			return
		}

		target := testutil.ExtractCallTarget(req.Params)
		if strings.ToLower(target) != multicall3Address {
			// Unknown direct call: return empty data so the worker does not crash.
			testutil.WriteRPCResult(w, req.ID, json.RawMessage(`"0x"`))
			return
		}

		calls := testutil.ExtractAggregate3Calls(req.Params)
		switch len(calls) {
		case 1:
			// One Morpho Blue call: market() (archived batch) vs idToMarketParams().
			if hasSelector(calls[0].CallData, marketSelector) {
				writeResult(w, req.ID, marketAggHex)
				return
			}
			if hasSelector(calls[0].CallData, marketParamsSelector) {
				writeResult(w, req.ID, marketParamsAggHex)
				return
			}
			testutil.WriteRPCError(w, req.ID, -32000, "unexpected single-call selector")
		case 4:
			// Token pair metadata (symbolA, decimalsA, symbolB, decimalsB).
			writeResult(w, req.ID, tokenPairAggHex)
		default:
			testutil.WriteRPCError(w, req.ID, -32000, fmt.Sprintf("unexpected aggregate3 call count: %d", len(calls)))
		}
	}))
}

// writeResult marshals a hex string into a JSON-RPC result and writes it.
func writeResult(w http.ResponseWriter, id json.RawMessage, hexResult string) {
	resultJSON, _ := json.Marshal(hexResult)
	testutil.WriteRPCResult(w, id, json.RawMessage(resultJSON))
}

// selectorOf returns the 4-byte selector of packed call data.
func selectorOf(t *testing.T, callData []byte) []byte {
	t.Helper()
	if len(callData) < 4 {
		t.Fatalf("call data too short for a selector: %d bytes", len(callData))
	}
	return callData[:4]
}

// hasSelector reports whether callData begins with the given 4-byte selector.
func hasSelector(callData, selector []byte) bool {
	if len(callData) < 4 || len(selector) < 4 {
		return false
	}
	for i := range 4 {
		if callData[i] != selector[i] {
			return false
		}
	}
	return true
}

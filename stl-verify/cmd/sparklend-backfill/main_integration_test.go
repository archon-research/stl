//go:build integration

package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/services/sparklend"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// ---------------------------------------------------------------------------
// Integration tests for run()
// ---------------------------------------------------------------------------

// TestRunIntegration_HappyPath verifies that run() completes successfully
// when S3 contains valid (empty) receipt files for the requested block range.
// Empty receipts produce no DB writes, so the test asserts that run() returns
// nil and that the protocol_event table stays empty.
func TestRunIntegration_HappyPath(t *testing.T) {
	ctx := context.Background()

	pool, dbURL, dbCleanup := testutil.SetupTimescaleDB(t)
	defer dbCleanup()

	s3Client, s3Endpoint, s3Cleanup := startLocalStackS3(t, ctx)
	defer s3Cleanup()

	const bucket = "test-sparklend-backfill"
	const fromBlock = 100
	const toBlock = 102
	if _, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	// Upload gzipped empty receipt lists for each block in the range.
	for blockNum := int64(fromBlock); blockNum <= toBlock; blockNum++ {
		uploadReceipts(t, ctx, s3Client, bucket, blockNum, 1, []sparklend.TransactionReceipt{})
	}

	// A minimal mock RPC — the backfill processes empty receipts so no
	// eth_call or eth_getBlockByNumber requests are made, but go-ethereum's
	// DialOptions validates the URL at connection time.
	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Return an empty JSON-RPC result for any method.
		var req testutil.JSONRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":null}`))
			return
		}
		resp := fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"result":null}`, req.ID)
		_, _ = w.Write([]byte(resp))
	}))
	defer rpcServer.Close()

	t.Setenv("AWS_ENDPOINT_URL", s3Endpoint)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	args := []string{
		"-rpc-url", rpcServer.URL,
		"-from", fmt.Sprintf("%d", fromBlock),
		"-to", fmt.Sprintf("%d", toBlock),
		"-bucket", bucket,
		"-db", dbURL,
		"-concurrency", "2",
		"-aws-region", "us-east-1",
	}

	if err := run(args); err != nil {
		t.Fatalf("run() failed: %v", err)
	}

	// Assert that no protocol events were written — empty receipts contain no logs.
	var eventCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM protocol_event`).Scan(&eventCount); err != nil {
		t.Fatalf("query protocol_event count: %v", err)
	}
	if eventCount != 0 {
		t.Errorf("expected 0 protocol_event rows, got %d", eventCount)
	}
}

// TestRunIntegration_BorrowEvent verifies the full end-to-end path for a receipt
// containing a SparkLend Borrow event. The test:
//   - Uploads a receipt with a realistic Borrow log to LocalStack S3
//   - Serves a mock RPC that handles eth_call (getUserReservesData → empty) and
//     multicall3 aggregate3 (ERC-20 metadata for the reserve token)
//   - Calls run() and asserts that exactly one protocol_event and one borrower
//     row appear in the database
func TestRunIntegration_BorrowEvent(t *testing.T) {
	ctx := context.Background()

	pool, dbURL, dbCleanup := testutil.SetupTimescaleDB(t)
	defer dbCleanup()

	s3Client, s3Endpoint, s3Cleanup := startLocalStackS3(t, ctx)
	defer s3Cleanup()

	// SparkLend's PoolDataProvider is only active from block 16776400 onwards.
	// Use a block well above that so GetPoolDataProviderForBlock succeeds.
	const blockNum = int64(16800000)
	const bucket = "test-sparklend-borrow"

	if _, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	// DAI address — reserve token for the Borrow event.
	const daiAddress = "0x6B175474E89094C44Da98b954EedeAC495271d0F"
	// SparkLend pool address (known protocol).
	const sparkLendPool = "0xC13e21B648A5Ee794902342038FF3aDAB66BE987"
	// A synthetic borrower address.
	const borrowerAddress = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"

	// Build a realistic SparkLend Borrow event log.
	// Borrow event topic: 0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0
	// Indexed fields: reserve (topic[1]), user (topic[2]), referralCode (topic[3])
	// Non-indexed fields (data): onBehalfOf (address), amount (uint256), interestRateMode (uint256),
	//                            borrowRate (uint256)
	borrowTopic := "0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0"
	// topic[1]: reserve = DAI (left-padded to 32 bytes)
	reserveTopic := "0x0000000000000000000000006b175474e89094c44da98b954eedeac495271d0f"
	// topic[2]: user = borrowerAddress (left-padded to 32 bytes)
	userTopic := "0x000000000000000000000000d8da6bf26964af9d7eed9e03e53415d37aa96045"
	// topic[3]: referralCode = 0
	referralTopic := "0x0000000000000000000000000000000000000000000000000000000000000000"

	// Data: onBehalfOf (32 bytes) + amount (100 DAI = 100e18) + interestRateMode (2) + borrowRate (arbitrary)
	// Pack as ABI-encoded tuple of (address, uint256, uint256, uint256).
	borrowAmount := new(big.Int).Mul(big.NewInt(100), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
	borrowData := fmt.Sprintf(""+
		"000000000000000000000000d8da6bf26964af9d7eed9e03e53415d37aa96045"+ // onBehalfOf
		"%064x"+ // amount = 100e18
		"0000000000000000000000000000000000000000000000000000000000000002"+ // interestRateMode
		"000000000000000000000000000000000000000000000004563918244f400000", // borrowRate (arbitrary)
		borrowAmount,
	)

	receipt := sparklend.TransactionReceipt{
		TransactionHash: "0xborrow" + strings.Repeat("0", 57),
		Logs: []sparklend.Log{
			{
				Address:         sparkLendPool,
				Topics:          []string{borrowTopic, reserveTopic, userTopic, referralTopic},
				Data:            "0x" + borrowData,
				LogIndex:        "0x0",
				TransactionHash: "0xborrow" + strings.Repeat("0", 57),
			},
		},
	}

	uploadReceipts(t, ctx, s3Client, bucket, blockNum, 1, []sparklend.TransactionReceipt{receipt})

	// Build ABI-encoded mock RPC responses.
	rpcServer := buildBorrowMockRPC(t, daiAddress)
	defer rpcServer.Close()

	t.Setenv("AWS_ENDPOINT_URL", s3Endpoint)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	args := []string{
		"-rpc-url", rpcServer.URL,
		"-from", fmt.Sprintf("%d", blockNum),
		"-to", fmt.Sprintf("%d", blockNum),
		"-bucket", bucket,
		"-db", dbURL,
		"-concurrency", "1",
		"-aws-region", "us-east-1",
	}

	if err := run(args); err != nil {
		t.Fatalf("run() failed: %v", err)
	}

	assertBorrowEventDBState(t, ctx, pool, 0)
}

// TestRunIntegration_BadDatabaseURL verifies that run() returns a descriptive
// error when the PostgreSQL connection string is unreachable.
func TestRunIntegration_BadDatabaseURL(t *testing.T) {
	ctx := context.Background()

	s3Client, s3Endpoint, s3Cleanup := startLocalStackS3(t, ctx)
	defer s3Cleanup()

	const bucket = "test-sparklend-backfill-baddb"
	if _, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	uploadReceipts(t, ctx, s3Client, bucket, 100, 1, []sparklend.TransactionReceipt{})

	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	defer rpcServer.Close()

	t.Setenv("AWS_ENDPOINT_URL", s3Endpoint)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	err := run([]string{
		"-rpc-url", rpcServer.URL,
		"-from", "100",
		"-to", "100",
		"-bucket", bucket,
		"-db", "postgres://invalid:invalid@localhost:1/nonexistent?connect_timeout=1",
		"-aws-region", "us-east-1",
	})
	if err == nil {
		t.Fatal("expected error for bad database URL, got nil")
	}
	if !strings.Contains(err.Error(), "database") && !strings.Contains(err.Error(), "connect") {
		t.Errorf("expected database/connect error, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// DB assertion helpers
// ---------------------------------------------------------------------------

// assertBorrowEventDBState asserts the expected database state after processing a single
// Borrow event for DAI by borrowerAddress from sparkLendPool at blockNum.
//
// expectedCollateralCount is the number of borrower_collateral rows expected.
//
// For every Borrow event the following rows must be present:
//   - exactly 1 protocol_event row with event_name = 'Borrow'
//   - at least 1 "user" row for the borrower address
//   - at least 1 protocol row for SparkLend
//   - at least 1 token row for the DAI reserve
//   - exactly 1 borrower row
//   - exactly expectedCollateralCount borrower_collateral rows
func assertBorrowEventDBState(t *testing.T, ctx context.Context, pool *pgxpool.Pool, expectedCollateralCount int) {
	t.Helper()

	// protocol_event: exactly 1 row with event_name = 'Borrow'
	var eventCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM protocol_event WHERE event_name = 'Borrow'`).Scan(&eventCount); err != nil {
		t.Fatalf("query protocol_event count: %v", err)
	}
	if eventCount != 1 {
		t.Errorf("expected 1 protocol_event row with event_name='Borrow', got %d", eventCount)
	}

	// "user": at least 1 row (borrower address)
	var userCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM "user"`).Scan(&userCount); err != nil {
		t.Fatalf("query user count: %v", err)
	}
	if userCount < 1 {
		t.Errorf("expected at least 1 user row, got %d", userCount)
	}

	// protocol: at least 1 row (SparkLend is seeded by migration)
	var protocolCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM protocol`).Scan(&protocolCount); err != nil {
		t.Fatalf("query protocol count: %v", err)
	}
	if protocolCount < 1 {
		t.Errorf("expected at least 1 protocol row, got %d", protocolCount)
	}

	// token: at least 1 row (DAI reserve token)
	var tokenCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM token`).Scan(&tokenCount); err != nil {
		t.Fatalf("query token count: %v", err)
	}
	if tokenCount < 1 {
		t.Errorf("expected at least 1 token row (DAI), got %d", tokenCount)
	}

	// borrower: exactly 1 row
	var borrowerCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM borrower`).Scan(&borrowerCount); err != nil {
		t.Fatalf("query borrower count: %v", err)
	}
	if borrowerCount != 1 {
		t.Errorf("expected 1 borrower row, got %d", borrowerCount)
	}

	// borrower_collateral: exactly expectedCollateralCount rows
	var collateralCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM borrower_collateral`).Scan(&collateralCount); err != nil {
		t.Fatalf("query borrower_collateral count: %v", err)
	}
	if collateralCount != expectedCollateralCount {
		t.Errorf("expected %d borrower_collateral rows, got %d", expectedCollateralCount, collateralCount)
	}
}

// TestRunIntegration_BorrowEvent_WithCollateral verifies the full end-to-end path for a
// receipt containing a SparkLend Borrow event where the borrower has active WETH collateral.
// The test asserts that exactly one borrower_collateral row is written in addition to the
// standard protocol_event / user / protocol / token (DAI + WETH) / borrower rows.
func TestRunIntegration_BorrowEvent_WithCollateral(t *testing.T) {
	ctx := context.Background()

	pool, dbURL, dbCleanup := testutil.SetupTimescaleDB(t)
	defer dbCleanup()

	s3Client, s3Endpoint, s3Cleanup := startLocalStackS3(t, ctx)
	defer s3Cleanup()

	const blockNum = int64(16800000)
	const bucket = "test-sparklend-borrow-collateral"

	if _, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	const daiAddress = "0x6B175474E89094C44Da98b954EedeAC495271d0F"
	const wethAddress = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
	const sparkLendPool = "0xC13e21B648A5Ee794902342038FF3aDAB66BE987"
	const borrowerAddress = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"

	borrowTopic := "0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0"
	reserveTopic := "0x0000000000000000000000006b175474e89094c44da98b954eedeac495271d0f"
	userTopic := "0x000000000000000000000000d8da6bf26964af9d7eed9e03e53415d37aa96045"
	referralTopic := "0x0000000000000000000000000000000000000000000000000000000000000000"

	borrowAmount := new(big.Int).Mul(big.NewInt(100), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
	borrowData := fmt.Sprintf(""+
		"000000000000000000000000d8da6bf26964af9d7eed9e03e53415d37aa96045"+
		"%064x"+
		"0000000000000000000000000000000000000000000000000000000000000002"+
		"000000000000000000000000000000000000000000000004563918244f400000",
		borrowAmount,
	)

	receipt := sparklend.TransactionReceipt{
		TransactionHash: "0xc0" + strings.Repeat("0", 62),
		Logs: []sparklend.Log{
			{
				Address:         sparkLendPool,
				Topics:          []string{borrowTopic, reserveTopic, userTopic, referralTopic},
				Data:            "0x" + borrowData,
				LogIndex:        "0x0",
				TransactionHash: "0xc0" + strings.Repeat("0", 62),
			},
		},
	}

	uploadReceipts(t, ctx, s3Client, bucket, blockNum, 1, []sparklend.TransactionReceipt{receipt})

	rpcServer := buildBorrowWithCollateralMockRPC(t, daiAddress, wethAddress, borrowerAddress)
	defer rpcServer.Close()

	t.Setenv("AWS_ENDPOINT_URL", s3Endpoint)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	args := []string{
		"-rpc-url", rpcServer.URL,
		"-from", fmt.Sprintf("%d", blockNum),
		"-to", fmt.Sprintf("%d", blockNum),
		"-bucket", bucket,
		"-db", dbURL,
		"-concurrency", "1",
		"-aws-region", "us-east-1",
	}

	if err := run(args); err != nil {
		t.Fatalf("run() failed: %v", err)
	}

	assertBorrowEventDBState(t, ctx, pool, 1)

	// Additionally verify that the collateral token (WETH) was created.
	var wethTokenCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM token WHERE symbol = 'WETH'`).Scan(&wethTokenCount); err != nil {
		t.Fatalf("query WETH token count: %v", err)
	}
	if wethTokenCount != 1 {
		t.Errorf("expected 1 WETH token row, got %d", wethTokenCount)
	}
}

// ---------------------------------------------------------------------------
// Mock RPC helpers
// ---------------------------------------------------------------------------

// buildBorrowMockRPC returns a mock JSON-RPC server that handles the two RPC
// call patterns made by the position tracker when processing a Borrow event:
//
//  1. Direct eth_call to UiPoolDataProvider.getUserReservesData → returns 0x
//     (empty response means no active reserves, so no collateral multicalls are
//     made and the test stays simple)
//
//  2. eth_call to Multicall3.aggregate3 → returns ABI-encoded ERC-20 metadata
//     (decimals=18, symbol="DAI", name="Dai Stablecoin") for the reserve token
func buildBorrowMockRPC(t *testing.T, reserveAddress string) *httptest.Server {
	t.Helper()

	// SparkLend UiPoolDataProvider address — direct eth_call goes here.
	const uiPoolDataProvider = "0x56b7a1012765c285afac8b8f25c69bf10ccfe978"
	// Multicall3 address — aggregate3 calls go here.
	const multicall3Address = "0xca11bde05977b3631167028862be2a173976ca11"

	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("load ERC20 ABI: %v", err)
	}
	multicallABI, err := abis.GetMulticall3ABI()
	if err != nil {
		t.Fatalf("load multicall3 ABI: %v", err)
	}

	// Pre-pack ERC-20 metadata responses.
	decimalsData, err := erc20ABI.Methods["decimals"].Outputs.Pack(uint8(18))
	if err != nil {
		t.Fatalf("pack decimals: %v", err)
	}
	symbolData, err := erc20ABI.Methods["symbol"].Outputs.Pack("DAI")
	if err != nil {
		t.Fatalf("pack symbol: %v", err)
	}
	nameData, err := erc20ABI.Methods["name"].Outputs.Pack("Dai Stablecoin")
	if err != nil {
		t.Fatalf("pack name: %v", err)
	}

	type mcResult struct {
		Success    bool
		ReturnData []byte
	}

	// aggregate3 response: 3 results (decimals, symbol, name) for 1 token.
	aggResult, err := multicallABI.Methods["aggregate3"].Outputs.Pack([]mcResult{
		{Success: true, ReturnData: decimalsData},
		{Success: true, ReturnData: symbolData},
		{Success: true, ReturnData: nameData},
	})
	if err != nil {
		t.Fatalf("pack aggregate3: %v", err)
	}
	aggHex := "0x" + hex.EncodeToString(aggResult)

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		var req testutil.JSONRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			testutil.WriteRPCError(w, json.RawMessage(`1`), -32700, "parse error")
			return
		}

		if req.Method != "eth_call" {
			testutil.WriteRPCError(w, req.ID, -32601, "method not found: "+req.Method)
			return
		}

		// Identify call target by reading the "to" field from params[0].
		target := extractCallTarget(req.Params)

		switch strings.ToLower(target) {
		case uiPoolDataProvider:
			// getUserReservesData — return empty (0x) so no collateral fetching occurs.
			testutil.WriteRPCResult(w, req.ID, json.RawMessage(`"0x"`))

		case multicall3Address:
			// aggregate3 for ERC-20 metadata — return valid encoded metadata.
			resultJSON, _ := json.Marshal(aggHex)
			testutil.WriteRPCResult(w, req.ID, json.RawMessage(resultJSON))

		default:
			// Unknown target — return empty result rather than error so the
			// process doesn't crash on unexpected calls.
			testutil.WriteRPCResult(w, req.ID, json.RawMessage(`"0x"`))
		}
	}))
}

// extractCallTarget reads the "to" field from an eth_call params array.
// Returns an empty string if parsing fails.
func extractCallTarget(params json.RawMessage) string {
	var p []json.RawMessage
	if err := json.Unmarshal(params, &p); err != nil || len(p) < 1 {
		return ""
	}
	var callObj map[string]string
	if err := json.Unmarshal(p[0], &callObj); err != nil {
		return ""
	}
	return strings.ToLower(callObj["to"])
}

// extractFirstAggregate3CallTarget inspects a Multicall3 aggregate3 calldata payload and
// returns the lowercase hex address of the first Call3 entry's target field.
// This allows the mock RPC to distinguish between different aggregate3 usages
// (e.g., ERC-20 metadata vs. PoolDataProvider getUserReserveData) by looking at
// which contract is being called inside the batch.
//
// Call3 ABI layout: aggregate3(Call3[] calls) where Call3 = (address target, bool allowFailure, bytes callData)
// The outer aggregate3 selector is 4 bytes; after that the tuple array is ABI-encoded.
// We skip the 4-byte selector, then parse the ABI-encoded Call3[] to get calls[0].target.
func extractFirstAggregate3CallTarget(t *testing.T, params json.RawMessage) string {
	t.Helper()

	var p []json.RawMessage
	if err := json.Unmarshal(params, &p); err != nil || len(p) < 1 {
		return ""
	}
	var callObj map[string]string
	if err := json.Unmarshal(p[0], &callObj); err != nil {
		return ""
	}
	// go-ethereum sends calldata as "input"; some clients use "data" — handle both.
	rawField := callObj["input"]
	if rawField == "" {
		rawField = callObj["data"]
	}
	dataHex := strings.TrimPrefix(strings.ToLower(rawField), "0x")
	if len(dataHex) < 8+128 { // at least 4-byte selector + 1 encoded Call3 element header
		return ""
	}
	raw, err := hex.DecodeString(dataHex)
	if err != nil || len(raw) < 4+32+32+32 { // selector + offset + length + first element start
		return ""
	}

	// Load the multicall ABI to unpack aggregate3 inputs.
	multicallABI, err := abis.GetMulticall3ABI()
	if err != nil {
		return ""
	}

	// raw[4:] is the ABI-encoded (Call3[] calls) input.
	unpacked, err := multicallABI.Methods["aggregate3"].Inputs.Unpack(raw[4:])
	if err != nil || len(unpacked) == 0 {
		return ""
	}

	// The unpacked value is a slice of anonymous structs matching the Call3 tuple.
	type call3 struct {
		Target       common.Address
		AllowFailure bool
		CallData     []byte
	}
	calls, ok := unpacked[0].([]struct {
		Target       common.Address `json:"target"`
		AllowFailure bool           `json:"allowFailure"`
		CallData     []byte         `json:"callData"`
	})
	if !ok || len(calls) == 0 {
		return ""
	}

	return strings.ToLower(calls[0].Target.Hex())
}

// buildBorrowWithCollateralMockRPC returns a mock JSON-RPC server that handles all
// three RPC call patterns made by the position tracker when processing a Borrow event
// where the borrower has active WETH collateral:
//
//  1. Direct eth_call to UiPoolDataProvider.getUserReservesData →
//     returns ABI-encoded SparkLend 4-field tuple for WETH with
//     scaledATokenBalance=1e18, usageAsCollateralEnabledOnUser=true
//
//  2. eth_call to Multicall3.aggregate3 where first call target is DAI →
//     returns ERC-20 metadata (decimals=18, symbol="DAI", name="Dai Stablecoin")
//
//  3. eth_call to Multicall3.aggregate3 where first call target is WETH →
//     returns ERC-20 metadata (decimals=18, symbol="WETH", name="Wrapped Ether")
//
//  4. eth_call to Multicall3.aggregate3 where first call target is PoolDataProvider →
//     returns getUserReserveData for WETH:
//     currentATokenBalance=1e18, usageAsCollateralEnabled=true
func buildBorrowWithCollateralMockRPC(t *testing.T, daiAddress, wethAddress, borrowerAddress string) *httptest.Server {
	t.Helper()

	const uiPoolDataProvider = "0x56b7a1012765c285afac8b8f25c69bf10ccfe978"
	const multicall3Address = "0xca11bde05977b3631167028862be2a173976ca11"
	// SparkLend PoolDataProvider active at block 16776400
	const poolDataProvider = "0xfc21d6d146e6086b8359705c8b28512a983db0cb"

	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("load ERC20 ABI: %v", err)
	}
	multicallABI, err := abis.GetMulticall3ABI()
	if err != nil {
		t.Fatalf("load multicall3 ABI: %v", err)
	}
	sparklendABI, err := abis.GetSparklendUserReservesDataABI()
	if err != nil {
		t.Fatalf("load sparklend ABI: %v", err)
	}
	userReserveDataABI, err := abis.GetPoolDataProviderUserReserveDataABI()
	if err != nil {
		t.Fatalf("load pool data provider ABI: %v", err)
	}

	type mcResult struct {
		Success    bool
		ReturnData []byte
	}

	// Pre-pack DAI ERC-20 metadata aggregate3 response.
	daiDecimals, _ := erc20ABI.Methods["decimals"].Outputs.Pack(uint8(18))
	daiSymbol, _ := erc20ABI.Methods["symbol"].Outputs.Pack("DAI")
	daiName, _ := erc20ABI.Methods["name"].Outputs.Pack("Dai Stablecoin")
	daiAggResult, err := multicallABI.Methods["aggregate3"].Outputs.Pack([]mcResult{
		{Success: true, ReturnData: daiDecimals},
		{Success: true, ReturnData: daiSymbol},
		{Success: true, ReturnData: daiName},
	})
	if err != nil {
		t.Fatalf("pack DAI aggregate3: %v", err)
	}

	// Pre-pack WETH ERC-20 metadata aggregate3 response.
	wethDecimals, _ := erc20ABI.Methods["decimals"].Outputs.Pack(uint8(18))
	wethSymbol, _ := erc20ABI.Methods["symbol"].Outputs.Pack("WETH")
	wethName, _ := erc20ABI.Methods["name"].Outputs.Pack("Wrapped Ether")
	wethAggResult, err := multicallABI.Methods["aggregate3"].Outputs.Pack([]mcResult{
		{Success: true, ReturnData: wethDecimals},
		{Success: true, ReturnData: wethSymbol},
		{Success: true, ReturnData: wethName},
	})
	if err != nil {
		t.Fatalf("pack WETH aggregate3: %v", err)
	}

	// Pre-pack getUserReserveData result for WETH (via PoolDataProvider).
	// currentATokenBalance=1e18, rest zeros, usageAsCollateralEnabled=true
	one := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	zero := big.NewInt(0)
	stableRateLastUpdated := big.NewInt(0) // uint40 packed as *big.Int in ABI output
	getUserReserveDataResult, err := userReserveDataABI.Methods["getUserReserveData"].Outputs.Pack(
		one,                   // currentATokenBalance
		zero,                  // currentStableDebt
		zero,                  // currentVariableDebt
		zero,                  // principalStableDebt
		zero,                  // scaledVariableDebt
		zero,                  // stableBorrowRate
		zero,                  // liquidityRate
		stableRateLastUpdated, // stableRateLastUpdated (uint40)
		true,                  // usageAsCollateralEnabled
	)
	if err != nil {
		t.Fatalf("pack getUserReserveData: %v", err)
	}
	poolDataProviderAggResult, err := multicallABI.Methods["aggregate3"].Outputs.Pack([]mcResult{
		{Success: true, ReturnData: getUserReserveDataResult},
	})
	if err != nil {
		t.Fatalf("pack poolDataProvider aggregate3: %v", err)
	}

	// Pre-encode getUserReservesData response: WETH with 1e18 scaledATokenBalance,
	// usageAsCollateralEnabledOnUser=true. The SparkLend ABI uses a 4-field tuple[].
	type sparklendReserve struct {
		UnderlyingAsset                common.Address
		ScaledATokenBalance            *big.Int
		UsageAsCollateralEnabledOnUser bool
		ScaledVariableDebt             *big.Int
	}
	wethAddr := common.HexToAddress(wethAddress)
	getUserReservesResult, err := sparklendABI.Methods["getUserReservesData"].Outputs.Pack(
		[]sparklendReserve{
			{
				UnderlyingAsset:                wethAddr,
				ScaledATokenBalance:            one,
				UsageAsCollateralEnabledOnUser: true,
				ScaledVariableDebt:             zero,
			},
		},
		uint8(0), // eMode category
	)
	if err != nil {
		t.Fatalf("pack getUserReservesData: %v", err)
	}

	getUserReservesHex := "0x" + hex.EncodeToString(getUserReservesResult)
	daiAggHex := "0x" + hex.EncodeToString(daiAggResult)
	wethAggHex := "0x" + hex.EncodeToString(wethAggResult)
	poolProviderAggHex := "0x" + hex.EncodeToString(poolDataProviderAggResult)

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		var req testutil.JSONRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			testutil.WriteRPCError(w, json.RawMessage(`1`), -32700, "parse error")
			return
		}

		if req.Method != "eth_call" {
			testutil.WriteRPCError(w, req.ID, -32601, "method not found: "+req.Method)
			return
		}

		target := extractCallTarget(req.Params)

		switch strings.ToLower(target) {
		case uiPoolDataProvider:
			// getUserReservesData — return WETH with collateral enabled.
			resultJSON, _ := json.Marshal(getUserReservesHex)
			testutil.WriteRPCResult(w, req.ID, json.RawMessage(resultJSON))

		case multicall3Address:
			// Distinguish aggregate3 calls by inspecting the first inner call target.
			firstTarget := extractFirstAggregate3CallTarget(t, req.Params)
			switch firstTarget {
			case strings.ToLower(wethAddress):
				// WETH ERC-20 metadata.
				resultJSON, _ := json.Marshal(wethAggHex)
				testutil.WriteRPCResult(w, req.ID, json.RawMessage(resultJSON))
			case poolDataProvider:
				// getUserReserveData via PoolDataProvider.
				resultJSON, _ := json.Marshal(poolProviderAggHex)
				testutil.WriteRPCResult(w, req.ID, json.RawMessage(resultJSON))
			default:
				// DAI ERC-20 metadata (or any other token metadata call).
				resultJSON, _ := json.Marshal(daiAggHex)
				testutil.WriteRPCResult(w, req.ID, json.RawMessage(resultJSON))
			}

		default:
			testutil.WriteRPCResult(w, req.ID, json.RawMessage(`"0x"`))
		}
	}))
}

// ---------------------------------------------------------------------------
// S3 helpers
// ---------------------------------------------------------------------------

// uploadReceipts serializes receipts to JSON, gzips the result, and uploads
// the object to S3 at the key computed for blockNum / version.
func uploadReceipts(t *testing.T, ctx context.Context, client *s3.Client, bucket string, blockNum int64, version int, receipts []sparklend.TransactionReceipt) {
	t.Helper()

	key := s3key.Build(blockNum, version, s3key.Receipts)

	jsonBytes, err := json.Marshal(receipts)
	if err != nil {
		t.Fatalf("marshal receipts: %v", err)
	}

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(jsonBytes); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(buf.Bytes()),
	})
	if err != nil {
		t.Fatalf("put object %s: %v", key, err)
	}
}

// startLocalStackS3 starts a LocalStack container configured for S3 only and
// returns an S3 client, the endpoint URL, and a cleanup function.
func startLocalStackS3(t *testing.T, ctx context.Context) (*s3.Client, string, func()) {
	t.Helper()

	req := testcontainers.ContainerRequest{
		Image:        testutil.ImageLocalStack,
		ExposedPorts: []string{"4566/tcp"},
		Env: map[string]string{
			"SERVICES": "s3",
			"DEBUG":    "0",
		},
		WaitingFor: wait.ForHTTP("/_localstack/health").
			WithPort("4566/tcp").
			WithStartupTimeout(120 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start localstack: %v", err)
	}

	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, "4566")
	endpoint := fmt.Sprintf("http://%s:%s", host, port.Port())

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true // Required for LocalStack
	})

	cleanup := func() {
		_ = container.Terminate(context.Background())
	}
	return client, endpoint, cleanup
}

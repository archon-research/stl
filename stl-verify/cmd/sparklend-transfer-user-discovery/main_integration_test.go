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

	redisadapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/redis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/rpcutil"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgxpool"
)

var erc20TransferTopic = common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

func TestMain(m *testing.M) {
	os.Exit(testutil.RunTestsWithLeakCheck(m))
}

func TestRunIntegration_TransferCreatesNewUserSnapshotWithoutResnapshottingExistingSender(t *testing.T) {
	// Spec coverage:
	// - plan-transfer-user-discovery.md step 7: SparkLend receipt tokens are seeded by migration
	// - plan-transfer-user-discovery.md step 8: full cmd integration path
	ctx := t.Context()

	pool, dbURL, dbCleanup := testutil.SetupTimescaleDB(t)
	defer dbCleanup()

	redisAddr, redisCleanup := testutil.StartRedis(t, ctx)
	defer redisCleanup()

	sqsServer, sqsState := testutil.StartMockSQS(t)
	defer sqsServer.Close()

	const (
		chainID     = int64(1)
		blockNumber = int64(24033627)
		version     = 0
	)

	trackedToken := loadSeededSparkLendReceiptToken(t, ctx, pool, "WETH")

	fromAddress := common.HexToAddress("0x00000000000000000000000000000000000000F1")
	toAddress := common.HexToAddress("0x00000000000000000000000000000000000000F2")
	txHash := common.HexToHash("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

	senderUserID := seedExistingSenderWithHistoricalCollateral(t, ctx, pool, fromAddress, trackedToken, blockNumber-1)

	cache, err := redisadapter.NewBlockCache(redisadapter.Config{
		Addr:      redisAddr,
		TTL:       time.Hour,
		KeyPrefix: "stl",
	}, testutil.DiscardLogger())
	if err != nil {
		t.Fatalf("create redis block cache: %v", err)
	}
	defer cache.Close()

	receiptsJSON, err := json.Marshal([]shared.TransactionReceipt{buildTrackedTransferReceipt(trackedToken.ReceiptTokenAddress, fromAddress, toAddress, txHash)})
	if err != nil {
		t.Fatalf("marshal receipts: %v", err)
	}
	if err := cache.SetReceipts(ctx, chainID, blockNumber, version, receiptsJSON); err != nil {
		t.Fatalf("seed receipts in cache: %v", err)
	}

	blockEventJSON, err := json.Marshal(outbound.BlockEvent{
		ChainID:        chainID,
		BlockNumber:    blockNumber,
		Version:        version,
		BlockHash:      "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		BlockTimestamp: 1_700_000_000,
	})
	if err != nil {
		t.Fatalf("marshal block event: %v", err)
	}
	sqsState.AddMessage(string(blockEventJSON))

	rpcServer := buildTransferDiscoveryMockRPC(t)
	defer rpcServer.Close()

	setTransferDiscoveryEnv(t, dbURL, redisAddr, rpcServer.URL, sqsServer.URL)

	runCtx, cancel := context.WithCancel(t.Context())
	defer cancel()

	errCh := startTransferDiscoveryWorker(runCtx)

	select {
	case <-sqsState.FirstCallReceived:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for transfer discovery worker to start polling SQS")
	}

	newUserID := waitForRecipientSnapshot(t, ctx, pool, chainID, toAddress, trackedToken, blockNumber, version)

	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run() returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run() did not return after context cancellation")
	}

	if newUserID == 0 {
		t.Fatal("expected new user row for transfer recipient")
	}

	recipientCollateralCount := countTransferSnapshots(t, ctx, pool, newUserID, trackedToken, blockNumber, version)
	if recipientCollateralCount == 0 {
		t.Fatal("expected borrower_collateral rows for newly discovered recipient user")
	}

	senderTransferSnapshots := countTransferEventSnapshotsForUser(t, ctx, pool, senderUserID, blockNumber, version)
	if senderTransferSnapshots != 0 {
		t.Fatalf("expected existing sender user not to be re-snapshotted, got %d transfer snapshots", senderTransferSnapshots)
	}

	if deletes := sqsState.Deletes(); deletes < 1 {
		t.Fatalf("expected at least 1 DeleteMessage call after successful block processing, got %d", deletes)
	}
}

func setTransferDiscoveryEnv(t *testing.T, dbURL, redisAddr, rpcURL, sqsURL string) {
	t.Helper()

	t.Setenv("QUEUE_URL", "http://localhost/test-queue")
	t.Setenv("DATABASE_URL", dbURL)
	t.Setenv("REDIS_ADDR", redisAddr)
	t.Setenv("REDIS_PASSWORD", "")
	t.Setenv("ALCHEMY_WS_URL", rpcURL)
	t.Setenv("S3_BUCKET", "stl-sentinelstaging-ethereum-raw-test")
	t.Setenv("DEPLOY_ENV", "staging")
	t.Setenv("CHAIN_ID", "1")
	t.Setenv("MAX_MESSAGES", "1")
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_SQS_ENDPOINT", sqsURL)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
}

func startTransferDiscoveryWorker(ctx context.Context) <-chan error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, nil)
	}()
	return errCh
}

func waitForRecipientSnapshot(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	chainID int64,
	address common.Address,
	trackedToken seededSparkReceiptToken,
	blockNumber int64,
	version int,
) int64 {
	t.Helper()

	var userID int64
	testutil.WaitForCondition(t, 30*time.Second, func() bool {
		foundUserID, found := lookupUserID(t, ctx, pool, chainID, address)
		if !found {
			return false
		}

		userID = foundUserID
		return countTransferSnapshots(t, ctx, pool, userID, trackedToken, blockNumber, version) > 0
	}, "new recipient user snapshot to be persisted")

	return userID
}

func lookupUserID(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, address common.Address) (int64, bool) {
	t.Helper()

	var userID int64
	if err := pool.QueryRow(ctx,
		`SELECT id FROM "user" WHERE chain_id = $1 AND address = $2`,
		chainID,
		address.Bytes(),
	).Scan(&userID); err != nil {
		return 0, false
	}

	return userID, true
}

func countTransferSnapshots(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	userID int64,
	trackedToken seededSparkReceiptToken,
	blockNumber int64,
	version int,
) int {
	t.Helper()

	var count int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*)
		 FROM borrower_collateral
		 WHERE user_id = $1 AND protocol_id = $2 AND token_id = $3 AND block_number = $4 AND block_version = $5 AND event_type = 'Transfer'`,
		userID,
		trackedToken.ProtocolID,
		trackedToken.UnderlyingTokenID,
		blockNumber,
		version,
	).Scan(&count); err != nil {
		t.Fatalf("count transfer borrower_collateral rows: %v", err)
	}

	return count
}

func countTransferEventSnapshotsForUser(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	userID int64,
	blockNumber int64,
	version int,
) int {
	t.Helper()

	var count int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*)
		 FROM borrower_collateral
		 WHERE user_id = $1 AND block_number = $2 AND block_version = $3 AND event_type = 'Transfer'`,
		userID,
		blockNumber,
		version,
	).Scan(&count); err != nil {
		t.Fatalf("count sender transfer snapshots: %v", err)
	}

	return count
}

type seededSparkReceiptToken struct {
	ProtocolID          int64
	UnderlyingTokenID   int64
	ProtocolAddress     common.Address
	ReceiptTokenAddress common.Address
}

func loadSeededSparkLendReceiptToken(t *testing.T, ctx context.Context, pool *pgxpool.Pool, underlyingSymbol string) seededSparkReceiptToken {
	t.Helper()

	var protocolID int64
	var underlyingTokenID int64
	var protocolAddress []byte
	var receiptTokenAddress []byte

	err := pool.QueryRow(ctx, `
		SELECT
			rt.protocol_id,
			rt.underlying_token_id,
			p.address,
			rt.receipt_token_address
		FROM receipt_token rt
		JOIN protocol p ON p.id = rt.protocol_id
		JOIN token tk ON tk.id = rt.underlying_token_id
		WHERE p.chain_id = 1
		  AND p.name = 'SparkLend'
		  AND tk.symbol = $1
		ORDER BY rt.id
		LIMIT 1
	`, underlyingSymbol).Scan(&protocolID, &underlyingTokenID, &protocolAddress, &receiptTokenAddress)
	if err != nil {
		t.Fatalf("expected seeded SparkLend receipt token for %s from step 7 migration: %v", underlyingSymbol, err)
	}

	return seededSparkReceiptToken{
		ProtocolID:          protocolID,
		UnderlyingTokenID:   underlyingTokenID,
		ProtocolAddress:     common.BytesToAddress(protocolAddress),
		ReceiptTokenAddress: common.BytesToAddress(receiptTokenAddress),
	}
}

func seedExistingSenderWithHistoricalCollateral(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sender common.Address, trackedToken seededSparkReceiptToken, historicalBlock int64) int64 {
	t.Helper()

	var senderUserID int64
	if err := pool.QueryRow(ctx,
		`INSERT INTO "user" (chain_id, address, first_seen_block, created_at, updated_at)
		 VALUES ($1, $2, $3, NOW(), NOW())
		 RETURNING id`,
		1,
		sender.Bytes(),
		historicalBlock,
	).Scan(&senderUserID); err != nil {
		t.Fatalf("insert existing sender user: %v", err)
	}

	if _, err := pool.Exec(ctx,
		`INSERT INTO borrower_collateral (
			user_id, protocol_id, token_id, block_number, block_version,
			amount, change, event_type, tx_hash, collateral_enabled, created_at
		 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())`,
		senderUserID,
		trackedToken.ProtocolID,
		trackedToken.UnderlyingTokenID,
		historicalBlock,
		0,
		"1.0",
		"0",
		"Borrow",
		common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111").Bytes(),
		true,
	); err != nil {
		t.Fatalf("insert existing sender collateral snapshot: %v", err)
	}

	return senderUserID
}

func buildTrackedTransferReceipt(tokenAddress common.Address, from common.Address, to common.Address, txHash common.Hash) shared.TransactionReceipt {
	amount := big.NewInt(1_000_000_000_000_000_000)

	return shared.TransactionReceipt{
		TransactionHash: txHash.Hex(),
		Logs: []shared.Log{{
			Address:         tokenAddress.Hex(),
			Topics:          []string{erc20TransferTopic.Hex(), topicAddressHash(from), topicAddressHash(to)},
			Data:            uint256Hex(amount),
			LogIndex:        "0x0",
			TransactionHash: txHash.Hex(),
		}},
	}
}

func topicAddressHash(address common.Address) string {
	return common.BytesToHash(common.LeftPadBytes(address.Bytes(), 32)).Hex()
}

func uint256Hex(value *big.Int) string {
	return fmt.Sprintf("0x%064x", value)
}

func buildTransferDiscoveryMockRPC(t *testing.T) *httptest.Server {
	t.Helper()

	const (
		uiPoolDataProvider = "0x56b7a1012765c285afac8b8f25c69bf10ccfe978"
		poolDataProvider   = "0xfc21d6d146e6086b8359705c8b28512a983db0cb"
		multicall3Address  = "0xca11bde05977b3631167028862be2a173976ca11"
		wethAddress        = "0xc02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
	)

	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("load ERC20 ABI: %v", err)
	}
	multicallABI, err := abis.GetMulticall3ABI()
	if err != nil {
		t.Fatalf("load Multicall3 ABI: %v", err)
	}
	sparklendABI, err := abis.GetSparklendUserReservesDataABI()
	if err != nil {
		t.Fatalf("load SparkLend ABI: %v", err)
	}
	userReserveDataABI, err := abis.GetPoolDataProviderUserReserveDataABI()
	if err != nil {
		t.Fatalf("load PoolDataProvider ABI: %v", err)
	}

	type multicallResult struct {
		Success    bool
		ReturnData []byte
	}

	oneEther := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	zero := big.NewInt(0)
	weth := common.HexToAddress(wethAddress)

	getUserReservesDataResult, err := sparklendABI.Methods["getUserReservesData"].Outputs.Pack(
		[]struct {
			UnderlyingAsset                common.Address
			ScaledATokenBalance            *big.Int
			UsageAsCollateralEnabledOnUser bool
			ScaledVariableDebt             *big.Int
		}{
			{
				UnderlyingAsset:                weth,
				ScaledATokenBalance:            oneEther,
				UsageAsCollateralEnabledOnUser: true,
				ScaledVariableDebt:             zero,
			},
		},
		uint8(0),
	)
	if err != nil {
		t.Fatalf("pack getUserReservesData response: %v", err)
	}

	wethDecimals, err := erc20ABI.Methods["decimals"].Outputs.Pack(uint8(18))
	if err != nil {
		t.Fatalf("pack decimals response: %v", err)
	}
	wethSymbol, err := erc20ABI.Methods["symbol"].Outputs.Pack("WETH")
	if err != nil {
		t.Fatalf("pack symbol response: %v", err)
	}
	wethName, err := erc20ABI.Methods["name"].Outputs.Pack("Wrapped Ether")
	if err != nil {
		t.Fatalf("pack name response: %v", err)
	}
	wethMetadataAggregate, err := multicallABI.Methods["aggregate3"].Outputs.Pack([]multicallResult{
		{Success: true, ReturnData: wethDecimals},
		{Success: true, ReturnData: wethSymbol},
		{Success: true, ReturnData: wethName},
	})
	if err != nil {
		t.Fatalf("pack metadata aggregate response: %v", err)
	}

	getUserReserveDataResult, err := userReserveDataABI.Methods["getUserReserveData"].Outputs.Pack(
		oneEther,
		zero,
		zero,
		zero,
		zero,
		zero,
		zero,
		zero,
		true,
	)
	if err != nil {
		t.Fatalf("pack getUserReserveData response: %v", err)
	}

	reserveDataByAsset := map[common.Address][]byte{
		weth: getUserReserveDataResult,
	}

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

		target := extractCallTarget(req.Params)
		switch strings.ToLower(target) {
		case uiPoolDataProvider:
			resultJSON, _ := json.Marshal("0x" + hex.EncodeToString(getUserReservesDataResult))
			testutil.WriteRPCResult(w, req.ID, json.RawMessage(resultJSON))
		case multicall3Address:
			firstTarget := extractFirstAggregate3CallTarget(req.Params)
			switch firstTarget {
			case poolDataProvider:
				calls := extractAggregate3Calls(req.Params)
				results := make([]multicallResult, 0, len(calls))
				for _, call := range calls {
					asset := extractAssetFromGetUserReserveDataCalldata(call.CallData)
					data, ok := reserveDataByAsset[asset]
					if !ok {
						results = append(results, multicallResult{Success: false})
						continue
					}
					results = append(results, multicallResult{Success: true, ReturnData: data})
				}
				aggOut, err := multicallABI.Methods["aggregate3"].Outputs.Pack(results)
				if err != nil {
					testutil.WriteRPCError(w, req.ID, -32000, "pack error: "+err.Error())
					return
				}
				resultJSON, _ := json.Marshal("0x" + hex.EncodeToString(aggOut))
				testutil.WriteRPCResult(w, req.ID, json.RawMessage(resultJSON))
			default:
				resultJSON, _ := json.Marshal("0x" + hex.EncodeToString(wethMetadataAggregate))
				testutil.WriteRPCResult(w, req.ID, json.RawMessage(resultJSON))
			}
		default:
			testutil.WriteRPCResult(w, req.ID, json.RawMessage(`"0x"`))
		}
	}))
}

func extractCallTarget(params json.RawMessage) string {
	var payload []json.RawMessage
	if err := json.Unmarshal(params, &payload); err != nil || len(payload) < 1 {
		return ""
	}

	var callObject map[string]string
	if err := json.Unmarshal(payload[0], &callObject); err != nil {
		return ""
	}

	return strings.ToLower(callObject["to"])
}

func extractFirstAggregate3CallTarget(params json.RawMessage) string {
	calls := extractAggregate3Calls(params)
	if len(calls) == 0 {
		return ""
	}
	return strings.ToLower(calls[0].Target.Hex())
}

func extractAggregate3Calls(params json.RawMessage) []struct {
	Target       common.Address `json:"target"`
	AllowFailure bool           `json:"allowFailure"`
	CallData     []byte         `json:"callData"`
} {
	var payload []json.RawMessage
	if err := json.Unmarshal(params, &payload); err != nil || len(payload) < 1 {
		return nil
	}

	var callObject map[string]string
	if err := json.Unmarshal(payload[0], &callObject); err != nil {
		return nil
	}

	rawField := callObject["input"]
	if rawField == "" {
		rawField = callObject["data"]
	}

	raw, err := hex.DecodeString(strings.TrimPrefix(rawField, "0x"))
	if err != nil || len(raw) < 4 {
		return nil
	}

	multicallABI, err := abis.GetMulticall3ABI()
	if err != nil {
		return nil
	}

	unpacked, err := multicallABI.Methods["aggregate3"].Inputs.Unpack(raw[4:])
	if err != nil || len(unpacked) == 0 {
		return nil
	}

	calls, ok := unpacked[0].([]struct {
		Target       common.Address `json:"target"`
		AllowFailure bool           `json:"allowFailure"`
		CallData     []byte         `json:"callData"`
	})
	if !ok {
		return nil
	}

	return calls
}

func extractAssetFromGetUserReserveDataCalldata(callData []byte) common.Address {
	if len(callData) < 4+32 {
		return common.Address{}
	}
	return common.BytesToAddress(callData[4 : 4+32])
}

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

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var sharedDSN string

func TestMain(m *testing.M) {
	dsn, cleanup := testutil.StartTimescaleDBForMain()
	sharedDSN = dsn

	code := m.Run()

	cleanup()
	code = testutil.CheckGoroutineLeaks(code)
	os.Exit(code)
}

// ---------------------------------------------------------------------------
// Base-chain fixture (CHAIN_ID=8453)
// ---------------------------------------------------------------------------

const testChainID = 8453

var (
	psm3Addr         = common.HexToAddress("0x1601843c5E9bC251A3272907010AFa41Fa18347E")
	usdsAddr         = common.HexToAddress("0x820C137fa70C8691f0e44Dc420a5e53c168921Dc")
	susdsAddr        = common.HexToAddress("0x5875eEE11Cf8398102FdAd704C9E96607675467a")
	usdcAddr         = common.HexToAddress("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913")
	rateProviderAddr = common.HexToAddress("0x2722C8f8A5F880401Fa5b01eD548d657F5Cd6175")

	usdsBalance    = big.NewInt(1_000_000_000_000_000_000)
	susdsBalance   = big.NewInt(2_000_000_000_000_000_000)
	usdcBalance    = big.NewInt(3_000_000)
	totalAssetsVal = big.NewInt(6_000_000_000_000_000_000)
	conversionRate = new(big.Int).Exp(big.NewInt(10), big.NewInt(27), nil)
)

func addressWord(addr common.Address) []byte {
	return common.LeftPadBytes(addr.Bytes(), 32)
}

func uintWord(v *big.Int) []byte {
	return common.BigToHash(v).Bytes()
}

// psm3Dispatcher answers PSM3, ERC-20 balanceOf and rate provider sub-calls by
// 4-byte selector. usdsMismatch makes usds() return the wrong address so the
// startup cross-check fails hard.
func psm3Dispatcher(t *testing.T, usdsMismatch bool) testutil.SubcallDispatcher {
	t.Helper()

	psm3ABI, err := abis.GetPSM3ABI()
	if err != nil {
		t.Fatalf("psm3 abi: %v", err)
	}
	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("erc20 abi: %v", err)
	}
	rateABI, err := abis.GetRateProviderABI()
	if err != nil {
		t.Fatalf("rate provider abi: %v", err)
	}

	selector := func(parsed *abi.ABI, method string) string {
		return hex.EncodeToString(parsed.Methods[method].ID)
	}

	balances := map[common.Address]*big.Int{
		usdsAddr:  usdsBalance,
		susdsAddr: susdsBalance,
		usdcAddr:  usdcBalance,
	}

	return func(target common.Address, callData []byte) ([]byte, bool) {
		if len(callData) < 4 {
			return nil, false
		}
		switch hex.EncodeToString(callData[:4]) {
		case selector(psm3ABI, "pocket"):
			return addressWord(psm3Addr), true // pocket == PSM3, as on-chain today
		case selector(psm3ABI, "rateProvider"):
			return addressWord(rateProviderAddr), true
		case selector(psm3ABI, "usds"):
			if usdsMismatch {
				return addressWord(common.HexToAddress("0xdead")), true
			}
			return addressWord(usdsAddr), true
		case selector(psm3ABI, "susds"):
			return addressWord(susdsAddr), true
		case selector(psm3ABI, "usdc"):
			return addressWord(usdcAddr), true
		case selector(psm3ABI, "totalAssets"):
			return uintWord(totalAssetsVal), true
		case selector(rateABI, "getConversionRate"):
			return uintWord(conversionRate), true
		case selector(erc20ABI, "balanceOf"):
			if bal, ok := balances[target]; ok {
				return uintWord(bal), true
			}
			return nil, false
		default:
			return nil, false
		}
	}
}

const mockBlockNumber = "0x1DCD6500" // 500000000

// mockPSM3RPC serves eth_blockNumber and multicall3 aggregate3 eth_calls.
func mockPSM3RPC(t *testing.T, dispatch testutil.SubcallDispatcher) *httptest.Server {
	t.Helper()

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string            `json:"method"`
			ID     json.RawMessage   `json:"id"`
			Params []json.RawMessage `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		switch req.Method {
		case "eth_blockNumber":
			testutil.WriteRPCResult(w, req.ID, json.RawMessage(`"`+mockBlockNumber+`"`))
			return
		case "eth_chainId":
			testutil.WriteRPCResult(w, req.ID, json.RawMessage(`"0x2105"`)) // 8453
			return
		case "eth_call":
			// handled below
		default:
			testutil.WriteRPCResult(w, req.ID, json.RawMessage(`"0x1"`))
			return
		}

		var msg struct {
			Data  string `json:"data"`
			Input string `json:"input"`
		}
		if len(req.Params) == 0 {
			http.Error(w, "missing params", http.StatusBadRequest)
			return
		}
		if err := json.Unmarshal(req.Params[0], &msg); err != nil {
			http.Error(w, "bad param unmarshal", http.StatusBadRequest)
			return
		}
		calldata := msg.Input
		if calldata == "" {
			calldata = msg.Data
		}

		calldataBytes, err := hex.DecodeString(strings.TrimPrefix(calldata, "0x"))
		if err != nil {
			http.Error(w, "bad calldata hex", http.StatusBadRequest)
			return
		}

		result, err := testutil.HandleMulticall3(calldataBytes, dispatch)
		if err != nil {
			t.Logf("handleMulticall3 error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		testutil.WriteRPCResult(w, req.ID, json.RawMessage(`"`+result+`"`))
	}))
}

// enqueueBlockEvents sends block events to the mock SQS server.
func enqueueBlockEvents(t *testing.T, sqsState *testutil.MockSQSServer, startBlock int64, count int, version int) {
	t.Helper()
	for i := 0; i < count; i++ {
		event := outbound.BlockEvent{
			ChainID:        testChainID,
			BlockNumber:    startBlock + int64(i),
			Version:        version,
			BlockHash:      fmt.Sprintf("0x%064x", startBlock+int64(i)),
			ParentHash:     fmt.Sprintf("0x%064x", startBlock+int64(i)-1),
			BlockTimestamp: 1700000000 + startBlock + int64(i),
			ReceivedAt:     time.Now(),
		}
		body, err := json.Marshal(event)
		if err != nil {
			t.Fatalf("marshal block event: %v", err)
		}
		sqsState.AddMessage(string(body))
	}
}

func setCommonEnv(t *testing.T, rpcURL, sqsURL string) {
	t.Helper()
	t.Setenv("BUILD_GIT_HASH", "test")
	t.Setenv("ETH_RPC_URL", rpcURL)
	t.Setenv("AWS_SQS_ENDPOINT", sqsURL)
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("CHAIN_ID", fmt.Sprint(testChainID))
	t.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "1000")
}

// ---------------------------------------------------------------------------
// Integration tests
// ---------------------------------------------------------------------------

func TestRunIntegration_BadConnectionConfig(t *testing.T) {
	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer rpcServer.Close()

	sqsServer, _ := testutil.StartMockSQS(t)
	defer sqsServer.Close()

	setCommonEnv(t, rpcServer.URL, sqsServer.URL)

	err := run(context.Background(), []string{
		"-db", "postgres://invalid:invalid@localhost:1/nonexistent?connect_timeout=1",
		"-queue", "http://localhost/test-queue",
	})
	if err == nil {
		t.Fatal("expected error for bad connection config")
	}
	if !strings.Contains(err.Error(), "database") && !strings.Contains(err.Error(), "connect") {
		t.Errorf("expected database connection error, got: %v", err)
	}
}

func TestRunIntegration_UnknownChainID(t *testing.T) {
	_, dbURL, dbCleanup := testutil.SetupTestSchema(t, sharedDSN)
	defer dbCleanup()

	sqsServer, _ := testutil.StartMockSQS(t)
	defer sqsServer.Close()

	setCommonEnv(t, "http://localhost:1", sqsServer.URL)
	t.Setenv("CHAIN_ID", "1") // mainnet has no PSM3 deployment

	err := run(context.Background(), []string{
		"-db", dbURL,
		"-queue", sqsServer.URL + "/queue/test",
	})
	if err == nil {
		t.Fatal("expected error for chain without a PSM3 deployment")
	}
	if !strings.Contains(err.Error(), "no PSM3 deployment") {
		t.Errorf("expected 'no PSM3 deployment' error, got: %v", err)
	}
}

func TestRunIntegration_ImmutableMismatch(t *testing.T) {
	_, dbURL, dbCleanup := testutil.SetupTestSchema(t, sharedDSN)
	defer dbCleanup()

	rpcServer := mockPSM3RPC(t, psm3Dispatcher(t, true)) // usds() disagrees with config
	defer rpcServer.Close()

	sqsServer, _ := testutil.StartMockSQS(t)
	defer sqsServer.Close()

	setCommonEnv(t, rpcServer.URL, sqsServer.URL)

	err := run(context.Background(), []string{
		"-db", dbURL,
		"-queue", sqsServer.URL + "/queue/test",
	})
	if err == nil {
		t.Fatal("expected error when on-chain usds() mismatches config")
	}
	if !strings.Contains(err.Error(), "usds()") {
		t.Errorf("expected usds mismatch error, got: %v", err)
	}
}

func TestRunIntegration_StartupAndShutdown(t *testing.T) {
	ctx := context.Background()

	pool, dbURL, dbCleanup := testutil.SetupTestSchema(t, sharedDSN)
	defer dbCleanup()

	rpcServer := mockPSM3RPC(t, psm3Dispatcher(t, false))
	defer rpcServer.Close()

	sqsServer, sqsState := testutil.StartMockSQS(t)
	defer sqsServer.Close()

	const startBlock = 31000000
	const blockVersion = 1
	enqueueBlockEvents(t, sqsState, startBlock, 5, blockVersion)

	setCommonEnv(t, rpcServer.URL, sqsServer.URL)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(runCtx, []string{
			"-db", dbURL,
			"-queue", sqsServer.URL + "/queue/test",
			"-sweep-blocks", "1",
		})
	}()

	deadline := time.After(15 * time.Second)
	for {
		var count int
		err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM psm3_reserves`).Scan(&count)
		if err == nil && count >= 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for psm3 snapshot to be written")
		case <-time.After(100 * time.Millisecond):
		}
	}

	var (
		addrBytes                              []byte
		usds, susds, usdc, total, rate, source string
		blockNumber                            int64
		blockVer                               int
	)
	err := pool.QueryRow(ctx, `
		SELECT address, usds_balance::text, susds_balance::text, usdc_balance::text,
		       total_assets::text, conversion_rate::text, block_number, block_version, source
		FROM psm3_reserves ORDER BY block_number LIMIT 1
	`).Scan(&addrBytes, &usds, &susds, &usdc, &total, &rate, &blockNumber, &blockVer, &source)
	if err != nil {
		t.Fatalf("query snapshot: %v", err)
	}

	if got := common.BytesToAddress(addrBytes); got != psm3Addr {
		t.Errorf("address = %s, want %s", got.Hex(), psm3Addr.Hex())
	}
	if usds != usdsBalance.String() {
		t.Errorf("usds_balance = %s, want %s", usds, usdsBalance)
	}
	if susds != susdsBalance.String() {
		t.Errorf("susds_balance = %s, want %s", susds, susdsBalance)
	}
	if usdc != usdcBalance.String() {
		t.Errorf("usdc_balance = %s, want %s", usdc, usdcBalance)
	}
	if total != totalAssetsVal.String() {
		t.Errorf("total_assets = %s, want %s", total, totalAssetsVal)
	}
	if rate != conversionRate.String() {
		t.Errorf("conversion_rate = %s, want %s", rate, conversionRate)
	}
	if blockNumber != startBlock {
		t.Errorf("block_number = %d, want %d", blockNumber, startBlock)
	}
	if blockVer != blockVersion {
		t.Errorf("block_version = %d, want %d (reorg re-emission must flow through)", blockVer, blockVersion)
	}
	if source != "sweep" {
		t.Errorf("source = %q, want %q", source, "sweep")
	}

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

func TestRunIntegration_SnapshotAccumulation(t *testing.T) {
	ctx := context.Background()

	pool, dbURL, dbCleanup := testutil.SetupTestSchema(t, sharedDSN)
	defer dbCleanup()

	rpcServer := mockPSM3RPC(t, psm3Dispatcher(t, false))
	defer rpcServer.Close()

	sqsServer, sqsState := testutil.StartMockSQS(t)
	defer sqsServer.Close()

	// Three distinct blocks plus an exact duplicate of the first (SQS is
	// at-least-once): the duplicate must dedupe via the processing-version
	// trigger + ON CONFLICT DO NOTHING, leaving exactly wantRows rows.
	const wantRows = 3
	enqueueBlockEvents(t, sqsState, 31000000, 1, 0)
	enqueueBlockEvents(t, sqsState, 31000000, 1, 0) // duplicate delivery
	enqueueBlockEvents(t, sqsState, 31000001, wantRows-1, 0)

	setCommonEnv(t, rpcServer.URL, sqsServer.URL)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(runCtx, []string{
			"-db", dbURL,
			"-queue", sqsServer.URL + "/queue/test",
			"-sweep-blocks", "1",
		})
	}()

	deadline := time.After(15 * time.Second)
	for {
		var count int
		err := pool.QueryRow(ctx, `SELECT COUNT(DISTINCT block_number) FROM psm3_reserves`).Scan(&count)
		if err == nil && count >= wantRows {
			break
		}
		select {
		case <-deadline:
			var got int
			if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM psm3_reserves`).Scan(&got); err != nil {
				t.Fatalf("timed out and failed to query count: %v", err)
			}
			t.Fatalf("timed out: want %d rows, have %d", wantRows, got)
		case <-time.After(100 * time.Millisecond):
		}
	}

	// Events are processed in order, so once the last distinct block landed
	// the earlier duplicate has been handled too — it must not add a row.
	var total int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM psm3_reserves`).Scan(&total); err != nil {
		t.Fatalf("query total rows: %v", err)
	}
	if total != wantRows {
		t.Errorf("total rows = %d, want %d (duplicate delivery must dedupe)", total, wantRows)
	}

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

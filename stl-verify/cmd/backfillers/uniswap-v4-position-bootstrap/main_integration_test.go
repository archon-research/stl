//go:build integration

package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/pkg/rpcutil"
	"github.com/archon-research/stl/stl-verify/internal/services/uniswapv4indexer"
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

const (
	// The seeded mainnet ETH/wstETH 0.01% pool and its StateView periphery.
	seededPoolIDHash = "0x1d5b2949ece8754c2d736991c62c5162bd144f497b2212182401b9bae77e2d76"
	stateViewAddr    = "0x7fFE42C4a5DEeA5b0feC41C94C136Cf115597227"
	poolManagerAddr  = "0x000000000004444c5dc75cB358380D2e3dE08A90"
	positionOwner    = "0xbD216513d74C8cf14cf4747E6AaA6420FF64ee9e"
	positionSalt     = "0x000000000000000000000000000000000000000000000000000000000000c8df"
	txHash           = "0xfeed000000000000000000000000000000000000000000000000000000000001"
	pinnedBlockHash  = "0x2222222222222222222222222222222222222222222222222222222222222222"

	// Above every seeded pool's deploy block, so the whole registry is in range.
	pinnedBlock = int64(25_600_000)
	// The one position the mock chain reports liquidity for.
	positionLiquidity = int64(123_456)
)

// mockChain answers the five JSON-RPC methods the bootstrap issues, over one
// pool with one historical ModifyLiquidity log.
type mockChain struct {
	t *testing.T
	// getLogsRefusals makes that many leading eth_getLogs answers a range
	// refusal, so a test can drive the adaptive bisect end to end.
	getLogsRefusals int
	getLogsCalls    int
	// chainID is what eth_chainId reports; empty makes it fail outright.
	chainID string
	// getLogsFatal makes every eth_getLogs answer an error the bisect cannot
	// recover from.
	getLogsFatal bool
}

// mockChainOptions varies the failure the mock injects; the zero value is the
// healthy mainnet chain every happy-path test drives.
type mockChainOptions struct {
	refusals     int
	chainID      string
	getLogsFatal bool
}

func startMockChain(t *testing.T, opts mockChainOptions) *httptest.Server {
	t.Helper()
	chainID := opts.chainID
	if chainID == "" && !opts.chainIDFails() {
		chainID = "0x1"
	}
	chain := &mockChain{t: t, getLogsRefusals: opts.refusals, chainID: chainID, getLogsFatal: opts.getLogsFatal}
	server := httptest.NewServer(http.HandlerFunc(chain.serve))
	t.Cleanup(server.Close)
	return server
}

// chainIDFails reports whether the caller asked for an eth_chainId failure,
// which the sentinel "fail" spells so the zero value stays healthy.
func (o mockChainOptions) chainIDFails() bool { return o.chainID == chainIDFailSentinel }

const chainIDFailSentinel = "fail"

func (c *mockChain) serve(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		c.t.Errorf("reading request: %v", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")

	var req rpcutil.Request
	if err := json.Unmarshal(body, &req); err != nil {
		testutil.WriteRPCError(w, json.RawMessage(`1`), -32700, "parse error")
		return
	}

	switch req.Method {
	case "eth_chainId":
		if c.chainID == "" {
			testutil.WriteRPCError(w, req.ID, -32000, "chain id unavailable")
			return
		}
		writeJSONResult(c.t, w, req.ID, c.chainID)
	case "eth_blockNumber":
		writeJSONResult(c.t, w, req.ID, "0x"+strconv.FormatInt(pinnedBlock+64, 16))
	case "eth_getBlockByNumber":
		c.serveHeader(w, req)
	case "eth_getLogs":
		c.serveLogs(w, req)
	case "eth_call":
		c.serveCall(w, req)
	default:
		testutil.WriteRPCError(w, req.ID, -32601, "method not found: "+req.Method)
	}
}

func (c *mockChain) serveHeader(w http.ResponseWriter, req rpcutil.Request) {
	var params []any
	if err := json.Unmarshal(req.Params, &params); err != nil || len(params) == 0 {
		testutil.WriteRPCError(w, req.ID, -32602, "bad eth_getBlockByNumber params")
		return
	}
	header := map[string]string{
		"number":    fmt.Sprint(params[0]),
		"hash":      pinnedBlockHash,
		"timestamp": "0x68a3f900",
	}
	raw, err := json.Marshal(header)
	if err != nil {
		c.t.Fatalf("marshalling header: %v", err)
	}
	testutil.WriteRPCResult(w, req.ID, raw)
}

// serveLogs refuses the configured number of leading queries with a
// response-size error, then answers with the pool's one historical log.
func (c *mockChain) serveLogs(w http.ResponseWriter, req rpcutil.Request) {
	c.getLogsCalls++
	if c.getLogsFatal {
		testutil.WriteRPCError(w, req.ID, -32000, "archive node unavailable")
		return
	}
	if c.getLogsCalls <= c.getLogsRefusals {
		testutil.WriteRPCError(w, req.ID, -32602, "Log response size exceeded. this block range should work: [0x0, 0x1]")
		return
	}
	raw, err := json.Marshal([]map[string]any{modifyLiquidityLogJSON(c.t)})
	if err != nil {
		c.t.Fatalf("marshalling logs: %v", err)
	}
	testutil.WriteRPCResult(w, req.ID, raw)
}

// serveCall answers the aggregate3 batch, giving every getPositionInfo sub-call
// the same liquidity and zero fee-growth checkpoints.
func (c *mockChain) serveCall(w http.ResponseWriter, req rpcutil.Request) {
	var params []json.RawMessage
	if err := json.Unmarshal(req.Params, &params); err != nil || len(params) == 0 {
		testutil.WriteRPCError(w, req.ID, -32602, "bad eth_call params")
		return
	}
	// go-ethereum sends the calldata as "input"; the raw JSON-RPC spec and older
	// clients use "data".
	var callObj struct {
		Input string `json:"input"`
		Data  string `json:"data"`
	}
	if err := json.Unmarshal(params[0], &callObj); err != nil {
		testutil.WriteRPCError(w, req.ID, -32602, "bad eth_call object")
		return
	}
	encoded := callObj.Input
	if encoded == "" {
		encoded = callObj.Data
	}
	calldata, err := hex.DecodeString(strings.TrimPrefix(encoded, "0x"))
	if err != nil {
		testutil.WriteRPCError(w, req.ID, -32602, "bad eth_call data")
		return
	}

	result, err := testutil.HandleMulticall3(calldata, func(target common.Address, _ []byte) ([]byte, bool) {
		if target != common.HexToAddress(stateViewAddr) {
			return nil, false
		}
		return packPositionInfoReturn(c.t, big.NewInt(positionLiquidity)), true
	})
	if err != nil {
		testutil.WriteRPCError(w, req.ID, -32000, err.Error())
		return
	}
	writeJSONResult(c.t, w, req.ID, result)
}

func writeJSONResult(t *testing.T, w http.ResponseWriter, id json.RawMessage, value string) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshalling result: %v", err)
	}
	testutil.WriteRPCResult(w, id, raw)
}

// modifyLiquidityLogJSON builds the one historical ModifyLiquidity log the mock
// chain returns, in eth_getLogs' wire shape.
func modifyLiquidityLogJSON(t *testing.T) map[string]any {
	t.Helper()
	poolManagerABI, err := uniswapv4indexer.PoolManagerABI()
	if err != nil {
		t.Fatalf("PoolManagerABI: %v", err)
	}
	ev := poolManagerABI.Events["ModifyLiquidity"]

	var nonIndexed abi.Arguments
	for _, arg := range ev.Inputs {
		if !arg.Indexed {
			nonIndexed = append(nonIndexed, arg)
		}
	}
	data, err := nonIndexed.Pack(big.NewInt(-100), big.NewInt(200), big.NewInt(1000), common.HexToHash(positionSalt))
	if err != nil {
		t.Fatalf("packing ModifyLiquidity data: %v", err)
	}

	return map[string]any{
		"address": poolManagerAddr,
		"topics": []string{
			ev.ID.Hex(),
			common.HexToHash(seededPoolIDHash).Hex(),
			common.BytesToHash(common.HexToAddress(positionOwner).Bytes()).Hex(),
		},
		"data":             "0x" + hex.EncodeToString(data),
		"blockHash":        pinnedBlockHash,
		"blockNumber":      "0x14bd868",
		"transactionHash":  txHash,
		"transactionIndex": "0x0",
		"logIndex":         "0x0",
		"removed":          false,
	}
}

func packPositionInfoReturn(t *testing.T, liquidity *big.Int) []byte {
	t.Helper()
	const j = `[
		{"name":"getPositionInfo","type":"function","stateMutability":"view","inputs":[
			{"name":"poolId","type":"bytes32"},
			{"name":"owner","type":"address"},
			{"name":"tickLower","type":"int24"},
			{"name":"tickUpper","type":"int24"},
			{"name":"salt","type":"bytes32"}
		],"outputs":[
			{"name":"liquidity","type":"uint128"},
			{"name":"feeGrowthInside0LastX128","type":"uint256"},
			{"name":"feeGrowthInside1LastX128","type":"uint256"}
		]}
	]`
	a, err := abi.JSON(strings.NewReader(j))
	if err != nil {
		t.Fatalf("parsing the position view ABI: %v", err)
	}
	packed, err := a.Methods["getPositionInfo"].Outputs.Pack(liquidity, big.NewInt(0), big.NewInt(0))
	if err != nil {
		t.Fatalf("packing getPositionInfo return: %v", err)
	}
	return packed
}

// runArgs is the flag set every test drives run() with; the pin is fixed so the
// assertions do not depend on the mock's head.
func runArgs(dbURL, rpcURL string) []string {
	return []string{
		"-db", dbURL,
		"-rpc-url", rpcURL,
		"-chain-id", "1",
		"-pin", strconv.FormatInt(pinnedBlock, 10),
		"-from", "21743144",
		"-initial-window", "10000000",
		"-max-window", "10000000",
	}
}

func setupRun(t *testing.T, opts mockChainOptions) (*pgxpool.Pool, []string) {
	t.Helper()
	db, dbURL, cleanup := testutil.SetupTestDB(t, sharedDSN)
	t.Cleanup(cleanup)
	t.Setenv("BUILD_GIT_HASH", "test")
	server := startMockChain(t, opts)
	return db, runArgs(dbURL, server.URL)
}

// withChainID rewrites the -chain-id flag in place.
func withChainID(args []string, chainID string) []string {
	for i, arg := range args {
		if arg == "-chain-id" {
			args[i+1] = chainID
		}
	}
	return args
}

func countPositions(t *testing.T, db *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := db.QueryRow(context.Background(), `SELECT COUNT(*) FROM uniswap_v4_position`).Scan(&n); err != nil {
		t.Fatalf("counting positions: %v", err)
	}
	return n
}

func TestRunIntegration_PersistsTheDiscoveredPosition(t *testing.T) {
	db, args := setupRun(t, mockChainOptions{})

	if err := run(context.Background(), args); err != nil {
		t.Fatalf("run: %v", err)
	}

	if got := countPositions(t, db); got != 1 {
		t.Fatalf("uniswap_v4_position rows = %d, want 1", got)
	}
	var (
		owner       []byte
		blockNumber int64
		liquidity   string
	)
	if err := db.QueryRow(context.Background(),
		`SELECT owner, block_number, liquidity::text FROM uniswap_v4_position`).
		Scan(&owner, &blockNumber, &liquidity); err != nil {
		t.Fatalf("reading back the position: %v", err)
	}
	if common.BytesToAddress(owner) != common.HexToAddress(positionOwner) {
		t.Errorf("owner = %s, want %s", common.BytesToAddress(owner), positionOwner)
	}
	if blockNumber != pinnedBlock {
		t.Errorf("block_number = %d, want the pinned block %d", blockNumber, pinnedBlock)
	}
	if liquidity != strconv.FormatInt(positionLiquidity, 10) {
		t.Errorf("liquidity = %s, want %d", liquidity, positionLiquidity)
	}
}

func TestRunIntegration_RerunWritesNoNewRows(t *testing.T) {
	db, args := setupRun(t, mockChainOptions{})

	if err := run(context.Background(), args); err != nil {
		t.Fatalf("first run: %v", err)
	}
	first := countPositions(t, db)

	if err := run(context.Background(), args); err != nil {
		t.Fatalf("second run: %v", err)
	}

	if got := countPositions(t, db); got != first {
		t.Errorf("rows after the rerun = %d, want %d: the run must be idempotent", got, first)
	}
}

func TestRunIntegration_BisectsPastARangeRefusal(t *testing.T) {
	db, args := setupRun(t, mockChainOptions{refusals: 2})

	if err := run(context.Background(), args); err != nil {
		t.Fatalf("run: %v", err)
	}

	if got := countPositions(t, db); got != 1 {
		t.Errorf("uniswap_v4_position rows = %d, want 1: the scan must recover from the refusals", got)
	}
}

func TestRunIntegration_RejectsAMissingDatabaseURL(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("ALCHEMY_API_KEY", "key")

	err := run(context.Background(), []string{"-rpc-url", "http://127.0.0.1:1"})
	if err == nil {
		t.Fatal("expected an error for a missing database URL")
	}
	if !strings.Contains(err.Error(), "database URL") {
		t.Errorf("error = %v, want it to name the database URL", err)
	}
}

func TestRunIntegration_RejectsAnUnreachableDatabase(t *testing.T) {
	t.Setenv("BUILD_GIT_HASH", "test")
	server := startMockChain(t, mockChainOptions{})

	err := run(context.Background(), runArgs("postgres://invalid:invalid@127.0.0.1:1/nonexistent?connect_timeout=1", server.URL))
	if err == nil {
		t.Fatal("expected an error for an unreachable database")
	}
}

func TestRunIntegration_RejectsAChainIDMismatch(t *testing.T) {
	_, args := setupRun(t, mockChainOptions{})

	err := run(context.Background(), withChainID(args, "8453"))
	if err == nil {
		t.Fatal("expected an error: the endpoint serves another chain")
	}
	if !strings.Contains(err.Error(), "chain id mismatch") {
		t.Errorf("error = %v, want it to name the chain id mismatch", err)
	}
}

func TestRunIntegration_RejectsAnUnknownFlag(t *testing.T) {
	if err := run(context.Background(), []string{"-nope"}); err == nil {
		t.Fatal("expected an error for an unknown flag")
	}
}

func TestRunIntegration_RejectsAnUnreadableChainID(t *testing.T) {
	_, args := setupRun(t, mockChainOptions{chainID: chainIDFailSentinel})

	err := run(context.Background(), args)
	if err == nil {
		t.Fatal("expected an error: the endpoint would not report its chain id")
	}
	if !strings.Contains(err.Error(), "chain id") {
		t.Errorf("error = %v, want it to name the chain id read", err)
	}
}

func TestRunIntegration_RejectsAChainWithNoRegisteredPools(t *testing.T) {
	_, args := setupRun(t, mockChainOptions{chainID: "0x2105"})

	err := run(context.Background(), withChainID(args, "8453"))
	if err == nil {
		t.Fatal("expected an error: chain 8453 has no seeded uniswap v4 registry")
	}
	if !strings.Contains(err.Error(), "no uniswap v4 pools registered") {
		t.Errorf("error = %v, want it to name the empty registry", err)
	}
}

func TestRunIntegration_PropagatesAScanFailure(t *testing.T) {
	db, args := setupRun(t, mockChainOptions{getLogsFatal: true})

	err := run(context.Background(), args)
	if err == nil {
		t.Fatal("expected an error: every log query failed")
	}
	if got := countPositions(t, db); got != 0 {
		t.Errorf("uniswap_v4_position rows = %d, want 0: a failed scan must write nothing", got)
	}
}

func TestRunIntegration_RejectsAnUndialableRPCEndpoint(t *testing.T) {
	_, dbURL, cleanup := testutil.SetupTestDB(t, sharedDSN)
	t.Cleanup(cleanup)
	t.Setenv("BUILD_GIT_HASH", "test")

	err := run(context.Background(), []string{
		"-db", dbURL,
		"-rpc-url", "://not-a-url",
		"-chain-id", "1",
		"-pin", strconv.FormatInt(pinnedBlock, 10),
	})
	if err == nil {
		t.Fatal("expected an error for an undialable RPC endpoint")
	}
}

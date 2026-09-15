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
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/rpcutil"
	"github.com/archon-research/stl/stl-verify/internal/services/uniswapv4bootstrap"
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
	seededPoolIDHash = "0x1d5b2949ece8754c2d736991c62c5162bd144f497b2212182401b9bae77e2d76"
	stateViewAddr    = "0x7fFE42C4a5DEeA5b0feC41C94C136Cf115597227"
	poolManagerAddr  = "0x000000000004444c5dc75cB358380D2e3dE08A90"
	positionOwner    = "0xbD216513d74C8cf14cf4747E6AaA6420FF64ee9e"
	positionSalt     = "0x000000000000000000000000000000000000000000000000000000000000c8df"
	txHash           = "0xfeed000000000000000000000000000000000000000000000000000000000001"
	pinnedBlockHash  = "0x2222222222222222222222222222222222222222222222222222222222222222"

	// posmAddr is the PositionManager the seed migration registers on chain 1; it
	// is also every posm-managed position's owner, hence positionOwner above.
	posmAddr        = positionOwner
	transferFrom    = "0x0000000000000000000000000000000000000000"
	transferTo      = "0x1111111111111111111111111111111111111111"
	transferTokenID = int64(51423)
	// Inside the scan range and below the pin.
	transferBlock    = int64(21_800_000)
	transferLogIndex = int64(7)

	// Above every seeded pool's deploy block, so the whole registry is in range.
	// The mock's head sits the default finality depth above it, so a run pins here.
	pinnedBlock       = int64(25_600_000)
	positionLiquidity = int64(123_456)
)

type mockChain struct {
	t               *testing.T
	getLogsRefusals int
	getLogsCalls    int
	chainID         string
	getLogsFatal    bool
}

// The zero value is the healthy chain every happy-path test drives.
type mockChainOptions struct {
	refusals     int
	chainID      string
	getLogsFatal bool
}

func startMockChain(t *testing.T, opts mockChainOptions) *httptest.Server {
	t.Helper()
	chainID := opts.chainID
	switch {
	case opts.chainIDFails():
		chainID = ""
	case chainID == "":
		chainID = "0x1"
	}
	chain := &mockChain{t: t, getLogsRefusals: opts.refusals, chainID: chainID, getLogsFatal: opts.getLogsFatal}
	server := httptest.NewServer(http.HandlerFunc(chain.serve))
	t.Cleanup(server.Close)
	return server
}

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
		writeJSONResult(c.t, w, req.ID, "0x"+strconv.FormatInt(pinnedBlock+uniswapv4bootstrap.DefaultFinalityDepth, 16))
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
	raw, err := json.Marshal(c.logsFor(req))
	if err != nil {
		c.t.Fatalf("marshalling logs: %v", err)
	}
	testutil.WriteRPCResult(w, req.ID, raw)
}

// The two workflow types scan different contracts, so the mock answers by the
// filter's address exactly as a node would — an address-blind mock would hand the
// posm scan a PoolManager log and the decoder would rightly refuse it.
func (c *mockChain) logsFor(req rpcutil.Request) []map[string]any {
	var params []struct {
		Address string `json:"address"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil || len(params) == 0 {
		c.t.Fatalf("bad eth_getLogs params: %s", req.Params)
	}
	if common.HexToAddress(params[0].Address) == common.HexToAddress(posmAddr) {
		return []map[string]any{posmTransferLogJSON(c.t)}
	}
	return []map[string]any{modifyLiquidityLogJSON(c.t)}
}

func posmTransferLogJSON(t *testing.T) map[string]any {
	t.Helper()
	return map[string]any{
		"address": posmAddr,
		"topics": []string{
			abis.TransferTopic0().Hex(),
			common.BytesToHash(common.HexToAddress(transferFrom).Bytes()).Hex(),
			common.BytesToHash(common.HexToAddress(transferTo).Bytes()).Hex(),
			common.BigToHash(big.NewInt(transferTokenID)).Hex(),
		},
		"data":             "0x",
		"blockHash":        pinnedBlockHash,
		"blockNumber":      "0x" + strconv.FormatInt(transferBlock, 16),
		"blockTimestamp":   "0x68a3f900",
		"transactionHash":  txHash,
		"transactionIndex": "0x0",
		"logIndex":         "0x" + strconv.FormatInt(transferLogIndex, 16),
		"removed":          false,
	}
}

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
		return testutil.PackPositionInfo(c.t, big.NewInt(positionLiquidity), big.NewInt(0), big.NewInt(0)), true
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

// setWorkerEnv installs the environment a deployed pod would have, so the tests
// below exercise the real config loading rather than a hand-built config. One
// window covers the whole seeded history, so a run is one GetLogs answer.
func setWorkerEnv(t *testing.T, chainID, rpcURL string) {
	t.Helper()
	t.Setenv("CHAIN_ID", chainID)
	t.Setenv("ALCHEMY_HTTP_URL", rpcURL)
	t.Setenv("ALCHEMY_API_KEY", "test-key")
	t.Setenv("INITIAL_WINDOW", "10000000")
	t.Setenv("MAX_WINDOW", "10000000")
	t.Setenv("BUILD_GIT_HASH", "test")
}

// deployment is one registered worker against one database and one mock chain,
// the way register wires it in production.
type deployment struct {
	db  *pgxpool.Pool
	env *testsuite.TestWorkflowEnvironment
}

func newDeployment(t *testing.T, opts mockChainOptions) *deployment {
	t.Helper()
	db, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	t.Cleanup(cleanup)
	server := startMockChain(t, opts)
	setWorkerEnv(t, "1", server.URL)

	env, err := registerWorker(t, db)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	return &deployment{db: db, env: env}
}

// registerWorker drives the deployed wiring — register, loadConfig, the real
// Alchemy client and the real repository — into a test workflow environment,
// so a run executes the real activity against the mock chain.
func registerWorker(t *testing.T, db *pgxpool.Pool) (*testsuite.TestWorkflowEnvironment, error) {
	t.Helper()
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	worker := &bootstrapWorker{}
	t.Cleanup(worker.close)
	err := worker.register(context.Background(), temporal.Dependencies{Pool: db, Logger: testutil.DiscardLogger()}, env)
	return env, err
}

func (d *deployment) run(t *testing.T) error {
	t.Helper()
	d.env.ExecuteWorkflow(positionWorkflowTypeName)
	return d.env.GetWorkflowError()
}

func countPositions(t *testing.T, db *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := db.QueryRow(context.Background(), `SELECT COUNT(*) FROM uniswap_v4_position`).Scan(&n); err != nil {
		t.Fatalf("counting positions: %v", err)
	}
	return n
}

func TestRunIntegration_PersistsTheDiscoveredPositionAtTheDerivedPin(t *testing.T) {
	d := newDeployment(t, mockChainOptions{})

	if err := d.run(t); err != nil {
		t.Fatalf("workflow: %v", err)
	}

	if got := countPositions(t, d.db); got != 1 {
		t.Fatalf("uniswap_v4_position rows = %d, want 1", got)
	}
	var (
		owner       []byte
		blockNumber int64
		liquidity   string
	)
	if err := d.db.QueryRow(context.Background(),
		`SELECT owner, block_number, liquidity::text FROM uniswap_v4_position`).
		Scan(&owner, &blockNumber, &liquidity); err != nil {
		t.Fatalf("reading back the position: %v", err)
	}
	if common.BytesToAddress(owner) != common.HexToAddress(positionOwner) {
		t.Errorf("owner = %s, want %s", common.BytesToAddress(owner), positionOwner)
	}
	if blockNumber != pinnedBlock {
		t.Errorf("block_number = %d, want head minus the finality depth, %d", blockNumber, pinnedBlock)
	}
	if liquidity != strconv.FormatInt(positionLiquidity, 10) {
		t.Errorf("liquidity = %s, want %d", liquidity, positionLiquidity)
	}
}

func TestRunIntegration_ASecondRunWritesNoNewRows(t *testing.T) {
	d := newDeployment(t, mockChainOptions{})
	if err := d.run(t); err != nil {
		t.Fatalf("first run: %v", err)
	}
	first := countPositions(t, d.db)

	// A hand-started rerun is a new execution on a fresh registration.
	again, err := registerWorker(t, d.db)
	if err != nil {
		t.Fatalf("register again: %v", err)
	}
	again.ExecuteWorkflow(positionWorkflowTypeName)
	if err := again.GetWorkflowError(); err != nil {
		t.Fatalf("second run: %v", err)
	}

	if got := countPositions(t, d.db); got != first {
		t.Errorf("rows after the rerun = %d, want %d: the run must be idempotent", got, first)
	}
}

func TestRunIntegration_BisectsPastARangeRefusal(t *testing.T) {
	d := newDeployment(t, mockChainOptions{refusals: 2})

	if err := d.run(t); err != nil {
		t.Fatalf("workflow: %v", err)
	}

	if got := countPositions(t, d.db); got != 1 {
		t.Errorf("uniswap_v4_position rows = %d, want 1: the scan must recover from the refusals", got)
	}
}

// The record a retry would resume from is what the activity heartbeats: the
// pin the rows were read at, and the pools finished so far. Seeing it arrive
// through the test environment's heartbeat listener proves the store the runner
// records into is the one the activity reports, not a second instance. The SDK
// throttles heartbeats, so the listener sees the first record and some of the
// later ones, never reliably the last.
func TestRunIntegration_HeartbeatsThePinAndTheFinishedPools(t *testing.T) {
	d := newDeployment(t, mockChainOptions{})
	var records []uniswapv4bootstrap.Progress
	d.env.SetOnActivityHeartbeatListener(func(_ *activity.Info, details converter.EncodedValues) {
		var progress uniswapv4bootstrap.Progress
		if err := details.Get(&progress); err != nil {
			t.Errorf("decoding heartbeat details: %v", err)
			return
		}
		records = append(records, progress)
	})

	if err := d.run(t); err != nil {
		t.Fatalf("workflow: %v", err)
	}

	if len(records) == 0 {
		t.Fatal("no progress was heartbeated; a killed worker would restart from a fresh pin")
	}
	for i, record := range records {
		if record.ChainID != 1 || record.PinnedBlock != pinnedBlock || record.PinnedHash != pinnedBlockHash {
			t.Errorf("record %d = %+v, want chain 1 pinned at %d %s", i, record, pinnedBlock, pinnedBlockHash)
		}
		if len(record.PoolsDone) == 0 {
			t.Errorf("record %d lists no finished pool", i)
		}
		if i > 0 && !slices.Equal(record.PoolsDone[:len(records[i-1].PoolsDone)], records[i-1].PoolsDone) {
			t.Errorf("record %d %v does not extend record %d %v", i, record.PoolsDone, i-1, records[i-1].PoolsDone)
		}
	}
}

func countRegisteredPools(t *testing.T, db *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := db.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM (SELECT DISTINCT ON (chain_id, pool_id) snapshot_supported FROM uniswap_v4_pool WHERE chain_id = 1 ORDER BY chain_id, pool_id, processing_version DESC) p WHERE p.snapshot_supported`).
		Scan(&n); err != nil {
		t.Fatalf("counting registered pools: %v", err)
	}
	return n
}

func TestRunIntegration_AFailedScanWritesNothingAndFailsTheRun(t *testing.T) {
	d := newDeployment(t, mockChainOptions{getLogsFatal: true})

	err := d.run(t)

	if err == nil {
		t.Fatal("expected an error: every log query failed")
	}
	if got := countPositions(t, d.db); got != 0 {
		t.Errorf("uniswap_v4_position rows = %d, want 0: a failed scan must write nothing", got)
	}
}

func TestRegisterIntegration_RefusesAChainIDMismatch(t *testing.T) {
	db, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	t.Cleanup(cleanup)
	server := startMockChain(t, mockChainOptions{})
	setWorkerEnv(t, "8453", server.URL)

	_, err := registerWorker(t, db)

	if err == nil || !strings.Contains(err.Error(), "chain ID mismatch") {
		t.Fatalf("register error = %v, want the chain ID mismatch: the endpoint serves another chain", err)
	}
}

func TestRegisterIntegration_RefusesAnUnreadableChainID(t *testing.T) {
	db, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	t.Cleanup(cleanup)
	server := startMockChain(t, mockChainOptions{chainID: chainIDFailSentinel})
	setWorkerEnv(t, "1", server.URL)

	_, err := registerWorker(t, db)

	if err == nil || !strings.Contains(err.Error(), "chain ID") {
		t.Fatalf("register error = %v, want it to name the chain ID read", err)
	}
}

func TestRegisterIntegration_RefusesAChainWithNoRegisteredPools(t *testing.T) {
	db, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	t.Cleanup(cleanup)
	server := startMockChain(t, mockChainOptions{chainID: "0x2105"})
	setWorkerEnv(t, "8453", server.URL)

	_, err := registerWorker(t, db)

	if err == nil || !strings.Contains(err.Error(), "no uniswap v4 pools registered") {
		t.Fatalf("register error = %v, want it to name the empty registry", err)
	}
}

func TestRegisterIntegration_RefusesAnIncompleteEnvironment(t *testing.T) {
	db, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	t.Cleanup(cleanup)
	setWorkerEnv(t, "1", "http://127.0.0.1:1")
	t.Setenv("ALCHEMY_API_KEY", "")

	_, err := registerWorker(t, db)

	if err == nil || !strings.Contains(err.Error(), "ALCHEMY_API_KEY") {
		t.Fatalf("register error = %v, want it to name ALCHEMY_API_KEY", err)
	}
}

func TestRunIntegration_RequiresADatabaseURL(t *testing.T) {
	t.Setenv("DATABASE_URL", "")

	err := run(context.Background())

	if err == nil || !strings.Contains(err.Error(), "DATABASE_URL") {
		t.Fatalf("run error = %v, want it to name DATABASE_URL", err)
	}
}

// run() is the whole binary from main()'s point of view. Cancelling before it
// reaches Temporal is the one path a test can drive without a server, and it is
// what proves the signal context actually stops the worker. A SIGTERM during
// startup — a pod rolled while it was still wiring itself up — is a shutdown,
// not a failure: surfacing the cancelled context would exit 1 and make an
// ordinary rollout read like a crash.
func TestRunIntegration_StopsCleanlyWhenTheContextIsCancelled(t *testing.T) {
	_, dsn, cleanup := testutil.SetupTestDB(t, sharedDSN)
	t.Cleanup(cleanup)
	t.Setenv("DATABASE_URL", dsn)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := run(ctx)

	if err != nil {
		t.Fatalf("run = %v, want a cancelled startup reported as a clean stop", err)
	}
}

func (d *deployment) runTransfers(t *testing.T) error {
	t.Helper()
	d.env.ExecuteWorkflow(transferWorkflowTypeName)
	return d.env.GetWorkflowError()
}

func countTransfers(t *testing.T, db *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := db.QueryRow(context.Background(), `SELECT COUNT(*) FROM uniswap_v4_position_nft_transfer`).Scan(&n); err != nil {
		t.Fatalf("counting nft transfers: %v", err)
	}
	return n
}

func TestTransferRunIntegration_PersistsTheScannedTransfer(t *testing.T) {
	d := newDeployment(t, mockChainOptions{})

	if err := d.runTransfers(t); err != nil {
		t.Fatalf("run: %v", err)
	}

	var (
		tokenID      string
		blockNumber  int64
		blockVersion int
		logIndex     int
		from, to     []byte
		blockTS      string
		version      int
	)
	err := d.db.QueryRow(context.Background(), `
		SELECT token_id::text, block_number, block_version, log_index,
		       from_address, to_address, to_char(block_timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS'),
		       processing_version
		FROM uniswap_v4_position_nft_transfer`).
		Scan(&tokenID, &blockNumber, &blockVersion, &logIndex, &from, &to, &blockTS, &version)
	if err != nil {
		t.Fatalf("reading the persisted transfer: %v", err)
	}

	if tokenID != strconv.FormatInt(transferTokenID, 10) {
		t.Errorf("token_id = %s, want %d", tokenID, transferTokenID)
	}
	if blockNumber != transferBlock {
		t.Errorf("block_number = %d, want the log's own height %d", blockNumber, transferBlock)
	}
	if blockVersion != 0 {
		t.Errorf("block_version = %d, want 0: the scan stops below the reorg window", blockVersion)
	}
	if logIndex != int(transferLogIndex) {
		t.Errorf("log_index = %d, want %d", logIndex, transferLogIndex)
	}
	// 0x68a3f900. Carried by the log, not read from a header.
	if blockTS != "2025-08-19 04:09:36" {
		t.Errorf("block_timestamp = %s, want the log's blockTimestamp", blockTS)
	}
	if got := common.BytesToAddress(from); got != common.HexToAddress(transferFrom) {
		t.Errorf("from_address = %s, want %s", got, transferFrom)
	}
	if got := common.BytesToAddress(to); got != common.HexToAddress(transferTo) {
		t.Errorf("to_address = %s, want %s", got, transferTo)
	}
	if version != 0 {
		t.Errorf("processing_version = %d, want 0 for a first write", version)
	}
}

func TestTransferRunIntegration_ASecondRunWritesNoNewRows(t *testing.T) {
	d := newDeployment(t, mockChainOptions{})
	if err := d.runTransfers(t); err != nil {
		t.Fatalf("first run: %v", err)
	}
	first := countTransfers(t, d.db)
	if first == 0 {
		t.Fatal("the first run persisted nothing; the rerun assertion would be vacuous")
	}

	again, err := registerWorker(t, d.db)
	if err != nil {
		t.Fatalf("re-register: %v", err)
	}
	again.ExecuteWorkflow(transferWorkflowTypeName)
	if err := again.GetWorkflowError(); err != nil {
		t.Fatalf("second run: %v", err)
	}

	if second := countTransfers(t, d.db); second != first {
		t.Errorf("row count went %d -> %d across a rerun, want it unchanged", first, second)
	}
}

// The reason SaveNFTTransfersIfAbsent exists. The live indexer writes a log site
// under its own build_id; a plain ON CONFLICT insert from the backfill's build
// would miss that conflict target (processing_version is trigger-assigned per
// build) and append a SECOND row at processing_version 1 — a correction version
// that corrects nothing, for every site the two writers' coverage shares.
func TestTransferRunIntegration_LeavesALogSiteAnotherBuildAlreadyWrote(t *testing.T) {
	d := newDeployment(t, mockChainOptions{})

	var posmID int64
	if err := d.db.QueryRow(context.Background(),
		`SELECT id FROM uniswap_v4_position_manager WHERE chain_id = 1`).Scan(&posmID); err != nil {
		t.Fatalf("reading the seeded PositionManager: %v", err)
	}
	// build_id 4242 stands in for the live indexer's build, which is never the
	// backfill's; the trigger keys its version reuse on exactly that column.
	_, err := d.db.Exec(context.Background(), `
		INSERT INTO uniswap_v4_position_nft_transfer
		  (position_manager_id, token_id, block_number, block_version, block_timestamp,
		   tx_hash, log_index, from_address, to_address, build_id)
		VALUES ($1, $2, $3, 0, '2025-08-19 04:09:36+00', $4, $5, $6, $7, 4242)`,
		posmID, transferTokenID, transferBlock,
		common.HexToHash(txHash).Bytes(), transferLogIndex,
		common.HexToAddress(transferFrom).Bytes(), common.HexToAddress(transferTo).Bytes())
	if err != nil {
		t.Fatalf("seeding the live indexer's row: %v", err)
	}

	if err := d.runTransfers(t); err != nil {
		t.Fatalf("run: %v", err)
	}

	if n := countTransfers(t, d.db); n != 1 {
		t.Fatalf("row count = %d, want 1: the backfill appended a correction version for a site already recorded", n)
	}
	var buildID, version int
	if err := d.db.QueryRow(context.Background(),
		`SELECT build_id, processing_version FROM uniswap_v4_position_nft_transfer`).Scan(&buildID, &version); err != nil {
		t.Fatalf("reading the surviving row: %v", err)
	}
	if buildID != 4242 || version != 0 {
		t.Errorf("surviving row is build_id %d / processing_version %d, want the pre-existing 4242 / 0", buildID, version)
	}
}

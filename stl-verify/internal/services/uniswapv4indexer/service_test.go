package uniswapv4indexer

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	metricsdk "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/dextelemetry"
	"github.com/archon-research/stl/stl-verify/internal/pkg/tickbitmap"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/dexconsumer"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// fakeUniswapRepo records the writes SaveBlock received; ignores the pgx.Tx
// (nil is fine). err, when set, makes SaveBlock fail so tests can exercise the
// failed-persist path (baseline/tracker must not be marked).
type fakeUniswapRepo struct {
	lastWrites     outbound.UniswapV4BlockWrites
	saveBlockCalls int
	// stateRowsReturn overrides SaveBlock's returned row count when non-nil. A
	// pointer, not a plain int64, so a test can stage the idempotent
	// ON CONFLICT DO NOTHING replay — an explicit 0 — which a zero-valued int64
	// could not express.
	stateRowsReturn *int64
	err             error

	priorTicks        map[fakePoolBlockKey][]int32
	ticksForPoolCalls []fakePoolBlockKey
	ticksForPoolErr   error

	poolsWithState      map[int64][]int64
	poolsWithStateCalls []fakeStateBlockKey
	poolsWithStateErr   error
}

type fakePoolBlockKey struct {
	poolID      int64
	blockNumber int64
}

// fakeStateBlockKey records the full scope PoolIDsWithStateAtBlock is called
// with, so a test can pin the chain and the chunk-bounding timestamp.
type fakeStateBlockKey struct {
	chainID     int64
	blockNumber int64
	blockTime   time.Time
}

func (r *fakeUniswapRepo) LoadPools(_ context.Context, _ int64) ([]outbound.UniswapV4PoolRow, error) {
	return nil, nil
}

func (r *fakeUniswapRepo) TicksForPoolAtBlock(_ context.Context, poolID int64, blockNumber int64) ([]int32, error) {
	key := fakePoolBlockKey{poolID: poolID, blockNumber: blockNumber}
	r.ticksForPoolCalls = append(r.ticksForPoolCalls, key)
	if r.ticksForPoolErr != nil {
		return nil, r.ticksForPoolErr
	}
	return r.priorTicks[key], nil
}

func (r *fakeUniswapRepo) PoolIDsWithStateAtBlock(_ context.Context, chainID int64, blockNumber int64, blockTime time.Time) ([]int64, error) {
	r.poolsWithStateCalls = append(r.poolsWithStateCalls,
		fakeStateBlockKey{chainID: chainID, blockNumber: blockNumber, blockTime: blockTime})
	if r.poolsWithStateErr != nil {
		return nil, r.poolsWithStateErr
	}
	return r.poolsWithState[blockNumber], nil
}

func (r *fakeUniswapRepo) SaveBlock(_ context.Context, _ pgx.Tx, w outbound.UniswapV4BlockWrites) (int64, error) {
	r.saveBlockCalls++
	if r.err != nil {
		return 0, r.err
	}
	r.lastWrites = w
	if r.stateRowsReturn != nil {
		return *r.stateRowsReturn, nil
	}
	return int64(len(w.States)), nil
}

// fakeEventRepo counts saved events, satisfying outbound.EventRepository. err,
// when set, makes SaveBatch fail so tests can exercise the persist path where
// the typed write succeeds but the captured-events write does not.
type fakeEventRepo struct {
	events []*entity.ProtocolEvent
	err    error
}

func (r *fakeEventRepo) SaveEvent(_ context.Context, _ pgx.Tx, e *entity.ProtocolEvent) error {
	r.events = append(r.events, e)
	return nil
}

func (r *fakeEventRepo) SaveBatch(_ context.Context, _ pgx.Tx, evts []*entity.ProtocolEvent) error {
	if r.err != nil {
		return r.err
	}
	r.events = append(r.events, evts...)
	return nil
}

// countingTxManager delegates to fn but counts invocations, so tests can assert
// whether a transaction was opened at all.
type countingTxManager struct {
	calls int
}

func (m *countingTxManager) WithTransaction(_ context.Context, fn func(pgx.Tx) error) error {
	m.calls++
	return fn(nil)
}

// recordingMulticaller serves canned results for the state batch, the
// touched-tick batch, and the baseline bitmap scan, disambiguating a batch by
// its first call's selector. It records how many times each kind of batch ran
// so tests can assert exactly-once baseline reads and no-RPC-on-quiet-block.
type recordingMulticaller struct {
	stateResults    []outbound.Result
	tickResults     map[int32]outbound.Result
	baselineResults map[int16]outbound.Result // unlisted words default to an all-zero (no initialized ticks) word

	executeAtHashCalls int
	pinnedHashes       []common.Hash
	stateCalls         int
	tickBatchCalls     int
	tickBatchSizes     []int
	baselineCalls      int

	stateErr    error
	tickErr     error
	baselineErr error
}

func (m *recordingMulticaller) Execute(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
	return nil, fmt.Errorf("Execute must not be called; all reads must pin to a block hash")
}

func (m *recordingMulticaller) ExecuteAtHash(_ context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
	m.executeAtHashCalls++
	m.pinnedHashes = append(m.pinnedHashes, blockHash)
	kind, err := m.batchKind(calls)
	if err != nil {
		return nil, err
	}
	switch kind {
	case "getSlot0":
		m.stateCalls++
		if m.stateErr != nil {
			return nil, m.stateErr
		}
		return m.stateResults, nil
	case "getTickInfo":
		m.tickBatchCalls++
		m.tickBatchSizes = append(m.tickBatchSizes, len(calls))
		if m.tickErr != nil {
			return nil, m.tickErr
		}
		return m.tickInfoResults(calls)
	case "getTickBitmap":
		m.baselineCalls++
		if m.baselineErr != nil {
			return nil, m.baselineErr
		}
		return m.tickBitmapResults(calls)
	default:
		return nil, fmt.Errorf("test stub: unrecognized batch %q", kind)
	}
}

// batchKind names the StateView method a batch is calling, by matching its
// first call's 4-byte selector.
func (m *recordingMulticaller) batchKind(calls []outbound.Call) (string, error) {
	if len(calls) == 0 || len(calls[0].CallData) < 4 {
		return "", fmt.Errorf("test stub: call data too short to carry a selector")
	}
	selector := string(calls[0].CallData[:4])
	stateABI, err := poolStateABI()
	if err != nil {
		return "", err
	}
	tickABI, err := tickViewABI()
	if err != nil {
		return "", err
	}
	for _, candidate := range []struct {
		name string
		id   []byte
	}{
		{"getSlot0", stateABI.Methods["getSlot0"].ID},
		{"getTickInfo", tickABI.Methods["getTickInfo"].ID},
		{"getTickBitmap", tickABI.Methods["getTickBitmap"].ID},
	} {
		if selector == string(candidate.id) {
			return candidate.name, nil
		}
	}
	return "", fmt.Errorf("test stub: unrecognized call selector")
}

func (m *recordingMulticaller) tickInfoResults(calls []outbound.Call) ([]outbound.Result, error) {
	a, err := tickViewABI()
	if err != nil {
		return nil, err
	}
	out := make([]outbound.Result, len(calls))
	for i, call := range calls {
		args, err := a.Methods["getTickInfo"].Inputs.Unpack(call.CallData[4:])
		if err != nil {
			return nil, fmt.Errorf("decoding fake getTickInfo call: %w", err)
		}
		tick := int32(args[1].(*big.Int).Int64())
		res, ok := m.tickResults[tick]
		if !ok {
			return nil, fmt.Errorf("test stub: no canned tick result for tick %d", tick)
		}
		out[i] = res
	}
	return out, nil
}

func (m *recordingMulticaller) tickBitmapResults(calls []outbound.Call) ([]outbound.Result, error) {
	a, err := tickViewABI()
	if err != nil {
		return nil, err
	}
	zero, err := a.Methods["getTickBitmap"].Outputs.Pack(big.NewInt(0))
	if err != nil {
		return nil, fmt.Errorf("packing default getTickBitmap result: %w", err)
	}
	out := make([]outbound.Result, len(calls))
	for i, call := range calls {
		args, err := a.Methods["getTickBitmap"].Inputs.Unpack(call.CallData[4:])
		if err != nil {
			return nil, fmt.Errorf("decoding fake getTickBitmap call: %w", err)
		}
		if res, ok := m.baselineResults[args[1].(int16)]; ok {
			out[i] = res
			continue
		}
		out[i] = outbound.Result{Success: true, ReturnData: zero}
	}
	return out, nil
}

func (m *recordingMulticaller) Address() common.Address { return common.Address{} }

// assertPinnedTo requires every read issued since the previous call to have been
// pinned to want, and drains the record so the next block can be checked on its
// own. A read answered at any other hash would silently mix forks.
func assertPinnedTo(t *testing.T, mc *recordingMulticaller, want common.Hash) {
	t.Helper()
	got := mc.pinnedHashes
	mc.pinnedHashes = nil
	if len(got) == 0 {
		t.Fatalf("no hash-pinned reads were issued; want reads pinned to %s", want)
	}
	for i, h := range got {
		if h != want {
			t.Errorf("read %d pinned to %s, want %s", i, h, want)
		}
	}
}

const testChainID = int64(1)

const (
	wbtcAddress = "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"
	// Mainnet WBTC/wstETH pools; their PoolIds really are the keccak of these
	// keys, so the service constructor's ValidatePoolKeys gate is exercised for
	// real rather than bypassed by a synthetic hash.
	wbtcWstethPoolID     = "0x58299b9ad89104f189f5efcdf4910615cb9e3296afb0c5a1d1d3befdd1bf7ae4"
	wbtcWstethLowFeePool = "0xef3a1d51982c20ee2f125e6d6d1f9d3a10c1e94391b828040943005a1ea27e14"
)

// servicePool is the primary fixture pool: WBTC/wstETH 0.25%, tickSpacing 50,
// deployed at block 100.
func servicePool() RegisteredPool {
	return RegisteredPool{
		ID:                7,
		PoolManager:       poolManagerAddress(),
		StateView:         common.HexToAddress(stateViewAddr),
		PoolIDHash:        common.HexToHash(wbtcWstethPoolID),
		Currency0:         common.HexToAddress(wbtcAddress),
		Currency1:         common.HexToAddress(wstethAddress),
		Currency0Decimals: 8,
		Currency1Decimals: 18,
		Fee:               2500,
		TickSpacing:       50,
		DeployBlock:       100,
		SnapshotSupported: true,
	}
}

// dynamicFeeServicePool is servicePool's key at the dynamic-LP-fee sentinel and
// therefore excluded from snapshots. Its PoolId is computed rather than
// transcribed: no mainnet pool carries this exact key.
func dynamicFeeServicePool(t *testing.T) RegisteredPool {
	t.Helper()
	pool := servicePool()
	pool.ID = 9
	pool.Fee = DynamicFeeFlag
	pool.SnapshotSupported = false
	hash, err := computePoolID(pool)
	if err != nil {
		t.Fatalf("computePoolID: %v", err)
	}
	pool.PoolIDHash = hash
	return pool
}

// secondServicePool is the same pair at 0.3% / tickSpacing 60, so a single
// receipt can touch two tracked pools of one PoolManager.
func secondServicePool() RegisteredPool {
	pool := servicePool()
	pool.ID = 8
	pool.PoolIDHash = common.HexToHash(wbtcWstethLowFeePool)
	pool.Fee = 3000
	pool.TickSpacing = 60
	return pool
}

// blockEvent builds a minimal outbound.BlockEvent with a non-zero block hash so
// the suite exercises the real hash-pinned read path.
func blockEvent(bn int64) outbound.BlockEvent {
	return outbound.BlockEvent{
		ChainID:        testChainID,
		BlockNumber:    bn,
		Version:        0,
		BlockTimestamp: bn,
		BlockHash:      common.HexToHash("0x01").Hex(),
	}
}

// swapLog builds a Swap log for pool, using the real PoolManager ABI so
// DecodeEvents can decode it.
func swapLog(t *testing.T, pool RegisteredPool, logIndexHex string) shared.Log {
	t.Helper()
	log := buildLog(t, "Swap",
		[]common.Hash{pool.PoolIDHash, addrTopic(common.HexToAddress("0xaaa"))},
		big.NewInt(-100), big.NewInt(200), big.NewInt(1234567890), big.NewInt(999), big.NewInt(60), big.NewInt(2500),
	)
	log.LogIndex = logIndexHex
	return log
}

// modifyLog builds a ModifyLiquidity log for pool over the given tick range.
func modifyLog(t *testing.T, pool RegisteredPool, logIndexHex string, tickLower, tickUpper, liquidityDelta int64) shared.Log {
	t.Helper()
	log := buildLog(t, "ModifyLiquidity",
		[]common.Hash{pool.PoolIDHash, addrTopic(common.HexToAddress("0xbbb"))},
		big.NewInt(tickLower), big.NewInt(tickUpper), big.NewInt(liquidityDelta), [32]byte{},
	)
	log.LogIndex = logIndexHex
	return log
}

// donateLog builds a Donate log: a third decoded-event kind (a low-frequency
// pool event) so the mixed-events test exercises all three DecodedEvents
// buckets routing into BlockWrites and the capture net.
func donateLog(t *testing.T, pool RegisteredPool, logIndexHex string) shared.Log {
	t.Helper()
	log := buildLog(t, "Donate",
		[]common.Hash{pool.PoolIDHash, addrTopic(common.HexToAddress("0xccc"))},
		big.NewInt(10), big.NewInt(20),
	)
	log.LogIndex = logIndexHex
	return log
}

func goodTickResult(t *testing.T) outbound.Result {
	t.Helper()
	return outbound.Result{Success: true, ReturnData: packTickInfoReturn(t, big.NewInt(1000), big.NewInt(500), big.NewInt(1), big.NewInt(2))}
}

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(os.Stderr, nil)) }

// validServiceDeps returns a fully-wired dependency set over pools, with the
// fakes a test needs to assert on.
func validServiceDeps(t *testing.T, pools []RegisteredPool) (UniswapV4ServiceDeps, *fakeUniswapRepo, *recordingMulticaller, *countingTxManager) {
	t.Helper()
	mc := &recordingMulticaller{
		stateResults: buildStateResults(t, defaultStateFixture()),
		tickResults:  map[int32]outbound.Result{},
	}
	repo := &fakeUniswapRepo{}
	txMgr := &countingTxManager{}
	deps := UniswapV4ServiceDeps{
		Pools:       pools,
		Multicaller: mc,
		Repo:        repo,
		EventWriter: dexconsumer.NewProtocolEventWriter(1, &fakeEventRepo{}),
		TxManager:   txMgr,
		ChainID:     testChainID,
		Logger:      testLogger(),
	}
	return deps, repo, mc, txMgr
}

func newTestService(t *testing.T, pools ...RegisteredPool) (*UniswapV4Service, *fakeUniswapRepo, *recordingMulticaller, *countingTxManager) {
	t.Helper()
	deps, repo, mc, txMgr := validServiceDeps(t, pools)
	svc, err := NewUniswapV4Service(deps)
	if err != nil {
		t.Fatalf("NewUniswapV4Service: %v", err)
	}
	return svc, repo, mc, txMgr
}

func TestNewUniswapV4Service_RejectsInvalidDeps(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(d UniswapV4ServiceDeps) UniswapV4ServiceDeps
		wantSub string
	}{
		{name: "empty pool registry", wantSub: "at least one pool", mutate: func(d UniswapV4ServiceDeps) UniswapV4ServiceDeps {
			d.Pools = nil
			return d
		}},
		{name: "non-positive chain id", wantSub: "chainID", mutate: func(d UniswapV4ServiceDeps) UniswapV4ServiceDeps {
			d.ChainID = 0
			return d
		}},
		{name: "nil multicaller", wantSub: "multicaller", mutate: func(d UniswapV4ServiceDeps) UniswapV4ServiceDeps {
			d.Multicaller = nil
			return d
		}},
		{name: "nil repo", wantSub: "repo", mutate: func(d UniswapV4ServiceDeps) UniswapV4ServiceDeps {
			d.Repo = nil
			return d
		}},
		{name: "nil event writer", wantSub: "eventWriter", mutate: func(d UniswapV4ServiceDeps) UniswapV4ServiceDeps {
			d.EventWriter = nil
			return d
		}},
		{name: "nil tx manager", wantSub: "txManager", mutate: func(d UniswapV4ServiceDeps) UniswapV4ServiceDeps {
			d.TxManager = nil
			return d
		}},
		{name: "nil logger", wantSub: "logger", mutate: func(d UniswapV4ServiceDeps) UniswapV4ServiceDeps {
			d.Logger = nil
			return d
		}},
		{name: "pool key disagrees with its PoolId", wantSub: "registry bug", mutate: func(d UniswapV4ServiceDeps) UniswapV4ServiceDeps {
			pools := slices.Clone(d.Pools)
			pools[0].Fee = 500
			d.Pools = pools
			return d
		}},
		{name: "duplicate PoolId", wantSub: "share PoolId", mutate: func(d UniswapV4ServiceDeps) UniswapV4ServiceDeps {
			duplicate := d.Pools[0]
			duplicate.ID = 99
			d.Pools = append(slices.Clone(d.Pools), duplicate)
			return d
		}},
		{name: "two PoolManager addresses", wantSub: "PoolManager", mutate: func(d UniswapV4ServiceDeps) UniswapV4ServiceDeps {
			other := secondServicePool()
			other.PoolManager = common.HexToAddress("0x1111111111111111111111111111111111111111")
			d.Pools = append(slices.Clone(d.Pools), other)
			return d
		}},
		{name: "two StateView addresses", wantSub: "StateView", mutate: func(d UniswapV4ServiceDeps) UniswapV4ServiceDeps {
			other := secondServicePool()
			other.StateView = common.HexToAddress("0x2222222222222222222222222222222222222222")
			d.Pools = append(slices.Clone(d.Pools), other)
			return d
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deps, _, _, _ := validServiceDeps(t, []RegisteredPool{servicePool()})
			_, err := NewUniswapV4Service(tt.mutate(deps))
			if err == nil {
				t.Fatalf("NewUniswapV4Service: want error for %s, got nil", tt.name)
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Errorf("error %q does not mention %q", err, tt.wantSub)
			}
		})
	}
}

// A dynamic-fee pool is schema-legal and its fee is hashed into its PoolId, so
// refusing to boot on one would be an unrepairable CrashLoop; the registry
// excludes it from snapshots instead.
func TestNewUniswapV4Service_AcceptsDynamicFeePool(t *testing.T) {
	deps, _, _, _ := validServiceDeps(t, []RegisteredPool{dynamicFeeServicePool(t)})
	if _, err := NewUniswapV4Service(deps); err != nil {
		t.Fatalf("NewUniswapV4Service with a dynamic-fee pool: %v", err)
	}
}

// TestBlockHandler_UnsnapshottablePoolPersistsEventsWithoutState pins the other
// half of the curated gate: excluding a pool from snapshots must not exclude it
// from event indexing, and must issue no chain read at all.
func TestBlockHandler_UnsnapshottablePoolPersistsEventsWithoutState(t *testing.T) {
	pool := dynamicFeeServicePool(t)
	svc, repo, mc, txMgr := newTestService(t, pool)

	receipt := shared.TransactionReceipt{Logs: []shared.Log{
		swapLog(t, pool, "0x0"),
		modifyLog(t, pool, "0x1", -100, 200, 5000),
	}}
	if err := svc.BlockHandler()(context.Background(), blockEvent(200), []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("BlockHandler: %v", err)
	}

	if txMgr.calls != 1 {
		t.Fatalf("WithTransaction calls = %d, want 1", txMgr.calls)
	}
	if mc.executeAtHashCalls != 0 {
		t.Errorf("chain reads = %d, want 0 for a pool excluded from snapshots", mc.executeAtHashCalls)
	}
	w := repo.lastWrites
	if len(w.Swaps) != 1 || len(w.LiquidityEvents) != 1 {
		t.Errorf("Swaps = %d, LiquidityEvents = %d, want 1 and 1 (events still index)", len(w.Swaps), len(w.LiquidityEvents))
	}
	if len(w.States) != 0 || len(w.Ticks) != 0 {
		t.Errorf("States = %d, Ticks = %d, want 0 and 0", len(w.States), len(w.Ticks))
	}
}

// An absent BlockTimestamp is not the zero time, so it would pass every
// downstream IsZero() guard and file the whole block in a 1970 chunk — acked,
// invisible, and only repairable by a backfill.
func TestBlockHandler_MissingBlockTimestamp_ErrorsWithoutPersisting(t *testing.T) {
	pool := servicePool()
	svc, repo, mc, txMgr := newTestService(t, pool)

	event := blockEvent(200)
	event.BlockTimestamp = 0
	receipt := shared.TransactionReceipt{Logs: []shared.Log{swapLog(t, pool, "0x0")}}
	err := svc.BlockHandler()(context.Background(), event, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("BlockHandler: want error for a block with no timestamp, got nil")
	}
	if !strings.Contains(err.Error(), "block timestamp") {
		t.Errorf("error %q does not identify the missing block timestamp", err)
	}
	if txMgr.calls != 0 || repo.saveBlockCalls != 0 || mc.executeAtHashCalls != 0 {
		t.Errorf("tx calls = %d, SaveBlock calls = %d, chain reads = %d, want 0, 0 and 0",
			txMgr.calls, repo.saveBlockCalls, mc.executeAtHashCalls)
	}
}

func TestBlockHandler_MixedEventsPersistsBlockWrites(t *testing.T) {
	pool := servicePool()
	svc, repo, mc, txMgr := newTestService(t, pool)
	mc.tickResults[-100] = goodTickResult(t)
	mc.tickResults[200] = goodTickResult(t)

	receipt := shared.TransactionReceipt{Logs: []shared.Log{
		swapLog(t, pool, "0x0"),
		modifyLog(t, pool, "0x1", -100, 200, 5000),
		donateLog(t, pool, "0x2"),
	}}

	event := blockEvent(200)
	if err := svc.BlockHandler()(context.Background(), event, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("BlockHandler: %v", err)
	}

	assertPinnedTo(t, mc, common.HexToHash(event.BlockHash))
	if txMgr.calls != 1 {
		t.Fatalf("WithTransaction calls = %d, want 1", txMgr.calls)
	}
	w := repo.lastWrites
	if len(w.States) != 1 {
		t.Errorf("States = %d, want 1", len(w.States))
	}
	if len(w.Swaps) != 1 {
		t.Errorf("Swaps = %d, want 1", len(w.Swaps))
	}
	if len(w.LiquidityEvents) != 1 {
		t.Errorf("LiquidityEvents = %d, want 1", len(w.LiquidityEvents))
	}
	if len(w.PoolEvents) != 1 {
		t.Errorf("PoolEvents = %d, want 1", len(w.PoolEvents))
	}
	// The ModifyLiquidity bounds; the first-touch baseline scan reports no
	// additional initialized ticks in this fixture.
	if len(w.Ticks) != 2 {
		t.Errorf("Ticks = %d, want 2 (the modify event's bounds)", len(w.Ticks))
	}
}

// TestBlockHandler_CapturesEveryDecodedLog checks the capture net mirrors all
// three decoded logs into protocol_event.
func TestBlockHandler_CapturesEveryDecodedLog(t *testing.T) {
	pool := servicePool()
	deps, _, mc, _ := validServiceDeps(t, []RegisteredPool{pool})
	eventRepo := &fakeEventRepo{}
	deps.EventWriter = dexconsumer.NewProtocolEventWriter(1, eventRepo)
	svc, err := NewUniswapV4Service(deps)
	if err != nil {
		t.Fatalf("NewUniswapV4Service: %v", err)
	}
	mc.tickResults[-100] = goodTickResult(t)
	mc.tickResults[200] = goodTickResult(t)

	receipt := shared.TransactionReceipt{Logs: []shared.Log{
		swapLog(t, pool, "0x0"),
		modifyLog(t, pool, "0x1", -100, 200, 5000),
		donateLog(t, pool, "0x2"),
	}}
	if err := svc.BlockHandler()(context.Background(), blockEvent(200), []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("BlockHandler: %v", err)
	}

	if len(eventRepo.events) != 3 {
		t.Errorf("protocol events = %d, want 3 (one per decoded PoolManager log)", len(eventRepo.events))
	}
}

// TestBlockHandler_MultiPoolReceiptDecodesEveryTouchedPool verifies a single
// receipt carrying logs for TWO tracked PoolIds decodes and snapshots both:
// with a singleton emitter this is the ordinary case, not an edge case.
func TestBlockHandler_MultiPoolReceiptDecodesEveryTouchedPool(t *testing.T) {
	poolA := servicePool()
	poolB := secondServicePool()
	svc, repo, _, txMgr := newTestService(t, poolA, poolB)

	receipt := shared.TransactionReceipt{Logs: []shared.Log{
		swapLog(t, poolA, "0x0"),
		swapLog(t, poolB, "0x1"),
	}}
	if err := svc.BlockHandler()(context.Background(), blockEvent(200), []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("BlockHandler: %v", err)
	}

	if txMgr.calls != 1 {
		t.Fatalf("WithTransaction calls = %d, want 1", txMgr.calls)
	}
	if len(repo.lastWrites.Swaps) != 2 {
		t.Errorf("Swaps = %d, want 2 (one per touched pool)", len(repo.lastWrites.Swaps))
	}
	if len(repo.lastWrites.States) != 2 {
		t.Errorf("States = %d, want 2 (both pools must be snapshotted)", len(repo.lastWrites.States))
	}
	got := map[int64]bool{}
	for _, s := range repo.lastWrites.Swaps {
		got[s.PoolID] = true
	}
	if !got[poolA.ID] || !got[poolB.ID] {
		t.Errorf("swaps cover pools %v, want both %d and %d", got, poolA.ID, poolB.ID)
	}
}

// TestBlockHandler_QuietBlock_NoTransaction covers the two shapes of block the
// indexer must leave no trace of: a PoolManager log for a PoolId outside the
// registry, and a V4-looking log from another contract entirely.
func TestBlockHandler_QuietBlock_NoTransaction(t *testing.T) {
	for _, tc := range []struct {
		name string
		log  func(t *testing.T, pool RegisteredPool) shared.Log
	}{
		{
			name: "unregistered pool id",
			log: func(t *testing.T, _ RegisteredPool) shared.Log {
				untracked := servicePool()
				untracked.PoolIDHash = common.HexToHash("0xdeadbeef")
				return swapLog(t, untracked, "0x0")
			},
		},
		{
			name: "log from another contract",
			log: func(t *testing.T, pool RegisteredPool) shared.Log {
				log := swapLog(t, pool, "0x0")
				log.Address = "0x1F98431c8aD98523631AE4a59f267346ea31F984"
				return log
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := servicePool()
			svc, repo, mc, txMgr := newTestService(t, pool)

			receipt := shared.TransactionReceipt{Logs: []shared.Log{tc.log(t, pool)}}
			if err := svc.BlockHandler()(context.Background(), blockEvent(200), []shared.TransactionReceipt{receipt}); err != nil {
				t.Fatalf("BlockHandler: %v", err)
			}

			if txMgr.calls != 0 {
				t.Errorf("WithTransaction calls = %d, want 0", txMgr.calls)
			}
			if mc.executeAtHashCalls != 0 {
				t.Errorf("ExecuteAtHash calls = %d, want 0 (no RPC for an untouched registry)", mc.executeAtHashCalls)
			}
			if repo.saveBlockCalls != 0 {
				t.Errorf("SaveBlock calls = %d, want 0", repo.saveBlockCalls)
			}
		})
	}
}

func TestBlockHandler_TouchedBelowDeployBlock_Errors(t *testing.T) {
	pool := servicePool()
	pool.DeployBlock = 500
	svc, repo, _, txMgr := newTestService(t, pool)

	receipt := shared.TransactionReceipt{Logs: []shared.Log{swapLog(t, pool, "0x0")}}
	err := svc.BlockHandler()(context.Background(), blockEvent(100), []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("BlockHandler: want error for a pool touched below its deploy block, got nil")
	}
	if !strings.Contains(err.Error(), "registry bug") {
		t.Errorf("error %q does not identify the registry bug", err)
	}
	if txMgr.calls != 0 || repo.saveBlockCalls != 0 {
		t.Errorf("tx calls = %d, SaveBlock calls = %d, want 0 and 0", txMgr.calls, repo.saveBlockCalls)
	}
}

func TestBlockHandler_UnusableBlockHash_Errors(t *testing.T) {
	for _, tc := range []struct {
		name      string
		blockHash string
	}{
		{name: "empty", blockHash: ""},
		{name: "missing 0x prefix", blockHash: strings.Repeat("a", 64)},
		{name: "too short", blockHash: "0xabc1"},
		{name: "non-hex digits", blockHash: "0x" + strings.Repeat("z", 64)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc, repo, mc, txMgr := newTestService(t, servicePool())

			event := blockEvent(200)
			event.BlockHash = tc.blockHash

			if err := svc.BlockHandler()(context.Background(), event, nil); err == nil {
				t.Fatalf("BlockHandler: want error for BlockHash %q, got nil", tc.blockHash)
			}
			if mc.executeAtHashCalls != 0 || txMgr.calls != 0 || repo.saveBlockCalls != 0 {
				t.Errorf("side effects before the hash check: rpc=%d tx=%d save=%d", mc.executeAtHashCalls, txMgr.calls, repo.saveBlockCalls)
			}
		})
	}
}

func TestBlockHandler_CanceledContext_Errors(t *testing.T) {
	pool := servicePool()
	svc, repo, _, txMgr := newTestService(t, pool)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	receipt := shared.TransactionReceipt{Logs: []shared.Log{swapLog(t, pool, "0x0")}}
	if err := svc.BlockHandler()(ctx, blockEvent(200), []shared.TransactionReceipt{receipt}); err == nil {
		t.Fatal("BlockHandler: want error on a canceled context, got nil")
	}
	if txMgr.calls != 0 || repo.saveBlockCalls != 0 {
		t.Errorf("tx calls = %d, SaveBlock calls = %d, want 0 and 0", txMgr.calls, repo.saveBlockCalls)
	}
}

func TestBlockHandler_MalformedLog_Errors(t *testing.T) {
	pool := servicePool()
	svc, repo, _, txMgr := newTestService(t, pool)

	bad := swapLog(t, pool, "0x0")
	bad.Data = "0xdead"
	receipt := shared.TransactionReceipt{Logs: []shared.Log{bad}}

	if err := svc.BlockHandler()(context.Background(), blockEvent(200), []shared.TransactionReceipt{receipt}); err == nil {
		t.Fatal("BlockHandler: want error on a decode failure, got nil")
	}
	if txMgr.calls != 0 || repo.saveBlockCalls != 0 {
		t.Errorf("tx calls = %d, SaveBlock calls = %d, want 0 and 0", txMgr.calls, repo.saveBlockCalls)
	}
}

func TestBlockHandler_ReadFailures_NoPersist(t *testing.T) {
	tests := []struct {
		name  string
		arm   func(mc *recordingMulticaller)
		logOf func(t *testing.T, pool RegisteredPool) shared.Log
	}{
		{
			name:  "state multicall fails",
			arm:   func(mc *recordingMulticaller) { mc.stateErr = fmt.Errorf("rpc down") },
			logOf: func(t *testing.T, pool RegisteredPool) shared.Log { return swapLog(t, pool, "0x0") },
		},
		{
			name: "touched-tick multicall fails",
			arm:  func(mc *recordingMulticaller) { mc.tickErr = fmt.Errorf("rpc down reading ticks") },
			logOf: func(t *testing.T, pool RegisteredPool) shared.Log {
				return modifyLog(t, pool, "0x0", -100, 200, 5000)
			},
		},
		{
			name:  "baseline bitmap multicall fails",
			arm:   func(mc *recordingMulticaller) { mc.baselineErr = fmt.Errorf("rpc down reading bitmap") },
			logOf: func(t *testing.T, pool RegisteredPool) shared.Log { return swapLog(t, pool, "0x0") },
		},
		{
			name: "a touched tick reverts",
			arm: func(mc *recordingMulticaller) {
				mc.tickResults[-100] = outbound.Result{Success: false}
			},
			logOf: func(t *testing.T, pool RegisteredPool) shared.Log {
				return modifyLog(t, pool, "0x0", -100, 200, 5000)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := servicePool()
			svc, repo, mc, txMgr := newTestService(t, pool)
			mc.tickResults[-100] = goodTickResult(t)
			mc.tickResults[200] = goodTickResult(t)
			tt.arm(mc)

			receipt := shared.TransactionReceipt{Logs: []shared.Log{tt.logOf(t, pool)}}
			if err := svc.BlockHandler()(context.Background(), blockEvent(200), []shared.TransactionReceipt{receipt}); err == nil {
				t.Fatalf("BlockHandler: want error when %s, got nil", tt.name)
			}
			if txMgr.calls != 0 || repo.saveBlockCalls != 0 {
				t.Errorf("tx calls = %d, SaveBlock calls = %d, want 0 and 0", txMgr.calls, repo.saveBlockCalls)
			}
		})
	}
}

// truncatingTickMulticaller drops the last result of any getTickInfo batch,
// simulating a provider returning fewer results than requested.
type truncatingTickMulticaller struct {
	*recordingMulticaller
}

func (m *truncatingTickMulticaller) ExecuteAtHash(ctx context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
	results, err := m.recordingMulticaller.ExecuteAtHash(ctx, calls, blockHash)
	if err != nil || len(results) == 0 {
		return results, err
	}
	kind, err := m.batchKind(calls)
	if err != nil {
		return nil, err
	}
	if kind == "getTickInfo" {
		return results[:len(results)-1], nil
	}
	return results, nil
}

func TestBlockHandler_TickResultCountMismatch_NoPersist(t *testing.T) {
	pool := servicePool()
	deps, repo, mc, txMgr := validServiceDeps(t, []RegisteredPool{pool})
	mc.tickResults[-100] = goodTickResult(t)
	mc.tickResults[200] = goodTickResult(t)
	deps.Multicaller = &truncatingTickMulticaller{recordingMulticaller: mc}
	svc, err := NewUniswapV4Service(deps)
	if err != nil {
		t.Fatalf("NewUniswapV4Service: %v", err)
	}

	receipt := shared.TransactionReceipt{Logs: []shared.Log{modifyLog(t, pool, "0x0", -100, 200, 5000)}}
	if err := svc.BlockHandler()(context.Background(), blockEvent(200), []shared.TransactionReceipt{receipt}); err == nil {
		t.Fatal("BlockHandler: want error when the tick multicall returns fewer results than requested, got nil")
	}
	if txMgr.calls != 0 || repo.saveBlockCalls != 0 {
		t.Errorf("tx calls = %d, SaveBlock calls = %d, want 0 and 0", txMgr.calls, repo.saveBlockCalls)
	}
}

func TestBlockHandler_PriorTickReadError_NoPersist(t *testing.T) {
	pool := servicePool()
	svc, repo, mc, txMgr := newTestService(t, pool)
	mc.tickResults[-100] = goodTickResult(t)
	mc.tickResults[200] = goodTickResult(t)

	receipt := shared.TransactionReceipt{Logs: []shared.Log{modifyLog(t, pool, "0x0", -100, 200, 5000)}}
	bh := svc.BlockHandler()
	if err := bh(context.Background(), blockEvent(200), []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("BlockHandler (v0): %v", err)
	}

	repo.ticksForPoolErr = fmt.Errorf("db down")
	reorg := blockEvent(200)
	reorg.Version = 1
	if err := bh(context.Background(), reorg, nil); err == nil {
		t.Fatal("BlockHandler: want error when the prior-version tick read fails, got nil")
	}
	if txMgr.calls != 1 {
		t.Errorf("WithTransaction calls = %d, want 1 (only the successful v0 block)", txMgr.calls)
	}
}

func TestBlockHandler_FirstTouchReadsBaselineTicksOnce(t *testing.T) {
	pool := servicePool()
	svc, _, mc, _ := newTestService(t, pool)
	mc.tickResults[-100] = goodTickResult(t)
	mc.tickResults[200] = goodTickResult(t)

	bh := svc.BlockHandler()
	first := shared.TransactionReceipt{Logs: []shared.Log{modifyLog(t, pool, "0x0", -100, 200, 5000)}}
	if err := bh(context.Background(), blockEvent(200), []shared.TransactionReceipt{first}); err != nil {
		t.Fatalf("BlockHandler (first touch): %v", err)
	}
	baselineAfterFirst := mc.baselineCalls
	if baselineAfterFirst == 0 {
		t.Fatal("the baseline bitmap scan should run on a pool's first touch")
	}

	second := shared.TransactionReceipt{Logs: []shared.Log{modifyLog(t, pool, "0x1", -100, 200, 5000)}}
	if err := bh(context.Background(), blockEvent(201), []shared.TransactionReceipt{second}); err != nil {
		t.Fatalf("BlockHandler (second touch): %v", err)
	}
	if mc.baselineCalls != baselineAfterFirst {
		t.Errorf("baseline scans after the second touch = %d, want %d (no re-scan)", mc.baselineCalls, baselineAfterFirst)
	}
}

// TestBlockHandler_BaselineTicksArePersisted proves the first-touch enumeration
// is written, not merely read: a bitmap word with a set bit must become a tick
// row even though this block's events did not touch that tick.
func TestBlockHandler_BaselineTicksArePersisted(t *testing.T) {
	pool := servicePool()
	svc, repo, mc, _ := newTestService(t, pool)

	// tickSpacing 50: word 0 bit 1 is tick 50.
	a, err := tickViewABI()
	if err != nil {
		t.Fatalf("tickViewABI: %v", err)
	}
	word, err := a.Methods["getTickBitmap"].Outputs.Pack(new(big.Int).SetBit(new(big.Int), 1, 1))
	if err != nil {
		t.Fatalf("packing bitmap word: %v", err)
	}
	mc.baselineResults = map[int16]outbound.Result{0: {Success: true, ReturnData: word}}
	mc.tickResults[50] = goodTickResult(t)

	receipt := shared.TransactionReceipt{Logs: []shared.Log{swapLog(t, pool, "0x0")}}
	if err := svc.BlockHandler()(context.Background(), blockEvent(200), []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("BlockHandler: %v", err)
	}

	if len(repo.lastWrites.Ticks) != 1 || repo.lastWrites.Ticks[0].Tick != 50 {
		t.Errorf("Ticks = %+v, want exactly the baseline tick 50", repo.lastWrites.Ticks)
	}
}

// TestBlockHandler_BaselineOverlappingTouchedTickIsWrittenOnce guards the
// first-touch union: a tick that is both a modify bound and already set in the
// bitmap must produce one row, not a duplicate the append-on-change writer
// would have to reconcile.
func TestBlockHandler_BaselineOverlappingTouchedTickIsWrittenOnce(t *testing.T) {
	pool := servicePool()
	svc, repo, mc, _ := newTestService(t, pool)

	a, err := tickViewABI()
	if err != nil {
		t.Fatalf("tickViewABI: %v", err)
	}
	// tickSpacing 50: word 0 bit 4 is tick 200, one of the modify bounds below.
	word, err := a.Methods["getTickBitmap"].Outputs.Pack(new(big.Int).SetBit(new(big.Int), 4, 1))
	if err != nil {
		t.Fatalf("packing bitmap word: %v", err)
	}
	mc.baselineResults = map[int16]outbound.Result{0: {Success: true, ReturnData: word}}
	mc.tickResults[-100] = goodTickResult(t)
	mc.tickResults[200] = goodTickResult(t)

	receipt := shared.TransactionReceipt{Logs: []shared.Log{modifyLog(t, pool, "0x0", -100, 200, 5000)}}
	if err := svc.BlockHandler()(context.Background(), blockEvent(200), []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("BlockHandler: %v", err)
	}

	seen := map[int]int{}
	for _, tick := range repo.lastWrites.Ticks {
		seen[tick.Tick]++
	}
	if len(repo.lastWrites.Ticks) != 2 || seen[-100] != 1 || seen[200] != 1 {
		t.Errorf("tick writes = %v, want exactly one row each for -100 and 200", seen)
	}
}

func TestBlockHandler_SwapOnlyTouchAfterBaselined_NoTickRead(t *testing.T) {
	pool := servicePool()
	svc, repo, mc, _ := newTestService(t, pool)
	mc.tickResults[-100] = goodTickResult(t)
	mc.tickResults[200] = goodTickResult(t)

	bh := svc.BlockHandler()
	modify := shared.TransactionReceipt{Logs: []shared.Log{modifyLog(t, pool, "0x0", -100, 200, 5000)}}
	if err := bh(context.Background(), blockEvent(200), []shared.TransactionReceipt{modify}); err != nil {
		t.Fatalf("BlockHandler (modify touch): %v", err)
	}
	tickBatchesAfterModify := mc.tickBatchCalls

	swap := shared.TransactionReceipt{Logs: []shared.Log{swapLog(t, pool, "0x0")}}
	if err := bh(context.Background(), blockEvent(201), []shared.TransactionReceipt{swap}); err != nil {
		t.Fatalf("BlockHandler (swap touch): %v", err)
	}
	if mc.tickBatchCalls != tickBatchesAfterModify {
		t.Errorf("tick batches after a swap-only touch = %d, want %d", mc.tickBatchCalls, tickBatchesAfterModify)
	}
	if len(repo.lastWrites.Ticks) != 0 {
		t.Errorf("Ticks = %d, want 0 for a swap-only touch on an already-baselined pool", len(repo.lastWrites.Ticks))
	}
}

// TestBlockHandler_ZeroDeltaModifyReadsNoTicks pins the poke rule end-to-end: a
// zero-delta ModifyLiquidity still writes its event row, but reads no ticks.
func TestBlockHandler_ZeroDeltaModifyReadsNoTicks(t *testing.T) {
	pool := servicePool()
	svc, repo, mc, _ := newTestService(t, pool)
	mc.tickResults[-100] = goodTickResult(t)
	mc.tickResults[200] = goodTickResult(t)

	bh := svc.BlockHandler()
	warmup := shared.TransactionReceipt{Logs: []shared.Log{modifyLog(t, pool, "0x0", -100, 200, 5000)}}
	if err := bh(context.Background(), blockEvent(200), []shared.TransactionReceipt{warmup}); err != nil {
		t.Fatalf("BlockHandler (warmup): %v", err)
	}
	tickBatchesAfterWarmup := mc.tickBatchCalls

	poke := shared.TransactionReceipt{Logs: []shared.Log{modifyLog(t, pool, "0x0", -100, 200, 0)}}
	if err := bh(context.Background(), blockEvent(201), []shared.TransactionReceipt{poke}); err != nil {
		t.Fatalf("BlockHandler (poke): %v", err)
	}

	if len(repo.lastWrites.LiquidityEvents) != 1 {
		t.Errorf("LiquidityEvents = %d, want 1 (the poke is still an event)", len(repo.lastWrites.LiquidityEvents))
	}
	if len(repo.lastWrites.Ticks) != 0 {
		t.Errorf("Ticks = %d, want 0 (a zero-delta poke changes no tick state)", len(repo.lastWrites.Ticks))
	}
	if mc.tickBatchCalls != tickBatchesAfterWarmup {
		t.Errorf("tick batches after the poke = %d, want %d", mc.tickBatchCalls, tickBatchesAfterWarmup)
	}
}

func TestBlockHandler_FailedPersist_DoesNotMarkBaselineOrTracker(t *testing.T) {
	pool := servicePool()
	svc, repo, mc, _ := newTestService(t, pool)
	mc.tickResults[-100] = goodTickResult(t)
	mc.tickResults[200] = goodTickResult(t)
	repo.err = fmt.Errorf("db down")

	receipt := shared.TransactionReceipt{Logs: []shared.Log{modifyLog(t, pool, "0x0", -100, 200, 5000)}}
	bh := svc.BlockHandler()
	if err := bh(context.Background(), blockEvent(200), []shared.TransactionReceipt{receipt}); err == nil {
		t.Fatal("BlockHandler: want error when SaveBlock fails, got nil")
	}
	baselineAfterFailure := mc.baselineCalls
	stateAfterFailure := mc.stateCalls
	if baselineAfterFailure == 0 {
		t.Fatal("the baseline scan should have been attempted on the failed first call")
	}

	repo.err = nil
	if err := bh(context.Background(), blockEvent(200), []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("BlockHandler (retry): %v", err)
	}
	if mc.baselineCalls <= baselineAfterFailure {
		t.Errorf("baseline scans after the retry = %d, want more than %d", mc.baselineCalls, baselineAfterFailure)
	}
	if mc.stateCalls <= stateAfterFailure {
		t.Errorf("state snapshots after the retry = %d, want more than %d", mc.stateCalls, stateAfterFailure)
	}
	if repo.saveBlockCalls != 2 {
		t.Errorf("SaveBlock calls = %d, want 2 (failed attempt + successful retry)", repo.saveBlockCalls)
	}
}

func TestBlockHandler_EventBatchPersistError_DoesNotMarkBaseline(t *testing.T) {
	pool := servicePool()
	deps, repo, mc, _ := validServiceDeps(t, []RegisteredPool{pool})
	eventRepo := &fakeEventRepo{err: fmt.Errorf("event db down")}
	deps.EventWriter = dexconsumer.NewProtocolEventWriter(1, eventRepo)
	svc, err := NewUniswapV4Service(deps)
	if err != nil {
		t.Fatalf("NewUniswapV4Service: %v", err)
	}
	mc.tickResults[-100] = goodTickResult(t)
	mc.tickResults[200] = goodTickResult(t)

	receipt := shared.TransactionReceipt{Logs: []shared.Log{modifyLog(t, pool, "0x0", -100, 200, 5000)}}
	bh := svc.BlockHandler()
	if err := bh(context.Background(), blockEvent(200), []shared.TransactionReceipt{receipt}); err == nil {
		t.Fatal("BlockHandler: want error when the captured-events batch write fails, got nil")
	}
	if repo.saveBlockCalls != 1 {
		t.Errorf("SaveBlock calls = %d, want 1", repo.saveBlockCalls)
	}
	baselineAfterFailure := mc.baselineCalls

	eventRepo.err = nil
	if err := bh(context.Background(), blockEvent(200), []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("BlockHandler (retry): %v", err)
	}
	if mc.baselineCalls <= baselineAfterFailure {
		t.Errorf("baseline scans after the retry = %d, want more than %d", mc.baselineCalls, baselineAfterFailure)
	}
}

func TestBlockHandler_NormalBlock_DoesNotReadPriorTicks(t *testing.T) {
	pool := servicePool()
	svc, repo, mc, _ := newTestService(t, pool)
	mc.tickResults[-100] = goodTickResult(t)
	mc.tickResults[200] = goodTickResult(t)

	receipt := shared.TransactionReceipt{Logs: []shared.Log{modifyLog(t, pool, "0x0", -100, 200, 5000)}}
	if err := svc.BlockHandler()(context.Background(), blockEvent(200), []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("BlockHandler: %v", err)
	}
	if len(repo.ticksForPoolCalls) != 0 {
		t.Errorf("TicksForPoolAtBlock calls = %v, want none on a normal (ver==0) block", repo.ticksForPoolCalls)
	}
}

func TestBlockHandler_ReorgRedelivery_RereadsPriorVersionTicks(t *testing.T) {
	pool := servicePool()
	svc, repo, mc, _ := newTestService(t, pool)
	mc.tickResults[-100] = goodTickResult(t)
	mc.tickResults[200] = goodTickResult(t)

	bh := svc.BlockHandler()
	receipt := shared.TransactionReceipt{Logs: []shared.Log{modifyLog(t, pool, "0x0", -100, 200, 5000)}}
	if err := bh(context.Background(), blockEvent(200), []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("BlockHandler (v0): %v", err)
	}

	const priorTick = int32(300)
	mc.tickResults[priorTick] = goodTickResult(t)
	repo.priorTicks = map[fakePoolBlockKey][]int32{{poolID: pool.ID, blockNumber: 200}: {priorTick}}

	reorg := blockEvent(200)
	reorg.Version = 1
	if err := bh(context.Background(), reorg, nil); err != nil {
		t.Fatalf("BlockHandler (v1 reorg redelivery): %v", err)
	}

	want := fakePoolBlockKey{poolID: pool.ID, blockNumber: 200}
	if !slices.Contains(repo.ticksForPoolCalls, want) {
		t.Fatalf("TicksForPoolAtBlock calls = %v, want to include %v", repo.ticksForPoolCalls, want)
	}
	if len(repo.lastWrites.Ticks) != 1 || repo.lastWrites.Ticks[0].Tick != int(priorTick) {
		t.Errorf("v1 tick writes = %+v, want exactly the re-read prior tick %d", repo.lastWrites.Ticks, priorTick)
	}
	if len(repo.lastWrites.States) != 1 {
		t.Errorf("v1 States = %d, want 1 (the reorg rule re-snapshots an untouched pool)", len(repo.lastWrites.States))
	}
}

// TestBlockHandler_ReorgAfterRestart_ResnapshotsPoolsWithStateAtThatBlock pins
// the half of reorg reconciliation the in-memory tracker cannot cover: a fresh
// process redelivered block N at v1 has no record of the orphaned fork, so the
// due set has to come from the rows already persisted at that height.
func TestBlockHandler_ReorgAfterRestart_ResnapshotsPoolsWithStateAtThatBlock(t *testing.T) {
	pool := servicePool()
	svc, repo, mc, txMgr := newTestService(t, pool)

	const priorTick = int32(300)
	mc.tickResults[priorTick] = goodTickResult(t)
	repo.poolsWithState = map[int64][]int64{200: {pool.ID}}
	repo.priorTicks = map[fakePoolBlockKey][]int32{{poolID: pool.ID, blockNumber: 200}: {priorTick}}

	reorg := blockEvent(200)
	reorg.Version = 1
	if err := svc.BlockHandler()(context.Background(), reorg, nil); err != nil {
		t.Fatalf("BlockHandler (v1 reorg redelivery on a fresh service): %v", err)
	}

	wantScope := fakeStateBlockKey{chainID: testChainID, blockNumber: 200, blockTime: time.Unix(200, 0).UTC()}
	if !slices.Contains(repo.poolsWithStateCalls, wantScope) {
		t.Fatalf("PoolIDsWithStateAtBlock calls = %v, want to include %v", repo.poolsWithStateCalls, wantScope)
	}
	if len(repo.lastWrites.States) != 1 || repo.lastWrites.States[0].PoolID != pool.ID {
		t.Fatalf("v1 States = %+v, want one snapshot of pool %d", repo.lastWrites.States, pool.ID)
	}
	want := fakePoolBlockKey{poolID: pool.ID, blockNumber: 200}
	if !slices.Contains(repo.ticksForPoolCalls, want) {
		t.Fatalf("TicksForPoolAtBlock calls = %v, want to include %v", repo.ticksForPoolCalls, want)
	}
	if len(repo.lastWrites.Ticks) != 1 || repo.lastWrites.Ticks[0].Tick != int(priorTick) {
		t.Errorf("v1 tick writes = %+v, want exactly the re-read prior tick %d", repo.lastWrites.Ticks, priorTick)
	}
	if txMgr.calls != 1 {
		t.Errorf("tx calls = %d, want 1", txMgr.calls)
	}
}

// TestBlockHandler_ReorgPriorStateBelowDeployBlock_IsNotScheduled mirrors
// DueSet's deploy gate on the restart path: below a pool's registered deploy
// height there is nothing for StateView to answer, so the pool is skipped
// rather than snapshotted at a height it did not exist at.
func TestBlockHandler_ReorgPriorStateBelowDeployBlock_IsNotScheduled(t *testing.T) {
	pool := servicePool()
	pool.DeployBlock = 500
	svc, repo, mc, txMgr := newTestService(t, pool)
	repo.poolsWithState = map[int64][]int64{200: {pool.ID}}

	reorg := blockEvent(200)
	reorg.Version = 1
	if err := svc.BlockHandler()(context.Background(), reorg, nil); err != nil {
		t.Fatalf("BlockHandler: %v", err)
	}
	if mc.executeAtHashCalls != 0 {
		t.Errorf("chain reads = %d, want 0 below the pool's deploy block", mc.executeAtHashCalls)
	}
	if txMgr.calls != 0 || repo.saveBlockCalls != 0 {
		t.Errorf("tx calls = %d, SaveBlock calls = %d, want 0 and 0", txMgr.calls, repo.saveBlockCalls)
	}
}

// TestBlockHandler_ReorgWithUnregisteredPriorState_Errors keeps the registry
// authoritative: the repository already resolves prior-state ids forward to
// their current version on this chain, so an id the registry still does not
// name is a genuine registry bug, and guessing past it would leave the orphaned
// fork canonical.
func TestBlockHandler_ReorgWithUnregisteredPriorState_Errors(t *testing.T) {
	pool := servicePool()
	svc, repo, _, txMgr := newTestService(t, pool)
	repo.poolsWithState = map[int64][]int64{200: {pool.ID + 1000}}

	reorg := blockEvent(200)
	reorg.Version = 1
	err := svc.BlockHandler()(context.Background(), reorg, nil)
	if err == nil {
		t.Fatal("BlockHandler: want error for a persisted pool absent from the registry, got nil")
	}
	if !strings.Contains(err.Error(), "registry bug") {
		t.Errorf("error %q does not identify the registry bug", err)
	}
	if txMgr.calls != 0 || repo.saveBlockCalls != 0 {
		t.Errorf("tx calls = %d, SaveBlock calls = %d, want 0 and 0", txMgr.calls, repo.saveBlockCalls)
	}
}

// TestBlockHandler_ReorgReReadOfOrphanedPool_PersistsZeroStateTombstone pins the
// counterpart of the tick path's zeroed re-read: a pool whose Initialize the
// reorg orphaned answers all zeros through StateView, and that row has to
// persist to supersede the orphaned fork's snapshot. Refusing it would stall
// the block forever on redelivery.
func TestBlockHandler_ReorgReReadOfOrphanedPool_PersistsZeroStateTombstone(t *testing.T) {
	pool := servicePool()
	svc, repo, mc, txMgr := newTestService(t, pool)

	zeroed := stateFixture{
		sqrtPriceX96:     big.NewInt(0),
		tick:             big.NewInt(0),
		protocolFee:      big.NewInt(0),
		lpFee:            big.NewInt(0),
		liquidity:        big.NewInt(0),
		feeGrowthGlobal0: big.NewInt(0),
		feeGrowthGlobal1: big.NewInt(0),
	}
	mc.stateResults = buildStateResults(t, zeroed)
	repo.poolsWithState = map[int64][]int64{200: {pool.ID}}

	reorg := blockEvent(200)
	reorg.Version = 1
	if err := svc.BlockHandler()(context.Background(), reorg, nil); err != nil {
		t.Fatalf("BlockHandler (v1 re-read of an orphaned pool): %v", err)
	}

	if txMgr.calls != 1 {
		t.Fatalf("tx calls = %d, want 1 (the tombstone must commit)", txMgr.calls)
	}
	if len(repo.lastWrites.States) != 1 {
		t.Fatalf("v1 States = %+v, want one zeroed tombstone row", repo.lastWrites.States)
	}
	if got := repo.lastWrites.States[0]; got.SqrtPriceX96.Sign() != 0 || got.BlockVersion != 1 {
		t.Errorf("tombstone = sqrtPriceX96 %s at block_version %d, want 0 at 1", got.SqrtPriceX96, got.BlockVersion)
	}
}

func TestBlockHandler_ReorgPriorStateReadError_NoPersist(t *testing.T) {
	pool := servicePool()
	svc, repo, _, txMgr := newTestService(t, pool)
	repo.poolsWithStateErr = fmt.Errorf("db down")

	reorg := blockEvent(200)
	reorg.Version = 1
	if err := svc.BlockHandler()(context.Background(), reorg, nil); err == nil {
		t.Fatal("BlockHandler: want error when the prior-state read fails, got nil")
	}
	if txMgr.calls != 0 || repo.saveBlockCalls != 0 {
		t.Errorf("tx calls = %d, SaveBlock calls = %d, want 0 and 0", txMgr.calls, repo.saveBlockCalls)
	}
}

// TestBlockHandler_GovernanceOnlyBlockPersistsCapturedLogsOnly covers a block
// touching no pool: the singleton's non-pool-keyed logs still have to reach
// protocol_event, and nothing may be read from the chain.
func TestBlockHandler_GovernanceOnlyBlockPersistsCapturedLogsOnly(t *testing.T) {
	pool := servicePool()
	deps, repo, mc, txMgr := validServiceDeps(t, []RegisteredPool{pool})
	eventRepo := &fakeEventRepo{}
	deps.EventWriter = dexconsumer.NewProtocolEventWriter(1, eventRepo)
	svc, err := NewUniswapV4Service(deps)
	if err != nil {
		t.Fatalf("NewUniswapV4Service: %v", err)
	}

	ownership := buildLog(t, "OwnershipTransferred",
		[]common.Hash{addrTopic(common.HexToAddress("0xaaa")), addrTopic(common.HexToAddress("0xbbb"))})
	ownership.LogIndex = "0x0"
	unknown := rawLog([]string{common.HexToHash("0xdeadbeef").Hex()}, "0x", "0x1")
	unknown.Address = poolManagerAddr

	receipt := shared.TransactionReceipt{Logs: []shared.Log{ownership, unknown}}
	if err := svc.BlockHandler()(context.Background(), blockEvent(200), []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("BlockHandler: %v", err)
	}

	if mc.executeAtHashCalls != 0 {
		t.Errorf("state reads = %d, want 0 (no pool was touched)", mc.executeAtHashCalls)
	}
	if txMgr.calls != 1 || repo.saveBlockCalls != 1 {
		t.Errorf("tx calls = %d, SaveBlock calls = %d, want 1 and 1", txMgr.calls, repo.saveBlockCalls)
	}
	w := repo.lastWrites
	if len(w.States) != 0 || len(w.Swaps) != 0 || len(w.LiquidityEvents) != 0 || len(w.Ticks) != 0 || len(w.PoolEvents) != 0 {
		t.Errorf("block writes = %+v, want every slice empty", w)
	}
	if len(eventRepo.events) != 2 {
		t.Errorf("protocol events = %d, want 2 (the governance log and the unknown topic0)", len(eventRepo.events))
	}
}

// TestBlockHandler_BaselineAboveTicksPerCallIsChunked pins the tick-read
// batching: a dense pool's first touch must be split into bounded multicalls
// and reassembled without losing or duplicating a tick.
func TestBlockHandler_BaselineAboveTicksPerCallIsChunked(t *testing.T) {
	pool := servicePool()
	svc, repo, mc, _ := newTestService(t, pool)

	const fullWords = 3
	allBits := make([]uint, 0, 256)
	for bit := range 256 {
		allBits = append(allBits, uint(bit))
	}
	wantTicks := make([]int, 0, fullWords*256)
	mc.baselineResults = map[int16]outbound.Result{}
	for word := range int16(fullWords) {
		mc.baselineResults[word] = bitmapWordResult(t, allBits...)
		for _, bit := range allBits {
			tick := tickbitmap.WordBitToTick(word, uint8(bit), pool.TickSpacing)
			wantTicks = append(wantTicks, int(tick))
			mc.tickResults[tick] = goodTickResult(t)
		}
	}

	receipt := shared.TransactionReceipt{Logs: []shared.Log{swapLog(t, pool, "0x0")}}
	if err := svc.BlockHandler()(context.Background(), blockEvent(200), []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("BlockHandler: %v", err)
	}

	wantBatches := (len(wantTicks) + tickbitmap.TicksPerCall - 1) / tickbitmap.TicksPerCall
	if mc.tickBatchCalls != wantBatches {
		t.Errorf("getTickInfo batches = %d, want %d (ceil(%d/%d))", mc.tickBatchCalls, wantBatches, len(wantTicks), tickbitmap.TicksPerCall)
	}
	for i, size := range mc.tickBatchSizes {
		if size > tickbitmap.TicksPerCall {
			t.Errorf("batch %d packed %d calls, want at most %d", i, size, tickbitmap.TicksPerCall)
		}
	}

	got := make([]int, 0, len(repo.lastWrites.Ticks))
	for _, row := range repo.lastWrites.Ticks {
		got = append(got, row.Tick)
	}
	slices.Sort(got)
	slices.Sort(wantTicks)
	if !slices.Equal(got, wantTicks) {
		t.Errorf("persisted %d ticks, want the %d-tick union of every chunk", len(got), len(wantTicks))
	}
}

// newTelemetryService builds a service wired to a REAL dextelemetry.Telemetry
// (the rest of the suite passes nil, making every Record* call a no-op) plus
// the ManualReader that collects it.
func newTelemetryService(t *testing.T, pools []RegisteredPool) (*UniswapV4Service, *fakeUniswapRepo, *metricsdk.ManualReader) {
	t.Helper()

	reader := metricsdk.NewManualReader()
	mp := metricsdk.NewMeterProvider(metricsdk.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		_ = mp.Shutdown(context.Background())
	})

	// NewTelemetry resolves the global meter at construction, so it must run
	// after SetMeterProvider above.
	tel, err := dextelemetry.NewTelemetry("uniswap_v4", testChainID)
	if err != nil {
		t.Fatalf("NewTelemetry: %v", err)
	}

	deps, repo, _, _ := validServiceDeps(t, pools)
	deps.Telemetry = tel
	svc, err := NewUniswapV4Service(deps)
	if err != nil {
		t.Fatalf("NewUniswapV4Service: %v", err)
	}
	return svc, repo, reader
}

// sumCounter returns the counter value for name, and whether the metric exists
// at all. Absent and zero are different states: RecordStateRows/RecordPoolsTouched
// no-op for n<=0, which is what makes the alerts' `A > 0 unless B > 0` shape
// necessary.
func sumCounter(t *testing.T, rm *metricdata.ResourceMetrics, name string) (int64, bool) {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric %s is %T, want Sum[int64]", name, m.Data)
			}
			var total int64
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
			return total, true
		}
	}
	return 0, false
}

func collect(t *testing.T, reader *metricsdk.ManualReader) *metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	return &rm
}

func TestBlockHandler_RecordsStateRowsAndPoolsTouched(t *testing.T) {
	pool := servicePool()
	svc, _, reader := newTelemetryService(t, []RegisteredPool{pool})

	receipt := shared.TransactionReceipt{Logs: []shared.Log{swapLog(t, pool, "0x0")}}
	if err := svc.BlockHandler()(context.Background(), blockEvent(200), []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("BlockHandler: %v", err)
	}

	rm := collect(t, reader)
	if touched, ok := sumCounter(t, rm, "uniswap_v4.pools.touched"); !ok || touched != 1 {
		t.Errorf("uniswap_v4.pools.touched = %d (present=%t), want 1", touched, ok)
	}
	if rows, ok := sumCounter(t, rm, "uniswap_v4.state.rows.written"); !ok || rows != 1 {
		t.Errorf("uniswap_v4.state.rows.written = %d (present=%t), want 1", rows, ok)
	}
}

// TestBlockHandler_RecordsPoolsTouchedOnZeroRowReplay pins the invariant the
// not-writing-state alert is built on: on an idempotent replay (0 state rows) a
// block that touched a pool must still advance pools_touched, or the alert's
// activity gate goes quiet in exactly the case it exists to detect.
func TestBlockHandler_RecordsPoolsTouchedOnZeroRowReplay(t *testing.T) {
	pool := servicePool()
	svc, repo, reader := newTelemetryService(t, []RegisteredPool{pool})

	var zero int64
	repo.stateRowsReturn = &zero

	receipt := shared.TransactionReceipt{Logs: []shared.Log{swapLog(t, pool, "0x0")}}
	if err := svc.BlockHandler()(context.Background(), blockEvent(200), []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("BlockHandler: %v", err)
	}

	rm := collect(t, reader)
	touched, ok := sumCounter(t, rm, "uniswap_v4.pools.touched")
	if !ok {
		t.Fatal("uniswap_v4.pools.touched absent: the alert's activity gate never fired")
	}
	if touched != 1 {
		t.Errorf("uniswap_v4.pools.touched = %d, want 1", touched)
	}
	if rows, ok := sumCounter(t, rm, "uniswap_v4.state.rows.written"); ok {
		t.Errorf("uniswap_v4.state.rows.written = %d, want the counter to be absent (0 rows inserted is a no-op)", rows)
	}
}

// TestBlockHandler_PoolsTouchedCountsTouchedNotDue pins pools_touched to the
// decode-stage touched set rather than the due set: on a reorg redelivery
// DueSet re-snapshots a pool the new fork never touched, so sourcing the gate
// from dueSet would over-count here and collapse to zero under the
// always-empty-DueSet bug the alert targets.
func TestBlockHandler_PoolsTouchedCountsTouchedNotDue(t *testing.T) {
	pool := servicePool()
	svc, _, reader := newTelemetryService(t, []RegisteredPool{pool})
	ctx := context.Background()

	receipt := shared.TransactionReceipt{Logs: []shared.Log{swapLog(t, pool, "0x0")}}
	if err := svc.BlockHandler()(ctx, blockEvent(200), []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("BlockHandler v0: %v", err)
	}

	reorg := blockEvent(200)
	reorg.Version = 1
	if err := svc.BlockHandler()(ctx, reorg, nil); err != nil {
		t.Fatalf("BlockHandler v1: %v", err)
	}

	if touched, _ := sumCounter(t, collect(t, reader), "uniswap_v4.pools.touched"); touched != 1 {
		t.Errorf("uniswap_v4.pools.touched = %d, want 1 (the reorg block touched nothing)", touched)
	}
}

func TestBlockHandler_RecordsErrorsAtTheBoundary(t *testing.T) {
	pool := servicePool()
	svc, _, reader := newTelemetryService(t, []RegisteredPool{pool})

	event := blockEvent(200)
	event.BlockHash = ""
	if err := svc.BlockHandler()(context.Background(), event, nil); err == nil {
		t.Fatal("BlockHandler: want error for an empty BlockHash, got nil")
	}

	if errs, ok := sumCounter(t, collect(t, reader), "uniswap_v4.errors.total"); !ok || errs != 1 {
		t.Errorf("uniswap_v4.errors.total = %d (present=%t), want 1", errs, ok)
	}
}

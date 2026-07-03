package uniswapv3indexer

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/dexconsumer"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

// fakeUniswapRepo records the writes SaveBlock received; ignores the pgx.Tx
// (nil is fine). err, when set, makes SaveBlock fail so tests can exercise
// the failed-persist path (baseline/tracker must not be marked).
type fakeUniswapRepo struct {
	lastWrites      outbound.UniswapV3BlockWrites
	saveBlockCalls  int
	stateRowsReturn int64
	err             error
}

func (r *fakeUniswapRepo) LoadPools(_ context.Context, _ int64) ([]outbound.UniswapV3PoolRow, error) {
	return nil, nil
}

func (r *fakeUniswapRepo) SaveBlock(_ context.Context, _ pgx.Tx, w outbound.UniswapV3BlockWrites) (int64, error) {
	r.saveBlockCalls++
	if r.err != nil {
		return 0, r.err
	}
	r.lastWrites = w
	if r.stateRowsReturn != 0 {
		return r.stateRowsReturn, nil
	}
	return int64(len(w.States)), nil
}

// fakeEventRepo counts saved events via SaveBatch/SaveEvent, satisfying
// outbound.EventRepository. err, when set, makes SaveBatch fail so tests can
// exercise the persistBlock path where the state/swap/tick write succeeds but
// the captured-events write fails.
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

// countingTxManager delegates to fn but counts invocations, so tests can
// assert whether a transaction was opened at all.
type countingTxManager struct {
	calls int
	err   error // if non-nil, returned on the NEXT call, then cleared
}

func (m *countingTxManager) WithTransaction(_ context.Context, fn func(pgx.Tx) error) error {
	m.calls++
	if m.err != nil {
		err := m.err
		m.err = nil
		return err
	}
	return fn(nil)
}

// State-read batch indices, matching the fixed order SnapshotState's reads
// pack calls in (slot0, liquidity, feeGrowthGlobal0/1, protocolFees, both
// token balances, observe). Local to this test file: state.go itself has no
// positional cursor to keep in sync (see shared.RunSnapshotReads).
const (
	testCallSlot0 = iota
	testCallLiquidity
	testCallFeeGrowthGlobal0
	testCallFeeGrowthGlobal1
	testCallProtocolFees
	testCallBalance0
	testCallBalance1
	testCallObserve
	testStateCallCount
)

// recordingMulticaller serves canned results for the state-read batch, the
// touched-tick batch, and the baseline tickBitmap scan, disambiguating a
// batch by decoding each call's selector and packed argument (BuildTickCalls
// packs ticks(int24); BaselineTicks packs tickBitmap(int16)). It records how
// many times each kind of batch ran so tests can assert exactly-once baseline
// reads and no-RPC-on-quiet-block behavior.
type recordingMulticaller struct {
	stateResults    []outbound.Result
	tickResults     map[int32]outbound.Result
	baselineResults map[int16]outbound.Result // defaults to an all-zero (no initialized ticks) word

	executeAtHashCalls int
	stateCalls         int
	tickBatchCalls     int
	baselineCalls      int

	stateErr    error
	tickErr     error // returned instead of dispatching any ticks(int24) batch
	baselineErr error // returned instead of dispatching any tickBitmap(int16) batch
}

func (m *recordingMulticaller) Execute(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
	return nil, fmt.Errorf("Execute must not be called; all reads must pin to a block hash")
}

func (m *recordingMulticaller) ExecuteAtHash(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
	m.executeAtHashCalls++
	if len(calls) == testStateCallCount && m.isStateBatch(calls) {
		m.stateCalls++
		if m.stateErr != nil {
			return nil, m.stateErr
		}
		return m.stateResults, nil
	}
	return m.dispatchTickBatch(calls)
}

// isStateBatch distinguishes the fixed 8-call state batch from a same-length
// coincidence in a tick batch by checking the first call's selector against
// poolStateABI's slot0 selector.
func (m *recordingMulticaller) isStateBatch(calls []outbound.Call) bool {
	stateABI, err := poolStateABI()
	if err != nil || len(calls) == 0 || len(calls[0].CallData) < 4 {
		return false
	}
	return string(calls[0].CallData[:4]) == string(stateABI.Methods["slot0"].ID)
}

// dispatchTickBatch resolves a batch of ticks(int24) or tickBitmap(int16)
// calls against the canned per-argument results, or returns a canned
// transport error for the batch's kind (tickErr/baselineErr) if set.
func (m *recordingMulticaller) dispatchTickBatch(calls []outbound.Call) ([]outbound.Result, error) {
	a, err := tickViewABI()
	if err != nil {
		return nil, err
	}
	if len(calls) > 0 && len(calls[0].CallData) >= 4 {
		switch string(calls[0].CallData[:4]) {
		case string(a.Methods["ticks"].ID):
			if m.tickErr != nil {
				return nil, m.tickErr
			}
		case string(a.Methods["tickBitmap"].ID):
			if m.baselineErr != nil {
				return nil, m.baselineErr
			}
		}
	}
	out := make([]outbound.Result, len(calls))
	isBaseline := false
	isTouchedTick := false
	for i, call := range calls {
		if len(call.CallData) < 4 {
			return nil, fmt.Errorf("test stub: call data too short to carry a selector")
		}
		selector := string(call.CallData[:4])
		switch selector {
		case string(a.Methods["ticks"].ID):
			isTouchedTick = true
			args, err := a.Methods["ticks"].Inputs.Unpack(call.CallData[4:])
			if err != nil {
				return nil, fmt.Errorf("decoding fake ticks() call: %w", err)
			}
			tick := int32(args[0].(*big.Int).Int64())
			res, ok := m.tickResults[tick]
			if !ok {
				return nil, fmt.Errorf("test stub: no canned tick result for tick %d", tick)
			}
			out[i] = res
		case string(a.Methods["tickBitmap"].ID):
			isBaseline = true
			args, err := a.Methods["tickBitmap"].Inputs.Unpack(call.CallData[4:])
			if err != nil {
				return nil, fmt.Errorf("decoding fake tickBitmap() call: %w", err)
			}
			word := args[0].(int16)
			if res, ok := m.baselineResults[word]; ok {
				out[i] = res
				continue
			}
			packed, err := a.Methods["tickBitmap"].Outputs.Pack(big.NewInt(0))
			if err != nil {
				return nil, fmt.Errorf("packing default tickBitmap result: %w", err)
			}
			out[i] = outbound.Result{Success: true, ReturnData: packed}
		default:
			return nil, fmt.Errorf("test stub: unrecognized call selector")
		}
	}
	if isBaseline {
		m.baselineCalls++
	}
	if isTouchedTick {
		m.tickBatchCalls++
	}
	return out, nil
}

func (m *recordingMulticaller) Address() common.Address { return common.Address{} }

// ---------------------------------------------------------------------------
// Fixture factories
// ---------------------------------------------------------------------------

const testChainID = int64(1)

// uniswapTestPool returns a single fixture RegisteredPool used across service
// tests: fee=3000, tickSpacing=60, deployed at block 100.
func uniswapTestPool() RegisteredPool {
	return RegisteredPool{
		ID:             7,
		Address:        common.HexToAddress("0x8ad599c3A0ff1De082011EFDDc58f1908eb6e6D8"),
		Token0:         common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"),
		Token1:         common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
		Token0Decimals: 18,
		Token1Decimals: 6,
		Fee:            3000,
		TickSpacing:    60,
		DeployBlock:    100,
	}
}

// blockEvent builds a minimal outbound.BlockEvent for the given block number,
// with a non-zero test block hash so the suite exercises the real hash-pinned
// read path rather than the zero hash by accident.
func blockEvent(bn int64) outbound.BlockEvent {
	return outbound.BlockEvent{
		ChainID:        testChainID,
		BlockNumber:    bn,
		Version:        0,
		BlockTimestamp: bn,
		BlockHash:      common.HexToHash("0x01").Hex(),
	}
}

// stateResultsFixture builds a full successful 8-result state-call batch,
// reusing state.go's own ABI helpers to stay in lockstep with SnapshotState's
// call ordering. observe() is left reverted (TWAP is optional) to keep the
// fixture simple.
func stateResultsFixture(t *testing.T) []outbound.Result {
	t.Helper()
	stateABI, err := poolStateABI()
	if err != nil {
		t.Fatalf("poolStateABI: %v", err)
	}
	erc20, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("GetERC20ABI: %v", err)
	}

	slot0, err := stateABI.Methods["slot0"].Outputs.Pack(
		big.NewInt(1_234_567_890), big.NewInt(60), uint16(3), uint16(5), uint16(10), uint8(0), true,
	)
	if err != nil {
		t.Fatalf("packing slot0: %v", err)
	}
	liquidity, err := stateABI.Methods["liquidity"].Outputs.Pack(big.NewInt(999_000_000))
	if err != nil {
		t.Fatalf("packing liquidity: %v", err)
	}
	fg0, err := stateABI.Methods["feeGrowthGlobal0X128"].Outputs.Pack(big.NewInt(111))
	if err != nil {
		t.Fatalf("packing feeGrowthGlobal0X128: %v", err)
	}
	fg1, err := stateABI.Methods["feeGrowthGlobal1X128"].Outputs.Pack(big.NewInt(222))
	if err != nil {
		t.Fatalf("packing feeGrowthGlobal1X128: %v", err)
	}
	protocolFees, err := stateABI.Methods["protocolFees"].Outputs.Pack(big.NewInt(10), big.NewInt(20))
	if err != nil {
		t.Fatalf("packing protocolFees: %v", err)
	}
	bal0, err := erc20.Methods["balanceOf"].Outputs.Pack(big.NewInt(5_000_000))
	if err != nil {
		t.Fatalf("packing balanceOf (token0): %v", err)
	}
	bal1, err := erc20.Methods["balanceOf"].Outputs.Pack(big.NewInt(6_000_000))
	if err != nil {
		t.Fatalf("packing balanceOf (token1): %v", err)
	}

	results := make([]outbound.Result, testStateCallCount)
	results[testCallSlot0] = outbound.Result{Success: true, ReturnData: slot0}
	results[testCallLiquidity] = outbound.Result{Success: true, ReturnData: liquidity}
	results[testCallFeeGrowthGlobal0] = outbound.Result{Success: true, ReturnData: fg0}
	results[testCallFeeGrowthGlobal1] = outbound.Result{Success: true, ReturnData: fg1}
	results[testCallProtocolFees] = outbound.Result{Success: true, ReturnData: protocolFees}
	results[testCallBalance0] = outbound.Result{Success: true, ReturnData: bal0}
	results[testCallBalance1] = outbound.Result{Success: true, ReturnData: bal1}
	results[testCallObserve] = outbound.Result{Success: false}
	return results
}

// goodTickResult returns a successful ticks() call result with sane default
// values.
func goodTickResult(t *testing.T) outbound.Result {
	t.Helper()
	a, err := tickViewABI()
	if err != nil {
		t.Fatalf("tickViewABI: %v", err)
	}
	packed, err := a.Methods["ticks"].Outputs.Pack(
		big.NewInt(1000), // liquidityGross
		big.NewInt(500),  // liquidityNet
		big.NewInt(1),    // feeGrowthOutside0X128
		big.NewInt(2),    // feeGrowthOutside1X128
		big.NewInt(0),    // tickCumulativeOutside
		big.NewInt(0),    // secondsPerLiquidityOutsideX128
		uint32(0),        // secondsOutside
		true,             // initialized
	)
	if err != nil {
		t.Fatalf("packing ticks() return: %v", err)
	}
	return outbound.Result{Success: true, ReturnData: packed}
}

// poolABIForTest, addrTopic, signedTopic, and buildLog are declared in
// event_decode_test.go (same package); reused here rather than redeclared.

// swapLog builds a Swap event log for pool, using the real pool ABI so
// DecodeEvents can decode it.
func swapLog(t *testing.T, pool RegisteredPool, logIndexHex string) shared.Log {
	t.Helper()
	a := poolABIForTest(t)
	return buildLog(t, a, "Swap", pool.Address, logIndexHex,
		[]common.Hash{addrTopic(common.HexToAddress("0xaaa")), addrTopic(common.HexToAddress("0xbbb"))},
		big.NewInt(-100), big.NewInt(200), big.NewInt(1234567890), big.NewInt(999), big.NewInt(60),
	)
}

// mintLog builds a Mint event log for pool with the given tick range.
func mintLog(t *testing.T, pool RegisteredPool, logIndexHex string, tickLower, tickUpper int64) shared.Log {
	t.Helper()
	a := poolABIForTest(t)
	owner := common.HexToAddress("0x2222222222222222222222222222222222222222")
	sender := common.HexToAddress("0x1111111111111111111111111111111111111111")
	return buildLog(t, a, "Mint", pool.Address, logIndexHex,
		[]common.Hash{addrTopic(owner), signedTopic(tickLower), signedTopic(tickUpper)},
		sender, big.NewInt(500000), big.NewInt(1000), big.NewInt(2000),
	)
}

// poolEventLog builds an IncreaseObservationCardinalityNext log for pool: a
// third, distinct decoded-event kind (a low-frequency PoolEvent, neither Swap
// nor a liquidity event) so the mixed-events test exercises all three
// DecodedEvents buckets routing into BlockWrites and the capture net.
//
// A genuinely-unknown-topic0 log is deliberately not used here: uniswapv3indexer's
// rawCapturedLog always passes EventName="" for an unrecognized topic0 (unlike
// curve, which forwards topic0's hex), which fails entity.ProtocolEvent's
// non-empty EventName invariant the moment it reaches the event writer. That
// gap predates this task (see event_decode.go and
// TestDecodeEvents_UnknownTopic0IsCapturedRaw) and is out of scope here; see
// the B9 report's nice-to-haves.
func poolEventLog(t *testing.T, pool RegisteredPool, logIndexHex string) shared.Log {
	t.Helper()
	a := poolABIForTest(t)
	return buildLog(t, a, "IncreaseObservationCardinalityNext", pool.Address, logIndexHex, nil,
		uint16(100), uint16(200),
	)
}

// newTestService builds a UniswapV3Service with a single pool, a
// recordingMulticaller pre-loaded with a successful state-read batch, and a
// simple fake repo/tx manager. Returns everything a test needs to make
// assertions.
func newTestService(t *testing.T, pool RegisteredPool) (*UniswapV3Service, *fakeUniswapRepo, *recordingMulticaller, *countingTxManager) {
	t.Helper()

	mc := &recordingMulticaller{
		stateResults: stateResultsFixture(t),
		tickResults:  map[int32]outbound.Result{},
	}
	repo := &fakeUniswapRepo{}
	txMgr := &countingTxManager{}
	writer := dexconsumer.NewProtocolEventWriter(1, &fakeEventRepo{})

	svc, err := NewUniswapV3Service(UniswapV3ServiceDeps{
		Pools:       []RegisteredPool{pool},
		Multicaller: mc,
		Repo:        repo,
		EventWriter: writer,
		TxManager:   txMgr,
		ChainID:     testChainID,
		Logger:      slog.New(slog.NewTextHandler(os.Stderr, nil)),
	})
	if err != nil {
		t.Fatalf("NewUniswapV3Service: %v", err)
	}
	return svc, repo, mc, txMgr
}

// ---------------------------------------------------------------------------
// Construction validation
// ---------------------------------------------------------------------------

func TestNewUniswapV3Service_ValidatesDeps(t *testing.T) {
	pool := uniswapTestPool()
	mc := &recordingMulticaller{}
	repo := &fakeUniswapRepo{}
	txMgr := &countingTxManager{}
	writer := dexconsumer.NewProtocolEventWriter(1, &fakeEventRepo{})
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	validDeps := func() UniswapV3ServiceDeps {
		return UniswapV3ServiceDeps{
			Pools:       []RegisteredPool{pool},
			Multicaller: mc,
			Repo:        repo,
			EventWriter: writer,
			TxManager:   txMgr,
			ChainID:     testChainID,
			Logger:      logger,
		}
	}

	tests := []struct {
		name   string
		mutate func(d UniswapV3ServiceDeps) UniswapV3ServiceDeps
	}{
		{
			name: "nil multicaller",
			mutate: func(d UniswapV3ServiceDeps) UniswapV3ServiceDeps {
				d.Multicaller = nil
				return d
			},
		},
		{
			name: "nil repo",
			mutate: func(d UniswapV3ServiceDeps) UniswapV3ServiceDeps {
				d.Repo = nil
				return d
			},
		},
		{
			name: "nil event writer",
			mutate: func(d UniswapV3ServiceDeps) UniswapV3ServiceDeps {
				d.EventWriter = nil
				return d
			},
		},
		{
			name: "nil tx manager",
			mutate: func(d UniswapV3ServiceDeps) UniswapV3ServiceDeps {
				d.TxManager = nil
				return d
			},
		},
		{
			name: "nil logger",
			mutate: func(d UniswapV3ServiceDeps) UniswapV3ServiceDeps {
				d.Logger = nil
				return d
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewUniswapV3Service(tt.mutate(validDeps()))
			if err == nil {
				t.Fatalf("NewUniswapV3Service: want error for %s, got nil", tt.name)
			}
		})
	}
}

func TestNewUniswapV3Service_EmptyPoolsIsAllowed(t *testing.T) {
	mc := &recordingMulticaller{}
	repo := &fakeUniswapRepo{}
	txMgr := &countingTxManager{}
	writer := dexconsumer.NewProtocolEventWriter(1, &fakeEventRepo{})

	_, err := NewUniswapV3Service(UniswapV3ServiceDeps{
		Pools:       nil,
		Multicaller: mc,
		Repo:        repo,
		EventWriter: writer,
		TxManager:   txMgr,
		ChainID:     testChainID,
		Logger:      slog.New(slog.NewTextHandler(os.Stderr, nil)),
	})
	if err != nil {
		t.Fatalf("NewUniswapV3Service: want no error with nil Pools, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// BlockHandler: core orchestration
// ---------------------------------------------------------------------------

// TestBlockHandler_MixedEventsPersistsBlockWrites verifies that a block with a
// swap, a mint, and a low-frequency pool event produces the right BlockWrites
// (one state row for the touched pool, one swap, one liquidity event, one
// pool event, two tick rows for the mint's bounds) and forwards the decoded
// logs to the event writer.
func TestBlockHandler_MixedEventsPersistsBlockWrites(t *testing.T) {
	pool := uniswapTestPool()
	svc, repo, mc, txMgr := newTestService(t, pool)
	mc.tickResults[-120] = goodTickResult(t)
	mc.tickResults[180] = goodTickResult(t)

	receipt := shared.TransactionReceipt{
		Logs: []shared.Log{
			swapLog(t, pool, "0x0"),
			mintLog(t, pool, "0x1", -120, 180),
			poolEventLog(t, pool, "0x2"),
		},
	}

	bh := svc.BlockHandler()
	event := blockEvent(200)
	if err := bh(context.Background(), event, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("BlockHandler: %v", err)
	}

	if txMgr.calls != 1 {
		t.Fatalf("WithTransaction calls = %d, want 1", txMgr.calls)
	}
	if len(repo.lastWrites.States) != 1 {
		t.Errorf("States = %d, want 1", len(repo.lastWrites.States))
	}
	if len(repo.lastWrites.Swaps) != 1 {
		t.Errorf("Swaps = %d, want 1", len(repo.lastWrites.Swaps))
	}
	if len(repo.lastWrites.LiquidityEvents) != 1 {
		t.Errorf("LiquidityEvents = %d, want 1", len(repo.lastWrites.LiquidityEvents))
	}
	// The mint's tickLower/tickUpper (-120, 180); the pool's first-touch
	// baseline scan reports no additional initialized ticks in this fixture
	// (recordingMulticaller defaults every unlisted tickBitmap word to zero).
	if len(repo.lastWrites.Ticks) != 2 {
		t.Errorf("Ticks = %d, want 2 (mint's tickLower/tickUpper)", len(repo.lastWrites.Ticks))
	}
	if len(repo.lastWrites.PoolEvents) != 1 {
		t.Errorf("PoolEvents = %d, want 1", len(repo.lastWrites.PoolEvents))
	}
}

// secondUniswapTestPool returns a second fixture pool, distinct in ID and
// address from uniswapTestPool, so tests can exercise a single receipt whose
// logs touch two registered pools (e.g. an aggregator split route across two
// fee tiers of the same token pair).
func secondUniswapTestPool() RegisteredPool {
	return RegisteredPool{
		ID:             8,
		Address:        common.HexToAddress("0x109830a1AAaD605BbF02a9dFA7B0B92EC2FB7dAa"),
		Token0:         common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"),
		Token1:         common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
		Token0Decimals: 18,
		Token1Decimals: 6,
		Fee:            500,
		TickSpacing:    10,
		DeployBlock:    100,
	}
}

// newTestServiceWithPools builds a UniswapV3Service registering every pool in
// pools, wired to a recordingMulticaller pre-loaded with a successful state
// batch. Mirrors newTestService but supports the multi-pool receipt case.
func newTestServiceWithPools(t *testing.T, pools []RegisteredPool) (*UniswapV3Service, *fakeUniswapRepo, *recordingMulticaller, *countingTxManager) {
	t.Helper()

	mc := &recordingMulticaller{
		stateResults: stateResultsFixture(t),
		tickResults:  map[int32]outbound.Result{},
	}
	repo := &fakeUniswapRepo{}
	txMgr := &countingTxManager{}
	writer := dexconsumer.NewProtocolEventWriter(1, &fakeEventRepo{})

	svc, err := NewUniswapV3Service(UniswapV3ServiceDeps{
		Pools:       pools,
		Multicaller: mc,
		Repo:        repo,
		EventWriter: writer,
		TxManager:   txMgr,
		ChainID:     testChainID,
		Logger:      slog.New(slog.NewTextHandler(os.Stderr, nil)),
	})
	if err != nil {
		t.Fatalf("NewUniswapV3Service: %v", err)
	}
	return svc, repo, mc, txMgr
}

// TestBlockHandler_MultiPoolReceiptDecodesEveryTouchedPool verifies that a
// single receipt carrying logs from TWO registered pools (an aggregator split
// route) decodes BOTH pools' swaps and snapshots BOTH pools' state — the
// second pool must not be silently dropped.
func TestBlockHandler_MultiPoolReceiptDecodesEveryTouchedPool(t *testing.T) {
	poolA := uniswapTestPool()
	poolB := secondUniswapTestPool()
	svc, repo, _, txMgr := newTestServiceWithPools(t, []RegisteredPool{poolA, poolB})

	receipt := shared.TransactionReceipt{
		Logs: []shared.Log{
			swapLog(t, poolA, "0x0"),
			swapLog(t, poolB, "0x1"),
		},
	}

	bh := svc.BlockHandler()
	if err := bh(context.Background(), blockEvent(200), []shared.TransactionReceipt{receipt}); err != nil {
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

	gotPools := map[int64]bool{}
	for _, s := range repo.lastWrites.Swaps {
		gotPools[s.PoolID] = true
	}
	if !gotPools[poolA.ID] {
		t.Errorf("swaps missing pool A (id=%d)", poolA.ID)
	}
	if !gotPools[poolB.ID] {
		t.Errorf("swaps missing pool B (id=%d) — second pool silently dropped", poolB.ID)
	}
}

// TestBlockHandler_QuietBlock_NoTransaction verifies a block whose receipts
// touch no registered pool returns nil without opening a transaction or
// issuing any multicall.
func TestBlockHandler_QuietBlock_NoTransaction(t *testing.T) {
	pool := uniswapTestPool()
	svc, repo, mc, txMgr := newTestService(t, pool)

	unrelated := shared.TransactionReceipt{
		Logs: []shared.Log{{
			Address:         "0x0000000000000000000000000000000000009999",
			Topics:          []string{"0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"},
			Data:            "0x",
			TransactionHash: "0xdeadbeef",
			LogIndex:        "0x0",
		}},
	}

	bh := svc.BlockHandler()
	if err := bh(context.Background(), blockEvent(200), []shared.TransactionReceipt{unrelated}); err != nil {
		t.Fatalf("BlockHandler: %v", err)
	}

	if txMgr.calls != 0 {
		t.Errorf("WithTransaction calls = %d, want 0 (quiet block must skip the transaction)", txMgr.calls)
	}
	if mc.executeAtHashCalls != 0 {
		t.Errorf("ExecuteAtHash calls = %d, want 0 (no RPC needed for an untouched pool)", mc.executeAtHashCalls)
	}
	if repo.saveBlockCalls != 0 {
		t.Errorf("SaveBlock calls = %d, want 0", repo.saveBlockCalls)
	}
}

// TestBlockHandler_TouchedBelowDeployBlock_Errors verifies that a pool touched
// at a block below its registered deploy block surfaces DueSet's registry-bug
// error and never persists.
func TestBlockHandler_TouchedBelowDeployBlock_Errors(t *testing.T) {
	pool := uniswapTestPool()
	pool.DeployBlock = 500 // above the block we're about to process
	svc, repo, _, txMgr := newTestService(t, pool)

	receipt := shared.TransactionReceipt{Logs: []shared.Log{swapLog(t, pool, "0x0")}}

	bh := svc.BlockHandler()
	if err := bh(context.Background(), blockEvent(100), []shared.TransactionReceipt{receipt}); err == nil {
		t.Fatal("BlockHandler: want error for a pool touched below its deploy block, got nil")
	}
	if txMgr.calls != 0 {
		t.Errorf("WithTransaction calls = %d, want 0 (registry-bug error must not persist)", txMgr.calls)
	}
	if repo.saveBlockCalls != 0 {
		t.Errorf("SaveBlock calls = %d, want 0", repo.saveBlockCalls)
	}
}

// TestBlockHandler_EmptyBlockHash_Errors verifies an event with an empty
// BlockHash fails loud before any multicall or transaction.
func TestBlockHandler_EmptyBlockHash_Errors(t *testing.T) {
	pool := uniswapTestPool()
	svc, repo, mc, txMgr := newTestService(t, pool)

	event := blockEvent(200)
	event.BlockHash = ""

	bh := svc.BlockHandler()
	if err := bh(context.Background(), event, nil); err == nil {
		t.Fatal("BlockHandler: want error for empty BlockHash, got nil")
	}
	if mc.executeAtHashCalls != 0 {
		t.Errorf("ExecuteAtHash calls = %d, want 0", mc.executeAtHashCalls)
	}
	if txMgr.calls != 0 {
		t.Errorf("WithTransaction calls = %d, want 0", txMgr.calls)
	}
	if repo.saveBlockCalls != 0 {
		t.Errorf("SaveBlock calls = %d, want 0", repo.saveBlockCalls)
	}
}

// TestBlockHandler_StateSnapshotError_NoPersist verifies that a multicall
// failure on the state-read path surfaces as a BlockHandler error with no
// persistence attempted.
func TestBlockHandler_StateSnapshotError_NoPersist(t *testing.T) {
	pool := uniswapTestPool()
	svc, repo, mc, txMgr := newTestService(t, pool)
	mc.stateErr = fmt.Errorf("rpc down")

	receipt := shared.TransactionReceipt{Logs: []shared.Log{swapLog(t, pool, "0x0")}}

	bh := svc.BlockHandler()
	if err := bh(context.Background(), blockEvent(200), []shared.TransactionReceipt{receipt}); err == nil {
		t.Fatal("BlockHandler: want error when state snapshot fails, got nil")
	}
	if txMgr.calls != 0 {
		t.Errorf("WithTransaction calls = %d, want 0", txMgr.calls)
	}
	if repo.saveBlockCalls != 0 {
		t.Errorf("SaveBlock calls = %d, want 0", repo.saveBlockCalls)
	}
}

// ---------------------------------------------------------------------------
// Baseline tick enumeration (first-seen pools)
// ---------------------------------------------------------------------------

// TestBlockHandler_FirstTouchReadsBaselineTicksOnce verifies a pool's baseline
// ticks are enumerated and persisted exactly once: on the pool's first touch,
// and NOT again on a later touch of the same pool.
func TestBlockHandler_FirstTouchReadsBaselineTicksOnce(t *testing.T) {
	pool := uniswapTestPool()
	svc, _, mc, _ := newTestService(t, pool)
	mc.tickResults[-120] = goodTickResult(t)
	mc.tickResults[180] = goodTickResult(t)

	receipt1 := shared.TransactionReceipt{Logs: []shared.Log{mintLog(t, pool, "0x0", -120, 180)}}
	bh := svc.BlockHandler()
	if err := bh(context.Background(), blockEvent(200), []shared.TransactionReceipt{receipt1}); err != nil {
		t.Fatalf("BlockHandler (first touch): %v", err)
	}
	firstBaselineCalls := mc.baselineCalls
	if firstBaselineCalls == 0 {
		t.Fatal("baseline tickBitmap scan should run on first touch")
	}

	// Second touch, later block: mints the same range again. Baseline must not
	// be re-scanned.
	receipt2 := shared.TransactionReceipt{Logs: []shared.Log{mintLog(t, pool, "0x1", -120, 180)}}
	if err := bh(context.Background(), blockEvent(201), []shared.TransactionReceipt{receipt2}); err != nil {
		t.Fatalf("BlockHandler (second touch): %v", err)
	}
	if mc.baselineCalls != firstBaselineCalls {
		t.Errorf("baseline tickBitmap scans after second touch = %d, want %d (no re-scan)", mc.baselineCalls, firstBaselineCalls)
	}
}

// TestBlockHandler_SwapOnlyTouchAfterBaselined_NoTickReadNeeded verifies that
// once a pool's baseline has been enumerated, a later block that touches the
// pool via a Swap only (no Mint/Burn/Collect) needs no tick multicall at all:
// TouchedTicks is empty and the baseline is already seen, so readTicks must
// short-circuit rather than issue a zero-length batch.
func TestBlockHandler_SwapOnlyTouchAfterBaselined_NoTickReadNeeded(t *testing.T) {
	pool := uniswapTestPool()
	svc, repo, mc, _ := newTestService(t, pool)
	mc.tickResults[-120] = goodTickResult(t)
	mc.tickResults[180] = goodTickResult(t)

	mintReceipt := shared.TransactionReceipt{Logs: []shared.Log{mintLog(t, pool, "0x0", -120, 180)}}
	bh := svc.BlockHandler()
	if err := bh(context.Background(), blockEvent(200), []shared.TransactionReceipt{mintReceipt}); err != nil {
		t.Fatalf("BlockHandler (mint touch): %v", err)
	}
	tickBatchCallsAfterMint := mc.tickBatchCalls

	swapReceipt := shared.TransactionReceipt{Logs: []shared.Log{swapLog(t, pool, "0x0")}}
	if err := bh(context.Background(), blockEvent(201), []shared.TransactionReceipt{swapReceipt}); err != nil {
		t.Fatalf("BlockHandler (swap touch): %v", err)
	}
	if mc.tickBatchCalls != tickBatchCallsAfterMint {
		t.Errorf("tick batch calls after a swap-only touch = %d, want %d (no ticks touched, baseline already seen)", mc.tickBatchCalls, tickBatchCallsAfterMint)
	}
	if len(repo.lastWrites.Ticks) != 0 {
		t.Errorf("Ticks = %d, want 0 for a swap-only touch on an already-baselined pool", len(repo.lastWrites.Ticks))
	}
}

// TestBlockHandler_FailedPersist_DoesNotMarkBaselineOrTracker verifies that
// when persistence fails, neither baselineSeen nor the snapshot tracker are
// updated, so a retried block re-enumerates the baseline and re-snapshots.
func TestBlockHandler_FailedPersist_DoesNotMarkBaselineOrTracker(t *testing.T) {
	pool := uniswapTestPool()
	svc, repo, mc, _ := newTestService(t, pool)
	mc.tickResults[-120] = goodTickResult(t)
	mc.tickResults[180] = goodTickResult(t)
	repo.err = fmt.Errorf("db down")

	receipt := shared.TransactionReceipt{Logs: []shared.Log{mintLog(t, pool, "0x0", -120, 180)}}
	bh := svc.BlockHandler()
	if err := bh(context.Background(), blockEvent(200), []shared.TransactionReceipt{receipt}); err == nil {
		t.Fatal("BlockHandler: want error when SaveBlock fails, got nil")
	}
	firstBaselineCalls := mc.baselineCalls
	firstStateCalls := mc.stateCalls
	if firstBaselineCalls == 0 {
		t.Fatal("baseline scan should have been attempted on the failed first call")
	}

	// Retry (as SQS redelivery would): repo now succeeds. Baseline must be
	// re-enumerated (not skipped as already-seen) and the pool must still be
	// snapshotted (tracker must not have marked it done from the failed call).
	repo.err = nil
	if err := bh(context.Background(), blockEvent(200), []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("BlockHandler (retry): %v", err)
	}
	if mc.baselineCalls <= firstBaselineCalls {
		t.Errorf("baseline tickBitmap scans after retry = %d, want more than %d (must re-enumerate after a failed persist)", mc.baselineCalls, firstBaselineCalls)
	}
	if mc.stateCalls <= firstStateCalls {
		t.Errorf("state snapshot calls after retry = %d, want more than %d (pool must still be due after a failed persist)", mc.stateCalls, firstStateCalls)
	}
	if repo.saveBlockCalls != 2 {
		t.Errorf("SaveBlock calls = %d, want 2 (failed attempt + successful retry)", repo.saveBlockCalls)
	}
}

// ---------------------------------------------------------------------------
// Error-path coverage: decode, cancellation, tick/baseline reads, event-batch persist
// ---------------------------------------------------------------------------

// TestBlockHandler_DecodeError_ReturnsNonNil verifies a receipt with a known
// topic0 but malformed (truncated) data surfaces DecodeEvents' error as a
// BlockHandler error, distinct from the invalid-log-address error path.
func TestBlockHandler_DecodeError_ReturnsNonNil(t *testing.T) {
	pool := uniswapTestPool()
	svc, repo, _, txMgr := newTestService(t, pool)

	a := poolABIForTest(t)
	ev := a.Events["Swap"]
	badReceipt := shared.TransactionReceipt{
		Logs: []shared.Log{{
			Address: pool.Address.Hex(),
			Topics: []string{
				ev.ID.Hex(),
				addrTopic(common.HexToAddress("0xaaa")).Hex(),
				addrTopic(common.HexToAddress("0xbbb")).Hex(),
			},
			Data:            "0xdead", // too short to unpack Swap's non-indexed args
			TransactionHash: common.HexToHash("0xdeadbeef").Hex(),
			LogIndex:        "0x0",
		}},
	}

	bh := svc.BlockHandler()
	if err := bh(context.Background(), blockEvent(200), []shared.TransactionReceipt{badReceipt}); err == nil {
		t.Fatal("BlockHandler: want error on decode failure, got nil")
	}
	if txMgr.calls != 0 {
		t.Errorf("WithTransaction calls = %d, want 0", txMgr.calls)
	}
	if repo.saveBlockCalls != 0 {
		t.Errorf("SaveBlock calls = %d, want 0", repo.saveBlockCalls)
	}
}

// TestBlockHandler_InvalidLogAddress_ReturnsNonNil verifies a log whose
// address is not valid hex surfaces poolsTouchedByReceipt's error rather than
// being silently skipped.
func TestBlockHandler_InvalidLogAddress_ReturnsNonNil(t *testing.T) {
	pool := uniswapTestPool()
	svc, repo, _, txMgr := newTestService(t, pool)

	badReceipt := shared.TransactionReceipt{
		Logs: []shared.Log{{
			Address:         "0x123", // too short to be a valid address
			Topics:          []string{"0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"},
			Data:            "0x",
			TransactionHash: "0xabc",
			LogIndex:        "0x0",
		}},
	}

	bh := svc.BlockHandler()
	if err := bh(context.Background(), blockEvent(200), []shared.TransactionReceipt{badReceipt}); err == nil {
		t.Fatal("BlockHandler: want error for an invalid log address, got nil")
	}
	if txMgr.calls != 0 {
		t.Errorf("WithTransaction calls = %d, want 0", txMgr.calls)
	}
	if repo.saveBlockCalls != 0 {
		t.Errorf("SaveBlock calls = %d, want 0", repo.saveBlockCalls)
	}
}

// TestBlockHandler_CanceledContext_ReturnsError verifies handleBlock checks
// ctx before decoding each receipt, so a canceled context is never silently
// acked as a successful (empty) block.
func TestBlockHandler_CanceledContext_ReturnsError(t *testing.T) {
	pool := uniswapTestPool()
	svc, repo, _, txMgr := newTestService(t, pool)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	receipt := shared.TransactionReceipt{Logs: []shared.Log{swapLog(t, pool, "0x0")}}
	bh := svc.BlockHandler()
	if err := bh(ctx, blockEvent(200), []shared.TransactionReceipt{receipt}); err == nil {
		t.Fatal("BlockHandler: want error on canceled context, got nil")
	}
	if txMgr.calls != 0 {
		t.Errorf("WithTransaction calls = %d, want 0", txMgr.calls)
	}
	if repo.saveBlockCalls != 0 {
		t.Errorf("SaveBlock calls = %d, want 0", repo.saveBlockCalls)
	}
}

// TestBlockHandler_TouchedTickReadError_NoPersist verifies a multicall
// failure while reading this block's touched ticks (distinct from the
// pool-state read) surfaces as a BlockHandler error with no persistence.
func TestBlockHandler_TouchedTickReadError_NoPersist(t *testing.T) {
	pool := uniswapTestPool()
	svc, repo, mc, txMgr := newTestService(t, pool)
	mc.tickErr = fmt.Errorf("rpc down reading ticks")

	receipt := shared.TransactionReceipt{Logs: []shared.Log{mintLog(t, pool, "0x0", -120, 180)}}
	bh := svc.BlockHandler()
	if err := bh(context.Background(), blockEvent(200), []shared.TransactionReceipt{receipt}); err == nil {
		t.Fatal("BlockHandler: want error when the touched-tick multicall fails, got nil")
	}
	if txMgr.calls != 0 {
		t.Errorf("WithTransaction calls = %d, want 0", txMgr.calls)
	}
	if repo.saveBlockCalls != 0 {
		t.Errorf("SaveBlock calls = %d, want 0", repo.saveBlockCalls)
	}
}

// TestBlockHandler_RevertedTouchedTick_NoPersist verifies a reverted
// ticks() call (DecodeTick's error path) surfaces as a BlockHandler error.
func TestBlockHandler_RevertedTouchedTick_NoPersist(t *testing.T) {
	pool := uniswapTestPool()
	svc, repo, mc, txMgr := newTestService(t, pool)
	mc.tickResults[-120] = outbound.Result{Success: false}
	mc.tickResults[180] = goodTickResult(t)

	receipt := shared.TransactionReceipt{Logs: []shared.Log{mintLog(t, pool, "0x0", -120, 180)}}
	bh := svc.BlockHandler()
	if err := bh(context.Background(), blockEvent(200), []shared.TransactionReceipt{receipt}); err == nil {
		t.Fatal("BlockHandler: want error when a touched tick's ticks() call reverted, got nil")
	}
	if txMgr.calls != 0 {
		t.Errorf("WithTransaction calls = %d, want 0", txMgr.calls)
	}
	if repo.saveBlockCalls != 0 {
		t.Errorf("SaveBlock calls = %d, want 0", repo.saveBlockCalls)
	}
}

// truncatingTickMulticaller wraps a recordingMulticaller and drops the last
// result of any ticks(int24) batch, simulating a multicall provider that
// returns fewer results than requested (a transport-level contract
// violation, not a per-call revert).
type truncatingTickMulticaller struct {
	*recordingMulticaller
}

func (m *truncatingTickMulticaller) ExecuteAtHash(ctx context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
	results, err := m.recordingMulticaller.ExecuteAtHash(ctx, calls, blockHash)
	if err != nil || len(results) == 0 {
		return results, err
	}
	a, abiErr := tickViewABI()
	if abiErr != nil {
		return nil, abiErr
	}
	if len(calls[0].CallData) >= 4 && string(calls[0].CallData[:4]) == string(a.Methods["ticks"].ID) {
		return results[:len(results)-1], nil
	}
	return results, nil
}

// TestBlockHandler_TickResultCountMismatch_NoPersist verifies that a
// multicall returning fewer tick results than requested (a transport
// contract violation, not a per-call revert) surfaces as a BlockHandler
// error rather than silently zipping mismatched results to ticks.
func TestBlockHandler_TickResultCountMismatch_NoPersist(t *testing.T) {
	pool := uniswapTestPool()
	inner := &recordingMulticaller{
		stateResults: stateResultsFixture(t),
		tickResults:  map[int32]outbound.Result{-120: goodTickResult(t), 180: goodTickResult(t)},
	}
	mc := &truncatingTickMulticaller{recordingMulticaller: inner}
	repo := &fakeUniswapRepo{}
	txMgr := &countingTxManager{}
	writer := dexconsumer.NewProtocolEventWriter(1, &fakeEventRepo{})

	svc, err := NewUniswapV3Service(UniswapV3ServiceDeps{
		Pools:       []RegisteredPool{pool},
		Multicaller: mc,
		Repo:        repo,
		EventWriter: writer,
		TxManager:   txMgr,
		ChainID:     testChainID,
		Logger:      slog.New(slog.NewTextHandler(os.Stderr, nil)),
	})
	if err != nil {
		t.Fatalf("NewUniswapV3Service: %v", err)
	}

	receipt := shared.TransactionReceipt{Logs: []shared.Log{mintLog(t, pool, "0x0", -120, 180)}}
	bh := svc.BlockHandler()
	if err := bh(context.Background(), blockEvent(200), []shared.TransactionReceipt{receipt}); err == nil {
		t.Fatal("BlockHandler: want error when the tick multicall returns fewer results than requested, got nil")
	}
	if txMgr.calls != 0 {
		t.Errorf("WithTransaction calls = %d, want 0", txMgr.calls)
	}
	if repo.saveBlockCalls != 0 {
		t.Errorf("SaveBlock calls = %d, want 0", repo.saveBlockCalls)
	}
}

// TestBlockHandler_BaselineReadError_NoPersist verifies a multicall failure
// while enumerating a first-touched pool's baseline ticks surfaces as a
// BlockHandler error, distinct from a touched-tick read failure.
func TestBlockHandler_BaselineReadError_NoPersist(t *testing.T) {
	pool := uniswapTestPool()
	svc, repo, mc, txMgr := newTestService(t, pool)
	mc.tickResults[-120] = goodTickResult(t)
	mc.tickResults[180] = goodTickResult(t)
	mc.baselineErr = fmt.Errorf("rpc down reading tickBitmap")

	receipt := shared.TransactionReceipt{Logs: []shared.Log{mintLog(t, pool, "0x0", -120, 180)}}
	bh := svc.BlockHandler()
	if err := bh(context.Background(), blockEvent(200), []shared.TransactionReceipt{receipt}); err == nil {
		t.Fatal("BlockHandler: want error when the baseline tickBitmap multicall fails, got nil")
	}
	if txMgr.calls != 0 {
		t.Errorf("WithTransaction calls = %d, want 0", txMgr.calls)
	}
	if repo.saveBlockCalls != 0 {
		t.Errorf("SaveBlock calls = %d, want 0", repo.saveBlockCalls)
	}
}

// TestBlockHandler_EventBatchPersistError_NoMark verifies that when the
// state/swap/tick write succeeds but the captured-events batch write fails,
// BlockHandler returns an error and baselineSeen is not marked (the whole
// block's persist is one transaction, so a partial failure must not leave
// the pool's baseline looking already-enumerated).
func TestBlockHandler_EventBatchPersistError_NoMark(t *testing.T) {
	pool := uniswapTestPool()
	mc := &recordingMulticaller{
		stateResults: stateResultsFixture(t),
		tickResults:  map[int32]outbound.Result{},
	}
	repo := &fakeUniswapRepo{}
	txMgr := &countingTxManager{}
	eventRepo := &fakeEventRepo{err: fmt.Errorf("event db down")}
	writer := dexconsumer.NewProtocolEventWriter(1, eventRepo)

	svc, err := NewUniswapV3Service(UniswapV3ServiceDeps{
		Pools:       []RegisteredPool{pool},
		Multicaller: mc,
		Repo:        repo,
		EventWriter: writer,
		TxManager:   txMgr,
		ChainID:     testChainID,
		Logger:      slog.New(slog.NewTextHandler(os.Stderr, nil)),
	})
	if err != nil {
		t.Fatalf("NewUniswapV3Service: %v", err)
	}

	receipt := shared.TransactionReceipt{Logs: []shared.Log{mintLog(t, pool, "0x0", -120, 180)}}
	mc.tickResults[-120] = goodTickResult(t)
	mc.tickResults[180] = goodTickResult(t)

	bh := svc.BlockHandler()
	if err := bh(context.Background(), blockEvent(200), []shared.TransactionReceipt{receipt}); err == nil {
		t.Fatal("BlockHandler: want error when the captured-events batch write fails, got nil")
	}
	if repo.saveBlockCalls != 1 {
		t.Errorf("SaveBlock calls = %d, want 1 (the state/swap write is attempted before the failing event batch)", repo.saveBlockCalls)
	}
	firstBaselineCalls := mc.baselineCalls
	if firstBaselineCalls == 0 {
		t.Fatal("baseline scan should have been attempted on the failed first call")
	}

	// baselineSeen must not have been marked for this pool (the whole block's
	// persist, including the event batch, is one transaction and it failed):
	// a retried block must re-enumerate the baseline rather than skip it.
	eventRepo.err = nil
	if err := bh(context.Background(), blockEvent(200), []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("BlockHandler (retry): %v", err)
	}
	if mc.baselineCalls <= firstBaselineCalls {
		t.Errorf("baseline tickBitmap scans after retry = %d, want more than %d (must re-enumerate after a failed persist)", mc.baselineCalls, firstBaselineCalls)
	}
}

// TestMergeTickSets_DedupsAndSortsOverlappingRanges verifies mergeTickSets
// unions touched and baseline ticks, dropping duplicates and sorting
// ascending even when the inputs interleave out of order.
func TestMergeTickSets_DedupsAndSortsOverlappingRanges(t *testing.T) {
	touched := []int32{180, -120}
	baseline := []int32{60, -120, 300}

	got := mergeTickSets(touched, baseline)
	want := []int32{-120, 60, 180, 300}

	if len(got) != len(want) {
		t.Fatalf("mergeTickSets() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("mergeTickSets()[%d] = %d, want %d (full: got=%v want=%v)", i, got[i], want[i], got, want)
		}
	}
}

package uniswapv3indexer

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

// stateTestPool is the fixture RegisteredPool for state-snapshot tests.
func stateTestPool() RegisteredPool {
	return RegisteredPool{
		ID:             7,
		Address:        common.HexToAddress("0x8ad599c3A0ff1De082011EFDDc58f1908eb6e6D8"),
		Token0:         common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"),
		Token1:         common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
		Token0Decimals: 18,
		Token1Decimals: 6,
		Fee:            3000,
		TickSpacing:    60,
		DeployBlock:    12369739,
	}
}

var stateBlockTS = time.Unix(1700000000, 0).UTC()

// stateMethodsTestABI independently parses the pool state-view and ERC20
// balanceOf method signatures, so tests can pack/unpack calldata and returns
// without depending on state.go's own (possibly buggy) ABI.
func stateMethodsTestABI(t *testing.T) *abi.ABI {
	t.Helper()
	const stateJSON = `[
		{"name":"slot0","type":"function","stateMutability":"view","inputs":[],"outputs":[
			{"name":"sqrtPriceX96","type":"uint160"},
			{"name":"tick","type":"int24"},
			{"name":"observationIndex","type":"uint16"},
			{"name":"observationCardinality","type":"uint16"},
			{"name":"observationCardinalityNext","type":"uint16"},
			{"name":"feeProtocol","type":"uint8"},
			{"name":"unlocked","type":"bool"}
		]},
		{"name":"liquidity","type":"function","stateMutability":"view","inputs":[],"outputs":[{"name":"","type":"uint128"}]},
		{"name":"feeGrowthGlobal0X128","type":"function","stateMutability":"view","inputs":[],"outputs":[{"name":"","type":"uint256"}]},
		{"name":"feeGrowthGlobal1X128","type":"function","stateMutability":"view","inputs":[],"outputs":[{"name":"","type":"uint256"}]},
		{"name":"protocolFees","type":"function","stateMutability":"view","inputs":[],"outputs":[
			{"name":"token0","type":"uint128"},
			{"name":"token1","type":"uint128"}
		]},
		{"name":"balanceOf","type":"function","stateMutability":"view","inputs":[{"name":"account","type":"address"}],"outputs":[{"name":"","type":"uint256"}]},
		{"name":"observe","type":"function","stateMutability":"view","inputs":[{"name":"secondsAgos","type":"uint32[]"}],"outputs":[
			{"name":"tickCumulatives","type":"int56[]"},
			{"name":"secondsPerLiquidityCumulativeX128s","type":"uint160[]"}
		]}
	]`
	a, err := abi.JSON(strings.NewReader(stateJSON))
	if err != nil {
		t.Fatalf("parsing state methods test ABI: %v", err)
	}
	return &a
}

// slot0Fixture holds the values packed into a slot0() return tuple, with
// sane defaults for fields not under test.
type slot0Fixture struct {
	sqrtPriceX96               *big.Int
	tick                       *big.Int
	observationIndex           uint16
	observationCardinality     uint16
	observationCardinalityNext uint16
	feeProtocol                uint8
	unlocked                   bool
}

func defaultSlot0Fixture() slot0Fixture {
	return slot0Fixture{
		sqrtPriceX96:               big.NewInt(1_234_567_890),
		tick:                       big.NewInt(100),
		observationIndex:           3,
		observationCardinality:     5,
		observationCardinalityNext: 10,
		feeProtocol:                0x34, // packed nibbles: feeProtocol1=3, feeProtocol0=4
		unlocked:                   true,
	}
}

func packSlot0Return(t *testing.T, a *abi.ABI, f slot0Fixture) []byte {
	t.Helper()
	packed, err := a.Methods["slot0"].Outputs.Pack(
		f.sqrtPriceX96,
		f.tick,
		f.observationIndex,
		f.observationCardinality,
		f.observationCardinalityNext,
		f.feeProtocol,
		f.unlocked,
	)
	if err != nil {
		t.Fatalf("packing slot0() return: %v", err)
	}
	return packed
}

func packUint256Return(t *testing.T, a *abi.ABI, method string, v *big.Int) []byte {
	t.Helper()
	packed, err := a.Methods[method].Outputs.Pack(v)
	if err != nil {
		t.Fatalf("packing %s() return: %v", method, err)
	}
	return packed
}

func packProtocolFeesReturn(t *testing.T, a *abi.ABI, token0, token1 *big.Int) []byte {
	t.Helper()
	packed, err := a.Methods["protocolFees"].Outputs.Pack(token0, token1)
	if err != nil {
		t.Fatalf("packing protocolFees() return: %v", err)
	}
	return packed
}

func packObserveReturn(t *testing.T, a *abi.ABI, tickCumulatives []*big.Int, secondsPerLiquidity []*big.Int) []byte {
	t.Helper()
	packed, err := a.Methods["observe"].Outputs.Pack(tickCumulatives, secondsPerLiquidity)
	if err != nil {
		t.Fatalf("packing observe() return: %v", err)
	}
	return packed
}

// stateFixtureValues holds every value needed to build a full set of 8
// successful state-call results (slot0, liquidity, feeGrowth0, feeGrowth1,
// protocolFees, balance0, balance1, observe), keyed positionally to match
// SnapshotState's call ordering.
type stateFixtureValues struct {
	slot0               slot0Fixture
	liquidity           *big.Int
	feeGrowthGlobal0    *big.Int
	feeGrowthGlobal1    *big.Int
	protocolFeesToken0  *big.Int
	protocolFeesToken1  *big.Int
	balance0            *big.Int
	balance1            *big.Int
	tickCumulatives     []*big.Int // len 2, [t0, t1]; used when observe succeeds
	secondsPerLiquidity []*big.Int // len 2
	observeSucceeds     bool
}

func defaultStateFixtureValues() stateFixtureValues {
	return stateFixtureValues{
		slot0:               defaultSlot0Fixture(),
		liquidity:           big.NewInt(999_000_000),
		feeGrowthGlobal0:    big.NewInt(111),
		feeGrowthGlobal1:    big.NewInt(222),
		protocolFeesToken0:  big.NewInt(10),
		protocolFeesToken1:  big.NewInt(20),
		balance0:            big.NewInt(5_000_000),
		balance1:            big.NewInt(6_000_000),
		tickCumulatives:     []*big.Int{big.NewInt(1000), big.NewInt(1000 + 1800*100)},
		secondsPerLiquidity: []*big.Int{big.NewInt(0), big.NewInt(0)},
		observeSucceeds:     true,
	}
}

// buildStateResults returns the 8 outbound.Result values (in SnapshotState's
// call order) for the given fixture, and the shared test ABI used to encode
// them, so tests can wire up a MockMulticaller.ExecuteAtHashFn.
func buildStateResults(t *testing.T, f stateFixtureValues) []outbound.Result {
	t.Helper()
	a := stateMethodsTestABI(t)

	results := make([]outbound.Result, 8)
	results[0] = outbound.Result{Success: true, ReturnData: packSlot0Return(t, a, f.slot0)}
	results[1] = outbound.Result{Success: true, ReturnData: packUint256Return(t, a, "liquidity", f.liquidity)}
	results[2] = outbound.Result{Success: true, ReturnData: packUint256Return(t, a, "feeGrowthGlobal0X128", f.feeGrowthGlobal0)}
	results[3] = outbound.Result{Success: true, ReturnData: packUint256Return(t, a, "feeGrowthGlobal1X128", f.feeGrowthGlobal1)}
	results[4] = outbound.Result{Success: true, ReturnData: packProtocolFeesReturn(t, a, f.protocolFeesToken0, f.protocolFeesToken1)}
	results[5] = outbound.Result{Success: true, ReturnData: packUint256Return(t, a, "balanceOf", f.balance0)}
	results[6] = outbound.Result{Success: true, ReturnData: packUint256Return(t, a, "balanceOf", f.balance1)}
	if f.observeSucceeds {
		results[7] = outbound.Result{Success: true, ReturnData: packObserveReturn(t, a, f.tickCumulatives, f.secondsPerLiquidity)}
	} else {
		results[7] = outbound.Result{Success: false}
	}
	return results
}

// mockMulticallerReturning wires a MockMulticaller whose ExecuteAtHash
// returns results (positionally matching the issued calls) and records the
// blockHash it was invoked with into gotHash.
func mockMulticallerReturning(results []outbound.Result, gotHash *common.Hash) *testutil.MockMulticaller {
	mc := testutil.NewMockMulticaller()
	mc.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
		*gotHash = blockHash
		if len(results) != len(calls) {
			return nil, fmt.Errorf("test stub: got %d calls, have %d canned results", len(calls), len(results))
		}
		return results, nil
	}
	return mc
}

// ---------------------------------------------------------------------------
// SnapshotState
// ---------------------------------------------------------------------------

func TestSnapshotState_MapsCoreFields(t *testing.T) {
	pool := stateTestPool()
	blockHash := common.HexToHash("0xabc0000000000000000000000000000000000000000000000000000000001")

	f := defaultStateFixtureValues()
	results := buildStateResults(t, f)
	var gotHash common.Hash
	mc := mockMulticallerReturning(results, &gotHash)

	got, err := SnapshotState(context.Background(), mc, pool, blockHash, 19000000, 2, stateBlockTS)
	if err != nil {
		t.Fatalf("SnapshotState: %v", err)
	}

	if got.PoolID != pool.ID {
		t.Errorf("PoolID = %d, want %d", got.PoolID, pool.ID)
	}
	if got.BlockNumber != 19000000 {
		t.Errorf("BlockNumber = %d, want 19000000", got.BlockNumber)
	}
	if got.BlockVersion != 2 {
		t.Errorf("BlockVersion = %d, want 2", got.BlockVersion)
	}
	if !got.BlockTimestamp.Equal(stateBlockTS) {
		t.Errorf("BlockTimestamp = %v, want %v", got.BlockTimestamp, stateBlockTS)
	}
	if got.SqrtPriceX96.Cmp(f.slot0.sqrtPriceX96) != 0 {
		t.Errorf("SqrtPriceX96 = %s, want %s", got.SqrtPriceX96, f.slot0.sqrtPriceX96)
	}
	if got.Tick != int(f.slot0.tick.Int64()) {
		t.Errorf("Tick = %d, want %d", got.Tick, f.slot0.tick.Int64())
	}
	if got.ObservationIndex != int(f.slot0.observationIndex) {
		t.Errorf("ObservationIndex = %d, want %d", got.ObservationIndex, f.slot0.observationIndex)
	}
	if got.ObservationCardinality != int(f.slot0.observationCardinality) {
		t.Errorf("ObservationCardinality = %d, want %d", got.ObservationCardinality, f.slot0.observationCardinality)
	}
	if got.ObservationCardinalityNext != int(f.slot0.observationCardinalityNext) {
		t.Errorf("ObservationCardinalityNext = %d, want %d", got.ObservationCardinalityNext, f.slot0.observationCardinalityNext)
	}
	if got.FeeProtocol != int(f.slot0.feeProtocol) {
		t.Errorf("FeeProtocol = %d, want %d (raw packed byte, not split)", got.FeeProtocol, f.slot0.feeProtocol)
	}
	if got.Unlocked != f.slot0.unlocked {
		t.Errorf("Unlocked = %v, want %v", got.Unlocked, f.slot0.unlocked)
	}
	if got.Liquidity.Cmp(f.liquidity) != 0 {
		t.Errorf("Liquidity = %s, want %s", got.Liquidity, f.liquidity)
	}
	if got.FeeGrowthGlobal0X128.Cmp(f.feeGrowthGlobal0) != 0 {
		t.Errorf("FeeGrowthGlobal0X128 = %s, want %s", got.FeeGrowthGlobal0X128, f.feeGrowthGlobal0)
	}
	if got.FeeGrowthGlobal1X128.Cmp(f.feeGrowthGlobal1) != 0 {
		t.Errorf("FeeGrowthGlobal1X128 = %s, want %s", got.FeeGrowthGlobal1X128, f.feeGrowthGlobal1)
	}
	if got.ProtocolFeesToken0.Cmp(f.protocolFeesToken0) != 0 {
		t.Errorf("ProtocolFeesToken0 = %s, want %s", got.ProtocolFeesToken0, f.protocolFeesToken0)
	}
	if got.ProtocolFeesToken1.Cmp(f.protocolFeesToken1) != 0 {
		t.Errorf("ProtocolFeesToken1 = %s, want %s", got.ProtocolFeesToken1, f.protocolFeesToken1)
	}
	if got.Balance0.Cmp(f.balance0) != 0 {
		t.Errorf("Balance0 = %s, want %s", got.Balance0, f.balance0)
	}
	if got.Balance1.Cmp(f.balance1) != 0 {
		t.Errorf("Balance1 = %s, want %s", got.Balance1, f.balance1)
	}
	if err := got.Validate(); err != nil {
		t.Errorf("Validate: %v", err)
	}

	if gotHash != blockHash {
		t.Errorf("ExecuteAtHash blockHash = %s, want %s", gotHash, blockHash)
	}
	if mc.CallCount != 1 {
		t.Errorf("ExecuteAtHash invocation count = %d, want 1 (single batched multicall)", mc.CallCount)
	}
}

func TestSnapshotState_NegativeSlot0Tick(t *testing.T) {
	pool := stateTestPool()
	blockHash := common.HexToHash("0xabc0000000000000000000000000000000000000000000000000000000002")

	f := defaultStateFixtureValues()
	f.slot0.tick = big.NewInt(-887220)
	results := buildStateResults(t, f)
	var gotHash common.Hash
	mc := mockMulticallerReturning(results, &gotHash)

	got, err := SnapshotState(context.Background(), mc, pool, blockHash, 19000000, 0, stateBlockTS)
	if err != nil {
		t.Fatalf("SnapshotState: %v", err)
	}
	if got.Tick != -887220 {
		t.Errorf("Tick = %d, want -887220 (negative int24 must decode correctly)", got.Tick)
	}
	if err := got.Validate(); err != nil {
		t.Errorf("Validate: %v", err)
	}
}

func TestSnapshotState_CoreCallRevertsReturnsError(t *testing.T) {
	tests := []struct {
		name        string
		revertIndex int // index into the 8-call batch to fail
	}{
		{name: "slot0 reverts", revertIndex: 0},
		{name: "liquidity reverts", revertIndex: 1},
		{name: "feeGrowthGlobal0X128 reverts", revertIndex: 2},
		{name: "feeGrowthGlobal1X128 reverts", revertIndex: 3},
		{name: "protocolFees reverts", revertIndex: 4},
		{name: "balanceOf token0 reverts", revertIndex: 5},
		{name: "balanceOf token1 reverts", revertIndex: 6},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := stateTestPool()
			blockHash := common.HexToHash("0xabc0000000000000000000000000000000000000000000000000000000003")

			f := defaultStateFixtureValues()
			results := buildStateResults(t, f)
			results[tt.revertIndex] = outbound.Result{Success: false}
			var gotHash common.Hash
			mc := mockMulticallerReturning(results, &gotHash)

			got, err := SnapshotState(context.Background(), mc, pool, blockHash, 19000000, 0, stateBlockTS)
			if err == nil {
				t.Fatal("SnapshotState: want error when a core call reverts, got nil")
			}
			if got != nil {
				t.Errorf("SnapshotState: want nil state on error, got %+v", got)
			}
		})
	}
}

func TestSnapshotState_ObserveRevertsLeavesTwapNil(t *testing.T) {
	pool := stateTestPool()
	blockHash := common.HexToHash("0xabc0000000000000000000000000000000000000000000000000000000004")

	f := defaultStateFixtureValues()
	f.observeSucceeds = false
	results := buildStateResults(t, f)
	var gotHash common.Hash
	mc := mockMulticallerReturning(results, &gotHash)

	got, err := SnapshotState(context.Background(), mc, pool, blockHash, 19000000, 0, stateBlockTS)
	if err != nil {
		t.Fatalf("SnapshotState: want no error when observe reverts (legitimately optional), got %v", err)
	}
	if got.TwapTick != nil {
		t.Errorf("TwapTick = %v, want nil when observe reverts", *got.TwapTick)
	}
	if got.TwapWindowSecs != nil {
		t.Errorf("TwapWindowSecs = %v, want nil when observe reverts", *got.TwapWindowSecs)
	}
	if err := got.Validate(); err != nil {
		t.Errorf("Validate: %v", err)
	}
}

func TestSnapshotState_ObserveSucceedsComputesTwap(t *testing.T) {
	pool := stateTestPool()
	blockHash := common.HexToHash("0xabc0000000000000000000000000000000000000000000000000000000005")

	f := defaultStateFixtureValues()
	// tickCumulatives delta of 180000 over the (implementation-chosen) window
	// must produce an exact-division TWAP tick so the test doesn't need to
	// know the window constant to compute an expected value independently:
	// choose a delta that is a clean multiple of common windows and instead
	// assert via recomputation below using the actual window returned.
	f.tickCumulatives = []*big.Int{big.NewInt(500), big.NewInt(500 + 1800*42)}
	results := buildStateResults(t, f)
	var gotHash common.Hash
	mc := mockMulticallerReturning(results, &gotHash)

	got, err := SnapshotState(context.Background(), mc, pool, blockHash, 19000000, 0, stateBlockTS)
	if err != nil {
		t.Fatalf("SnapshotState: %v", err)
	}
	if got.TwapWindowSecs == nil {
		t.Fatal("TwapWindowSecs = nil, want a window to be set when observe succeeds")
	}
	if got.TwapTick == nil {
		t.Fatal("TwapTick = nil, want a computed tick when observe succeeds")
	}

	delta := new(big.Int).Sub(f.tickCumulatives[1], f.tickCumulatives[0])
	wantTick := int(new(big.Int).Div(delta, big.NewInt(int64(*got.TwapWindowSecs))).Int64())
	if *got.TwapTick != wantTick {
		t.Errorf("TwapTick = %d, want %d (delta=%s / window=%d)", *got.TwapTick, wantTick, delta, *got.TwapWindowSecs)
	}
	if err := got.Validate(); err != nil {
		t.Errorf("Validate: %v", err)
	}
}

func TestSnapshotState_UsesExecuteAtHashWithGivenBlockHash(t *testing.T) {
	pool := stateTestPool()
	blockHash := common.HexToHash("0xabc0000000000000000000000000000000000000000000000000000000006")

	f := defaultStateFixtureValues()
	results := buildStateResults(t, f)

	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		t.Fatal("Execute must not be called; SnapshotState must pin reads via ExecuteAtHash")
		return nil, nil
	}
	var gotHash common.Hash
	mc.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, hash common.Hash) ([]outbound.Result, error) {
		gotHash = hash
		if len(calls) != len(results) {
			return nil, fmt.Errorf("got %d calls, want %d", len(calls), len(results))
		}
		return results, nil
	}

	if _, err := SnapshotState(context.Background(), mc, pool, blockHash, 19000000, 0, stateBlockTS); err != nil {
		t.Fatalf("SnapshotState: %v", err)
	}
	if gotHash != blockHash {
		t.Errorf("ExecuteAtHash blockHash = %s, want %s", gotHash, blockHash)
	}
}

func TestSnapshotState_TransportErrorReturnsError(t *testing.T) {
	pool := stateTestPool()
	blockHash := common.HexToHash("0xabc0000000000000000000000000000000000000000000000000000000007")

	mc := testutil.NewMockMulticaller()
	mc.ExecuteAtHashFn = func(_ context.Context, _ []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		return nil, fmt.Errorf("rpc down")
	}

	got, err := SnapshotState(context.Background(), mc, pool, blockHash, 19000000, 0, stateBlockTS)
	if err == nil {
		t.Fatal("SnapshotState: want error on transport failure, got nil")
	}
	if got != nil {
		t.Errorf("SnapshotState: want nil state on error, got %+v", got)
	}
}

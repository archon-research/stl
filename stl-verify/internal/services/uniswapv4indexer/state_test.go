package uniswapv4indexer

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// stateTestPool is the fixture RegisteredPool for state-snapshot tests.
func stateTestPool() RegisteredPool {
	return decodeTestPool(7, ethWstethPoolID)
}

// stateViewTestABI independently parses the StateView methods, so tests can
// pack returns and unpack calldata without depending on state.go's own
// (possibly buggy) ABI.
func stateViewTestABI(t *testing.T) *abi.ABI {
	t.Helper()
	const j = `[
		{"name":"getSlot0","type":"function","stateMutability":"view","inputs":[{"name":"poolId","type":"bytes32"}],"outputs":[
			{"name":"sqrtPriceX96","type":"uint160"},
			{"name":"tick","type":"int24"},
			{"name":"protocolFee","type":"uint24"},
			{"name":"lpFee","type":"uint24"}
		]},
		{"name":"getLiquidity","type":"function","stateMutability":"view","inputs":[{"name":"poolId","type":"bytes32"}],"outputs":[{"name":"liquidity","type":"uint128"}]},
		{"name":"getFeeGrowthGlobals","type":"function","stateMutability":"view","inputs":[{"name":"poolId","type":"bytes32"}],"outputs":[
			{"name":"feeGrowthGlobal0","type":"uint256"},
			{"name":"feeGrowthGlobal1","type":"uint256"}
		]}
	]`
	a, err := abi.JSON(strings.NewReader(j))
	if err != nil {
		t.Fatalf("parsing StateView test ABI: %v", err)
	}
	return &a
}

// stateFixture holds every value SnapshotState reads, with sane defaults for
// the fields a given test is not varying.
type stateFixture struct {
	sqrtPriceX96      *big.Int
	tick              *big.Int
	protocolFee       *big.Int
	lpFee             *big.Int
	liquidity         *big.Int
	feeGrowthGlobal0  *big.Int
	feeGrowthGlobal1  *big.Int
	slot0Reverts      bool
	liquidityReverts  bool
	feeGrowthsReverts bool
}

func defaultStateFixture() stateFixture {
	return stateFixture{
		sqrtPriceX96:     mustBigInt("9046142643671156900152450452733"),
		tick:             big.NewInt(94759),
		protocolFee:      big.NewInt(0),
		lpFee:            big.NewInt(100),
		liquidity:        mustBigInt("3647259153468706670918"),
		feeGrowthGlobal0: big.NewInt(111),
		feeGrowthGlobal1: big.NewInt(222),
	}
}

func mustBigInt(s string) *big.Int {
	v, ok := new(big.Int).SetString(s, 10)
	if !ok {
		panic("bad big.Int literal: " + s)
	}
	return v
}

// buildStateResults returns the three outbound.Results in SnapshotState's call
// order (getSlot0, getLiquidity, getFeeGrowthGlobals).
func buildStateResults(t *testing.T, f stateFixture) []outbound.Result {
	t.Helper()
	a := stateViewTestABI(t)

	slot0, err := a.Methods["getSlot0"].Outputs.Pack(f.sqrtPriceX96, f.tick, f.protocolFee, f.lpFee)
	if err != nil {
		t.Fatalf("packing getSlot0 return: %v", err)
	}
	liquidity, err := a.Methods["getLiquidity"].Outputs.Pack(f.liquidity)
	if err != nil {
		t.Fatalf("packing getLiquidity return: %v", err)
	}
	feeGrowths, err := a.Methods["getFeeGrowthGlobals"].Outputs.Pack(f.feeGrowthGlobal0, f.feeGrowthGlobal1)
	if err != nil {
		t.Fatalf("packing getFeeGrowthGlobals return: %v", err)
	}

	return []outbound.Result{
		{Success: !f.slot0Reverts, ReturnData: slot0},
		{Success: !f.liquidityReverts, ReturnData: liquidity},
		{Success: !f.feeGrowthsReverts, ReturnData: feeGrowths},
	}
}

// mockMulticallerReturning wires a MockMulticaller whose ExecuteAtHash returns
// results positionally and records the calls and block hash it saw.
func mockMulticallerReturning(results []outbound.Result, gotHash *common.Hash, gotCalls *[]outbound.Call) *testutil.MockMulticaller {
	mc := testutil.NewMockMulticaller()
	mc.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
		*gotHash = blockHash
		*gotCalls = calls
		if len(results) != len(calls) {
			return nil, fmt.Errorf("test stub: got %d calls, have %d canned results", len(calls), len(results))
		}
		return results, nil
	}
	return mc
}

func snapshotWith(t *testing.T, pool RegisteredPool, f stateFixture) (*testutil.MockMulticaller, common.Hash, []outbound.Call, error) {
	t.Helper()
	blockHash := common.HexToHash("0xabc1")
	var gotHash common.Hash
	var gotCalls []outbound.Call
	mc := mockMulticallerReturning(buildStateResults(t, f), &gotHash, &gotCalls)
	_, err := SnapshotState(context.Background(), mc, pool, blockHash, blockNumber, blockVer, blockTS)
	return mc, gotHash, gotCalls, err
}

// ---------------------------------------------------------------------------
// SnapshotState
// ---------------------------------------------------------------------------

func TestSnapshotState_MapsCoreFields(t *testing.T) {
	pool := stateTestPool()
	blockHash := common.HexToHash("0xabc1")
	f := defaultStateFixture()
	var gotHash common.Hash
	var gotCalls []outbound.Call
	mc := mockMulticallerReturning(buildStateResults(t, f), &gotHash, &gotCalls)

	got, err := SnapshotState(context.Background(), mc, pool, blockHash, blockNumber, 2, blockTS)
	if err != nil {
		t.Fatalf("SnapshotState: %v", err)
	}

	if got.PoolID != pool.ID {
		t.Errorf("PoolID = %d, want %d", got.PoolID, pool.ID)
	}
	if got.BlockNumber != blockNumber || got.BlockVersion != 2 || !got.BlockTimestamp.Equal(blockTS) {
		t.Errorf("block identity = (%d, %d, %v), want (%d, 2, %v)", got.BlockNumber, got.BlockVersion, got.BlockTimestamp, blockNumber, blockTS)
	}
	if got.SqrtPriceX96.Cmp(f.sqrtPriceX96) != 0 {
		t.Errorf("SqrtPriceX96 = %s, want %s", got.SqrtPriceX96, f.sqrtPriceX96)
	}
	if got.Tick != 94759 {
		t.Errorf("Tick = %d, want 94759", got.Tick)
	}
	if got.ProtocolFee != 0 {
		t.Errorf("ProtocolFee = %d, want 0", got.ProtocolFee)
	}
	if got.LpFee != 100 {
		t.Errorf("LpFee = %d, want 100", got.LpFee)
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

// TestSnapshotState_ReadsStateViewWithPoolID pins the two things V4 changes
// versus V3: state comes from the periphery StateView contract, not the pool,
// and every call is keyed by the on-chain PoolId.
func TestSnapshotState_ReadsStateViewWithPoolID(t *testing.T) {
	pool := stateTestPool()
	_, _, gotCalls, err := snapshotWith(t, pool, defaultStateFixture())
	if err != nil {
		t.Fatalf("SnapshotState: %v", err)
	}

	if len(gotCalls) != 3 {
		t.Fatalf("packed %d calls, want 3 (getSlot0, getLiquidity, getFeeGrowthGlobals)", len(gotCalls))
	}
	for i, call := range gotCalls {
		if call.Target != pool.StateView {
			t.Errorf("call %d target = %s, want the StateView %s", i, call.Target, pool.StateView)
		}
		if call.AllowFailure {
			t.Errorf("call %d is AllowFailure; every V4 state read is core", i)
		}
		if len(call.CallData) != 4+32 {
			t.Fatalf("call %d calldata is %d bytes, want selector + one bytes32", i, len(call.CallData))
		}
		if got := common.BytesToHash(call.CallData[4:]); got != pool.PoolIDHash {
			t.Errorf("call %d argument = %s, want the PoolId %s", i, got, pool.PoolIDHash)
		}
	}
}

func TestSnapshotState_NegativeTick(t *testing.T) {
	pool := stateTestPool()
	blockHash := common.HexToHash("0xabc1")
	f := defaultStateFixture()
	f.tick = big.NewInt(-887200)
	var gotHash common.Hash
	var gotCalls []outbound.Call
	mc := mockMulticallerReturning(buildStateResults(t, f), &gotHash, &gotCalls)

	got, err := SnapshotState(context.Background(), mc, pool, blockHash, blockNumber, blockVer, blockTS)
	if err != nil {
		t.Fatalf("SnapshotState: %v", err)
	}
	if got.Tick != -887200 {
		t.Errorf("Tick = %d, want -887200 (int24 two's complement)", got.Tick)
	}
}

// TestSnapshotState_CoreRevertFailsSnapshot proves no read degrades to a zero
// value: all three are core, so any revert stops the block.
func TestSnapshotState_CoreRevertFailsSnapshot(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(f stateFixture) stateFixture
		wantSub string
	}{
		{name: "getSlot0", mutate: func(f stateFixture) stateFixture { f.slot0Reverts = true; return f }, wantSub: "getSlot0"},
		{name: "getLiquidity", mutate: func(f stateFixture) stateFixture { f.liquidityReverts = true; return f }, wantSub: "getLiquidity"},
		{name: "getFeeGrowthGlobals", mutate: func(f stateFixture) stateFixture { f.feeGrowthsReverts = true; return f }, wantSub: "getFeeGrowthGlobals"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, _, err := snapshotWith(t, stateTestPool(), tt.mutate(defaultStateFixture()))
			if err == nil {
				t.Fatalf("SnapshotState: want error when %s reverts, got nil", tt.name)
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Errorf("error %q does not name the failing read %q", err, tt.wantSub)
			}
		})
	}
}

// TestSnapshotState_UnknownPoolIDFailsValidation pins the fail-loud rule that
// makes a registry typo detectable: StateView answers all-zeros for a PoolId
// the PoolManager never initialized instead of reverting, so a zero price must
// be rejected rather than persisted as a real snapshot.
func TestSnapshotState_UnknownPoolIDFailsValidation(t *testing.T) {
	pool := stateTestPool()
	f := defaultStateFixture()
	f.sqrtPriceX96 = big.NewInt(0)
	f.tick = big.NewInt(0)
	f.lpFee = big.NewInt(0)
	f.liquidity = big.NewInt(0)

	_, _, _, err := snapshotWith(t, pool, f)
	if err == nil {
		t.Fatal("SnapshotState: want error for an all-zero slot0, got nil")
	}
	if !strings.Contains(err.Error(), pool.PoolIDHash.Hex()) {
		t.Errorf("error %q does not name the offending pool %s", err, pool.PoolIDHash)
	}
	if !strings.Contains(err.Error(), "sqrtPriceX96") {
		t.Errorf("error %q does not explain which field is invalid", err)
	}
}

func TestSnapshotState_ResultCountMismatchFails(t *testing.T) {
	pool := stateTestPool()
	mc := testutil.NewMockMulticaller()
	mc.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		return make([]outbound.Result, len(calls)-1), nil
	}

	_, err := SnapshotState(context.Background(), mc, pool, common.HexToHash("0xabc1"), blockNumber, blockVer, blockTS)
	if err == nil {
		t.Fatal("SnapshotState: want error when the multicaller returns fewer results than calls, got nil")
	}
}

func TestSnapshotState_TransportErrorFails(t *testing.T) {
	pool := stateTestPool()
	mc := testutil.NewMockMulticaller()
	mc.ExecuteAtHashFn = func(_ context.Context, _ []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		return nil, fmt.Errorf("rpc down")
	}

	_, err := SnapshotState(context.Background(), mc, pool, common.HexToHash("0xabc1"), blockNumber, blockVer, blockTS)
	if err == nil {
		t.Fatal("SnapshotState: want error on a transport failure, got nil")
	}
	if !strings.Contains(err.Error(), "rpc down") {
		t.Errorf("error %q does not wrap the transport failure", err)
	}
}

// TestSnapshotState_MalformedReturnDataFails covers a successful call whose
// payload cannot be decoded: it must fail loud rather than yield a partly
// zeroed snapshot.
func TestSnapshotState_MalformedReturnDataFails(t *testing.T) {
	pool := stateTestPool()
	results := buildStateResults(t, defaultStateFixture())
	results[0] = outbound.Result{Success: true, ReturnData: []byte{0x01, 0x02}}
	var gotHash common.Hash
	var gotCalls []outbound.Call
	mc := mockMulticallerReturning(results, &gotHash, &gotCalls)

	_, err := SnapshotState(context.Background(), mc, pool, common.HexToHash("0xabc1"), blockNumber, blockVer, blockTS)
	if err == nil {
		t.Fatal("SnapshotState: want error for an undecodable getSlot0 payload, got nil")
	}
}

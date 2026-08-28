package uniswapv4indexer

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// The recorded StateView reads below all come from this pool at this block, so
// a reader can re-fetch and re-verify any of them:
//
//	cast call 0x7fFE42C4a5DEeA5b0feC41C94C136Cf115597227 "getSlot0(bytes32)" \
//	  0x1d5b2949ece8754c2d736991c62c5162bd144f497b2212182401b9bae77e2d76 \
//	  -r https://eth.drpc.org -b 23200000
const (
	fixtureBlock     = 23200000
	fixtureBlockHash = "0xd6b7f6f0a976ff4ad7d28dfb50b5dd3cda99a5c41c73fe55e297edbeeb1953e5"
)

// Verbatim StateView return payloads: real contract bytes, not a round-trip
// through a copy of state.go's own ABI, so a wrong belief about the return
// layout cannot hide in the code and the fixture at once.
const (
	mainnetSlot0Return = "0000000000000000000000000000000000000000e8f3c82a9548345da47b990b" +
		"fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff8a1" +
		"0000000000000000000000000000000000000000000000000000000000000000" +
		"0000000000000000000000000000000000000000000000000000000000000064"
	mainnetLiquidityReturn        = "0000000000000000000000000000000000000000000001c7d9b92e2754e66d8f"
	mainnetFeeGrowthGlobalsReturn = "00000000000000000000000000000000001473b86de01b2c78943b25510f85a1" +
		"0000000000000000000000000000000000110126c6876960202ee205d7f0e477"
)

// Decoded from the words above by hand and corroborated against the
// PoolManager's raw storage for this pool, where v4-core's Pool.State puts
// slot0 at +0, feeGrowthGlobal0X128 at +1, feeGrowthGlobal1X128 at +2 and
// liquidity at +3.
const (
	mainnetSqrtPriceX96         = "72095236511535141145217308939"
	mainnetTick                 = -1887
	mainnetProtocolFee          = 0
	mainnetLpFee                = 100
	mainnetLiquidity            = "8408957175061230808463"
	mainnetFeeGrowthGlobal0X128 = "106193026261812897668181834909844897"
	mainnetFeeGrowthGlobal1X128 = "88292401116605815361741247342503031"
)

// Selectors the recorded calls were actually made with.
const (
	getSlot0Selector            = "c815641c"
	getLiquiditySelector        = "fa6793d5"
	getFeeGrowthGlobalsSelector = "9ec538c8"
)

// stateTestPool is the fixture RegisteredPool for state-snapshot tests.
func stateTestPool() RegisteredPool {
	return decodeTestPool(7, ethWstethPoolID)
}

func mustBigInt(s string) *big.Int {
	v, ok := new(big.Int).SetString(s, 10)
	if !ok {
		panic("bad big.Int literal: " + s)
	}
	return v
}

func hexBytes(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("decoding fixture hex %q: %v", s, err)
	}
	return b
}

// abiWords renders values as consecutive 32-byte return words, two's
// complement for negatives. This is the raw wire shape, so the synthetic
// fixtures below stay independent of any ABI definition.
func abiWords(values ...*big.Int) []byte {
	out := make([]byte, 0, 32*len(values))
	for _, v := range values {
		w := new(big.Int).Set(v)
		if w.Sign() < 0 {
			w.Add(w, new(big.Int).Lsh(big.NewInt(1), 256))
		}
		out = append(out, common.LeftPadBytes(w.Bytes(), 32)...)
	}
	return out
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
		sqrtPriceX96:     mustBigInt(mainnetSqrtPriceX96),
		tick:             big.NewInt(mainnetTick),
		protocolFee:      big.NewInt(mainnetProtocolFee),
		lpFee:            big.NewInt(mainnetLpFee),
		liquidity:        mustBigInt(mainnetLiquidity),
		feeGrowthGlobal0: mustBigInt(mainnetFeeGrowthGlobal0X128),
		feeGrowthGlobal1: mustBigInt(mainnetFeeGrowthGlobal1X128),
	}
}

// buildStateResults returns the three outbound.Results in SnapshotState's call
// order (getSlot0, getLiquidity, getFeeGrowthGlobals).
func buildStateResults(t *testing.T, f stateFixture) []outbound.Result {
	t.Helper()
	return []outbound.Result{
		{Success: !f.slot0Reverts, ReturnData: abiWords(f.sqrtPriceX96, f.tick, f.protocolFee, f.lpFee)},
		{Success: !f.liquidityReverts, ReturnData: abiWords(f.liquidity)},
		{Success: !f.feeGrowthsReverts, ReturnData: abiWords(f.feeGrowthGlobal0, f.feeGrowthGlobal1)},
	}
}

// recordedStateResults feeds the mainnet payloads through untouched, so the
// decode path sees exactly the bytes StateView returned.
func recordedStateResults(t *testing.T) []outbound.Result {
	t.Helper()
	return []outbound.Result{
		{Success: true, ReturnData: hexBytes(t, mainnetSlot0Return)},
		{Success: true, ReturnData: hexBytes(t, mainnetLiquidityReturn)},
		{Success: true, ReturnData: hexBytes(t, mainnetFeeGrowthGlobalsReturn)},
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

// TestSnapshotState_DecodesRecordedMainnetReturns is the independent oracle for
// the whole pool-state decode: verbatim StateView bytes in, hand-decoded
// expectations out. Every field is a distinct value, so any transposition of
// the return layout shows up as a named mismatch.
func TestSnapshotState_DecodesRecordedMainnetReturns(t *testing.T) {
	pool := stateTestPool()
	blockHash := common.HexToHash(fixtureBlockHash)
	var gotHash common.Hash
	var gotCalls []outbound.Call
	mc := mockMulticallerReturning(recordedStateResults(t), &gotHash, &gotCalls)

	got, err := SnapshotState(context.Background(), mc, pool, blockHash, fixtureBlock, 2, blockTS)
	if err != nil {
		t.Fatalf("SnapshotState: %v", err)
	}
	if err := got.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	if got.PoolID != pool.ID {
		t.Errorf("PoolID = %d, want %d", got.PoolID, pool.ID)
	}
	if got.BlockNumber != fixtureBlock || got.BlockVersion != 2 || !got.BlockTimestamp.Equal(blockTS) {
		t.Errorf("block identity = (%d, %d, %v), want (%d, 2, %v)", got.BlockNumber, got.BlockVersion, got.BlockTimestamp, fixtureBlock, blockTS)
	}
	if want := mustBigInt(mainnetSqrtPriceX96); got.SqrtPriceX96.Cmp(want) != 0 {
		t.Errorf("SqrtPriceX96 = %s, want %s", got.SqrtPriceX96, want)
	}
	if got.Tick != mainnetTick {
		t.Errorf("Tick = %d, want %d", got.Tick, mainnetTick)
	}
	if got.ProtocolFee != mainnetProtocolFee {
		t.Errorf("ProtocolFee = %d, want %d", got.ProtocolFee, mainnetProtocolFee)
	}
	if got.LpFee != mainnetLpFee {
		t.Errorf("LpFee = %d, want %d", got.LpFee, mainnetLpFee)
	}
	if want := mustBigInt(mainnetLiquidity); got.Liquidity.Cmp(want) != 0 {
		t.Errorf("Liquidity = %s, want %s", got.Liquidity, want)
	}
	if want := mustBigInt(mainnetFeeGrowthGlobal0X128); got.FeeGrowthGlobal0X128.Cmp(want) != 0 {
		t.Errorf("FeeGrowthGlobal0X128 = %s, want %s", got.FeeGrowthGlobal0X128, want)
	}
	if want := mustBigInt(mainnetFeeGrowthGlobal1X128); got.FeeGrowthGlobal1X128.Cmp(want) != 0 {
		t.Errorf("FeeGrowthGlobal1X128 = %s, want %s", got.FeeGrowthGlobal1X128, want)
	}
	if gotHash != blockHash {
		t.Errorf("ExecuteAtHash blockHash = %s, want %s", gotHash, blockHash)
	}
	if mc.CallCount != 1 {
		t.Errorf("ExecuteAtHash invocation count = %d, want 1 (single batched multicall)", mc.CallCount)
	}
}

// TestStateFixtureReproducesMainnetBytes keeps the synthetic packer the
// variation tests use honest: for the recorded values it must emit exactly what
// StateView returned, byte for byte.
func TestStateFixtureReproducesMainnetBytes(t *testing.T) {
	got := buildStateResults(t, defaultStateFixture())
	want := recordedStateResults(t)

	names := []string{"getSlot0", "getLiquidity", "getFeeGrowthGlobals"}
	for i, name := range names {
		if !bytes.Equal(got[i].ReturnData, want[i].ReturnData) {
			t.Errorf("%s packed as %x, want the recorded %x", name, got[i].ReturnData, want[i].ReturnData)
		}
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

	wantSelectors := []string{getSlot0Selector, getLiquiditySelector, getFeeGrowthGlobalsSelector}
	if len(gotCalls) != len(wantSelectors) {
		t.Fatalf("packed %d calls, want %d (getSlot0, getLiquidity, getFeeGrowthGlobals)", len(gotCalls), len(wantSelectors))
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
		if got := hex.EncodeToString(call.CallData[:4]); got != wantSelectors[i] {
			t.Errorf("call %d selector = %s, want %s", i, got, wantSelectors[i])
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

// TestSnapshotState_UnusableReadFails covers every way a read can come back
// unusable: each must fail loud rather than yield a partly zeroed snapshot.
func TestSnapshotState_UnusableReadFails(t *testing.T) {
	for _, tc := range []struct {
		name    string
		stub    func(t *testing.T) *testutil.MockMulticaller
		wantSub string
	}{
		{
			name: "fewer results than calls",
			stub: func(*testing.T) *testutil.MockMulticaller {
				mc := testutil.NewMockMulticaller()
				mc.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
					return make([]outbound.Result, len(calls)-1), nil
				}
				return mc
			},
		},
		{
			name: "transport failure",
			stub: func(*testing.T) *testutil.MockMulticaller {
				mc := testutil.NewMockMulticaller()
				mc.ExecuteAtHashFn = func(_ context.Context, _ []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
					return nil, fmt.Errorf("rpc down")
				}
				return mc
			},
			wantSub: "rpc down",
		},
		{
			name: "undecodable getSlot0 payload",
			stub: func(t *testing.T) *testutil.MockMulticaller {
				results := buildStateResults(t, defaultStateFixture())
				results[0] = outbound.Result{Success: true, ReturnData: []byte{0x01, 0x02}}
				var gotHash common.Hash
				var gotCalls []outbound.Call
				return mockMulticallerReturning(results, &gotHash, &gotCalls)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := SnapshotState(context.Background(), tc.stub(t), stateTestPool(), common.HexToHash("0xabc1"), blockNumber, blockVer, blockTS)
			if err == nil {
				t.Fatalf("SnapshotState: want error for %s, got nil", tc.name)
			}
			if tc.wantSub != "" && !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("error %q does not mention %q", err, tc.wantSub)
			}
		})
	}
}

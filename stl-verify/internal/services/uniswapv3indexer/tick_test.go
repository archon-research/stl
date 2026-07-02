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

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

// tickTestPool is the fixture RegisteredPool for tick-read tests.
func tickTestPool() RegisteredPool {
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

var tickBlockTS = time.Unix(1700000000, 0).UTC()

// liquidityEvent builds a fixture UniswapV3LiquidityEvent with the given
// tick range, defaulting every other field to a valid, self-consistent mint.
func liquidityEvent(tickLower, tickUpper int) *entity.UniswapV3LiquidityEvent {
	sender := common.HexToAddress("0x1111111111111111111111111111111111111111")
	return &entity.UniswapV3LiquidityEvent{
		PoolID:         7,
		BlockNumber:    19000000,
		BlockVersion:   0,
		BlockTimestamp: tickBlockTS,
		TxHash:         common.HexToHash("0xfeed000000000000000000000000000000000000000000000000000000000001"),
		LogIndex:       0,
		EventName:      entity.LiquidityEventMint,
		Owner:          common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Sender:         &sender,
		TickLower:      tickLower,
		TickUpper:      tickUpper,
		Amount:         big.NewInt(1000),
		Amount0:        big.NewInt(1),
		Amount1:        big.NewInt(1),
	}
}

// ticksMethodABI returns an ABI exposing only the ticks(int24) view method,
// used by tests to independently pack/unpack calldata and returns without
// depending on tick.go's own (possibly buggy) packing.
func ticksMethodABI(t *testing.T) *abi.ABI {
	t.Helper()
	const ticksJSON = `[
		{"name":"ticks","type":"function","stateMutability":"view","inputs":[{"name":"tick","type":"int24"}],"outputs":[
			{"name":"liquidityGross","type":"uint128"},
			{"name":"liquidityNet","type":"int128"},
			{"name":"feeGrowthOutside0X128","type":"uint256"},
			{"name":"feeGrowthOutside1X128","type":"uint256"},
			{"name":"tickCumulativeOutside","type":"int56"},
			{"name":"secondsPerLiquidityOutsideX128","type":"uint160"},
			{"name":"secondsOutside","type":"uint32"},
			{"name":"initialized","type":"bool"}
		]}
	]`
	a, err := abi.JSON(strings.NewReader(ticksJSON))
	if err != nil {
		t.Fatalf("parsing ticks() test ABI: %v", err)
	}
	return &a
}

// packTicksReturn ABI-encodes a ticks() return tuple for use as a fixture
// outbound.Result.ReturnData.
func packTicksReturn(t *testing.T, a *abi.ABI, liquidityGross, liquidityNet, feeGrowthOutside0, feeGrowthOutside1 *big.Int, initialized bool) []byte {
	t.Helper()
	packed, err := a.Methods["ticks"].Outputs.Pack(
		liquidityGross,
		liquidityNet,
		feeGrowthOutside0,
		feeGrowthOutside1,
		big.NewInt(0), // tickCumulativeOutside
		big.NewInt(0), // secondsPerLiquidityOutsideX128
		uint32(0),     // secondsOutside
		initialized,
	)
	if err != nil {
		t.Fatalf("packing ticks() return: %v", err)
	}
	return packed
}

// ---------------------------------------------------------------------------
// TouchedTicks
// ---------------------------------------------------------------------------

func TestTouchedTicks(t *testing.T) {
	tests := []struct {
		name string
		evs  DecodedEvents
		want []int32
	}{
		{
			name: "empty events returns empty slice",
			evs:  DecodedEvents{},
			want: []int32{},
		},
		{
			name: "single mint contributes both bounds",
			evs: DecodedEvents{
				LiquidityEvents: []*entity.UniswapV3LiquidityEvent{
					liquidityEvent(-120, 180),
				},
			},
			want: []int32{-120, 180},
		},
		{
			name: "mint burn collect union deduped and sorted, including negatives",
			evs: DecodedEvents{
				LiquidityEvents: []*entity.UniswapV3LiquidityEvent{
					liquidityEvent(-120, 180),
					liquidityEvent(-240, -60),
					liquidityEvent(-60, 60),
				},
			},
			want: []int32{-240, -120, -60, 60, 180},
		},
		{
			name: "duplicate ticks across events collapse to one entry",
			evs: DecodedEvents{
				LiquidityEvents: []*entity.UniswapV3LiquidityEvent{
					liquidityEvent(-120, 180),
					liquidityEvent(-120, 180),
				},
			},
			want: []int32{-120, 180},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := TouchedTicks(tt.evs)
			if len(got) != len(tt.want) {
				t.Fatalf("TouchedTicks() = %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("TouchedTicks()[%d] = %d, want %d (full: got=%v want=%v)", i, got[i], tt.want[i], got, tt.want)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// BuildTickCalls
// ---------------------------------------------------------------------------

func TestBuildTickCalls(t *testing.T) {
	pool := tickTestPool()
	a := ticksMethodABI(t)
	selector := a.Methods["ticks"].ID

	tests := []struct {
		name  string
		ticks []int32
	}{
		{name: "single positive tick", ticks: []int32{180}},
		{name: "single negative tick", ticks: []int32{-887220}},
		{name: "mixed ticks preserve order", ticks: []int32{60, -60, 0, -887272, 887272}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls, err := BuildTickCalls(pool, tt.ticks)
			if err != nil {
				t.Fatalf("BuildTickCalls: %v", err)
			}
			if len(calls) != len(tt.ticks) {
				t.Fatalf("len(calls) = %d, want %d", len(calls), len(tt.ticks))
			}
			for i, call := range calls {
				if call.Target != pool.Address {
					t.Errorf("calls[%d].Target = %s, want %s", i, call.Target, pool.Address)
				}
				if call.AllowFailure {
					t.Errorf("calls[%d].AllowFailure = true, want false (authoritative read)", i)
				}
				if len(call.CallData) < 4 {
					t.Fatalf("calls[%d].CallData too short: %x", i, call.CallData)
				}
				gotSelector := [4]byte{}
				copy(gotSelector[:], call.CallData[:4])
				if gotSelector != [4]byte(selector) {
					t.Errorf("calls[%d] selector = %x, want %x", i, gotSelector, selector)
				}

				args, err := a.Methods["ticks"].Inputs.Unpack(call.CallData[4:])
				if err != nil {
					t.Fatalf("unpacking calls[%d] input: %v", i, err)
				}
				gotTick, ok := args[0].(*big.Int)
				if !ok {
					t.Fatalf("calls[%d] arg[0] type = %T, want *big.Int", i, args[0])
				}
				if gotTick.Int64() != int64(tt.ticks[i]) {
					t.Errorf("calls[%d] round-tripped tick = %d, want %d", i, gotTick.Int64(), tt.ticks[i])
				}
			}
		})
	}
}

func TestBuildTickCalls_EmptyTicksReturnsEmptyCalls(t *testing.T) {
	pool := tickTestPool()
	calls, err := BuildTickCalls(pool, nil)
	if err != nil {
		t.Fatalf("BuildTickCalls: %v", err)
	}
	if len(calls) != 0 {
		t.Errorf("len(calls) = %d, want 0", len(calls))
	}
}

// ---------------------------------------------------------------------------
// DecodeTick
// ---------------------------------------------------------------------------

func TestDecodeTick(t *testing.T) {
	pool := tickTestPool()
	a := ticksMethodABI(t)

	tests := []struct {
		name              string
		tick              int32
		liquidityGross    *big.Int
		liquidityNet      *big.Int
		feeGrowthOutside0 *big.Int
		feeGrowthOutside1 *big.Int
		initialized       bool
	}{
		{
			name:              "positive liquidityNet, initialized",
			tick:              180,
			liquidityGross:    big.NewInt(5000),
			liquidityNet:      big.NewInt(5000),
			feeGrowthOutside0: big.NewInt(111),
			feeGrowthOutside1: big.NewInt(222),
			initialized:       true,
		},
		{
			name:              "negative liquidityNet (liquidity removed crossing left-to-right)",
			tick:              -120,
			liquidityGross:    big.NewInt(5000),
			liquidityNet:      big.NewInt(-5000),
			feeGrowthOutside0: big.NewInt(0),
			feeGrowthOutside1: big.NewInt(0),
			initialized:       true,
		},
		{
			name:              "uninitialized tick with zero liquidity",
			tick:              60,
			liquidityGross:    big.NewInt(0),
			liquidityNet:      big.NewInt(0),
			feeGrowthOutside0: big.NewInt(0),
			feeGrowthOutside1: big.NewInt(0),
			initialized:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			returnData := packTicksReturn(t, a, tt.liquidityGross, tt.liquidityNet, tt.feeGrowthOutside0, tt.feeGrowthOutside1, tt.initialized)
			res := outbound.Result{Success: true, ReturnData: returnData}

			got, err := DecodeTick(pool, tt.tick, 19000000, 2, tickBlockTS, res)
			if err != nil {
				t.Fatalf("DecodeTick: %v", err)
			}
			if got.PoolID != pool.ID {
				t.Errorf("PoolID = %d, want %d", got.PoolID, pool.ID)
			}
			if got.Tick != int(tt.tick) {
				t.Errorf("Tick = %d, want %d", got.Tick, tt.tick)
			}
			if got.BlockNumber != 19000000 {
				t.Errorf("BlockNumber = %d, want 19000000", got.BlockNumber)
			}
			if got.BlockVersion != 2 {
				t.Errorf("BlockVersion = %d, want 2", got.BlockVersion)
			}
			if !got.BlockTimestamp.Equal(tickBlockTS) {
				t.Errorf("BlockTimestamp = %v, want %v", got.BlockTimestamp, tickBlockTS)
			}
			if got.LiquidityGross.Cmp(tt.liquidityGross) != 0 {
				t.Errorf("LiquidityGross = %s, want %s", got.LiquidityGross, tt.liquidityGross)
			}
			if got.LiquidityNet.Cmp(tt.liquidityNet) != 0 {
				t.Errorf("LiquidityNet = %s, want %s", got.LiquidityNet, tt.liquidityNet)
			}
			if got.LiquidityNet.Sign() != tt.liquidityNet.Sign() {
				t.Errorf("LiquidityNet sign = %d, want %d", got.LiquidityNet.Sign(), tt.liquidityNet.Sign())
			}
			if got.FeeGrowthOutside0X128.Cmp(tt.feeGrowthOutside0) != 0 {
				t.Errorf("FeeGrowthOutside0X128 = %s, want %s", got.FeeGrowthOutside0X128, tt.feeGrowthOutside0)
			}
			if got.FeeGrowthOutside1X128.Cmp(tt.feeGrowthOutside1) != 0 {
				t.Errorf("FeeGrowthOutside1X128 = %s, want %s", got.FeeGrowthOutside1X128, tt.feeGrowthOutside1)
			}
			if got.Initialized != tt.initialized {
				t.Errorf("Initialized = %v, want %v", got.Initialized, tt.initialized)
			}
			if err := got.Validate(); err != nil {
				t.Errorf("Validate: %v", err)
			}
		})
	}
}

func TestDecodeTick_RevertedCallReturnsError(t *testing.T) {
	pool := tickTestPool()
	res := outbound.Result{Success: false, ReturnData: nil}

	got, err := DecodeTick(pool, 60, 19000000, 0, tickBlockTS, res)
	if err == nil {
		t.Fatal("DecodeTick: want error for !Success result, got nil")
	}
	if got != nil {
		t.Errorf("DecodeTick: want nil tick on error, got %+v", got)
	}
}

func TestDecodeTick_MalformedReturnDataReturnsError(t *testing.T) {
	pool := tickTestPool()
	res := outbound.Result{Success: true, ReturnData: []byte{0x01, 0x02}}

	got, err := DecodeTick(pool, 60, 19000000, 0, tickBlockTS, res)
	if err == nil {
		t.Fatal("DecodeTick: want error for malformed return data, got nil")
	}
	if got != nil {
		t.Errorf("DecodeTick: want nil tick on error, got %+v", got)
	}
}

// ---------------------------------------------------------------------------
// Floor-div helpers (tick <-> word/bit)
// ---------------------------------------------------------------------------

func TestTickToWordBit(t *testing.T) {
	tests := []struct {
		name        string
		tick        int32
		tickSpacing int
		wantWord    int16
		wantBit     uint8
	}{
		{name: "zero tick, positive spacing", tick: 0, tickSpacing: 60, wantWord: 0, wantBit: 0},
		{name: "positive tick within word 0", tick: 60, tickSpacing: 60, wantWord: 0, wantBit: 1},
		{name: "positive tick crossing into word 1", tick: 60 * 256, tickSpacing: 60, wantWord: 1, wantBit: 0},
		{name: "tick=-1 spacing=60 floors to word -1", tick: -60, tickSpacing: 60, wantWord: -1, wantBit: 255},
		{name: "most-negative bit of word -1", tick: -60 * 256, tickSpacing: 60, wantWord: -1, wantBit: 0},
		{name: "one below word -1 boundary falls to word -2", tick: -60 * 257, tickSpacing: 60, wantWord: -2, wantBit: 255},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotWord, gotBit := tickToWordBit(tt.tick, tt.tickSpacing)
			if gotWord != tt.wantWord || gotBit != tt.wantBit {
				t.Errorf("tickToWordBit(%d, %d) = (%d, %d), want (%d, %d)", tt.tick, tt.tickSpacing, gotWord, gotBit, tt.wantWord, tt.wantBit)
			}

			// Inverse must recover the original tick: only meaningful at
			// bitmap-aligned ticks (exact multiples of tickSpacing), which is
			// every tick in this table.
			gotTick := wordBitToTick(gotWord, gotBit, tt.tickSpacing)
			if gotTick != tt.tick {
				t.Errorf("wordBitToTick(%d, %d, %d) = %d, want %d (round-trip)", gotWord, gotBit, tt.tickSpacing, gotTick, tt.tick)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// BaselineTicks
// ---------------------------------------------------------------------------

// bitmapWord returns a uint256 (as *big.Int) with the given bit indices set.
func bitmapWord(bits ...uint) *big.Int {
	w := new(big.Int)
	for _, b := range bits {
		w.SetBit(w, int(b), 1)
	}
	return w
}

func TestBaselineTicks_DecodesSetBitsToTicks(t *testing.T) {
	pool := tickTestPool() // TickSpacing = 60
	blockHash := common.HexToHash("0xabc0000000000000000000000000000000000000000000000000000000001")

	minWord, maxWord := wordBounds(pool.TickSpacing)
	var gotCallCount int

	mc := testutil.NewMockMulticaller()
	mc.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, gotHash common.Hash) ([]outbound.Result, error) {
		if gotHash != blockHash {
			t.Errorf("ExecuteAtHash blockHash = %s, want %s", gotHash, blockHash)
		}
		gotCallCount = len(calls)
		results := make([]outbound.Result, len(calls))
		for i := range calls {
			results[i] = outbound.Result{Success: true, ReturnData: common.LeftPadBytes(big.NewInt(0).Bytes(), 32)}
		}
		// word 0: bits 1 (tick 60) and 3 (tick 180) set.
		// word -1: bit 255 (tick -60, the highest bit, i.e. closest to zero) set.
		for i, call := range calls {
			word := wordFromCallData(t, call.CallData)
			switch word {
			case 0:
				results[i] = outbound.Result{Success: true, ReturnData: common.LeftPadBytes(bitmapWord(1, 3).Bytes(), 32)}
			case -1:
				results[i] = outbound.Result{Success: true, ReturnData: common.LeftPadBytes(bitmapWord(255).Bytes(), 32)}
			}
		}
		return results, nil
	}

	got, err := BaselineTicks(context.Background(), mc, pool, blockHash)
	if err != nil {
		t.Fatalf("BaselineTicks: %v", err)
	}

	want := []int32{-60, 60, 180}
	if len(got) != len(want) {
		t.Fatalf("BaselineTicks() = %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("BaselineTicks()[%d] = %d, want %d (full: got=%v want=%v)", i, got[i], want[i], got, want)
		}
	}

	if mc.CallCount != 1 {
		t.Errorf("ExecuteAtHash invocation count = %d, want 1 (single batched multicall)", mc.CallCount)
	}
	wantCalls := int(maxWord) - int(minWord) + 1
	if gotCallCount != wantCalls {
		t.Errorf("calls issued = %d, want %d (bounded word range for tickSpacing=%d)", gotCallCount, wantCalls, pool.TickSpacing)
	}
	if gotCallCount >= 65536 {
		t.Errorf("calls issued = %d, must not scan all int16 words", gotCallCount)
	}
}

func TestBaselineTicks_UsesExecuteAtHashNotExecute(t *testing.T) {
	pool := tickTestPool()
	blockHash := common.HexToHash("0xdef0000000000000000000000000000000000000000000000000000000002")

	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		t.Fatal("Execute must not be called; BaselineTicks must pin reads via ExecuteAtHash")
		return nil, nil
	}
	mc.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		results := make([]outbound.Result, len(calls))
		for i := range calls {
			results[i] = outbound.Result{Success: true, ReturnData: common.LeftPadBytes([]byte{}, 32)}
		}
		return results, nil
	}

	if _, err := BaselineTicks(context.Background(), mc, pool, blockHash); err != nil {
		t.Fatalf("BaselineTicks: %v", err)
	}
}

func TestBaselineTicks_RevertedCallReturnsErrorImmediately(t *testing.T) {
	pool := tickTestPool()
	blockHash := common.HexToHash("0xdef0000000000000000000000000000000000000000000000000000000003")

	mc := testutil.NewMockMulticaller()
	mc.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		results := make([]outbound.Result, len(calls))
		for i := range calls {
			results[i] = outbound.Result{Success: false}
		}
		return results, nil
	}

	got, err := BaselineTicks(context.Background(), mc, pool, blockHash)
	if err == nil {
		t.Fatal("BaselineTicks: want error when a tickBitmap call reverts, got nil")
	}
	if got != nil {
		t.Errorf("BaselineTicks: want nil ticks on error, got %v", got)
	}
}

func TestBaselineTicks_TransportErrorReturnsError(t *testing.T) {
	pool := tickTestPool()
	blockHash := common.HexToHash("0xdef0000000000000000000000000000000000000000000000000000000004")

	mc := testutil.NewMockMulticaller()
	mc.ExecuteAtHashFn = func(_ context.Context, _ []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		return nil, fmt.Errorf("rpc down")
	}

	got, err := BaselineTicks(context.Background(), mc, pool, blockHash)
	if err == nil {
		t.Fatal("BaselineTicks: want error on transport failure, got nil")
	}
	if got != nil {
		t.Errorf("BaselineTicks: want nil ticks on error, got %v", got)
	}
}

// wordFromCallData decodes the int16 wordPosition argument packed into a
// tickBitmap(int16) call, for use by test stubs that need to key their
// response on which word is being requested.
func wordFromCallData(t *testing.T, callData []byte) int16 {
	t.Helper()
	a := tickBitmapMethodABI(t)
	args, err := a.Methods["tickBitmap"].Inputs.Unpack(callData[4:])
	if err != nil {
		t.Fatalf("unpacking tickBitmap call data: %v", err)
	}
	word, ok := args[0].(int16)
	if !ok {
		t.Fatalf("tickBitmap arg[0] type = %T, want int16", args[0])
	}
	return word
}

func tickBitmapMethodABI(t *testing.T) *abi.ABI {
	t.Helper()
	const bitmapJSON = `[
		{"name":"tickBitmap","type":"function","stateMutability":"view","inputs":[{"name":"wordPosition","type":"int16"}],"outputs":[{"name":"","type":"uint256"}]}
	]`
	a, err := abi.JSON(strings.NewReader(bitmapJSON))
	if err != nil {
		t.Fatalf("parsing tickBitmap() test ABI: %v", err)
	}
	return &a
}

package uniswapv4indexer

import (
	"context"
	"fmt"
	"math/big"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/tickbitmap"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// tickTestPool is the fixture RegisteredPool for tick-read tests (tickSpacing 60).
func tickTestPool() RegisteredPool {
	pool := decodeTestPool(7, ethWstethPoolID)
	pool.TickSpacing = 60
	return pool
}

// liquidityEvent builds a ModifyLiquidity entity with the given bounds and
// delta; only those three fields matter to TouchedTicks.
func liquidityEvent(tickLower, tickUpper int, liquidityDelta int64) *entity.UniswapV4LiquidityEvent {
	return &entity.UniswapV4LiquidityEvent{
		PoolID:         7,
		TickLower:      tickLower,
		TickUpper:      tickUpper,
		LiquidityDelta: big.NewInt(liquidityDelta),
	}
}

// tickViewTestABI independently parses the StateView tick getters so tests can
// pack returns and unpack calldata without depending on tick.go's own ABI.
func tickViewTestABI(t *testing.T) *abi.ABI {
	t.Helper()
	const j = `[
		{"name":"getTickInfo","type":"function","stateMutability":"view","inputs":[{"name":"poolId","type":"bytes32"},{"name":"tick","type":"int24"}],"outputs":[
			{"name":"liquidityGross","type":"uint128"},
			{"name":"liquidityNet","type":"int128"},
			{"name":"feeGrowthOutside0X128","type":"uint256"},
			{"name":"feeGrowthOutside1X128","type":"uint256"}
		]},
		{"name":"getTickBitmap","type":"function","stateMutability":"view","inputs":[{"name":"poolId","type":"bytes32"},{"name":"tick","type":"int16"}],"outputs":[{"name":"","type":"uint256"}]}
	]`
	a, err := abi.JSON(strings.NewReader(j))
	if err != nil {
		t.Fatalf("parsing tick view test ABI: %v", err)
	}
	return &a
}

func packTickInfoReturn(t *testing.T, liquidityGross, liquidityNet, fg0, fg1 *big.Int) []byte {
	t.Helper()
	packed, err := tickViewTestABI(t).Methods["getTickInfo"].Outputs.Pack(liquidityGross, liquidityNet, fg0, fg1)
	if err != nil {
		t.Fatalf("packing getTickInfo return: %v", err)
	}
	return packed
}

// ---------------------------------------------------------------------------
// TouchedTicks
// ---------------------------------------------------------------------------

func TestTouchedTicks(t *testing.T) {
	tests := []struct {
		name   string
		events []*entity.UniswapV4LiquidityEvent
		want   []int32
	}{
		{name: "no events", events: nil, want: nil},
		{name: "single position", events: []*entity.UniswapV4LiquidityEvent{liquidityEvent(-120, 180, 1000)}, want: []int32{-120, 180}},
		{
			name: "overlapping positions dedup and sort",
			events: []*entity.UniswapV4LiquidityEvent{
				liquidityEvent(180, 300, 1000),
				liquidityEvent(-120, 180, -500),
			},
			want: []int32{-120, 180, 300},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := TouchedTicks(DecodedEvents{LiquidityEvents: tt.events})
			if !slices.Equal(got, tt.want) {
				t.Errorf("TouchedTicks() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestTouchedTicks_ExcludesZeroDeltaPokes pins the read-amplification guard:
// v4-core's Pool.modifyLiquidity only calls updateTick when liquidityDelta is
// non-zero, so a fee-collecting poke leaves tick state untouched and re-reading
// its bounds would be pure waste (and would let a permissionless zero-delta
// call with arbitrary bounds mint junk uninitialized-tick rows).
func TestTouchedTicks_ExcludesZeroDeltaPokes(t *testing.T) {
	events := []*entity.UniswapV4LiquidityEvent{
		liquidityEvent(-120, 180, 0),
		liquidityEvent(-600, 600, 42),
	}

	got := TouchedTicks(DecodedEvents{LiquidityEvents: events})

	if want := []int32{-600, 600}; !slices.Equal(got, want) {
		t.Errorf("TouchedTicks() = %v, want %v (the zero-delta poke's bounds must be excluded)", got, want)
	}
}

// ---------------------------------------------------------------------------
// BuildTickCalls / DecodeTick
// ---------------------------------------------------------------------------

func TestBuildTickCalls(t *testing.T) {
	pool := tickTestPool()
	ticks := []int32{-120, 0, 180}

	calls, err := BuildTickCalls(pool, ticks)
	if err != nil {
		t.Fatalf("BuildTickCalls: %v", err)
	}
	if len(calls) != len(ticks) {
		t.Fatalf("got %d calls, want %d", len(calls), len(ticks))
	}

	a := tickViewTestABI(t)
	for i, call := range calls {
		if call.Target != pool.StateView {
			t.Errorf("call %d target = %s, want the StateView %s", i, call.Target, pool.StateView)
		}
		if call.AllowFailure {
			t.Errorf("call %d is AllowFailure; an authoritative tick read must fail loud", i)
		}
		args, err := a.Methods["getTickInfo"].Inputs.Unpack(call.CallData[4:])
		if err != nil {
			t.Fatalf("unpacking call %d args: %v", i, err)
		}
		if got := common.Hash(args[0].([32]byte)); got != pool.PoolIDHash {
			t.Errorf("call %d poolId = %s, want %s", i, got, pool.PoolIDHash)
		}
		if got := int32(args[1].(*big.Int).Int64()); got != ticks[i] {
			t.Errorf("call %d tick = %d, want %d (order must match the input)", i, got, ticks[i])
		}
	}
}

func TestBuildTickCalls_EmptyInput(t *testing.T) {
	calls, err := BuildTickCalls(tickTestPool(), nil)
	if err != nil {
		t.Fatalf("BuildTickCalls: %v", err)
	}
	if len(calls) != 0 {
		t.Errorf("got %d calls, want 0", len(calls))
	}
}

func TestDecodeTick(t *testing.T) {
	pool := tickTestPool()
	liquidityNet := big.NewInt(-500)
	fg0 := mustBigInt("340282366920938463463374607431768211456")
	res := outbound.Result{Success: true, ReturnData: packTickInfoReturn(t, big.NewInt(1000), liquidityNet, fg0, big.NewInt(2))}

	got, err := DecodeTick(pool, -120, blockNumber, 3, blockTS, res)
	if err != nil {
		t.Fatalf("DecodeTick: %v", err)
	}
	if err := got.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if got.PoolID != pool.ID || got.Tick != -120 {
		t.Errorf("identity = (pool %d, tick %d), want (%d, -120)", got.PoolID, got.Tick, pool.ID)
	}
	if got.BlockNumber != blockNumber || got.BlockVersion != 3 || !got.BlockTimestamp.Equal(blockTS) {
		t.Errorf("block identity = (%d, %d, %v), want (%d, 3, %v)", got.BlockNumber, got.BlockVersion, got.BlockTimestamp, blockNumber, blockTS)
	}
	if got.LiquidityGross.Cmp(big.NewInt(1000)) != 0 {
		t.Errorf("LiquidityGross = %s, want 1000", got.LiquidityGross)
	}
	if got.LiquidityNet.Cmp(liquidityNet) != 0 {
		t.Errorf("LiquidityNet = %s, want %s (signed int128)", got.LiquidityNet, liquidityNet)
	}
	if got.FeeGrowthOutside0X128.Cmp(fg0) != 0 {
		t.Errorf("FeeGrowthOutside0X128 = %s, want %s", got.FeeGrowthOutside0X128, fg0)
	}
	if got.FeeGrowthOutside1X128.Cmp(big.NewInt(2)) != 0 {
		t.Errorf("FeeGrowthOutside1X128 = %s, want 2", got.FeeGrowthOutside1X128)
	}
}

// TestDecodeTick_ClearedTickIsAllZeros documents the absent Initialized flag:
// v4-core's TickInfo has none, so a cleared tick reads back as an all-zero row.
func TestDecodeTick_ClearedTickIsAllZeros(t *testing.T) {
	res := outbound.Result{Success: true, ReturnData: packTickInfoReturn(t, big.NewInt(0), big.NewInt(0), big.NewInt(0), big.NewInt(0))}

	got, err := DecodeTick(tickTestPool(), 180, blockNumber, 0, blockTS, res)
	if err != nil {
		t.Fatalf("DecodeTick: %v", err)
	}
	if got.LiquidityGross.Sign() != 0 {
		t.Errorf("LiquidityGross = %s, want 0 for a cleared tick", got.LiquidityGross)
	}
}

func TestDecodeTick_FailureModes(t *testing.T) {
	tests := []struct {
		name    string
		res     outbound.Result
		wantSub string
	}{
		{name: "reverted call", res: outbound.Result{Success: false}, wantSub: "reverted"},
		{name: "undecodable payload", res: outbound.Result{Success: true, ReturnData: []byte{0x01}}, wantSub: "unpacking"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeTick(tickTestPool(), -120, blockNumber, 0, blockTS, tt.res)
			if err == nil {
				t.Fatalf("DecodeTick: want error for %s, got nil", tt.name)
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Errorf("error %q does not mention %q", err, tt.wantSub)
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

// wordFromCallData recovers the int16 word position from a packed
// getTickBitmap(bytes32,int16) call.
func wordFromCallData(t *testing.T, callData []byte) int16 {
	t.Helper()
	args, err := tickViewTestABI(t).Methods["getTickBitmap"].Inputs.Unpack(callData[4:])
	if err != nil {
		t.Fatalf("unpacking getTickBitmap args: %v", err)
	}
	return args[1].(int16)
}

func TestBaselineTicks_DecodesSetBitsToTicks(t *testing.T) {
	pool := tickTestPool()
	blockHash := common.HexToHash("0xabc1")
	minWord, maxWord := tickbitmap.WordBounds(pool.TickSpacing)
	var gotCallCount int

	mc := testutil.NewMockMulticaller()
	mc.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, gotHash common.Hash) ([]outbound.Result, error) {
		if gotHash != blockHash {
			t.Errorf("ExecuteAtHash blockHash = %s, want %s", gotHash, blockHash)
		}
		gotCallCount = len(calls)
		results := make([]outbound.Result, len(calls))
		for i, call := range calls {
			if call.Target != pool.StateView {
				t.Errorf("call %d target = %s, want the StateView %s", i, call.Target, pool.StateView)
			}
			word := new(big.Int)
			switch wordFromCallData(t, call.CallData) {
			case 0:
				word = bitmapWord(1, 3) // ticks 60 and 180
			case -1:
				word = bitmapWord(255) // tick -60
			}
			results[i] = outbound.Result{Success: true, ReturnData: common.LeftPadBytes(word.Bytes(), 32)}
		}
		return results, nil
	}

	got, err := BaselineTicks(context.Background(), mc, pool, blockHash)
	if err != nil {
		t.Fatalf("BaselineTicks: %v", err)
	}

	if want := []int32{-60, 60, 180}; !slices.Equal(got, want) {
		t.Errorf("BaselineTicks() = %v, want %v", got, want)
	}
	if mc.CallCount != 1 {
		t.Errorf("ExecuteAtHash invocation count = %d, want 1", mc.CallCount)
	}
	if want := int(maxWord) - int(minWord) + 1; gotCallCount != want {
		t.Errorf("calls issued = %d, want %d (the bounded word range for tickSpacing=%d)", gotCallCount, want, pool.TickSpacing)
	}
}

// TestBaselineTicks_ChunksWideWordRange uses tickSpacing=1, whose word range
// exceeds the per-multicall cap, to prove the scan splits into bounded batches
// rather than one aggregate call an RPC provider would reject.
func TestBaselineTicks_ChunksWideWordRange(t *testing.T) {
	pool := tickTestPool()
	pool.TickSpacing = 1
	blockHash := common.HexToHash("0xabc5")

	minWord, maxWord := tickbitmap.WordBounds(pool.TickSpacing)
	totalWords := int(maxWord) - int(minWord) + 1
	if totalWords <= baselineTickBitmapWordsPerCall {
		t.Fatalf("fixture invalid: %d words, want more than %d", totalWords, baselineTickBitmapWordsPerCall)
	}

	var (
		mu         sync.Mutex
		batchSizes []int
		seenWords  = make(map[int16]bool)
	)
	mc := testutil.NewMockMulticaller()
	mc.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		mu.Lock()
		defer mu.Unlock()
		batchSizes = append(batchSizes, len(calls))
		results := make([]outbound.Result, len(calls))
		for i, call := range calls {
			seenWords[wordFromCallData(t, call.CallData)] = true
			results[i] = outbound.Result{Success: true, ReturnData: common.LeftPadBytes(big.NewInt(0).Bytes(), 32)}
		}
		return results, nil
	}

	if _, err := BaselineTicks(context.Background(), mc, pool, blockHash); err != nil {
		t.Fatalf("BaselineTicks: %v", err)
	}

	if len(batchSizes) < 2 {
		t.Fatalf("batches = %d, want more than one for a %d-word range", len(batchSizes), totalWords)
	}
	for i, size := range batchSizes {
		if size > baselineTickBitmapWordsPerCall {
			t.Errorf("batch %d has %d calls, want at most %d", i, size, baselineTickBitmapWordsPerCall)
		}
	}
	if len(seenWords) != totalWords {
		t.Errorf("scanned %d distinct words, want the full range of %d", len(seenWords), totalWords)
	}
}

func TestBaselineTicks_FailureModes(t *testing.T) {
	tests := []struct {
		name      string
		executeFn func(ctx context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error)
		wantSub   string
	}{
		{
			name: "transport error",
			executeFn: func(context.Context, []outbound.Call, common.Hash) ([]outbound.Result, error) {
				return nil, fmt.Errorf("rpc down")
			},
			wantSub: "rpc down",
		},
		{
			name: "reverted sub-call",
			executeFn: func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
				results := make([]outbound.Result, len(calls))
				for i := range results {
					results[i] = outbound.Result{Success: true, ReturnData: common.LeftPadBytes(big.NewInt(0).Bytes(), 32)}
				}
				results[0] = outbound.Result{Success: false}
				return results, nil
			},
			wantSub: "reverted",
		},
		{
			name: "result count mismatch",
			executeFn: func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
				return make([]outbound.Result, len(calls)-1), nil
			},
			wantSub: "result count",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := testutil.NewMockMulticaller()
			mc.ExecuteAtHashFn = tt.executeFn
			_, err := BaselineTicks(context.Background(), mc, tickTestPool(), common.HexToHash("0xabc1"))
			if err == nil {
				t.Fatalf("BaselineTicks: want error for %s, got nil", tt.name)
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Errorf("error %q does not mention %q", err, tt.wantSub)
			}
		})
	}
}

// TestBaselineTicks_PinsToBlockHash guards the reorg-correctness rule: state
// reads must never resolve by block number.
func TestBaselineTicks_PinsToBlockHash(t *testing.T) {
	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(context.Context, []outbound.Call, *big.Int) ([]outbound.Result, error) {
		t.Fatal("Execute must not be called; every state read pins to a block hash")
		return nil, nil
	}
	mc.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		results := make([]outbound.Result, len(calls))
		for i := range results {
			results[i] = outbound.Result{Success: true, ReturnData: common.LeftPadBytes(big.NewInt(0).Bytes(), 32)}
		}
		return results, nil
	}

	if _, err := BaselineTicks(context.Background(), mc, tickTestPool(), common.HexToHash("0xabc1")); err != nil {
		t.Fatalf("BaselineTicks: %v", err)
	}
}

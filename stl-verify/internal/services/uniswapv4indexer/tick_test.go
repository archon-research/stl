package uniswapv4indexer

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math/big"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/tickbitmap"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// Recorded from mainnet at the state fixtures' pool and block (state_test.go):
// cast call <StateView> "getTickInfo(bytes32,int24)" <ethWstethPoolID> -1873.
const (
	mainnetTickInfoReturn = "0000000000000000000000000000000000000000000000027769c57fee0baf02" +
		"fffffffffffffffffffffffffffffffffffffffffffffffd88963a8011f450fe" +
		"0000000000000000000000000000000000003284b0bf979fb9f9a08768ee417f" +
		"0000000000000000000000000000000000002b121f4f9d09a00dcb9dbe17097d"
	mainnetTickBitmapWordMinus8Return = "0000000000000000001080000000008004000000000000000000000000000000"
)

// Hand-decoded from the words above. v4-core's TickInfo packs liquidityGross
// low and liquidityNet high in slot +0, then fee growths at +1 and +2.
const (
	mainnetTickInfoTick          = -1873
	mainnetLiquidityGross        = "45498113863732408066"
	mainnetLiquidityNet          = "-45498113863732408066"
	mainnetFeeGrowthOutside0X128 = "1024633298617050289025684201161087"
	mainnetFeeGrowthOutside1X128 = "873579410164311889164547676113277"
)

const (
	getTickInfoSelector   = "7c40f1fe"
	getTickBitmapSelector = "1c7ccb4c"
)

func tickTestPool() RegisteredPool {
	pool := decodeTestPool(7, ethWstethPoolID)
	pool.TickSpacing = 60
	return pool
}

func liquidityEvent(tickLower, tickUpper int, liquidityDelta int64) *entity.UniswapV4LiquidityEvent {
	return &entity.UniswapV4LiquidityEvent{
		PoolID:         7,
		TickLower:      tickLower,
		TickUpper:      tickUpper,
		LiquidityDelta: big.NewInt(liquidityDelta),
	}
}

func packTickInfoReturn(t *testing.T, liquidityGross, liquidityNet, fg0, fg1 *big.Int) []byte {
	t.Helper()
	return abiWords(liquidityGross, liquidityNet, fg0, fg1)
}

func signedWord(b []byte) *big.Int {
	v := new(big.Int).SetBytes(b)
	if v.Bit(255) == 1 {
		v.Sub(v, new(big.Int).Lsh(big.NewInt(1), 256))
	}
	return v
}

func argWords(t *testing.T, callData []byte, wantSelector string, wantArgs int) [][]byte {
	t.Helper()
	if len(callData) != 4+32*wantArgs {
		t.Fatalf("calldata is %d bytes, want selector + %d words", len(callData), wantArgs)
	}
	if got := hex.EncodeToString(callData[:4]); got != wantSelector {
		t.Fatalf("selector = %s, want %s", got, wantSelector)
	}
	args := make([][]byte, wantArgs)
	for i := range args {
		args[i] = callData[4+32*i : 4+32*(i+1)]
	}
	return args
}

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
			got := TouchedTicks(tt.events)
			if !slices.Equal(got, tt.want) {
				t.Errorf("TouchedTicks() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTouchedTicks_ExcludesZeroDeltaPokes(t *testing.T) {
	events := []*entity.UniswapV4LiquidityEvent{
		liquidityEvent(-120, 180, 0),
		liquidityEvent(-600, 600, 42),
	}

	got := TouchedTicks(events)

	if want := []int32{-600, 600}; !slices.Equal(got, want) {
		t.Errorf("TouchedTicks() = %v, want %v (the zero-delta poke's bounds must be excluded)", got, want)
	}
}

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

	for i, call := range calls {
		if call.Target != pool.StateView {
			t.Errorf("call %d target = %s, want the StateView %s", i, call.Target, pool.StateView)
		}
		if call.AllowFailure {
			t.Errorf("call %d is AllowFailure; an authoritative tick read must fail loud", i)
		}
		args := argWords(t, call.CallData, getTickInfoSelector, 2)
		if got := common.BytesToHash(args[0]); got != pool.PoolIDHash {
			t.Errorf("call %d poolId = %s, want %s", i, got, pool.PoolIDHash)
		}
		if got := int32(signedWord(args[1]).Int64()); got != ticks[i] {
			t.Errorf("call %d tick = %d, want %d (order must match the input)", i, got, ticks[i])
		}
	}
}

func TestBuildTickCalls_MatchesRecordedCall(t *testing.T) {
	pool := decodeTestPool(7, ethWstethPoolID)

	calls, err := BuildTickCalls(pool, []int32{mainnetTickInfoTick})
	if err != nil {
		t.Fatalf("BuildTickCalls: %v", err)
	}

	want := append(hexBytes(t, getTickInfoSelector),
		abiWords(new(big.Int).SetBytes(pool.PoolIDHash.Bytes()), big.NewInt(mainnetTickInfoTick))...)
	if !bytes.Equal(calls[0].CallData, want) {
		t.Errorf("calldata = %x, want %x", calls[0].CallData, want)
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

func TestDecodeTick_DecodesRecordedMainnetReturn(t *testing.T) {
	pool := decodeTestPool(7, ethWstethPoolID)
	res := outbound.Result{Success: true, ReturnData: hexBytes(t, mainnetTickInfoReturn)}

	got, err := DecodeTick(pool, mainnetTickInfoTick, fixtureBlock, 3, blockTS, res)
	if err != nil {
		t.Fatalf("DecodeTick: %v", err)
	}
	if err := got.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	if got.PoolID != pool.ID || got.Tick != mainnetTickInfoTick {
		t.Errorf("identity = (pool %d, tick %d), want (%d, %d)", got.PoolID, got.Tick, pool.ID, mainnetTickInfoTick)
	}
	if got.BlockNumber != fixtureBlock || got.BlockVersion != 3 || !got.BlockTimestamp.Equal(blockTS) {
		t.Errorf("block identity = (%d, %d, %v), want (%d, 3, %v)", got.BlockNumber, got.BlockVersion, got.BlockTimestamp, fixtureBlock, blockTS)
	}
	if want := mustBigInt(mainnetLiquidityGross); got.LiquidityGross.Cmp(want) != 0 {
		t.Errorf("LiquidityGross = %s, want %s", got.LiquidityGross, want)
	}
	if want := mustBigInt(mainnetLiquidityNet); got.LiquidityNet.Cmp(want) != 0 {
		t.Errorf("LiquidityNet = %s, want %s (signed int128)", got.LiquidityNet, want)
	}
	if want := mustBigInt(mainnetFeeGrowthOutside0X128); got.FeeGrowthOutside0X128.Cmp(want) != 0 {
		t.Errorf("FeeGrowthOutside0X128 = %s, want %s", got.FeeGrowthOutside0X128, want)
	}
	if want := mustBigInt(mainnetFeeGrowthOutside1X128); got.FeeGrowthOutside1X128.Cmp(want) != 0 {
		t.Errorf("FeeGrowthOutside1X128 = %s, want %s", got.FeeGrowthOutside1X128, want)
	}
}

func TestTickFixtureReproducesMainnetBytes(t *testing.T) {
	got := packTickInfoReturn(t,
		mustBigInt(mainnetLiquidityGross),
		mustBigInt(mainnetLiquidityNet),
		mustBigInt(mainnetFeeGrowthOutside0X128),
		mustBigInt(mainnetFeeGrowthOutside1X128),
	)
	if want := hexBytes(t, mainnetTickInfoReturn); !bytes.Equal(got, want) {
		t.Errorf("packed getTickInfo return = %x, want the recorded %x", got, want)
	}
}

// v4-core's TickInfo carries no Initialized flag.
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

func bitmapWord(bits ...uint) *big.Int {
	w := new(big.Int)
	for _, b := range bits {
		w.SetBit(w, int(b), 1)
	}
	return w
}

func bitmapWordResult(t *testing.T, bits ...uint) outbound.Result {
	t.Helper()
	return outbound.Result{Success: true, ReturnData: abiWords(bitmapWord(bits...))}
}

func wordFromCallData(t *testing.T, callData []byte) int16 {
	t.Helper()
	return int16(signedWord(argWords(t, callData, getTickBitmapSelector, 2)[1]).Int64())
}

// Bit 175 of word -8 is tick -1873, the recorded getTickInfo tick.
func TestBaselineTicks_DecodesRecordedBitmapWord(t *testing.T) {
	pool := decodeTestPool(7, ethWstethPoolID)
	recorded := hexBytes(t, mainnetTickBitmapWordMinus8Return)

	mc := testutil.NewMockMulticaller()
	mc.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		results := make([]outbound.Result, len(calls))
		for i, call := range calls {
			data := abiWords(big.NewInt(0))
			if wordFromCallData(t, call.CallData) == -8 {
				data = recorded
			}
			results[i] = outbound.Result{Success: true, ReturnData: data}
		}
		return results, nil
	}

	got, err := BaselineTicks(context.Background(), mc, pool, common.HexToHash(fixtureBlockHash))
	if err != nil {
		t.Fatalf("BaselineTicks: %v", err)
	}
	if want := []int32{-1926, -1913, mainnetTickInfoTick, -1868}; !slices.Equal(got, want) {
		t.Errorf("BaselineTicks() = %v, want %v", got, want)
	}
}

func TestBaselineTicks_DecodesSetBitsToTicks(t *testing.T) {
	pool := tickTestPool()
	blockHash := common.HexToHash("0xabc1")
	minWord, maxWord, err := tickbitmap.WordBounds(pool.TickSpacing)
	if err != nil {
		t.Fatalf("WordBounds: %v", err)
	}
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
			results[i] = outbound.Result{Success: true, ReturnData: abiWords(word)}
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

func TestBaselineTicks_ChunksWideWordRange(t *testing.T) {
	pool := tickTestPool()
	pool.TickSpacing = 1
	blockHash := common.HexToHash("0xabc5")

	minWord, maxWord, err := tickbitmap.WordBounds(pool.TickSpacing)
	if err != nil {
		t.Fatalf("WordBounds: %v", err)
	}
	totalWords := int(maxWord) - int(minWord) + 1
	if totalWords <= tickbitmap.BitmapWordsPerCall {
		t.Fatalf("fixture invalid: %d words, want more than %d", totalWords, tickbitmap.BitmapWordsPerCall)
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
			results[i] = outbound.Result{Success: true, ReturnData: abiWords(big.NewInt(0))}
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
		if size > tickbitmap.BitmapWordsPerCall {
			t.Errorf("batch %d has %d calls, want at most %d", i, size, tickbitmap.BitmapWordsPerCall)
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
					results[i] = outbound.Result{Success: true, ReturnData: abiWords(big.NewInt(0))}
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

func TestBaselineTicks_PinsToBlockHash(t *testing.T) {
	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(context.Context, []outbound.Call, *big.Int) ([]outbound.Result, error) {
		t.Fatal("Execute must not be called; every state read pins to a block hash")
		return nil, nil
	}
	mc.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		results := make([]outbound.Result, len(calls))
		for i := range results {
			results[i] = outbound.Result{Success: true, ReturnData: abiWords(big.NewInt(0))}
		}
		return results, nil
	}

	if _, err := BaselineTicks(context.Background(), mc, tickTestPool(), common.HexToHash("0xabc1")); err != nil {
		t.Fatalf("BaselineTicks: %v", err)
	}
}

package uniswapv4indexer

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var (
	posmOwner   = common.HexToAddress("0xbD216513d74C8cf14cf4747E6AaA6420FF64ee9e")
	hookOwner   = common.HexToAddress("0x4440854B2d02C57A0Dc5c58b7A884562D875c0c4")
	tokenIDSalt = common.BigToHash(big.NewInt(51423))
)

func positionTestPool() RegisteredPool {
	return decodeTestPool(7, ethWstethPoolID)
}

func positionKey(owner common.Address, tickLower, tickUpper int, salt common.Hash) entity.UniswapV4PositionKey {
	return entity.UniswapV4PositionKey{Owner: owner, TickLower: tickLower, TickUpper: tickUpper, Salt: salt}
}

func positionLiquidityEvent(owner common.Address, tickLower, tickUpper int, salt common.Hash, liquidityDelta int64) *entity.UniswapV4LiquidityEvent {
	return &entity.UniswapV4LiquidityEvent{
		PoolID:         7,
		Sender:         owner,
		TickLower:      tickLower,
		TickUpper:      tickUpper,
		Salt:           salt,
		LiquidityDelta: big.NewInt(liquidityDelta),
	}
}

// positionViewTestABI parses the getter independently of position.go's own ABI.
func positionViewTestABI(t *testing.T) *abi.ABI {
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
		t.Fatalf("parsing position view test ABI: %v", err)
	}
	return &a
}

func packPositionInfoReturn(t *testing.T, liquidity, feeGrowthInside0, feeGrowthInside1 *big.Int) []byte {
	t.Helper()
	packed, err := positionViewTestABI(t).Methods["getPositionInfo"].Outputs.Pack(liquidity, feeGrowthInside0, feeGrowthInside1)
	if err != nil {
		t.Fatalf("packing getPositionInfo return: %v", err)
	}
	return packed
}

func TestPositionViewABI_SelectorIsPinned(t *testing.T) {
	a, err := positionViewABI()
	if err != nil {
		t.Fatalf("loading StateView position ABI: %v", err)
	}

	method, ok := a.Methods["getPositionInfo"]
	if !ok {
		t.Fatal("getPositionInfo missing from the StateView position ABI")
	}
	if want := "getPositionInfo(bytes32,address,int24,int24,bytes32)"; method.Sig != want {
		t.Errorf("signature = %q, want %q", method.Sig, want)
	}
	if got, want := common.Bytes2Hex(method.ID), "dacf1d2f"; got != want {
		t.Errorf("selector = 0x%s, want 0x%s", got, want)
	}
}

func TestTouchedPositions(t *testing.T) {
	tests := []struct {
		name   string
		events []*entity.UniswapV4LiquidityEvent
		want   []entity.UniswapV4PositionKey
	}{
		{name: "no events", events: nil, want: nil},
		{
			name:   "single position",
			events: []*entity.UniswapV4LiquidityEvent{positionLiquidityEvent(posmOwner, -60, 60, tokenIDSalt, 1000)},
			want:   []entity.UniswapV4PositionKey{positionKey(posmOwner, -60, 60, tokenIDSalt)},
		},
		{
			name: "repeated touches of one position collapse",
			events: []*entity.UniswapV4LiquidityEvent{
				positionLiquidityEvent(posmOwner, -60, 60, tokenIDSalt, 1000),
				positionLiquidityEvent(posmOwner, -60, 60, tokenIDSalt, -400),
			},
			want: []entity.UniswapV4PositionKey{positionKey(posmOwner, -60, 60, tokenIDSalt)},
		},
		{
			name: "same range different salt stays two positions, sorted",
			events: []*entity.UniswapV4LiquidityEvent{
				positionLiquidityEvent(posmOwner, -60, 60, common.BigToHash(big.NewInt(2)), 1000),
				positionLiquidityEvent(posmOwner, -60, 60, common.BigToHash(big.NewInt(1)), 1000),
			},
			want: []entity.UniswapV4PositionKey{
				positionKey(posmOwner, -60, 60, common.BigToHash(big.NewInt(1))),
				positionKey(posmOwner, -60, 60, common.BigToHash(big.NewInt(2))),
			},
		},
		{
			name: "owner orders ahead of the tick range",
			events: []*entity.UniswapV4LiquidityEvent{
				positionLiquidityEvent(posmOwner, -60, 60, tokenIDSalt, 1000),
				positionLiquidityEvent(hookOwner, -600, 600, tokenIDSalt, 1000),
			},
			want: []entity.UniswapV4PositionKey{
				positionKey(hookOwner, -600, 600, tokenIDSalt),
				positionKey(posmOwner, -60, 60, tokenIDSalt),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := TouchedPositions(tt.events)
			if !slices.Equal(got, tt.want) {
				t.Errorf("TouchedPositions() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestTouchedPositions_IncludesZeroDeltaPokes(t *testing.T) {
	events := []*entity.UniswapV4LiquidityEvent{positionLiquidityEvent(posmOwner, -60, 60, tokenIDSalt, 0)}

	got := TouchedPositions(events)

	want := []entity.UniswapV4PositionKey{positionKey(posmOwner, -60, 60, tokenIDSalt)}
	if !slices.Equal(got, want) {
		t.Errorf("TouchedPositions() = %+v, want %+v (a poke moves the fee-growth checkpoints)", got, want)
	}
}

func TestMergePositionKeys(t *testing.T) {
	tests := []struct {
		name string
		a, b []entity.UniswapV4PositionKey
		want []entity.UniswapV4PositionKey
	}{
		{name: "both empty", a: nil, b: nil, want: nil},
		{
			name: "overlapping sets dedup and sort",
			a: []entity.UniswapV4PositionKey{
				positionKey(posmOwner, -60, 60, tokenIDSalt),
				positionKey(hookOwner, -600, 600, tokenIDSalt),
			},
			b: []entity.UniswapV4PositionKey{
				positionKey(posmOwner, -60, 60, tokenIDSalt),
				positionKey(posmOwner, -120, 120, tokenIDSalt),
			},
			want: []entity.UniswapV4PositionKey{
				positionKey(hookOwner, -600, 600, tokenIDSalt),
				positionKey(posmOwner, -120, 120, tokenIDSalt),
				positionKey(posmOwner, -60, 60, tokenIDSalt),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MergePositionKeys(tt.a, tt.b)
			if !slices.Equal(got, tt.want) {
				t.Errorf("MergePositionKeys() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestBuildPositionCalls(t *testing.T) {
	pool := positionTestPool()
	keys := []entity.UniswapV4PositionKey{
		positionKey(posmOwner, -60, 60, tokenIDSalt),
		positionKey(hookOwner, -887272, 887272, common.Hash{}),
	}

	calls, err := BuildPositionCalls(pool, keys)
	if err != nil {
		t.Fatalf("BuildPositionCalls: %v", err)
	}
	if len(calls) != len(keys) {
		t.Fatalf("got %d calls, want %d", len(calls), len(keys))
	}

	a := positionViewTestABI(t)
	for i, call := range calls {
		if call.Target != pool.StateView {
			t.Errorf("call %d target = %s, want the StateView %s", i, call.Target, pool.StateView)
		}
		if call.AllowFailure {
			t.Errorf("call %d is AllowFailure; an authoritative position read must fail loud", i)
		}
		args, err := a.Methods["getPositionInfo"].Inputs.Unpack(call.CallData[4:])
		if err != nil {
			t.Fatalf("unpacking call %d args: %v", i, err)
		}
		if got := common.Hash(args[0].([32]byte)); got != pool.PoolIDHash {
			t.Errorf("call %d poolId = %s, want %s", i, got, pool.PoolIDHash)
		}
		if got := args[1].(common.Address); got != keys[i].Owner {
			t.Errorf("call %d owner = %s, want %s (order must match the input)", i, got, keys[i].Owner)
		}
		if got := int(args[2].(*big.Int).Int64()); got != keys[i].TickLower {
			t.Errorf("call %d tickLower = %d, want %d", i, got, keys[i].TickLower)
		}
		if got := int(args[3].(*big.Int).Int64()); got != keys[i].TickUpper {
			t.Errorf("call %d tickUpper = %d, want %d", i, got, keys[i].TickUpper)
		}
		if got := common.Hash(args[4].([32]byte)); got != keys[i].Salt {
			t.Errorf("call %d salt = %s, want %s", i, got, keys[i].Salt)
		}
	}
}

func TestBuildPositionCalls_EmptyInput(t *testing.T) {
	calls, err := BuildPositionCalls(positionTestPool(), nil)
	if err != nil {
		t.Fatalf("BuildPositionCalls: %v", err)
	}
	if len(calls) != 0 {
		t.Errorf("got %d calls, want 0", len(calls))
	}
}

func TestDecodePosition(t *testing.T) {
	pool := positionTestPool()
	key := positionKey(posmOwner, -60, 60, tokenIDSalt)
	feeGrowth0 := mustBigInt("340282366920938463463374607431768211456")
	res := outbound.Result{Success: true, ReturnData: packPositionInfoReturn(t, big.NewInt(1000), feeGrowth0, big.NewInt(2))}

	got, err := DecodePosition(pool, key, blockNumber, 3, blockTS, res)
	if err != nil {
		t.Fatalf("DecodePosition: %v", err)
	}
	if err := got.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if got.PoolID != pool.ID || got.Key() != key {
		t.Errorf("identity = (pool %d, %+v), want (%d, %+v)", got.PoolID, got.Key(), pool.ID, key)
	}
	if got.BlockNumber != blockNumber || got.BlockVersion != 3 || !got.BlockTimestamp.Equal(blockTS) {
		t.Errorf("block identity = (%d, %d, %v), want (%d, 3, %v)", got.BlockNumber, got.BlockVersion, got.BlockTimestamp, blockNumber, blockTS)
	}
	if got.Liquidity.Cmp(big.NewInt(1000)) != 0 {
		t.Errorf("Liquidity = %s, want 1000", got.Liquidity)
	}
	if got.FeeGrowthInside0LastX128.Cmp(feeGrowth0) != 0 {
		t.Errorf("FeeGrowthInside0LastX128 = %s, want %s", got.FeeGrowthInside0LastX128, feeGrowth0)
	}
	if got.FeeGrowthInside1LastX128.Cmp(big.NewInt(2)) != 0 {
		t.Errorf("FeeGrowthInside1LastX128 = %s, want 2", got.FeeGrowthInside1LastX128)
	}
}

func TestDecodePosition_BurnedPositionIsAllZeros(t *testing.T) {
	res := outbound.Result{Success: true, ReturnData: packPositionInfoReturn(t, big.NewInt(0), big.NewInt(0), big.NewInt(0))}

	got, err := DecodePosition(positionTestPool(), positionKey(posmOwner, -60, 60, tokenIDSalt), blockNumber, 0, blockTS, res)
	if err != nil {
		t.Fatalf("DecodePosition: %v", err)
	}
	if got.Liquidity.Sign() != 0 {
		t.Errorf("Liquidity = %s, want 0 for a burned position", got.Liquidity)
	}
}

func TestDecodePosition_FailureModes(t *testing.T) {
	tests := []struct {
		name    string
		key     entity.UniswapV4PositionKey
		res     outbound.Result
		wantSub string
	}{
		{
			name:    "reverted call",
			key:     positionKey(posmOwner, -60, 60, tokenIDSalt),
			res:     outbound.Result{Success: false},
			wantSub: "reverted",
		},
		{
			name:    "undecodable payload",
			key:     positionKey(posmOwner, -60, 60, tokenIDSalt),
			res:     outbound.Result{Success: true, ReturnData: []byte{0x01}},
			wantSub: "unpacking",
		},
		{
			name:    "unordered tick bounds",
			key:     positionKey(posmOwner, 60, -60, tokenIDSalt),
			res:     outbound.Result{Success: true, ReturnData: packPositionInfoReturn(t, big.NewInt(1), big.NewInt(0), big.NewInt(0))},
			wantSub: "validating",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodePosition(positionTestPool(), tt.key, blockNumber, 0, blockTS, tt.res)
			if err == nil {
				t.Fatalf("DecodePosition: want error for %s, got nil", tt.name)
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Errorf("error %q does not mention %q", err, tt.wantSub)
			}
		})
	}
}

// positionKeysN builds n distinct keys, varying only the salt.
func positionKeysN(n int) []entity.UniswapV4PositionKey {
	keys := make([]entity.UniswapV4PositionKey, n)
	for i := range keys {
		keys[i] = positionKey(posmOwner, -60, 60, common.BigToHash(big.NewInt(int64(i))))
	}
	return keys
}

func TestReadPositions_NoKeysIssuesNoCall(t *testing.T) {
	mc := testutil.NewMockMulticaller()

	rows, err := ReadPositions(context.Background(), mc, positionTestPool(), nil,
		common.HexToHash("0xabc1"), blockNumber, blockVer, blockTS)
	if err != nil {
		t.Fatalf("ReadPositions: %v", err)
	}
	if rows != nil {
		t.Errorf("rows = %v, want nil", rows)
	}
	if mc.CallCount != 0 {
		t.Errorf("multicall invocations = %d, want 0", mc.CallCount)
	}
}

func TestReadPositions_PinsEveryBatchToTheBlockHashAndStampsTheBlock(t *testing.T) {
	pool := positionTestPool()
	blockHash := common.HexToHash("0xabc1")
	keys := positionKeysN(2)

	mc := testutil.NewMockMulticaller()
	mc.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		results := make([]outbound.Result, len(calls))
		for i := range results {
			results[i] = outbound.Result{Success: true, ReturnData: packPositionInfoReturn(t, big.NewInt(11), big.NewInt(22), big.NewInt(33))}
		}
		return results, nil
	}

	rows, err := ReadPositions(context.Background(), mc, pool, keys, blockHash, blockNumber, blockVer, blockTS)
	if err != nil {
		t.Fatalf("ReadPositions: %v", err)
	}
	if len(rows) != len(keys) {
		t.Fatalf("rows = %d, want %d", len(rows), len(keys))
	}
	for i, row := range rows {
		if row.Key() != keys[i] {
			t.Errorf("row %d key = %+v, want %+v", i, row.Key(), keys[i])
		}
		if row.BlockNumber != blockNumber || row.BlockVersion != blockVer || !row.BlockTimestamp.Equal(blockTS) {
			t.Errorf("row %d block identity = (%d, %d, %s)", i, row.BlockNumber, row.BlockVersion, row.BlockTimestamp)
		}
		if row.PoolID != pool.ID {
			t.Errorf("row %d poolID = %d, want %d", i, row.PoolID, pool.ID)
		}
	}
	for i, inv := range mc.Invocations {
		if !inv.ViaHash || inv.BlockHash != blockHash {
			t.Errorf("invocation %d pinned to %s (viaHash=%v), want %s", i, inv.BlockHash, inv.ViaHash, blockHash)
		}
	}
}

func TestReadPositions_SplitsAboveTheBatchCapAndKeepsInputOrder(t *testing.T) {
	keys := positionKeysN(positionsPerCall + 3)

	mc := testutil.NewMockMulticaller()
	mc.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		if len(calls) > positionsPerCall {
			return nil, fmt.Errorf("batch of %d exceeds the %d cap", len(calls), positionsPerCall)
		}
		results := make([]outbound.Result, len(calls))
		for i := range results {
			results[i] = outbound.Result{Success: true, ReturnData: packPositionInfoReturn(t, big.NewInt(1), big.NewInt(0), big.NewInt(0))}
		}
		return results, nil
	}

	rows, err := ReadPositions(context.Background(), mc, positionTestPool(), keys,
		common.HexToHash("0xabc1"), blockNumber, blockVer, blockTS)
	if err != nil {
		t.Fatalf("ReadPositions: %v", err)
	}
	if len(rows) != len(keys) {
		t.Fatalf("rows = %d, want %d", len(rows), len(keys))
	}
	if mc.CallCount != 2 {
		t.Errorf("multicall invocations = %d, want 2", mc.CallCount)
	}
	for i, row := range rows {
		if row.Key() != keys[i] {
			t.Fatalf("row %d key = %+v, want %+v: batches must concatenate in input order", i, row.Key(), keys[i])
		}
	}
}

func TestReadPositions_FailsOnEveryBrokenBatch(t *testing.T) {
	tests := []struct {
		name      string
		executeFn func(context.Context, []outbound.Call, common.Hash) ([]outbound.Result, error)
	}{
		{
			name: "multicall error",
			executeFn: func(context.Context, []outbound.Call, common.Hash) ([]outbound.Result, error) {
				return nil, errors.New("archive node unavailable")
			},
		},
		{
			name: "result count mismatch",
			executeFn: func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
				return make([]outbound.Result, len(calls)-1), nil
			},
		},
		{
			name: "sub-call reverted",
			executeFn: func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
				results := make([]outbound.Result, len(calls))
				for i := range results {
					results[i] = outbound.Result{Success: false}
				}
				return results, nil
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := testutil.NewMockMulticaller()
			mc.ExecuteAtHashFn = tt.executeFn

			if _, err := ReadPositions(context.Background(), mc, positionTestPool(), positionKeysN(2),
				common.HexToHash("0xabc1"), blockNumber, blockVer, blockTS); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

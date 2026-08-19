package entity

import (
	"encoding/json"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

var (
	v4Sender = common.HexToAddress("0x0102030405060708090a0b0c0d0e0f1011121314")
	v4TxHash = common.HexToHash("0xaa02030405060708090a0b0c0d0e0f1011121314aabbccddeeff001122334455")
	v4Salt   = common.HexToHash("0x00000000000000000000000000000000000000000000000000000000000000ff")
)

func validV4PoolState() *UniswapV4PoolState {
	return &UniswapV4PoolState{
		PoolID:               1,
		BlockNumber:          100,
		BlockVersion:         0,
		BlockTimestamp:       time.Unix(1, 0).UTC(),
		SqrtPriceX96:         big.NewInt(1),
		Tick:                 0,
		ProtocolFee:          0,
		LpFee:                3000,
		Liquidity:            big.NewInt(1),
		FeeGrowthGlobal0X128: big.NewInt(0),
		FeeGrowthGlobal1X128: big.NewInt(0),
	}
}

func validV4Swap() *UniswapV4Swap {
	return &UniswapV4Swap{
		PoolID:         1,
		BlockNumber:    100,
		BlockVersion:   0,
		BlockTimestamp: time.Unix(1, 0).UTC(),
		TxHash:         v4TxHash,
		LogIndex:       0,
		Sender:         v4Sender,
		Amount0:        big.NewInt(-100),
		Amount1:        big.NewInt(100),
		SqrtPriceX96:   big.NewInt(1),
		Liquidity:      big.NewInt(1),
		Tick:           0,
		Fee:            3000,
	}
}

func validV4LiquidityEvent() *UniswapV4LiquidityEvent {
	return &UniswapV4LiquidityEvent{
		PoolID:         1,
		BlockNumber:    100,
		BlockVersion:   0,
		BlockTimestamp: time.Unix(1, 0).UTC(),
		TxHash:         v4TxHash,
		LogIndex:       0,
		Sender:         v4Sender,
		TickLower:      -100,
		TickUpper:      100,
		LiquidityDelta: big.NewInt(1000),
		Salt:           v4Salt,
	}
}

func validV4Tick() *UniswapV4Tick {
	return &UniswapV4Tick{
		PoolID:                1,
		Tick:                  100,
		BlockNumber:           100,
		BlockVersion:          0,
		BlockTimestamp:        time.Unix(1, 0).UTC(),
		LiquidityGross:        big.NewInt(1000),
		LiquidityNet:          big.NewInt(-500),
		FeeGrowthOutside0X128: big.NewInt(0),
		FeeGrowthOutside1X128: big.NewInt(0),
	}
}

func validV4PoolEvent() *UniswapV4PoolEvent {
	return &UniswapV4PoolEvent{
		PoolID:         1,
		BlockNumber:    100,
		BlockVersion:   0,
		BlockTimestamp: time.Unix(1, 0).UTC(),
		TxHash:         v4TxHash,
		LogIndex:       0,
		EventName:      UniswapV4PoolEventInitialize,
		Params:         json.RawMessage(`{"sqrtPriceX96":"1000","tick":10}`),
	}
}

func TestUniswapV4PoolState_Validate(t *testing.T) {
	cases := []struct {
		name    string
		mut     func(*UniswapV4PoolState)
		wantErr bool
	}{
		{"ok", func(*UniswapV4PoolState) {}, false},
		{"missing pool id", func(s *UniswapV4PoolState) { s.PoolID = 0 }, true},
		{"missing block number", func(s *UniswapV4PoolState) { s.BlockNumber = 0 }, true},
		{"negative block version", func(s *UniswapV4PoolState) { s.BlockVersion = -1 }, true},
		{"missing block timestamp", func(s *UniswapV4PoolState) { s.BlockTimestamp = time.Time{} }, true},
		{"nil sqrt price", func(s *UniswapV4PoolState) { s.SqrtPriceX96 = nil }, true},
		// StateView.getSlot0 answers all-zeros for an unregistered PoolId instead of
		// reverting, so a zero price is a registry bug rather than a real snapshot.
		{"zero sqrt price", func(s *UniswapV4PoolState) { s.SqrtPriceX96 = big.NewInt(0) }, true},
		{"negative sqrt price", func(s *UniswapV4PoolState) { s.SqrtPriceX96 = big.NewInt(-1) }, true},
		{"tick below int24 min", func(s *UniswapV4PoolState) { s.Tick = -8388609 }, true},
		{"tick above int24 max", func(s *UniswapV4PoolState) { s.Tick = 8388608 }, true},
		{"tick at int24 min boundary", func(s *UniswapV4PoolState) { s.Tick = -8388608 }, false},
		{"tick at int24 max boundary", func(s *UniswapV4PoolState) { s.Tick = 8388607 }, false},
		{"negative protocol fee", func(s *UniswapV4PoolState) { s.ProtocolFee = -1 }, true},
		{"protocol fee above uint24", func(s *UniswapV4PoolState) { s.ProtocolFee = 0x1000000 }, true},
		{"protocol fee at both halves max", func(s *UniswapV4PoolState) { s.ProtocolFee = 1000 | (1000 << 12) }, false},
		{"zero-for-one protocol fee above max", func(s *UniswapV4PoolState) { s.ProtocolFee = 1001 }, true},
		{"one-for-zero protocol fee above max", func(s *UniswapV4PoolState) { s.ProtocolFee = 1001 << 12 }, true},
		{"negative lp fee", func(s *UniswapV4PoolState) { s.LpFee = -1 }, true},
		{"lp fee at max", func(s *UniswapV4PoolState) { s.LpFee = 1_000_000 }, false},
		{"lp fee above max", func(s *UniswapV4PoolState) { s.LpFee = 1_000_001 }, true},
		{"nil liquidity", func(s *UniswapV4PoolState) { s.Liquidity = nil }, true},
		{"nil fee growth global0", func(s *UniswapV4PoolState) { s.FeeGrowthGlobal0X128 = nil }, true},
		{"nil fee growth global1", func(s *UniswapV4PoolState) { s.FeeGrowthGlobal1X128 = nil }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := validV4PoolState()
			tc.mut(s)
			assertValidateErr(t, s.Validate(), tc.wantErr)
		})
	}
}

func TestUniswapV4Swap_Validate(t *testing.T) {
	cases := []struct {
		name    string
		mut     func(*UniswapV4Swap)
		wantErr bool
	}{
		{"ok", func(*UniswapV4Swap) {}, false},
		{"missing pool id", func(s *UniswapV4Swap) { s.PoolID = 0 }, true},
		{"missing block number", func(s *UniswapV4Swap) { s.BlockNumber = 0 }, true},
		{"negative block version", func(s *UniswapV4Swap) { s.BlockVersion = -1 }, true},
		{"missing block timestamp", func(s *UniswapV4Swap) { s.BlockTimestamp = time.Time{} }, true},
		{"missing tx hash", func(s *UniswapV4Swap) { s.TxHash = common.Hash{} }, true},
		{"negative log index", func(s *UniswapV4Swap) { s.LogIndex = -1 }, true},
		{"missing sender", func(s *UniswapV4Swap) { s.Sender = common.Address{} }, true},
		{"nil amount0", func(s *UniswapV4Swap) { s.Amount0 = nil }, true},
		{"nil amount1", func(s *UniswapV4Swap) { s.Amount1 = nil }, true},
		// Both amounts carry the swapper's BalanceDelta, so any sign pairing is
		// legal on-chain (exact-in, exact-out, and zero-amount no-op swaps).
		{"both amounts positive", func(s *UniswapV4Swap) { s.Amount0, s.Amount1 = big.NewInt(1), big.NewInt(1) }, false},
		{"both amounts negative", func(s *UniswapV4Swap) { s.Amount0, s.Amount1 = big.NewInt(-1), big.NewInt(-1) }, false},
		{"both amounts zero", func(s *UniswapV4Swap) { s.Amount0, s.Amount1 = big.NewInt(0), big.NewInt(0) }, false},
		{"nil sqrt price", func(s *UniswapV4Swap) { s.SqrtPriceX96 = nil }, true},
		{"nil liquidity", func(s *UniswapV4Swap) { s.Liquidity = nil }, true},
		{"tick below int24 min", func(s *UniswapV4Swap) { s.Tick = -8388609 }, true},
		{"tick above int24 max", func(s *UniswapV4Swap) { s.Tick = 8388608 }, true},
		{"negative fee", func(s *UniswapV4Swap) { s.Fee = -1 }, true},
		{"fee at max", func(s *UniswapV4Swap) { s.Fee = 1_000_000 }, false},
		{"fee above max", func(s *UniswapV4Swap) { s.Fee = 1_000_001 }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := validV4Swap()
			tc.mut(s)
			assertValidateErr(t, s.Validate(), tc.wantErr)
		})
	}
}

func TestUniswapV4LiquidityEvent_Validate(t *testing.T) {
	cases := []struct {
		name    string
		mut     func(*UniswapV4LiquidityEvent)
		wantErr bool
	}{
		{"ok", func(*UniswapV4LiquidityEvent) {}, false},
		{"missing pool id", func(e *UniswapV4LiquidityEvent) { e.PoolID = 0 }, true},
		{"missing block number", func(e *UniswapV4LiquidityEvent) { e.BlockNumber = 0 }, true},
		{"negative block version", func(e *UniswapV4LiquidityEvent) { e.BlockVersion = -1 }, true},
		{"missing block timestamp", func(e *UniswapV4LiquidityEvent) { e.BlockTimestamp = time.Time{} }, true},
		{"missing tx hash", func(e *UniswapV4LiquidityEvent) { e.TxHash = common.Hash{} }, true},
		{"negative log index", func(e *UniswapV4LiquidityEvent) { e.LogIndex = -1 }, true},
		{"missing sender", func(e *UniswapV4LiquidityEvent) { e.Sender = common.Address{} }, true},
		{"tick_lower below int24 min", func(e *UniswapV4LiquidityEvent) { e.TickLower = -8388609 }, true},
		{"tick_upper above int24 max", func(e *UniswapV4LiquidityEvent) { e.TickUpper = 8388608 }, true},
		{"tick_lower equal tick_upper", func(e *UniswapV4LiquidityEvent) { e.TickUpper = e.TickLower }, true},
		{"tick_lower greater than tick_upper", func(e *UniswapV4LiquidityEvent) { e.TickLower, e.TickUpper = 100, -100 }, true},
		{"nil liquidity delta", func(e *UniswapV4LiquidityEvent) { e.LiquidityDelta = nil }, true},
		// modifyLiquidity(0) is the canonical fee-collect poke, so a zero delta is a
		// real event rather than a decode failure.
		{"zero liquidity delta", func(e *UniswapV4LiquidityEvent) { e.LiquidityDelta = big.NewInt(0) }, false},
		{"negative liquidity delta", func(e *UniswapV4LiquidityEvent) { e.LiquidityDelta = big.NewInt(-1000) }, false},
		{"zero salt", func(e *UniswapV4LiquidityEvent) { e.Salt = common.Hash{} }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := validV4LiquidityEvent()
			tc.mut(e)
			assertValidateErr(t, e.Validate(), tc.wantErr)
		})
	}
}

func TestUniswapV4Tick_Validate(t *testing.T) {
	cases := []struct {
		name    string
		mut     func(*UniswapV4Tick)
		wantErr bool
	}{
		{"ok", func(*UniswapV4Tick) {}, false},
		{"missing pool id", func(tk *UniswapV4Tick) { tk.PoolID = 0 }, true},
		{"tick below int24 min", func(tk *UniswapV4Tick) { tk.Tick = -8388609 }, true},
		{"tick above int24 max", func(tk *UniswapV4Tick) { tk.Tick = 8388608 }, true},
		{"missing block number", func(tk *UniswapV4Tick) { tk.BlockNumber = 0 }, true},
		{"negative block version", func(tk *UniswapV4Tick) { tk.BlockVersion = -1 }, true},
		{"missing block timestamp", func(tk *UniswapV4Tick) { tk.BlockTimestamp = time.Time{} }, true},
		{"nil liquidity gross", func(tk *UniswapV4Tick) { tk.LiquidityGross = nil }, true},
		{"nil liquidity net", func(tk *UniswapV4Tick) { tk.LiquidityNet = nil }, true},
		{"nil fee growth outside0", func(tk *UniswapV4Tick) { tk.FeeGrowthOutside0X128 = nil }, true},
		{"nil fee growth outside1", func(tk *UniswapV4Tick) { tk.FeeGrowthOutside1X128 = nil }, true},
		// An all-zero TickInfo is how V4 reports a never-initialized or cleared
		// tick; the row records that erasure and must validate.
		{"all zero tick info", func(tk *UniswapV4Tick) {
			tk.LiquidityGross = big.NewInt(0)
			tk.LiquidityNet = big.NewInt(0)
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tk := validV4Tick()
			tc.mut(tk)
			assertValidateErr(t, tk.Validate(), tc.wantErr)
		})
	}
}

func TestUniswapV4PoolEvent_Validate(t *testing.T) {
	cases := []struct {
		name    string
		mut     func(*UniswapV4PoolEvent)
		wantErr bool
	}{
		{"ok initialize", func(*UniswapV4PoolEvent) {}, false},
		{"ok donate", func(e *UniswapV4PoolEvent) { e.EventName = UniswapV4PoolEventDonate }, false},
		{"ok protocol_fee_updated", func(e *UniswapV4PoolEvent) { e.EventName = UniswapV4PoolEventProtocolFeeUpdated }, false},
		{"missing pool id", func(e *UniswapV4PoolEvent) { e.PoolID = 0 }, true},
		{"missing block number", func(e *UniswapV4PoolEvent) { e.BlockNumber = 0 }, true},
		{"negative block version", func(e *UniswapV4PoolEvent) { e.BlockVersion = -1 }, true},
		{"missing block timestamp", func(e *UniswapV4PoolEvent) { e.BlockTimestamp = time.Time{} }, true},
		{"missing tx hash", func(e *UniswapV4PoolEvent) { e.TxHash = common.Hash{} }, true},
		{"negative log index", func(e *UniswapV4PoolEvent) { e.LogIndex = -1 }, true},
		{"bad event name", func(e *UniswapV4PoolEvent) { e.EventName = "swap" }, true},
		{"v3 event name is not allowed", func(e *UniswapV4PoolEvent) { e.EventName = "flash" }, true},
		{"empty params", func(e *UniswapV4PoolEvent) { e.Params = nil }, true},
		{"invalid json params", func(e *UniswapV4PoolEvent) { e.Params = json.RawMessage(`not valid json`) }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := validV4PoolEvent()
			tc.mut(e)
			assertValidateErr(t, e.Validate(), tc.wantErr)
		})
	}
}

func assertValidateErr(t *testing.T, err error, wantErr bool) {
	t.Helper()
	if wantErr && err == nil {
		t.Error("expected error, got nil")
	}
	if !wantErr && err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

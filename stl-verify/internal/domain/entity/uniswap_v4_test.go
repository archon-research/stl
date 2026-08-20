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

func validV4Position() *UniswapV4Position {
	return &UniswapV4Position{
		PoolID:                   1,
		Owner:                    v4Sender,
		TickLower:                -100,
		TickUpper:                100,
		Salt:                     v4Salt,
		BlockNumber:              100,
		BlockVersion:             0,
		BlockTimestamp:           time.Unix(1, 0).UTC(),
		Liquidity:                big.NewInt(1000),
		FeeGrowthInside0LastX128: big.NewInt(0),
		FeeGrowthInside1LastX128: big.NewInt(0),
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
		{"zero sqrt price", func(s *UniswapV4PoolState) { s.SqrtPriceX96 = big.NewInt(0) }, true},
		{"negative sqrt price", func(s *UniswapV4PoolState) { s.SqrtPriceX96 = big.NewInt(-1) }, true},
		{"tick below TickMath min", func(s *UniswapV4PoolState) { s.Tick = -887273 }, true},
		{"tick above TickMath max", func(s *UniswapV4PoolState) { s.Tick = 887273 }, true},
		{"tick at TickMath min boundary", func(s *UniswapV4PoolState) { s.Tick = -887272 }, false},
		{"tick at TickMath max boundary", func(s *UniswapV4PoolState) { s.Tick = 887272 }, false},
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

func zeroV4PoolState(s *UniswapV4PoolState) {
	s.SqrtPriceX96 = big.NewInt(0)
	s.Tick = 0
	s.ProtocolFee = 0
	s.LpFee = 0
	s.Liquidity = big.NewInt(0)
	s.FeeGrowthGlobal0X128 = big.NewInt(0)
	s.FeeGrowthGlobal1X128 = big.NewInt(0)
}

func TestUniswapV4PoolState_ValidateAcceptsAllZeroReorgReRead(t *testing.T) {
	cases := []struct {
		name    string
		mut     func(*UniswapV4PoolState)
		wantErr bool
	}{
		{"all-zero re-read at block_version 1", func(s *UniswapV4PoolState) {
			zeroV4PoolState(s)
			s.BlockVersion = 1
		}, false},
		{"all-zero read at block_version 0", zeroV4PoolState, true},
		{"partly-zero re-read at block_version 1", func(s *UniswapV4PoolState) {
			zeroV4PoolState(s)
			s.BlockVersion = 1
			s.Liquidity = big.NewInt(5)
		}, true},
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
		{"both amounts positive", func(s *UniswapV4Swap) { s.Amount0, s.Amount1 = big.NewInt(1), big.NewInt(1) }, false},
		{"both amounts negative", func(s *UniswapV4Swap) { s.Amount0, s.Amount1 = big.NewInt(-1), big.NewInt(-1) }, false},
		{"both amounts zero", func(s *UniswapV4Swap) { s.Amount0, s.Amount1 = big.NewInt(0), big.NewInt(0) }, false},
		{"nil sqrt price", func(s *UniswapV4Swap) { s.SqrtPriceX96 = nil }, true},
		{"zero sqrt price", func(s *UniswapV4Swap) { s.SqrtPriceX96 = big.NewInt(0) }, true},
		{"negative sqrt price", func(s *UniswapV4Swap) { s.SqrtPriceX96 = big.NewInt(-1) }, true},
		{"nil liquidity", func(s *UniswapV4Swap) { s.Liquidity = nil }, true},
		{"tick below TickMath min", func(s *UniswapV4Swap) { s.Tick = -887273 }, true},
		{"tick above TickMath max", func(s *UniswapV4Swap) { s.Tick = 887273 }, true},
		{"tick at TickMath min boundary", func(s *UniswapV4Swap) { s.Tick = -887272 }, false},
		{"tick at TickMath max boundary", func(s *UniswapV4Swap) { s.Tick = 887272 }, false},
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
		{"tick_lower below TickMath min", func(e *UniswapV4LiquidityEvent) { e.TickLower = -887273 }, true},
		{"tick_upper above TickMath max", func(e *UniswapV4LiquidityEvent) { e.TickUpper = 887273 }, true},
		{"full-range ticks at TickMath boundaries", func(e *UniswapV4LiquidityEvent) { e.TickLower, e.TickUpper = -887272, 887272 }, false},
		{"tick_lower equal tick_upper", func(e *UniswapV4LiquidityEvent) { e.TickUpper = e.TickLower }, true},
		{"tick_lower greater than tick_upper", func(e *UniswapV4LiquidityEvent) { e.TickLower, e.TickUpper = 100, -100 }, true},
		{"nil liquidity delta", func(e *UniswapV4LiquidityEvent) { e.LiquidityDelta = nil }, true},
		{"zero liquidity delta from a fee-collect poke", func(e *UniswapV4LiquidityEvent) { e.LiquidityDelta = big.NewInt(0) }, false},
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
		{"tick below TickMath min", func(tk *UniswapV4Tick) { tk.Tick = -887273 }, true},
		{"tick above TickMath max", func(tk *UniswapV4Tick) { tk.Tick = 887273 }, true},
		{"tick at TickMath min boundary", func(tk *UniswapV4Tick) { tk.Tick = -887272 }, false},
		{"tick at TickMath max boundary", func(tk *UniswapV4Tick) { tk.Tick = 887272 }, false},
		{"missing block number", func(tk *UniswapV4Tick) { tk.BlockNumber = 0 }, true},
		{"negative block version", func(tk *UniswapV4Tick) { tk.BlockVersion = -1 }, true},
		{"missing block timestamp", func(tk *UniswapV4Tick) { tk.BlockTimestamp = time.Time{} }, true},
		{"nil liquidity gross", func(tk *UniswapV4Tick) { tk.LiquidityGross = nil }, true},
		{"nil liquidity net", func(tk *UniswapV4Tick) { tk.LiquidityNet = nil }, true},
		{"nil fee growth outside0", func(tk *UniswapV4Tick) { tk.FeeGrowthOutside0X128 = nil }, true},
		{"nil fee growth outside1", func(tk *UniswapV4Tick) { tk.FeeGrowthOutside1X128 = nil }, true},
		{"all zero tick info records a cleared tick", func(tk *UniswapV4Tick) {
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

func TestUniswapV4Position_Validate(t *testing.T) {
	cases := []struct {
		name    string
		mut     func(*UniswapV4Position)
		wantErr bool
	}{
		{"ok", func(*UniswapV4Position) {}, false},
		{"missing pool id", func(p *UniswapV4Position) { p.PoolID = 0 }, true},
		{"missing owner", func(p *UniswapV4Position) { p.Owner = common.Address{} }, true},
		{"tick_lower below TickMath min", func(p *UniswapV4Position) { p.TickLower = -887273 }, true},
		{"tick_upper above TickMath max", func(p *UniswapV4Position) { p.TickUpper = 887273 }, true},
		{"full-range ticks at TickMath boundaries", func(p *UniswapV4Position) { p.TickLower, p.TickUpper = -887272, 887272 }, false},
		{"tick_lower equal tick_upper", func(p *UniswapV4Position) { p.TickUpper = p.TickLower }, true},
		{"tick_lower greater than tick_upper", func(p *UniswapV4Position) { p.TickLower, p.TickUpper = 100, -100 }, true},
		{"zero salt", func(p *UniswapV4Position) { p.Salt = common.Hash{} }, false},
		{"missing block number", func(p *UniswapV4Position) { p.BlockNumber = 0 }, true},
		{"negative block version", func(p *UniswapV4Position) { p.BlockVersion = -1 }, true},
		{"missing block timestamp", func(p *UniswapV4Position) { p.BlockTimestamp = time.Time{} }, true},
		{"nil liquidity", func(p *UniswapV4Position) { p.Liquidity = nil }, true},
		{"nil fee growth inside0", func(p *UniswapV4Position) { p.FeeGrowthInside0LastX128 = nil }, true},
		{"nil fee growth inside1", func(p *UniswapV4Position) { p.FeeGrowthInside1LastX128 = nil }, true},
		// getPositionInfo answers a burned or never-opened position with explicit
		// zeros rather than reverting, so the erasure is a real row.
		{"all zero position info", func(p *UniswapV4Position) { p.Liquidity = big.NewInt(0) }, false},
		// The three getters are uint128/uint256 on chain, so a negative can only
		// come from a decode defect.
		{"negative liquidity", func(p *UniswapV4Position) { p.Liquidity = big.NewInt(-1) }, true},
		{"negative fee growth inside0", func(p *UniswapV4Position) { p.FeeGrowthInside0LastX128 = big.NewInt(-1) }, true},
		{"negative fee growth inside1", func(p *UniswapV4Position) { p.FeeGrowthInside1LastX128 = big.NewInt(-1) }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := validV4Position()
			tc.mut(p)
			assertValidateErr(t, p.Validate(), tc.wantErr)
		})
	}
}

// The key validates on its own because it crosses the repository boundary
// inbound, ahead of any row that would carry it.
func TestUniswapV4PositionKey_Validate(t *testing.T) {
	cases := []struct {
		name    string
		mut     func(*UniswapV4PositionKey)
		wantErr bool
	}{
		{"ok", func(*UniswapV4PositionKey) {}, false},
		{"missing owner", func(k *UniswapV4PositionKey) { k.Owner = common.Address{} }, true},
		{"tick_lower below TickMath min", func(k *UniswapV4PositionKey) { k.TickLower = -887273 }, true},
		{"tick_upper above TickMath max", func(k *UniswapV4PositionKey) { k.TickUpper = 887273 }, true},
		{"tick_lower greater than tick_upper", func(k *UniswapV4PositionKey) { k.TickLower, k.TickUpper = 100, -100 }, true},
		{"zero salt", func(k *UniswapV4PositionKey) { k.Salt = common.Hash{} }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			k := validV4Position().Key()
			tc.mut(&k)
			assertValidateErr(t, k.Validate(), tc.wantErr)
		})
	}
}

func TestUniswapV4Position_Key(t *testing.T) {
	p := validV4Position()

	want := UniswapV4PositionKey{Owner: v4Sender, TickLower: -100, TickUpper: 100, Salt: v4Salt}
	if got := p.Key(); got != want {
		t.Errorf("Key() = %+v, want %+v", got, want)
	}
}

// The comparator is the canonical order both the snapshot read and the
// repository's advisory-lock sequence derive from, so each field's precedence is
// pinned rather than left to whichever one a caller happens to vary.
func TestUniswapV4PositionKey_Compare(t *testing.T) {
	base := UniswapV4PositionKey{
		Owner:     common.HexToAddress("0x22"),
		TickLower: -100,
		TickUpper: 100,
		Salt:      common.HexToHash("0x22"),
	}
	lowerOwner := base
	lowerOwner.Owner = common.HexToAddress("0x11")
	lowerTickLower := base
	lowerTickLower.TickLower = -200
	lowerTickUpper := base
	lowerTickUpper.TickUpper = 50
	lowerSalt := base
	lowerSalt.Salt = common.HexToHash("0x11")

	cases := []struct {
		name string
		a, b UniswapV4PositionKey
		want int
	}{
		{"equal keys", base, base, 0},
		{"owner takes precedence over the tick range", lowerOwner, lowerTickLower, -1},
		{"tick_lower breaks an owner tie", lowerTickLower, base, -1},
		{"tick_upper breaks a tick_lower tie", lowerTickUpper, base, -1},
		{"salt breaks a tick-range tie", lowerSalt, base, -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.a.Compare(tc.b); got != tc.want {
				t.Errorf("a.Compare(b) = %d, want %d", got, tc.want)
			}
			if got := tc.b.Compare(tc.a); got != -tc.want {
				t.Errorf("b.Compare(a) = %d, want %d (the order must be antisymmetric)", got, -tc.want)
			}
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

package entity

import (
	"encoding/json"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

func intPtr(v int) *int { return &v }

func TestUniswapV3PoolState_Validate(t *testing.T) {
	valid := func() *UniswapV3PoolState {
		return &UniswapV3PoolState{
			PoolID:                     1,
			BlockNumber:                100,
			BlockVersion:               0,
			BlockTimestamp:             time.Unix(1, 0).UTC(),
			SqrtPriceX96:               big.NewInt(1),
			Tick:                       0,
			ObservationIndex:           0,
			ObservationCardinality:     1,
			ObservationCardinalityNext: 1,
			FeeProtocol:                0,
			Unlocked:                   true,
			Liquidity:                  big.NewInt(1),
			FeeGrowthGlobal0X128:       big.NewInt(0),
			FeeGrowthGlobal1X128:       big.NewInt(0),
			ProtocolFeesToken0:         big.NewInt(0),
			ProtocolFeesToken1:         big.NewInt(0),
			Balance0:                   big.NewInt(1),
			Balance1:                   big.NewInt(1),
			TwapTick:                   nil,
			TwapWindowSecs:             nil,
		}
	}

	cases := []struct {
		name    string
		mut     func(*UniswapV3PoolState)
		wantErr bool
	}{
		{"ok", func(*UniswapV3PoolState) {}, false},
		{"ok with twap set", func(s *UniswapV3PoolState) {
			s.TwapTick = intPtr(100)
			s.TwapWindowSecs = intPtr(3600)
		}, false},
		{"missing pool id", func(s *UniswapV3PoolState) { s.PoolID = 0 }, true},
		{"missing block number", func(s *UniswapV3PoolState) { s.BlockNumber = 0 }, true},
		{"negative block version", func(s *UniswapV3PoolState) { s.BlockVersion = -1 }, true},
		{"missing block timestamp", func(s *UniswapV3PoolState) { s.BlockTimestamp = time.Time{} }, true},
		{"nil sqrt price", func(s *UniswapV3PoolState) { s.SqrtPriceX96 = nil }, true},
		{"tick below int24 min", func(s *UniswapV3PoolState) { s.Tick = -8388609 }, true},
		{"tick above int24 max", func(s *UniswapV3PoolState) { s.Tick = 8388608 }, true},
		{"tick at int24 min boundary", func(s *UniswapV3PoolState) { s.Tick = -8388608 }, false},
		{"tick at int24 max boundary", func(s *UniswapV3PoolState) { s.Tick = 8388607 }, false},
		{"nil liquidity", func(s *UniswapV3PoolState) { s.Liquidity = nil }, true},
		{"nil fee growth global0", func(s *UniswapV3PoolState) { s.FeeGrowthGlobal0X128 = nil }, true},
		{"nil fee growth global1", func(s *UniswapV3PoolState) { s.FeeGrowthGlobal1X128 = nil }, true},
		{"nil protocol fees token0", func(s *UniswapV3PoolState) { s.ProtocolFeesToken0 = nil }, true},
		{"nil protocol fees token1", func(s *UniswapV3PoolState) { s.ProtocolFeesToken1 = nil }, true},
		{"nil balance0", func(s *UniswapV3PoolState) { s.Balance0 = nil }, true},
		{"nil balance1", func(s *UniswapV3PoolState) { s.Balance1 = nil }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := valid()
			tc.mut(s)
			err := s.Validate()
			if tc.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestUniswapV3Swap_Validate(t *testing.T) {
	addr := common.HexToAddress("0x0102030405060708090a0b0c0d0e0f1011121314")
	hash := common.HexToHash("0xaa02030405060708090a0b0c0d0e0f1011121314aabbccddeeff001122334455")

	valid := func() *UniswapV3Swap {
		return &UniswapV3Swap{
			PoolID:         1,
			BlockNumber:    100,
			BlockVersion:   0,
			BlockTimestamp: time.Unix(1, 0).UTC(),
			TxHash:         hash,
			LogIndex:       0,
			Sender:         addr,
			Recipient:      addr,
			Amount0:        big.NewInt(-100),
			Amount1:        big.NewInt(100),
			SqrtPriceX96:   big.NewInt(1),
			Liquidity:      big.NewInt(1),
			Tick:           0,
		}
	}

	cases := []struct {
		name    string
		mut     func(*UniswapV3Swap)
		wantErr bool
	}{
		{"ok", func(*UniswapV3Swap) {}, false},
		{"missing pool id", func(s *UniswapV3Swap) { s.PoolID = 0 }, true},
		{"missing block number", func(s *UniswapV3Swap) { s.BlockNumber = 0 }, true},
		{"negative block version", func(s *UniswapV3Swap) { s.BlockVersion = -1 }, true},
		{"missing block timestamp", func(s *UniswapV3Swap) { s.BlockTimestamp = time.Time{} }, true},
		{"missing tx hash", func(s *UniswapV3Swap) { s.TxHash = common.Hash{} }, true},
		{"negative log index", func(s *UniswapV3Swap) { s.LogIndex = -1 }, true},
		{"missing sender", func(s *UniswapV3Swap) { s.Sender = common.Address{} }, true},
		{"missing recipient", func(s *UniswapV3Swap) { s.Recipient = common.Address{} }, true},
		{"nil amount0", func(s *UniswapV3Swap) { s.Amount0 = nil }, true},
		{"nil amount1", func(s *UniswapV3Swap) { s.Amount1 = nil }, true},
		{"nil sqrt price", func(s *UniswapV3Swap) { s.SqrtPriceX96 = nil }, true},
		{"nil liquidity", func(s *UniswapV3Swap) { s.Liquidity = nil }, true},
		{"tick below int24 min", func(s *UniswapV3Swap) { s.Tick = -8388609 }, true},
		{"tick above int24 max", func(s *UniswapV3Swap) { s.Tick = 8388608 }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := valid()
			tc.mut(s)
			err := s.Validate()
			if tc.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestUniswapV3LiquidityEvent_Validate(t *testing.T) {
	addr := common.HexToAddress("0x0102030405060708090a0b0c0d0e0f1011121314")
	hash := common.HexToHash("0xaa02030405060708090a0b0c0d0e0f1011121314aabbccddeeff001122334455")

	validMint := func() *UniswapV3LiquidityEvent {
		return &UniswapV3LiquidityEvent{
			PoolID:         1,
			BlockNumber:    100,
			BlockVersion:   0,
			BlockTimestamp: time.Unix(1, 0).UTC(),
			TxHash:         hash,
			LogIndex:       0,
			Kind:           LiquidityEventMint,
			Owner:          addr,
			Sender:         &addr,
			Recipient:      nil,
			TickLower:      -100,
			TickUpper:      100,
			Amount:         big.NewInt(1000),
			Amount0:        big.NewInt(1),
			Amount1:        big.NewInt(1),
		}
	}
	validBurn := func() *UniswapV3LiquidityEvent {
		e := validMint()
		e.Kind = LiquidityEventBurn
		e.Sender = nil
		return e
	}
	validCollect := func() *UniswapV3LiquidityEvent {
		e := validMint()
		e.Kind = LiquidityEventCollect
		e.Sender = nil
		e.Recipient = &addr
		e.Amount = nil
		return e
	}

	cases := []struct {
		name    string
		base    func() *UniswapV3LiquidityEvent
		mut     func(*UniswapV3LiquidityEvent)
		wantErr bool
	}{
		{"ok mint", validMint, func(*UniswapV3LiquidityEvent) {}, false},
		{"ok burn", validBurn, func(*UniswapV3LiquidityEvent) {}, false},
		{"ok collect", validCollect, func(*UniswapV3LiquidityEvent) {}, false},
		{"missing pool id", validMint, func(e *UniswapV3LiquidityEvent) { e.PoolID = 0 }, true},
		{"missing block number", validMint, func(e *UniswapV3LiquidityEvent) { e.BlockNumber = 0 }, true},
		{"negative block version", validMint, func(e *UniswapV3LiquidityEvent) { e.BlockVersion = -1 }, true},
		{"missing block timestamp", validMint, func(e *UniswapV3LiquidityEvent) { e.BlockTimestamp = time.Time{} }, true},
		{"missing tx hash", validMint, func(e *UniswapV3LiquidityEvent) { e.TxHash = common.Hash{} }, true},
		{"negative log index", validMint, func(e *UniswapV3LiquidityEvent) { e.LogIndex = -1 }, true},
		{"bad kind", validMint, func(e *UniswapV3LiquidityEvent) { e.Kind = "sideways" }, true},
		{"missing owner", validMint, func(e *UniswapV3LiquidityEvent) { e.Owner = common.Address{} }, true},
		{"tick_lower below int24 min", validMint, func(e *UniswapV3LiquidityEvent) { e.TickLower = -8388609 }, true},
		{"tick_upper above int24 max", validMint, func(e *UniswapV3LiquidityEvent) { e.TickUpper = 8388608 }, true},
		{"tick_lower equal tick_upper", validMint, func(e *UniswapV3LiquidityEvent) { e.TickUpper = e.TickLower }, true},
		{"tick_lower greater than tick_upper", validMint, func(e *UniswapV3LiquidityEvent) { e.TickLower, e.TickUpper = 100, -100 }, true},
		{"nil amount0", validMint, func(e *UniswapV3LiquidityEvent) { e.Amount0 = nil }, true},
		{"nil amount1", validMint, func(e *UniswapV3LiquidityEvent) { e.Amount1 = nil }, true},
		{"mint missing amount", validMint, func(e *UniswapV3LiquidityEvent) { e.Amount = nil }, true},
		{"mint missing sender", validMint, func(e *UniswapV3LiquidityEvent) { e.Sender = nil }, true},
		{"mint with recipient set", validMint, func(e *UniswapV3LiquidityEvent) { e.Recipient = &e.Owner }, true},
		{"burn missing amount", validBurn, func(e *UniswapV3LiquidityEvent) { e.Amount = nil }, true},
		{"burn with sender set", validBurn, func(e *UniswapV3LiquidityEvent) { e.Sender = &e.Owner }, true},
		{"burn with recipient set", validBurn, func(e *UniswapV3LiquidityEvent) { e.Recipient = &e.Owner }, true},
		{"collect with amount set", validCollect, func(e *UniswapV3LiquidityEvent) { e.Amount = big.NewInt(1) }, true},
		{"collect missing recipient", validCollect, func(e *UniswapV3LiquidityEvent) { e.Recipient = nil }, true},
		{"collect with sender set", validCollect, func(e *UniswapV3LiquidityEvent) { e.Sender = &e.Owner }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := tc.base()
			tc.mut(e)
			err := e.Validate()
			if tc.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestUniswapV3Tick_Validate(t *testing.T) {
	valid := func() *UniswapV3Tick {
		return &UniswapV3Tick{
			PoolID:                1,
			Tick:                  100,
			BlockNumber:           100,
			BlockVersion:          0,
			BlockTimestamp:        time.Unix(1, 0).UTC(),
			LiquidityGross:        big.NewInt(1000),
			LiquidityNet:          big.NewInt(-500),
			FeeGrowthOutside0X128: big.NewInt(0),
			FeeGrowthOutside1X128: big.NewInt(0),
			Initialized:           true,
		}
	}

	cases := []struct {
		name    string
		mut     func(*UniswapV3Tick)
		wantErr bool
	}{
		{"ok", func(*UniswapV3Tick) {}, false},
		{"missing pool id", func(s *UniswapV3Tick) { s.PoolID = 0 }, true},
		{"tick below int24 min", func(s *UniswapV3Tick) { s.Tick = -8388609 }, true},
		{"tick above int24 max", func(s *UniswapV3Tick) { s.Tick = 8388608 }, true},
		{"missing block number", func(s *UniswapV3Tick) { s.BlockNumber = 0 }, true},
		{"negative block version", func(s *UniswapV3Tick) { s.BlockVersion = -1 }, true},
		{"missing block timestamp", func(s *UniswapV3Tick) { s.BlockTimestamp = time.Time{} }, true},
		{"nil liquidity gross", func(s *UniswapV3Tick) { s.LiquidityGross = nil }, true},
		{"nil liquidity net", func(s *UniswapV3Tick) { s.LiquidityNet = nil }, true},
		{"nil fee growth outside0", func(s *UniswapV3Tick) { s.FeeGrowthOutside0X128 = nil }, true},
		{"nil fee growth outside1", func(s *UniswapV3Tick) { s.FeeGrowthOutside1X128 = nil }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := valid()
			tc.mut(s)
			err := s.Validate()
			if tc.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestUniswapV3PoolEvent_Validate(t *testing.T) {
	hash := common.HexToHash("0xaa02030405060708090a0b0c0d0e0f1011121314aabbccddeeff001122334455")
	validParams := json.RawMessage(`{"sqrtPriceX96":"1000","tick":10}`)

	valid := func() *UniswapV3PoolEvent {
		return &UniswapV3PoolEvent{
			PoolID:         1,
			BlockNumber:    100,
			BlockVersion:   0,
			BlockTimestamp: time.Unix(1, 0).UTC(),
			TxHash:         hash,
			LogIndex:       0,
			EventName:      PoolEventInitialize,
			Params:         validParams,
		}
	}

	cases := []struct {
		name    string
		mut     func(*UniswapV3PoolEvent)
		wantErr bool
	}{
		{"ok initialize", func(*UniswapV3PoolEvent) {}, false},
		{"ok flash", func(e *UniswapV3PoolEvent) { e.EventName = PoolEventFlash }, false},
		{"ok set_fee_protocol", func(e *UniswapV3PoolEvent) { e.EventName = PoolEventSetFeeProtocol }, false},
		{"ok collect_protocol", func(e *UniswapV3PoolEvent) { e.EventName = PoolEventCollectProtocol }, false},
		{"ok increase_observation_cardinality_next", func(e *UniswapV3PoolEvent) {
			e.EventName = PoolEventIncreaseObservationCardinalityNext
		}, false},
		{"missing pool id", func(e *UniswapV3PoolEvent) { e.PoolID = 0 }, true},
		{"missing block number", func(e *UniswapV3PoolEvent) { e.BlockNumber = 0 }, true},
		{"negative block version", func(e *UniswapV3PoolEvent) { e.BlockVersion = -1 }, true},
		{"missing block timestamp", func(e *UniswapV3PoolEvent) { e.BlockTimestamp = time.Time{} }, true},
		{"missing tx hash", func(e *UniswapV3PoolEvent) { e.TxHash = common.Hash{} }, true},
		{"negative log index", func(e *UniswapV3PoolEvent) { e.LogIndex = -1 }, true},
		{"bad event name", func(e *UniswapV3PoolEvent) { e.EventName = "unknown_event" }, true},
		{"empty params", func(e *UniswapV3PoolEvent) { e.Params = nil }, true},
		{"invalid json params", func(e *UniswapV3PoolEvent) { e.Params = json.RawMessage(`not valid json`) }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := valid()
			tc.mut(e)
			err := e.Validate()
			if tc.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

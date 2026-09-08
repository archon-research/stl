package entity

import (
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// Ticks are Solidity int24; Uniswap V3 enforces this range on-chain (TickMath).
const (
	minTick = -8388608
	maxTick = 8388607
)

func validateTickRange(field string, tick int) error {
	if tick < minTick || tick > maxTick {
		return fmt.Errorf("%s must be within int24 range [%d, %d], got %d", field, minTick, maxTick, tick)
	}
	return nil
}

// validateTicks enforces the ordered, in-range tick pair that a real Mint/Burn
// position must have. It is intentionally NOT applied to Collect (see
// LiquidityEvent.Validate).
func validateTicks(tickLower, tickUpper int) error {
	if err := validateTickRange("tickLower", tickLower); err != nil {
		return err
	}
	if err := validateTickRange("tickUpper", tickUpper); err != nil {
		return err
	}
	if tickLower >= tickUpper {
		return fmt.Errorf("tickLower (%d) must be less than tickUpper (%d)", tickLower, tickUpper)
	}
	return nil
}

// UniswapV3PoolState is a periodic per-touched-block snapshot of pool slot0 /
// liquidity / fee-growth / protocol-fee / balance state.
type UniswapV3PoolState struct {
	PoolID                     int64
	BlockNumber                int64
	BlockVersion               int
	BlockTimestamp             time.Time
	SqrtPriceX96               *big.Int
	Tick                       int
	ObservationIndex           int
	ObservationCardinality     int
	ObservationCardinalityNext int
	FeeProtocol                int
	Unlocked                   bool
	Liquidity                  *big.Int
	FeeGrowthGlobal0X128       *big.Int
	FeeGrowthGlobal1X128       *big.Int
	ProtocolFeesToken0         *big.Int
	ProtocolFeesToken1         *big.Int
	Balance0                   *big.Int
	Balance1                   *big.Int
	TwapTick                   *int // nil when TWAP observation is unavailable
	TwapWindowSecs             *int // nil when TWAP observation is unavailable
}

func (s *UniswapV3PoolState) Validate() error {
	if s.PoolID <= 0 {
		return fmt.Errorf("poolID must be positive, got %d", s.PoolID)
	}
	if s.BlockNumber <= 0 {
		return fmt.Errorf("blockNumber must be positive, got %d", s.BlockNumber)
	}
	if s.BlockVersion < 0 {
		return fmt.Errorf("blockVersion must be non-negative, got %d", s.BlockVersion)
	}
	if s.BlockTimestamp.IsZero() {
		return fmt.Errorf("blockTimestamp must not be zero")
	}
	if s.SqrtPriceX96 == nil {
		return fmt.Errorf("sqrtPriceX96 must not be nil")
	}
	if err := validateTickRange("tick", s.Tick); err != nil {
		return err
	}
	if s.Liquidity == nil {
		return fmt.Errorf("liquidity must not be nil")
	}
	if s.FeeGrowthGlobal0X128 == nil {
		return fmt.Errorf("feeGrowthGlobal0X128 must not be nil")
	}
	if s.FeeGrowthGlobal1X128 == nil {
		return fmt.Errorf("feeGrowthGlobal1X128 must not be nil")
	}
	if s.ProtocolFeesToken0 == nil {
		return fmt.Errorf("protocolFeesToken0 must not be nil")
	}
	if s.ProtocolFeesToken1 == nil {
		return fmt.Errorf("protocolFeesToken1 must not be nil")
	}
	if s.Balance0 == nil {
		return fmt.Errorf("balance0 must not be nil")
	}
	if s.Balance1 == nil {
		return fmt.Errorf("balance1 must not be nil")
	}
	return nil
}

// UniswapV3Swap is an on-chain Swap event.
type UniswapV3Swap struct {
	PoolID         int64
	BlockNumber    int64
	BlockVersion   int
	BlockTimestamp time.Time
	TxHash         common.Hash
	LogIndex       int
	Sender         common.Address
	Recipient      common.Address
	Amount0        *big.Int // signed: negative means the pool paid out token0
	Amount1        *big.Int // signed: negative means the pool paid out token1
	SqrtPriceX96   *big.Int
	Liquidity      *big.Int
	Tick           int
}

func (s *UniswapV3Swap) Validate() error {
	if s.PoolID <= 0 {
		return fmt.Errorf("poolID must be positive, got %d", s.PoolID)
	}
	if s.BlockNumber <= 0 {
		return fmt.Errorf("blockNumber must be positive, got %d", s.BlockNumber)
	}
	if s.BlockVersion < 0 {
		return fmt.Errorf("blockVersion must be non-negative, got %d", s.BlockVersion)
	}
	if s.BlockTimestamp.IsZero() {
		return fmt.Errorf("blockTimestamp must not be zero")
	}
	if s.TxHash == (common.Hash{}) {
		return fmt.Errorf("txHash is required")
	}
	if s.LogIndex < 0 {
		return fmt.Errorf("logIndex must be non-negative, got %d", s.LogIndex)
	}
	// Sender is v3-core's msg.sender (the swap caller): genuinely never zero, so
	// a zero sender signals a malformed decode. Recipient is caller-supplied and
	// may legally be address(0) (WETH permits transfer-to-zero), so it is NOT
	// rejected here — doing so would let a dust swap poison-stall the block.
	if s.Sender == (common.Address{}) {
		return fmt.Errorf("sender is required")
	}
	if s.Amount0 == nil {
		return fmt.Errorf("amount0 must not be nil")
	}
	if s.Amount1 == nil {
		return fmt.Errorf("amount1 must not be nil")
	}
	if s.SqrtPriceX96 == nil {
		return fmt.Errorf("sqrtPriceX96 must not be nil")
	}
	if s.Liquidity == nil {
		return fmt.Errorf("liquidity must not be nil")
	}
	if err := validateTickRange("tick", s.Tick); err != nil {
		return err
	}
	return nil
}

// LiquidityEventKind identifies which Uniswap V3 position-liquidity event produced a row.
type LiquidityEventKind string

const (
	LiquidityEventMint    LiquidityEventKind = "mint"
	LiquidityEventBurn    LiquidityEventKind = "burn"
	LiquidityEventCollect LiquidityEventKind = "collect"
)

var validLiquidityEventKinds = map[LiquidityEventKind]struct{}{
	LiquidityEventMint:    {},
	LiquidityEventBurn:    {},
	LiquidityEventCollect: {},
}

// UniswapV3LiquidityEvent is a Mint, Burn, or Collect event against a position.
type UniswapV3LiquidityEvent struct {
	PoolID         int64
	BlockNumber    int64
	BlockVersion   int
	BlockTimestamp time.Time
	TxHash         common.Hash
	LogIndex       int
	EventName      LiquidityEventKind
	Owner          common.Address
	Sender         *common.Address // set for mint only
	Recipient      *common.Address // set for collect only
	TickLower      int
	TickUpper      int
	Amount         *big.Int // liquidity delta; nil for collect
	Amount0        *big.Int
	Amount1        *big.Int
}

func (e *UniswapV3LiquidityEvent) Validate() error {
	if e.PoolID <= 0 {
		return fmt.Errorf("poolID must be positive, got %d", e.PoolID)
	}
	if e.BlockNumber <= 0 {
		return fmt.Errorf("blockNumber must be positive, got %d", e.BlockNumber)
	}
	if e.BlockVersion < 0 {
		return fmt.Errorf("blockVersion must be non-negative, got %d", e.BlockVersion)
	}
	if e.BlockTimestamp.IsZero() {
		return fmt.Errorf("blockTimestamp must not be zero")
	}
	if e.TxHash == (common.Hash{}) {
		return fmt.Errorf("txHash is required")
	}
	if e.LogIndex < 0 {
		return fmt.Errorf("logIndex must be non-negative, got %d", e.LogIndex)
	}
	if _, ok := validLiquidityEventKinds[e.EventName]; !ok {
		return fmt.Errorf("event name %q is not allowed", e.EventName)
	}
	// Owner is caller-supplied on Mint (a mint may target address(0)), so a zero
	// owner is a legal on-chain sink and is NOT rejected — rejecting it would let
	// a dust mint poison-stall the block. On Burn/Collect owner is msg.sender and
	// so never zero on-chain anyway.
	if e.Amount0 == nil {
		return fmt.Errorf("amount0 must not be nil")
	}
	if e.Amount1 == nil {
		return fmt.Errorf("amount1 must not be nil")
	}

	// Sender/Recipient/Amount presence mirrors the v3-core event shapes: Mint carries a
	// sender (the position manager) and a liquidity delta; Burn carries only the delta
	// (msg.sender is implicitly the owner); Collect carries a payout recipient and no
	// delta (amount0/amount1 are the withdrawn fees, not a liquidity change).
	//
	// The tick range/ordering checks apply to Mint and Burn ONLY: both route
	// through v3-core's checkTicks/_updatePosition, which reverts on-chain for a
	// bad tick pair, so a bad Mint/Burn log cannot exist. collect() deliberately
	// omits checkTicks (invalid positions can only ever carry zero tokensOwed),
	// so a permissionless collect(recipient, tickLower, tickUpper, 0, 0) with ANY
	// int24 pair emits a valid log; rejecting it here would poison-stall the block.
	switch e.EventName {
	case LiquidityEventMint:
		if err := validateTicks(e.TickLower, e.TickUpper); err != nil {
			return err
		}
		if e.Amount == nil {
			return fmt.Errorf("amount must not be nil for mint")
		}
		if e.Sender == nil {
			return fmt.Errorf("sender is required for mint")
		}
		if e.Recipient != nil {
			return fmt.Errorf("recipient must be nil for mint")
		}
	case LiquidityEventBurn:
		if err := validateTicks(e.TickLower, e.TickUpper); err != nil {
			return err
		}
		if e.Amount == nil {
			return fmt.Errorf("amount must not be nil for burn")
		}
		if e.Sender != nil {
			return fmt.Errorf("sender must be nil for burn")
		}
		if e.Recipient != nil {
			return fmt.Errorf("recipient must be nil for burn")
		}
	case LiquidityEventCollect:
		if e.Amount != nil {
			return fmt.Errorf("amount must be nil for collect")
		}
		if e.Sender != nil {
			return fmt.Errorf("sender must be nil for collect")
		}
		if e.Recipient == nil {
			return fmt.Errorf("recipient is required for collect")
		}
	}
	return nil
}

// UniswapV3Tick is the append-on-change authoritative per-tick state
// (liquidity gross/net, fee-growth-outside, initialized).
type UniswapV3Tick struct {
	PoolID                int64
	Tick                  int
	BlockNumber           int64
	BlockVersion          int
	BlockTimestamp        time.Time
	LiquidityGross        *big.Int
	LiquidityNet          *big.Int // signed: net liquidity added/removed crossing the tick left-to-right
	FeeGrowthOutside0X128 *big.Int
	FeeGrowthOutside1X128 *big.Int
	Initialized           bool
}

func (t *UniswapV3Tick) Validate() error {
	if t.PoolID <= 0 {
		return fmt.Errorf("poolID must be positive, got %d", t.PoolID)
	}
	if err := validateTickRange("tick", t.Tick); err != nil {
		return err
	}
	if t.BlockNumber <= 0 {
		return fmt.Errorf("blockNumber must be positive, got %d", t.BlockNumber)
	}
	if t.BlockVersion < 0 {
		return fmt.Errorf("blockVersion must be non-negative, got %d", t.BlockVersion)
	}
	if t.BlockTimestamp.IsZero() {
		return fmt.Errorf("blockTimestamp must not be zero")
	}
	if t.LiquidityGross == nil {
		return fmt.Errorf("liquidityGross must not be nil")
	}
	if t.LiquidityNet == nil {
		return fmt.Errorf("liquidityNet must not be nil")
	}
	if t.FeeGrowthOutside0X128 == nil {
		return fmt.Errorf("feeGrowthOutside0X128 must not be nil")
	}
	if t.FeeGrowthOutside1X128 == nil {
		return fmt.Errorf("feeGrowthOutside1X128 must not be nil")
	}
	return nil
}

// PoolEventName identifies which typed low-frequency Uniswap V3 pool event produced a row.
type PoolEventName string

const (
	PoolEventInitialize                         PoolEventName = "initialize"
	PoolEventFlash                              PoolEventName = "flash"
	PoolEventSetFeeProtocol                     PoolEventName = "set_fee_protocol"
	PoolEventCollectProtocol                    PoolEventName = "collect_protocol"
	PoolEventIncreaseObservationCardinalityNext PoolEventName = "increase_observation_cardinality_next"
)

var validPoolEventNames = map[PoolEventName]struct{}{
	PoolEventInitialize:                         {},
	PoolEventFlash:                              {},
	PoolEventSetFeeProtocol:                     {},
	PoolEventCollectProtocol:                    {},
	PoolEventIncreaseObservationCardinalityNext: {},
}

// UniswapV3PoolEvent is a typed low-frequency pool event (Initialize, Flash,
// SetFeeProtocol, CollectProtocol, IncreaseObservationCardinalityNext).
type UniswapV3PoolEvent struct {
	PoolID         int64
	BlockNumber    int64
	BlockVersion   int
	BlockTimestamp time.Time
	TxHash         common.Hash
	LogIndex       int
	EventName      PoolEventName
	Params         json.RawMessage
}

func (e *UniswapV3PoolEvent) Validate() error {
	if e.PoolID <= 0 {
		return fmt.Errorf("poolID must be positive, got %d", e.PoolID)
	}
	if e.BlockNumber <= 0 {
		return fmt.Errorf("blockNumber must be positive, got %d", e.BlockNumber)
	}
	if e.BlockVersion < 0 {
		return fmt.Errorf("blockVersion must be non-negative, got %d", e.BlockVersion)
	}
	if e.BlockTimestamp.IsZero() {
		return fmt.Errorf("blockTimestamp must not be zero")
	}
	if e.TxHash == (common.Hash{}) {
		return fmt.Errorf("txHash is required")
	}
	if e.LogIndex < 0 {
		return fmt.Errorf("logIndex must be non-negative, got %d", e.LogIndex)
	}
	if _, ok := validPoolEventNames[e.EventName]; !ok {
		return fmt.Errorf("eventName %q is not allowed", e.EventName)
	}
	if len(e.Params) == 0 {
		return fmt.Errorf("params must not be empty")
	}
	if !json.Valid(e.Params) {
		return fmt.Errorf("params must be valid JSON")
	}
	return nil
}

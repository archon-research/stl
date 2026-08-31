package entity

import (
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// v4-core fee ceilings in hundredths of a bip; MAX_PROTOCOL_FEE bounds each
// 12-bit half of the packed uint24 separately.
const (
	maxV4LpFee             = 1_000_000
	maxV4ProtocolFee       = 0xFFFFFF
	maxV4ProtocolFeeHalf   = 1_000
	v4ProtocolFeeHalfMask  = 0xFFF
	v4ProtocolFeeHalfShift = 12
)

// v4-core TickMath bounds, tighter than the int24 wire range.
const (
	minV4Tick = -887272
	maxV4Tick = 887272
)

func validatePoolBlockKey(poolID, blockNumber int64, blockVersion int, blockTimestamp time.Time) error {
	if poolID <= 0 {
		return fmt.Errorf("poolID must be positive, got %d", poolID)
	}
	if blockNumber <= 0 {
		return fmt.Errorf("blockNumber must be positive, got %d", blockNumber)
	}
	if blockVersion < 0 {
		return fmt.Errorf("blockVersion must be non-negative, got %d", blockVersion)
	}
	if blockTimestamp.IsZero() {
		return fmt.Errorf("blockTimestamp must not be zero")
	}
	return nil
}

func validatePoolLogKey(txHash common.Hash, logIndex int) error {
	if txHash == (common.Hash{}) {
		return fmt.Errorf("txHash is required")
	}
	if logIndex < 0 {
		return fmt.Errorf("logIndex must be non-negative, got %d", logIndex)
	}
	return nil
}

func validateV4LpFee(field string, fee int) error {
	if fee < 0 || fee > maxV4LpFee {
		return fmt.Errorf("%s must be within [0, %d], got %d", field, maxV4LpFee, fee)
	}
	return nil
}

func validateV4TickRange(field string, tick int) error {
	if tick < minV4Tick || tick > maxV4Tick {
		return fmt.Errorf("%s must be within TickMath range [%d, %d], got %d", field, minV4Tick, maxV4Tick, tick)
	}
	return nil
}

func validateV4Ticks(tickLower, tickUpper int) error {
	if err := validateV4TickRange("tickLower", tickLower); err != nil {
		return err
	}
	if err := validateV4TickRange("tickUpper", tickUpper); err != nil {
		return err
	}
	if tickLower >= tickUpper {
		return fmt.Errorf("tickLower (%d) must be less than tickUpper (%d)", tickLower, tickUpper)
	}
	return nil
}

func requireBigInt(field string, v *big.Int) error {
	if v == nil {
		return fmt.Errorf("%s must not be nil", field)
	}
	return nil
}

// UniswapV4PoolState is snapshotted only on touched blocks; updateDynamicLPFee
// emits no event, so dynamic-fee pools are registered snapshot_supported = false.
type UniswapV4PoolState struct {
	PoolID               int64
	BlockNumber          int64
	BlockVersion         int
	BlockTimestamp       time.Time
	SqrtPriceX96         *big.Int
	Tick                 int
	ProtocolFee          int // uint24: low 12 bits zeroForOne, high 12 bits oneForZero
	LpFee                int // hundredths of a bip
	Liquidity            *big.Int
	FeeGrowthGlobal0X128 *big.Int
	FeeGrowthGlobal1X128 *big.Int
}

func (s *UniswapV4PoolState) Validate() error {
	if err := validatePoolBlockKey(s.PoolID, s.BlockNumber, s.BlockVersion, s.BlockTimestamp); err != nil {
		return err
	}
	if s.isOrphanedReRead() {
		return nil
	}
	if err := requirePositiveSqrtPrice(s.SqrtPriceX96); err != nil {
		return err
	}
	if err := validateV4TickRange("tick", s.Tick); err != nil {
		return err
	}
	if err := s.validateProtocolFee(); err != nil {
		return err
	}
	if err := validateV4LpFee("lpFee", s.LpFee); err != nil {
		return err
	}
	if err := requireBigInt("liquidity", s.Liquidity); err != nil {
		return err
	}
	if err := requireBigInt("feeGrowthGlobal0X128", s.FeeGrowthGlobal0X128); err != nil {
		return err
	}
	return requireBigInt("feeGrowthGlobal1X128", s.FeeGrowthGlobal1X128)
}

// A reorg that orphans a pool's Initialize makes StateView answer all-zeros; that
// row must still persist, to supersede the orphaned fork's snapshot.
func (s *UniswapV4PoolState) isOrphanedReRead() bool {
	if s.BlockVersion <= 0 {
		return false
	}
	if s.Tick != 0 || s.ProtocolFee != 0 || s.LpFee != 0 {
		return false
	}
	for _, v := range []*big.Int{s.SqrtPriceX96, s.Liquidity, s.FeeGrowthGlobal0X128, s.FeeGrowthGlobal1X128} {
		if v == nil || v.Sign() != 0 {
			return false
		}
	}
	return true
}

// Zero is what a StateView getter answers for a PoolId the PoolManager never
// initialized; no live pool can report it.
func requirePositiveSqrtPrice(v *big.Int) error {
	if err := requireBigInt("sqrtPriceX96", v); err != nil {
		return err
	}
	if v.Sign() <= 0 {
		return fmt.Errorf("sqrtPriceX96 must be positive, got %s", v)
	}
	return nil
}

func (s *UniswapV4PoolState) validateProtocolFee() error {
	if s.ProtocolFee < 0 || s.ProtocolFee > maxV4ProtocolFee {
		return fmt.Errorf("protocolFee must be within uint24 range [0, %d], got %d", maxV4ProtocolFee, s.ProtocolFee)
	}
	if zeroForOne := s.ProtocolFee & v4ProtocolFeeHalfMask; zeroForOne > maxV4ProtocolFeeHalf {
		return fmt.Errorf("zeroForOne protocolFee must be at most %d, got %d", maxV4ProtocolFeeHalf, zeroForOne)
	}
	if oneForZero := s.ProtocolFee >> v4ProtocolFeeHalfShift; oneForZero > maxV4ProtocolFeeHalf {
		return fmt.Errorf("oneForZero protocolFee must be at most %d, got %d", maxV4ProtocolFeeHalf, oneForZero)
	}
	return nil
}

// Amount0/Amount1 are the swapper-perspective BalanceDelta (negative = swapper
// owes the PoolManager), inverse of UniswapV3Swap, before any hook delta.
type UniswapV4Swap struct {
	PoolID         int64
	BlockNumber    int64
	BlockVersion   int
	BlockTimestamp time.Time
	TxHash         common.Hash
	LogIndex       int
	Sender         common.Address // the unlocking router, not the trader
	Amount0        *big.Int
	Amount1        *big.Int
	SqrtPriceX96   *big.Int
	Liquidity      *big.Int
	Tick           int
	// Fee is the LP fee composed with the protocol fee
	// (ProtocolFeeLibrary.calculateSwapFee), hundredths of a bip.
	Fee int
}

func (s *UniswapV4Swap) Validate() error {
	if err := validatePoolBlockKey(s.PoolID, s.BlockNumber, s.BlockVersion, s.BlockTimestamp); err != nil {
		return err
	}
	if err := validatePoolLogKey(s.TxHash, s.LogIndex); err != nil {
		return err
	}
	if s.Sender == (common.Address{}) {
		return fmt.Errorf("sender is required")
	}
	if err := requireBigInt("amount0", s.Amount0); err != nil {
		return err
	}
	if err := requireBigInt("amount1", s.Amount1); err != nil {
		return err
	}
	if err := requirePositiveSqrtPrice(s.SqrtPriceX96); err != nil {
		return err
	}
	if err := requireBigInt("liquidity", s.Liquidity); err != nil {
		return err
	}
	if err := validateV4TickRange("tick", s.Tick); err != nil {
		return err
	}
	return validateV4LpFee("fee", s.Fee)
}

// Flash accounting means the ModifyLiquidity log carries no token amounts; the
// position is identified by (Sender, TickLower, TickUpper, Salt).
type UniswapV4LiquidityEvent struct {
	PoolID         int64
	BlockNumber    int64
	BlockVersion   int
	BlockTimestamp time.Time
	TxHash         common.Hash
	LogIndex       int
	Sender         common.Address
	TickLower      int
	TickUpper      int
	LiquidityDelta *big.Int    // signed; zero on a fee-collecting poke
	Salt           common.Hash // caller-chosen position discriminator
}

func (e *UniswapV4LiquidityEvent) Validate() error {
	if err := validatePoolBlockKey(e.PoolID, e.BlockNumber, e.BlockVersion, e.BlockTimestamp); err != nil {
		return err
	}
	if err := validatePoolLogKey(e.TxHash, e.LogIndex); err != nil {
		return err
	}
	if e.Sender == (common.Address{}) {
		return fmt.Errorf("sender is required")
	}
	if err := validateV4Ticks(e.TickLower, e.TickUpper); err != nil {
		return err
	}
	return requireBigInt("liquidityDelta", e.LiquidityDelta)
}

// A tick is initialized exactly when LiquidityGross > 0 — v4-core's TickInfo has
// no Initialized flag — so an all-zero row records a cleared tick.
type UniswapV4Tick struct {
	PoolID                int64
	Tick                  int
	BlockNumber           int64
	BlockVersion          int
	BlockTimestamp        time.Time
	LiquidityGross        *big.Int
	LiquidityNet          *big.Int // signed: liquidity added crossing the tick left-to-right
	FeeGrowthOutside0X128 *big.Int
	FeeGrowthOutside1X128 *big.Int
}

func (t *UniswapV4Tick) Validate() error {
	if err := validatePoolBlockKey(t.PoolID, t.BlockNumber, t.BlockVersion, t.BlockTimestamp); err != nil {
		return err
	}
	if err := validateV4TickRange("tick", t.Tick); err != nil {
		return err
	}
	if err := requireBigInt("liquidityGross", t.LiquidityGross); err != nil {
		return err
	}
	if err := requireBigInt("liquidityNet", t.LiquidityNet); err != nil {
		return err
	}
	if err := requireBigInt("feeGrowthOutside0X128", t.FeeGrowthOutside0X128); err != nil {
		return err
	}
	return requireBigInt("feeGrowthOutside1X128", t.FeeGrowthOutside1X128)
}

// Deliberately a separate set from V3's PoolEventName: V4 has no per-pool Flash,
// no observation cardinality, and sets protocol fees through a controller.
type UniswapV4PoolEventName string

const (
	UniswapV4PoolEventInitialize         UniswapV4PoolEventName = "initialize"
	UniswapV4PoolEventDonate             UniswapV4PoolEventName = "donate"
	UniswapV4PoolEventProtocolFeeUpdated UniswapV4PoolEventName = "protocol_fee_updated"
)

var validUniswapV4PoolEventNames = map[UniswapV4PoolEventName]struct{}{
	UniswapV4PoolEventInitialize:         {},
	UniswapV4PoolEventDonate:             {},
	UniswapV4PoolEventProtocolFeeUpdated: {},
}

type UniswapV4PoolEvent struct {
	PoolID         int64
	BlockNumber    int64
	BlockVersion   int
	BlockTimestamp time.Time
	TxHash         common.Hash
	LogIndex       int
	EventName      UniswapV4PoolEventName
	Params         json.RawMessage
}

func (e *UniswapV4PoolEvent) Validate() error {
	if err := validatePoolBlockKey(e.PoolID, e.BlockNumber, e.BlockVersion, e.BlockTimestamp); err != nil {
		return err
	}
	if err := validatePoolLogKey(e.TxHash, e.LogIndex); err != nil {
		return err
	}
	if _, ok := validUniswapV4PoolEventNames[e.EventName]; !ok {
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

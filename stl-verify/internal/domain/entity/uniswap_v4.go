package entity

import (
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// Fee ceilings enforced by v4-core, both in hundredths of a bip:
// LPFeeLibrary.MAX_LP_FEE is 100%, and ProtocolFeeLibrary.MAX_PROTOCOL_FEE (0.1%)
// applies to each 12-bit half of the packed uint24 protocol fee separately.
const (
	maxV4LpFee             = 1_000_000
	maxV4ProtocolFee       = 0xFFFFFF
	maxV4ProtocolFeeHalf   = 1_000
	v4ProtocolFeeHalfMask  = 0xFFF
	v4ProtocolFeeHalfShift = 12
)

// validateV4BlockKey checks the versioned block coordinates that every
// uniswap_v4 row carries as part of its primary key.
func validateV4BlockKey(poolID, blockNumber int64, blockVersion int, blockTimestamp time.Time) error {
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

// validateV4LogKey checks the log coordinates that make an event row unique
// within its block.
func validateV4LogKey(txHash common.Hash, logIndex int) error {
	if txHash == (common.Hash{}) {
		return fmt.Errorf("txHash is required")
	}
	if logIndex < 0 {
		return fmt.Errorf("logIndex must be non-negative, got %d", logIndex)
	}
	return nil
}

// validateV4LpFee bounds an LP fee against LPFeeLibrary.MAX_LP_FEE.
func validateV4LpFee(field string, fee int) error {
	if fee < 0 || fee > maxV4LpFee {
		return fmt.Errorf("%s must be within [0, %d], got %d", field, maxV4LpFee, fee)
	}
	return nil
}

// UniswapV4PoolState is a per-touched-block snapshot of one pool's StateView
// slot0, liquidity and global fee growth. LpFee is only refreshed on blocks the
// pool is touched: updateDynamicLPFee emits no event, so a dynamic-fee pool's
// fee can move unobserved between snapshots.
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

// Validate rejects a zero SqrtPriceX96: StateView.getSlot0 answers all-zeros for
// an unknown PoolId instead of reverting, so a zero price means the registry row
// points at a pool this PoolManager never initialized.
func (s *UniswapV4PoolState) Validate() error {
	if err := validateV4BlockKey(s.PoolID, s.BlockNumber, s.BlockVersion, s.BlockTimestamp); err != nil {
		return err
	}
	if s.SqrtPriceX96 == nil {
		return fmt.Errorf("sqrtPriceX96 must not be nil")
	}
	if s.SqrtPriceX96.Sign() <= 0 {
		return fmt.Errorf("sqrtPriceX96 must be positive, got %s", s.SqrtPriceX96)
	}
	if err := validateTickRange("tick", s.Tick); err != nil {
		return err
	}
	if err := s.validateProtocolFee(); err != nil {
		return err
	}
	if err := validateV4LpFee("lpFee", s.LpFee); err != nil {
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

// UniswapV4Swap is a PoolManager Swap event. Amount0/Amount1 are the swapper's
// BalanceDelta — negative was paid into the PoolManager, positive was received —
// the inverse of the pool-perspective signs on UniswapV3Swap.
type UniswapV4Swap struct {
	PoolID         int64
	BlockNumber    int64
	BlockVersion   int
	BlockTimestamp time.Time
	TxHash         common.Hash
	LogIndex       int
	// Sender is the PoolManager's msg.sender (the unlocking router), never zero
	// on-chain, so a zero sender is a malformed decode rather than a real log.
	Sender       common.Address
	Amount0      *big.Int
	Amount1      *big.Int
	SqrtPriceX96 *big.Int
	Liquidity    *big.Int
	Tick         int
	// Fee is the total swap fee charged (LP fee composed with the protocol fee
	// via ProtocolFeeLibrary.calculateSwapFee), hundredths of a bip — not the
	// bare LP fee, which is what pool_state.lp_fee holds.
	Fee int
}

func (s *UniswapV4Swap) Validate() error {
	if err := validateV4BlockKey(s.PoolID, s.BlockNumber, s.BlockVersion, s.BlockTimestamp); err != nil {
		return err
	}
	if err := validateV4LogKey(s.TxHash, s.LogIndex); err != nil {
		return err
	}
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
	return validateV4LpFee("fee", s.Fee)
}

// UniswapV4LiquidityEvent is a PoolManager ModifyLiquidity event. V4 settles
// through flash accounting, so the log carries no token amounts; the position it
// touches is identified by (Sender, TickLower, TickUpper, Salt).
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
	Salt           common.Hash // caller-chosen position discriminator, commonly zero
}

func (e *UniswapV4LiquidityEvent) Validate() error {
	if err := validateV4BlockKey(e.PoolID, e.BlockNumber, e.BlockVersion, e.BlockTimestamp); err != nil {
		return err
	}
	if err := validateV4LogKey(e.TxHash, e.LogIndex); err != nil {
		return err
	}
	if e.Sender == (common.Address{}) {
		return fmt.Errorf("sender is required")
	}
	// modifyLiquidity always routes through Pool.checkTicks, which reverts on a
	// bad pair, so an out-of-order or out-of-range log cannot exist on-chain.
	if err := validateTicks(e.TickLower, e.TickUpper); err != nil {
		return err
	}
	if e.LiquidityDelta == nil {
		return fmt.Errorf("liquidityDelta must not be nil")
	}
	return nil
}

// UniswapV4Tick is the append-on-change authoritative per-tick state. It carries
// no Initialized flag because v4-core's TickInfo has none: a tick is initialized
// exactly when LiquidityGross > 0, so an all-zero row records a cleared tick.
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
	if err := validateV4BlockKey(t.PoolID, t.BlockNumber, t.BlockVersion, t.BlockTimestamp); err != nil {
		return err
	}
	if err := validateTickRange("tick", t.Tick); err != nil {
		return err
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

// UniswapV4PoolEventName identifies which typed low-frequency PoolManager event
// produced a row. It is a separate set from V3's PoolEventName: V4 has no
// per-pool Flash, protocol fees are set through a controller, and observation
// cardinality does not exist.
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

// UniswapV4PoolEvent is a typed low-frequency pool event whose decoded arguments
// are kept verbatim in Params rather than promoted to columns.
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
	if err := validateV4BlockKey(e.PoolID, e.BlockNumber, e.BlockVersion, e.BlockTimestamp); err != nil {
		return err
	}
	if err := validateV4LogKey(e.TxHash, e.LogIndex); err != nil {
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

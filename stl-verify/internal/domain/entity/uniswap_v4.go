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

// v4-core's TickMath bounds, tighter than the int24 wire range: no pool can
// reach a tick outside them, so anything wider is a decode or read defect.
const (
	minV4Tick = -887272
	maxV4Tick = 887272
)

// validatePoolBlockKey checks the versioned block coordinates that every
// uniswap_v4 row carries as part of its primary key. poolID is the
// uniswap_v4_pool surrogate id, not the on-chain PoolId hash.
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

// validatePoolLogKey checks the log coordinates that make an event row unique
// within its block.
func validatePoolLogKey(txHash common.Hash, logIndex int) error {
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

func validateV4TickRange(field string, tick int) error {
	if tick < minV4Tick || tick > maxV4Tick {
		return fmt.Errorf("%s must be within TickMath range [%d, %d], got %d", field, minV4Tick, maxV4Tick, tick)
	}
	return nil
}

// validateV4Ticks enforces the ordered, in-range tick pair Pool.checkTicks
// requires, so a bad pair is a decode defect rather than a real position.
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

// requireBigInt rejects a nil numeric field: every uniswap_v4 numeric column is
// NOT NULL, so a nil here would surface as an opaque driver error at insert.
func requireBigInt(field string, v *big.Int) error {
	if v == nil {
		return fmt.Errorf("%s must not be nil", field)
	}
	return nil
}

// UniswapV4PoolState is a per-touched-block snapshot of one pool's StateView
// slot0, liquidity and global fee growth. LpFee is only refreshed on blocks the
// pool is touched, which is sound only for static-fee pools: updateDynamicLPFee
// emits no event, so a dynamic-fee pool is registered with
// uniswap_v4_pool.snapshot_supported = false and produces no row here until
// lp_fee has a refresh path.
type UniswapV4PoolState struct {
	PoolID               int64 // uniswap_v4_pool surrogate id
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
// points at a pool this PoolManager never initialized — except on a reorg
// re-read, see isOrphanedReRead.
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

// isOrphanedReRead reports the one legal all-zero snapshot: a reorg re-read
// (BlockVersion > 0) of a pool whose Initialize the reorg orphaned. StateView
// then answers all-zeros for every getter, and that row must persist to
// supersede the orphaned fork's — the same tolerance the tick path has. Nothing
// short of a complete zero read qualifies, so a half-decoded row still fails.
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

// requirePositiveSqrtPrice rejects the all-zeros a StateView getter returns for
// an unknown PoolId, which no initialized pool can ever report.
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

// UniswapV4Swap is a PoolManager Swap event. Amount0/Amount1 are the swap
// BalanceDelta from the swapper's perspective (v4-core applies swapDelta to
// msg.sender): negative means the swapper owes the PoolManager, positive means
// the PoolManager owes the swapper — the inverse of the pool-perspective signs
// on UniswapV3Swap. PoolManager emits the delta before afterSwap applies any
// hook delta, so it equals what the swapper settled only when the pool's hooks
// carry no *_RETURNS_DELTA permission.
type UniswapV4Swap struct {
	PoolID         int64 // uniswap_v4_pool surrogate id
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

// UniswapV4LiquidityEvent is a PoolManager ModifyLiquidity event. V4 settles
// through flash accounting, so the log carries no token amounts; the position it
// touches is identified by (Sender, TickLower, TickUpper, Salt).
type UniswapV4LiquidityEvent struct {
	PoolID         int64 // uniswap_v4_pool surrogate id
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

// UniswapV4Tick is the append-on-change authoritative per-tick state. It carries
// no Initialized flag because v4-core's TickInfo has none: a tick is initialized
// exactly when LiquidityGross > 0, so an all-zero row records a cleared tick.
type UniswapV4Tick struct {
	PoolID                int64 // uniswap_v4_pool surrogate id
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
	PoolID         int64 // uniswap_v4_pool surrogate id
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

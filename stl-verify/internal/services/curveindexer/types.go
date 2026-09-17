package curveindexer

import (
	"encoding/json"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/services/dexconsumer"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

type PoolKind string

const (
	KindStableswapPreNG PoolKind = "plain_pre_ng"
	KindStableswapNG    PoolKind = "plain_ng"
	KindCryptoswap      PoolKind = "cryptoswap"
)

type RegisteredPool struct {
	ID      int64 // curve_pool.id
	Address common.Address
	Kind    PoolKind
	NCoins  int
	// DeployBlock is the pool's on-chain deployment block (0 when not yet
	// backfilled); gates snapshot sweeps via dexconsumer's deploy-gate tracker
	// (see DeployBlockNum) so a newly-registered pool isn't multicalled before
	// it exists on chain.
	DeployBlock  int64
	CoinDecimals []int
	// LpTokenAddress is the separate LP token for pre-NG pools (where totalSupply
	// lives), nil when the pool is its own LP token.
	LpTokenAddress *common.Address
	// HasAPrecise records whether this stableswap pool exposes A_precise(). Some of
	// the oldest pre-NG pools (e.g. 3pool) do not, so the snapshot must gate the call
	// on this flag. Curated in the DB (curve_pool.has_a_precise) and carried through
	// LoadPools; irrelevant (and false) for cryptoswap pools, which never call A_precise.
	HasAPrecise bool
	// HasNoArgOracleGetters records whether this plain_ng pool exposes the no-arg
	// oracle getters price_oracle(), last_price(), ema_price(), get_p() and
	// oracle_method(). Later stableswap-NG implementations expose only the indexed
	// price_oracle(uint256) form and revert on the no-arg selector, and an issued
	// read that reverts stops the block, so the snapshot gates all five on this
	// flag. Curated in the DB (curve_pool.has_no_arg_oracle_getters) and carried
	// through LoadPools; false for every non-NG pool, which never issues them.
	HasNoArgOracleGetters bool
	// CalcTokenAmountDynArray records the argument shape calc_token_amount takes on
	// this pool: a dynamic uint256[] (true) or a fixed uint256[N] (false). The two
	// have different selectors and each reverts on the other, and the shape cannot
	// be read back from the chain, so it is curated in the DB
	// (curve_pool.calc_token_amount_dyn_array) and carried through LoadPools. nil
	// means "not probed": the snapshot then issues no calc_token_amount call at all
	// rather than guessing a shape that could revert and stop the block.
	CalcTokenAmountDynArray *bool
	// HasFutureFee and HasOffpegFeeMultiplier record which fee-schedule getter this
	// pool exposes. The pre-NG pools and the original NG implementation have
	// future_fee(); later NG implementations dropped it for
	// offpeg_fee_multiplier() and revert on future_fee(). No pool exposes both, and
	// each read is gated on its own flag so neither is issued where it reverts.
	// Curated in the DB and carried through LoadPools.
	HasFutureFee           bool
	HasOffpegFeeMultiplier bool
}

// hasNoArgOracleGetters reports whether the five no-arg oracle reads may be
// issued for this pool. The curated flag is only meaningful for plain_ng, so the
// class is checked here rather than at each of the three call sites.
func (p RegisteredPool) hasNoArgOracleGetters() bool {
	return p.Kind == KindStableswapNG && p.HasNoArgOracleGetters
}

// PoolID and DeployBlockNum implement dexconsumer.SnapshotPool, letting
// RegisteredPool feed the shared sweep/deploy-gate tracker without
// dexconsumer depending on curveindexer.
func (p RegisteredPool) PoolID() int64         { return p.ID }
func (p RegisteredPool) DeployBlockNum() int64 { return p.DeployBlock }

type SwapRecord struct {
	Pool         RegisteredPool
	LogIndex     uint
	TxHash       common.Hash
	Buyer        common.Address
	SoldID       int
	BoughtID     int
	TokensSold   *big.Int
	TokensBought *big.Int
	Fee          *big.Int // nil when the event carries none (stableswap)
	// IsUnderlying is true when the row came from TokenExchangeUnderlying
	// (meta/lending underlying swap) rather than TokenExchange.
	IsUnderlying bool
}

// ParameterEventRecord is a decoded on-chain admin/governance parameter event
// (RampA, NewFee, NewAdmin, ...). Params carries the decoded named fields as
// JSONB; the exact keys per EventName are documented on curve_parameter_event.params.
type ParameterEventRecord struct {
	Pool      RegisteredPool
	LogIndex  uint
	TxHash    common.Hash
	EventName string
	Params    json.RawMessage
}

// LpTokenEventRecord is a decoded LP-token ERC-20 Transfer or Approval event.
// For Approval, From holds the owner and To holds the spender.
type LpTokenEventRecord struct {
	Pool      RegisteredPool
	LogIndex  uint
	TxHash    common.Hash
	EventName string // "transfer" or "approval"
	From      common.Address
	To        common.Address
	Value     *big.Int
}

type LiquidityKind string

const (
	LiquidityAdd             LiquidityKind = "add"
	LiquidityRemove          LiquidityKind = "remove"
	LiquidityRemoveOne       LiquidityKind = "remove_one"
	LiquidityRemoveImbalance LiquidityKind = "remove_imbalance"
)

type LiquidityRecord struct {
	Pool         RegisteredPool
	LogIndex     uint
	TxHash       common.Hash
	Provider     common.Address
	Kind         LiquidityKind
	TokenAmounts []*big.Int
	CoinIndex    *int       // remove_one only
	Fees         []*big.Int // nil when absent
	Invariant    *big.Int   // nil when absent
	TokenSupply  *big.Int   // nil when absent
}

type DecodedEvents struct {
	Swaps           []SwapRecord
	Liquidity       []LiquidityRecord
	ParameterEvents []ParameterEventRecord
	LpTokenEvents   []LpTokenEventRecord
	Captured        []dexconsumer.CapturedLog
}

type PoolClassHandler interface {
	DecodeEvents(receipt shared.TransactionReceipt, pool RegisteredPool, chainID, blockNumber int64, version int, ts time.Time) (DecodedEvents, error)
	// Warm precomputes any per-coin-count state (e.g. event-signature hashes) for
	// pools with nCoins coins. The coordinator calls it once per registered pool at
	// construction so the per-block decode path is a pure cache read, keeping the
	// handler's caches free of hot-path writes that would otherwise depend on the
	// single-goroutine processing contract for safety.
	Warm(nCoins int)
}

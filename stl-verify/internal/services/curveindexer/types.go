package curveindexer

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

type PoolKind string

const (
	KindStableswapPreNG PoolKind = "plain_pre_ng"
	KindStableswapNG    PoolKind = "plain_ng"
	KindCryptoswap      PoolKind = "cryptoswap"
)

type RegisteredPool struct {
	ID           int64 // curve_pool.id
	Address      common.Address
	Kind         PoolKind
	NCoins       int
	CoinDecimals []int
	DeployBlock  int64
	// LpTokenAddress is the separate LP token for pre-NG pools (where totalSupply
	// lives), nil when the pool is its own LP token.
	LpTokenAddress *common.Address
	// HasAPrecise records whether this stableswap pool exposes A_precise(). Some of
	// the oldest pre-NG pools (e.g. 3pool) do not, so the snapshot must gate the call
	// on this flag. Curated in the DB (curve_pool.has_a_precise) and carried through
	// LoadPools; irrelevant (and false) for cryptoswap pools, which never call A_precise.
	HasAPrecise bool
}

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

type CapturedEvent struct { // -> protocol_event capture net
	// Address is the log's emitting contract: the pool itself for most logs, but
	// the separate LP-token contract for pre-NG pools' Transfer/Approval. A captured
	// event carries only its emitting address, not pool identity; protocol_event
	// records this address verbatim.
	Address   common.Address
	LogIndex  uint
	TxHash    common.Hash
	EventName string
	Payload   json.RawMessage
}

type DecodedEvents struct {
	Swaps           []SwapRecord
	Liquidity       []LiquidityRecord
	ParameterEvents []ParameterEventRecord
	LpTokenEvents   []LpTokenEventRecord
	Captured        []CapturedEvent
}

// StateSnapshot is a pool-class-tagged state row plus its close-to-static
// governance config. Exactly one of the two state pointers is non-nil, matching
// Pool.Kind; the matching config pointer carries the per-block config reads (the
// repo decides via append-on-change whether to persist a new config row).
type StateSnapshot struct {
	Pool             RegisteredPool
	BlockNumber      int64
	BlockVersion     int
	Timestamp        time.Time
	Stableswap       *entity.CurveStableswapState
	Cryptoswap       *entity.CurveCryptoswapState
	StableswapConfig *entity.CurveStableswapConfig
	CryptoswapConfig *entity.CurveCryptoswapConfig
}

// Validate enforces that exactly one state pointer is set matching Pool.Kind,
// and that any attached config pointer also matches Kind (a cross-class config
// is a handler bug and must fail at the boundary, not become a mismatched row).
func (s StateSnapshot) Validate() error {
	switch s.Pool.Kind {
	case KindStableswapPreNG, KindStableswapNG:
		if s.Stableswap == nil || s.Cryptoswap != nil {
			return fmt.Errorf("pool %s (kind %s): expected stableswap snapshot only", s.Pool.Address, s.Pool.Kind)
		}
		if s.CryptoswapConfig != nil {
			return fmt.Errorf("pool %s (kind %s): stableswap snapshot carries a cryptoswap config", s.Pool.Address, s.Pool.Kind)
		}
	case KindCryptoswap:
		if s.Cryptoswap == nil || s.Stableswap != nil {
			return fmt.Errorf("pool %s (kind %s): expected cryptoswap snapshot only", s.Pool.Address, s.Pool.Kind)
		}
		if s.StableswapConfig != nil {
			return fmt.Errorf("pool %s (kind %s): cryptoswap snapshot carries a stableswap config", s.Pool.Address, s.Pool.Kind)
		}
	default:
		return fmt.Errorf("pool %s: unknown kind %s", s.Pool.Address, s.Pool.Kind)
	}
	return nil
}

type PoolClassHandler interface {
	DecodeEvents(receipt shared.TransactionReceipt, pool RegisteredPool, chainID, blockNumber int64, version int, ts time.Time) (DecodedEvents, error)
	SnapshotState(ctx context.Context, mc outbound.Multicaller, pool RegisteredPool, blockNumber int64, version int, ts time.Time) (StateSnapshot, error)
	// Warm precomputes any per-coin-count state (e.g. event-signature hashes) for
	// pools with nCoins coins. The coordinator calls it once per registered pool at
	// construction so the per-block decode path is a pure cache read, keeping the
	// handler's caches free of hot-path writes that would otherwise depend on the
	// single-goroutine processing contract for safety.
	Warm(nCoins int)
}

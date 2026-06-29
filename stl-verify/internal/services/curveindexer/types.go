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
	Pool      RegisteredPool
	LogIndex  uint
	TxHash    common.Hash
	EventName string
	Payload   json.RawMessage
}

type DecodedEvents struct {
	Swaps     []SwapRecord
	Liquidity []LiquidityRecord
	Captured  []CapturedEvent
}

// StateSnapshot is a pool-class-tagged state row. Exactly one of the two
// pointers is non-nil, matching Pool.Kind.
type StateSnapshot struct {
	Pool         RegisteredPool
	BlockNumber  int64
	BlockVersion int
	Timestamp    time.Time
	Stableswap   *entity.CurveStableswapState
	Cryptoswap   *entity.CurveCryptoswapState
}

// Validate enforces that exactly one state pointer is set and it matches Pool.Kind.
func (s StateSnapshot) Validate() error {
	switch s.Pool.Kind {
	case KindStableswapPreNG, KindStableswapNG:
		if s.Stableswap == nil || s.Cryptoswap != nil {
			return fmt.Errorf("pool %s (kind %s): expected stableswap snapshot only", s.Pool.Address, s.Pool.Kind)
		}
	case KindCryptoswap:
		if s.Cryptoswap == nil || s.Stableswap != nil {
			return fmt.Errorf("pool %s (kind %s): expected cryptoswap snapshot only", s.Pool.Address, s.Pool.Kind)
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

package curveindexer

import (
	"context"
	"encoding/json"
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
	CoinTokenIDs []int64 // index-aligned to on-chain coins(i)
	DeployBlock  int64
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

type PoolClassHandler interface {
	Handles(kind PoolKind) bool
	DecodeEvents(receipt shared.TransactionReceipt, pool RegisteredPool, chainID, blockNumber int64, version int, ts time.Time) (DecodedEvents, error)
	SnapshotState(ctx context.Context, mc outbound.Multicaller, pool RegisteredPool, blockNumber int64, version int, ts time.Time) (StateSnapshot, error)
}

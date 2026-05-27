package uniswap_v3_dex

import (
	"math/big"
	"sync"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// uniswapV3EventName is the decoded Solidity event name for V3 pool / NFPM
// emissions. Used as protocol_event.event_name and to dispatch typed handling.
type uniswapV3EventName string

const (
	// Pool-emitted events.
	eventSwap                               uniswapV3EventName = "Swap"
	eventMint                               uniswapV3EventName = "Mint"
	eventBurn                               uniswapV3EventName = "Burn"
	eventCollect                            uniswapV3EventName = "Collect"
	eventInitialize                         uniswapV3EventName = "Initialize"
	eventIncreaseObservationCardinalityNext uniswapV3EventName = "IncreaseObservationCardinalityNext"
	eventSetFeeProtocol                     uniswapV3EventName = "SetFeeProtocol"
	eventCollectProtocol                    uniswapV3EventName = "CollectProtocol"
	eventFlash                              uniswapV3EventName = "Flash"
	// NFPM-emitted events.
	eventNFPMIncreaseLiquidity uniswapV3EventName = "IncreaseLiquidity"
	eventNFPMDecreaseLiquidity uniswapV3EventName = "DecreaseLiquidity"
	eventNFPMCollect           uniswapV3EventName = "Collect" // NFPM "Collect" — distinct decoder path from the pool's Collect.
	eventNFPMTransfer          uniswapV3EventName = "Transfer"
)

// decodedEvent carries an event the worker decoded from a log. Exactly one
// of the typed pointers is populated based on Name.
type decodedEvent struct {
	Name    uniswapV3EventName
	Address common.Address
	LogIdx  int32
	TxHash  common.Hash

	Swap         *swapEvent
	Liquidity    *liquidityEvent
	Parameter    *parameterEvent
	NFPMIncrease *nfpmLiquidityEvent
	NFPMDecrease *nfpmLiquidityEvent
	NFPMCollect  *nfpmCollectEvent
	NFPMTransfer *nfpmTransferEvent
}

type swapEvent struct {
	Sender       common.Address
	Recipient    common.Address
	Amount0      *big.Int
	Amount1      *big.Int
	SqrtPriceX96 *big.Int
	Liquidity    *big.Int
	Tick         int32
}

type liquidityEvent struct {
	Kind      string // entity.UniswapV3PoolLiquidityEvent* constant
	Owner     common.Address
	TickLower int32
	TickUpper int32
	Amount    *big.Int // Mint/Burn liquidity delta; nil for Collect.
	Amount0   *big.Int
	Amount1   *big.Int
	Sender    *common.Address // Mint only.
	Recipient *common.Address // Collect only.
}

type parameterEvent struct {
	Kind                      string // entity.UniswapV3PoolParameterEvent* constant
	SqrtPriceX96              *big.Int
	Tick                      *int32
	ObservationCardinalityOld *int32
	ObservationCardinalityNew *int32
	FeeProtocol0Old           *int32
	FeeProtocol0New           *int32
	FeeProtocol1Old           *int32
	FeeProtocol1New           *int32
	Amount0                   *big.Int
	Amount1                   *big.Int
	Sender                    *common.Address
	Recipient                 *common.Address
}

type nfpmLiquidityEvent struct {
	TokenID   *big.Int
	Liquidity *big.Int // Liquidity delta — both IncreaseLiquidity and DecreaseLiquidity emit a delta on NFPM.
	Amount0   *big.Int
	Amount1   *big.Int
}

type nfpmCollectEvent struct {
	TokenID   *big.Int
	Recipient common.Address
	Amount0   *big.Int
	Amount1   *big.Int
}

type nfpmTransferEvent struct {
	From    common.Address
	To      common.Address
	TokenID *big.Int
}

// poolMulticallResult bundles the typed outputs of a single event-triggered
// pool multicall: slot0 + liquidity + observe([0]) + balanceOf(token0) +
// balanceOf(token1).
type poolMulticallResult struct {
	SqrtPriceX96               *big.Int
	Tick                       int32
	ObservationIndex           int32
	ObservationCardinality     int32
	ObservationCardinalityNext int32
	FeeProtocol                int32
	Unlocked                   bool
	Liquidity                  *big.Int

	// observe([0]) cumulatives — required on every state row (plan §12.4 #12).
	TickCumulative                 *big.Int
	SecsPerLiquidityCumulativeX128 *big.Int

	// Pool ERC-20 reserves; nullable on the row but always populated here.
	Balance0 *big.Int
	Balance1 *big.Int
}

// nfpmPositionResult bundles NFPM.positions(tokenId) outputs needed for the
// per-position state row and for cold position discovery on IncreaseLiquidity.
type nfpmPositionResult struct {
	Token0                   common.Address
	Token1                   common.Address
	Fee                      int32
	TickLower                int32
	TickUpper                int32
	Liquidity                *big.Int
	FeeGrowthInside0LastX128 *big.Int
	FeeGrowthInside1LastX128 *big.Int
	TokensOwed0              *big.Int
	TokensOwed1              *big.Int
}

// poolRegistry is the in-memory snapshot of `uniswap_v3_pool`, indexed by
// pool address. Updated only at Start; position registry is separate. Each
// entry also caches the pool's token0/token1 addresses so the event-triggered
// multicall can issue balanceOf reads without an extra lookup per event.
type poolRegistry struct {
	mu             sync.RWMutex
	poolsByAddress map[common.Address]*entity.UniswapV3Pool
	poolsByID      map[int64]*entity.UniswapV3Pool
	// Token addresses for each pool, resolved lazily on first event so the
	// balanceOf calls in readPoolState can be issued without an extra round-trip.
	token0 map[int64]common.Address
	token1 map[int64]common.Address
}

func newPoolRegistry() *poolRegistry {
	return &poolRegistry{
		poolsByAddress: make(map[common.Address]*entity.UniswapV3Pool),
		poolsByID:      make(map[int64]*entity.UniswapV3Pool),
		token0:         make(map[int64]common.Address),
		token1:         make(map[int64]common.Address),
	}
}

func (r *poolRegistry) addPool(p *entity.UniswapV3Pool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.poolsByAddress[p.Address] = p
	r.poolsByID[p.ID] = p
}

func (r *poolRegistry) setTokenAddresses(poolID int64, t0, t1 common.Address) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.token0[poolID] = t0
	r.token1[poolID] = t1
}

func (r *poolRegistry) tokenAddresses(poolID int64) (common.Address, common.Address, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t0, ok0 := r.token0[poolID]
	t1, ok1 := r.token1[poolID]
	return t0, t1, ok0 && ok1
}

func (r *poolRegistry) poolByAddress(a common.Address) *entity.UniswapV3Pool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.poolsByAddress[a]
}

func (r *poolRegistry) poolByID(id int64) *entity.UniswapV3Pool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.poolsByID[id]
}

func (r *poolRegistry) poolCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.poolsByAddress)
}

// positionCache maps NFPM tokenId → loaded position so cold-path positions()
// reads are amortised across events on the same NFT.
type positionCache struct {
	mu        sync.RWMutex
	byTokenID map[string]*entity.UniswapV3Position // big.Int.String() key for stable map equality.
}

func newPositionCache() *positionCache {
	return &positionCache{byTokenID: make(map[string]*entity.UniswapV3Position)}
}

func (c *positionCache) get(tokenID *big.Int) *entity.UniswapV3Position {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.byTokenID[tokenID.String()]
}

func (c *positionCache) put(pos *entity.UniswapV3Position) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byTokenID[pos.TokenID.String()] = pos
}

// putMissing marks `tokenID` as confirmed-out-of-scope so future events on the
// same NFT skip the NFPM.positions() round-trip. Stores the marker under the
// caller's tokenID key, not under the marker entity's nil TokenID — and takes
// the cache write lock so concurrent goroutines don't race.
func (c *positionCache) putMissing(tokenID *big.Int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byTokenID[tokenID.String()] = missingPositionMarker
}

// markBurned flips Burned=true on the cached entry for tokenID under the
// cache write lock. No-op if tokenID is absent or marked missing. Use this
// instead of `cached.Burned = true` — direct field writes on the cached
// pointer race against put/putMissing/markBurned from other goroutines.
func (c *positionCache) markBurned(tokenID *big.Int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	pos, ok := c.byTokenID[tokenID.String()]
	if !ok || isMissingMarker(pos) {
		return
	}
	pos.Burned = true
}

// missingPositionMarker is stored in the cache for tokenIds we have already
// confirmed live outside our tracked pool set. Avoids re-querying NFPM.positions
// on every event for the same NFT. Lookups treat this sentinel as "drop".
var missingPositionMarker = &entity.UniswapV3Position{ID: -1}

func isMissingMarker(p *entity.UniswapV3Position) bool {
	return p != nil && p.ID == -1
}

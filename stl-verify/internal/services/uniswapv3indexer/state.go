package uniswapv3indexer

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// twapWindowSecs is the lookback window used for the observe() TWAP read.
// 30 minutes balances smoothing against observation-cardinality coverage:
// pools with a small cardinality are more likely to have enough history to
// answer a shorter window without reverting.
const twapWindowSecs = 1800

const stateViewMethodsJSON = `[
	{"name":"slot0","type":"function","stateMutability":"view","inputs":[],"outputs":[
		{"name":"sqrtPriceX96","type":"uint160"},
		{"name":"tick","type":"int24"},
		{"name":"observationIndex","type":"uint16"},
		{"name":"observationCardinality","type":"uint16"},
		{"name":"observationCardinalityNext","type":"uint16"},
		{"name":"feeProtocol","type":"uint8"},
		{"name":"unlocked","type":"bool"}
	]},
	{"name":"liquidity","type":"function","stateMutability":"view","inputs":[],"outputs":[{"name":"","type":"uint128"}]},
	{"name":"feeGrowthGlobal0X128","type":"function","stateMutability":"view","inputs":[],"outputs":[{"name":"","type":"uint256"}]},
	{"name":"feeGrowthGlobal1X128","type":"function","stateMutability":"view","inputs":[],"outputs":[{"name":"","type":"uint256"}]},
	{"name":"protocolFees","type":"function","stateMutability":"view","inputs":[],"outputs":[
		{"name":"token0","type":"uint128"},
		{"name":"token1","type":"uint128"}
	]},
	{"name":"observe","type":"function","stateMutability":"view","inputs":[{"name":"secondsAgos","type":"uint32[]"}],"outputs":[
		{"name":"tickCumulatives","type":"int56[]"},
		{"name":"secondsPerLiquidityCumulativeX128s","type":"uint160[]"}
	]}
]`

const erc20BalanceOfJSON = `[
	{"name":"balanceOf","type":"function","stateMutability":"view","inputs":[{"name":"account","type":"address"}],"outputs":[{"name":"","type":"uint256"}]}
]`

// poolStateABI returns the ABI fragment for the pool's state-reading view
// methods (slot0, liquidity, feeGrowthGlobal0/1X128, protocolFees, observe).
// These are not events, so they live apart from PoolABI in abi.go, and apart
// from the tick-reading methods in tick.go (a distinct, single-responsibility
// read path per B7/B8 split).
func poolStateABI() (*abi.ABI, error) {
	parsed, err := abi.JSON(strings.NewReader(stateViewMethodsJSON))
	if err != nil {
		return nil, fmt.Errorf("parsing pool state ABI: %w", err)
	}
	return &parsed, nil
}

// erc20ABI returns the ABI fragment for the ERC20 balanceOf view method,
// used to read a pool's real token balances (the pool itself exposes no
// balance accessor).
func erc20ABI() (*abi.ABI, error) {
	parsed, err := abi.JSON(strings.NewReader(erc20BalanceOfJSON))
	if err != nil {
		return nil, fmt.Errorf("parsing ERC20 balanceOf ABI: %w", err)
	}
	return &parsed, nil
}

// SnapshotState reads a pool's slot0/liquidity/fee-growth/protocol-fee/real-
// balance state, plus a best-effort TWAP, all pinned to blockHash in a single
// multicall batch. It returns ONLY pool-level state: tick-level reads live in
// tick.go, and the service composing both (B9) owns that join.
//
// Every read except observe() is CORE: a revert on any of them returns an
// error rather than a silently zeroed/NULL field, since a partial snapshot
// would misrepresent real on-chain state. observe() alone is optional
// because it legitimately reverts with `OLD` when the pool's observation
// cardinality can't cover the requested window; that is not a data-quality
// problem, so it degrades to a nil TWAP instead of failing the whole
// snapshot.
func SnapshotState(ctx context.Context, mc outbound.Multicaller, pool RegisteredPool, blockHash common.Hash, blockNumber int64, version int, ts time.Time) (*entity.UniswapV3PoolState, error) {
	stateABI, err := poolStateABI()
	if err != nil {
		return nil, err
	}
	erc20, err := erc20ABI()
	if err != nil {
		return nil, err
	}

	state := &entity.UniswapV3PoolState{}
	reads := stateSnapshotReads(state, stateABI, erc20)
	if err := shared.RunSnapshotReads(ctx, mc, pool, blockHash, reads); err != nil {
		return nil, fmt.Errorf("snapshotting pool %s state: %w", pool.Address, err)
	}

	state.PoolID = pool.ID
	state.BlockNumber = blockNumber
	state.BlockVersion = version
	state.BlockTimestamp = ts

	if err := state.Validate(); err != nil {
		return nil, fmt.Errorf("validating pool %s state snapshot: %w", pool.Address, err)
	}
	return state, nil
}

// stateSnapshotReads describes the 8-call state batch as self-contained
// pack/decode units, each closing over the state being built. Keeping every
// read's Pack next to its Decode means the call order below is the only
// place that determines wire order: there is no separate positional index to
// keep in sync.
func stateSnapshotReads(state *entity.UniswapV3PoolState, stateABI, erc20 *abi.ABI) []shared.SnapshotRead[RegisteredPool] {
	return []shared.SnapshotRead[RegisteredPool]{
		{
			Name: "slot0",
			Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
				data, err := stateABI.Pack("slot0")
				if err != nil {
					return nil, fmt.Errorf("packing slot0(): %w", err)
				}
				return []outbound.Call{{Target: pool.Address, AllowFailure: false, CallData: data}}, nil
			},
			Decode: func(pool RegisteredPool, results []outbound.Result) error {
				return decodeSlot0(pool, stateABI, results[0], state)
			},
		},
		{
			Name: "liquidity",
			Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
				data, err := stateABI.Pack("liquidity")
				if err != nil {
					return nil, fmt.Errorf("packing liquidity(): %w", err)
				}
				return []outbound.Call{{Target: pool.Address, AllowFailure: false, CallData: data}}, nil
			},
			Decode: func(pool RegisteredPool, results []outbound.Result) error {
				liquidity, err := shared.UnpackUint(stateABI, "liquidity", results[0])
				if err != nil {
					return fmt.Errorf("pool %s liquidity(): %w", pool.Address, err)
				}
				state.Liquidity = liquidity
				return nil
			},
		},
		{
			Name: "feeGrowthGlobal0X128",
			Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
				data, err := stateABI.Pack("feeGrowthGlobal0X128")
				if err != nil {
					return nil, fmt.Errorf("packing feeGrowthGlobal0X128(): %w", err)
				}
				return []outbound.Call{{Target: pool.Address, AllowFailure: false, CallData: data}}, nil
			},
			Decode: func(pool RegisteredPool, results []outbound.Result) error {
				feeGrowth0, err := shared.UnpackUint(stateABI, "feeGrowthGlobal0X128", results[0])
				if err != nil {
					return fmt.Errorf("pool %s feeGrowthGlobal0X128(): %w", pool.Address, err)
				}
				state.FeeGrowthGlobal0X128 = feeGrowth0
				return nil
			},
		},
		{
			Name: "feeGrowthGlobal1X128",
			Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
				data, err := stateABI.Pack("feeGrowthGlobal1X128")
				if err != nil {
					return nil, fmt.Errorf("packing feeGrowthGlobal1X128(): %w", err)
				}
				return []outbound.Call{{Target: pool.Address, AllowFailure: false, CallData: data}}, nil
			},
			Decode: func(pool RegisteredPool, results []outbound.Result) error {
				feeGrowth1, err := shared.UnpackUint(stateABI, "feeGrowthGlobal1X128", results[0])
				if err != nil {
					return fmt.Errorf("pool %s feeGrowthGlobal1X128(): %w", pool.Address, err)
				}
				state.FeeGrowthGlobal1X128 = feeGrowth1
				return nil
			},
		},
		{
			Name: "protocolFees",
			Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
				data, err := stateABI.Pack("protocolFees")
				if err != nil {
					return nil, fmt.Errorf("packing protocolFees(): %w", err)
				}
				return []outbound.Call{{Target: pool.Address, AllowFailure: false, CallData: data}}, nil
			},
			Decode: func(pool RegisteredPool, results []outbound.Result) error {
				return decodeProtocolFees(pool, stateABI, results[0], state)
			},
		},
		{
			Name: "balanceOf token0",
			Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
				data, err := erc20.Pack("balanceOf", pool.Address)
				if err != nil {
					return nil, fmt.Errorf("packing balanceOf(pool) for token0: %w", err)
				}
				return []outbound.Call{{Target: pool.Token0, AllowFailure: false, CallData: data}}, nil
			},
			Decode: func(pool RegisteredPool, results []outbound.Result) error {
				balance0, err := shared.UnpackUint(erc20, "balanceOf", results[0])
				if err != nil {
					return fmt.Errorf("pool %s token0 balanceOf(): %w", pool.Address, err)
				}
				state.Balance0 = balance0
				return nil
			},
		},
		{
			Name: "balanceOf token1",
			Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
				data, err := erc20.Pack("balanceOf", pool.Address)
				if err != nil {
					return nil, fmt.Errorf("packing balanceOf(pool) for token1: %w", err)
				}
				return []outbound.Call{{Target: pool.Token1, AllowFailure: false, CallData: data}}, nil
			},
			Decode: func(pool RegisteredPool, results []outbound.Result) error {
				balance1, err := shared.UnpackUint(erc20, "balanceOf", results[0])
				if err != nil {
					return fmt.Errorf("pool %s token1 balanceOf(): %w", pool.Address, err)
				}
				state.Balance1 = balance1
				return nil
			},
		},
		{
			Name: "observe",
			Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
				data, err := stateABI.Pack("observe", []uint32{uint32(twapWindowSecs), 0})
				if err != nil {
					return nil, fmt.Errorf("packing observe(): %w", err)
				}
				return []outbound.Call{{Target: pool.Address, AllowFailure: true, CallData: data}}, nil
			},
			Decode: func(pool RegisteredPool, results []outbound.Result) error {
				decodeTwap(stateABI, results[0], state)
				return nil
			},
		},
	}
}

// decodeSlot0 unpacks slot0()'s 7-tuple into state. tick and feeProtocol need
// dedicated handling: tick is a signed int24 (go-ethereum decodes it as
// *big.Int, not a native int type), and feeProtocol is stored as the raw
// packed uint8 rather than split into its two nibbles.
func decodeSlot0(pool RegisteredPool, a *abi.ABI, res outbound.Result, state *entity.UniswapV3PoolState) error {
	if !res.Success {
		return fmt.Errorf("pool %s slot0() reverted", pool.Address)
	}
	out, err := a.Unpack("slot0", res.ReturnData)
	if err != nil {
		return fmt.Errorf("unpacking pool %s slot0(): %w", pool.Address, err)
	}
	if len(out) != 7 {
		return fmt.Errorf("pool %s slot0() returned %d values, want 7", pool.Address, len(out))
	}

	sqrtPriceX96, ok := out[0].(*big.Int)
	if !ok {
		return fmt.Errorf("pool %s slot0() sqrtPriceX96 type = %T, want *big.Int", pool.Address, out[0])
	}
	tick, ok := out[1].(*big.Int)
	if !ok {
		return fmt.Errorf("pool %s slot0() tick type = %T, want *big.Int", pool.Address, out[1])
	}
	observationIndex, ok := out[2].(uint16)
	if !ok {
		return fmt.Errorf("pool %s slot0() observationIndex type = %T, want uint16", pool.Address, out[2])
	}
	observationCardinality, ok := out[3].(uint16)
	if !ok {
		return fmt.Errorf("pool %s slot0() observationCardinality type = %T, want uint16", pool.Address, out[3])
	}
	observationCardinalityNext, ok := out[4].(uint16)
	if !ok {
		return fmt.Errorf("pool %s slot0() observationCardinalityNext type = %T, want uint16", pool.Address, out[4])
	}
	feeProtocol, ok := out[5].(uint8)
	if !ok {
		return fmt.Errorf("pool %s slot0() feeProtocol type = %T, want uint8", pool.Address, out[5])
	}
	unlocked, ok := out[6].(bool)
	if !ok {
		return fmt.Errorf("pool %s slot0() unlocked type = %T, want bool", pool.Address, out[6])
	}

	state.SqrtPriceX96 = sqrtPriceX96
	state.Tick = int(tick.Int64())
	state.ObservationIndex = int(observationIndex)
	state.ObservationCardinality = int(observationCardinality)
	state.ObservationCardinalityNext = int(observationCardinalityNext)
	state.FeeProtocol = int(feeProtocol)
	state.Unlocked = unlocked
	return nil
}

// decodeProtocolFees unpacks protocolFees()'s (token0, token1) pair into state.
func decodeProtocolFees(pool RegisteredPool, a *abi.ABI, res outbound.Result, state *entity.UniswapV3PoolState) error {
	if !res.Success {
		return fmt.Errorf("pool %s protocolFees() reverted", pool.Address)
	}
	out, err := a.Unpack("protocolFees", res.ReturnData)
	if err != nil {
		return fmt.Errorf("unpacking pool %s protocolFees(): %w", pool.Address, err)
	}
	if len(out) != 2 {
		return fmt.Errorf("pool %s protocolFees() returned %d values, want 2", pool.Address, len(out))
	}
	token0, ok := out[0].(*big.Int)
	if !ok {
		return fmt.Errorf("pool %s protocolFees() token0 type = %T, want *big.Int", pool.Address, out[0])
	}
	token1, ok := out[1].(*big.Int)
	if !ok {
		return fmt.Errorf("pool %s protocolFees() token1 type = %T, want *big.Int", pool.Address, out[1])
	}
	state.ProtocolFeesToken0 = token0
	state.ProtocolFeesToken1 = token1
	return nil
}

// decodeTwap unpacks observe()'s two int56[]/uint160[] arrays into a single
// TWAP tick over twapWindowSecs. observe reverts with `OLD` when the pool's
// observation cardinality doesn't cover the window; that is a legitimate,
// expected condition (not a data-quality error), so a revert here leaves
// TwapTick/TwapWindowSecs nil instead of failing the whole snapshot.
func decodeTwap(a *abi.ABI, res outbound.Result, state *entity.UniswapV3PoolState) {
	if !res.Success {
		return
	}
	out, err := a.Unpack("observe", res.ReturnData)
	if err != nil {
		return
	}
	if len(out) != 2 {
		return
	}
	tickCumulatives, ok := out[0].([]*big.Int)
	if !ok || len(tickCumulatives) != 2 {
		return
	}

	delta := new(big.Int).Sub(tickCumulatives[1], tickCumulatives[0])
	tick := int(new(big.Int).Div(delta, big.NewInt(twapWindowSecs)).Int64())
	window := twapWindowSecs
	state.TwapTick = &tick
	state.TwapWindowSecs = &window
}

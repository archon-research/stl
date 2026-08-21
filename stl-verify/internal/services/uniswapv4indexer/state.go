package uniswapv4indexer

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

const stateViewMethodsJSON = `[
	{"name":"getSlot0","type":"function","stateMutability":"view","inputs":[{"name":"poolId","type":"bytes32"}],"outputs":[
		{"name":"sqrtPriceX96","type":"uint160"},
		{"name":"tick","type":"int24"},
		{"name":"protocolFee","type":"uint24"},
		{"name":"lpFee","type":"uint24"}
	]},
	{"name":"getLiquidity","type":"function","stateMutability":"view","inputs":[{"name":"poolId","type":"bytes32"}],"outputs":[{"name":"liquidity","type":"uint128"}]},
	{"name":"getFeeGrowthGlobals","type":"function","stateMutability":"view","inputs":[{"name":"poolId","type":"bytes32"}],"outputs":[
		{"name":"feeGrowthGlobal0","type":"uint256"},
		{"name":"feeGrowthGlobal1","type":"uint256"}
	]}
]`

// poolStateABIOnce parses stateViewMethodsJSON exactly once: SnapshotState runs
// per due pool per block, so re-parsing the JSON each call is pure waste.
var poolStateABIOnce = sync.OnceValues(func() (*abi.ABI, error) {
	parsed, err := abis.ParseABI(stateViewMethodsJSON)
	if err != nil {
		return nil, fmt.Errorf("parsing StateView pool-state ABI: %w", err)
	}
	return parsed, nil
})

// poolStateABI returns the ABI fragment for the pool-level StateView getters.
// Unlike V3 these live on a periphery contract, not the pool, and take the
// PoolId as an argument — so unlike V3 their calldata cannot be pre-packed once
// for every pool. The tick-level getters live in tick.go.
func poolStateABI() (*abi.ABI, error) {
	return poolStateABIOnce()
}

// SnapshotState reads a pool's slot0, liquidity and global fee growth from
// StateView in a single multicall batch pinned to blockHash. It returns ONLY
// pool-level state: tick-level reads live in tick.go.
//
// Every read is CORE (AllowFailure=false). V4 has no optional read: unlike V3's
// observe(), none of these getters has a legitimate revert, so a failure is a
// data-quality signal that must stop the block rather than zero a column.
func SnapshotState(ctx context.Context, mc outbound.Multicaller, pool RegisteredPool, blockHash common.Hash, blockNumber int64, version int, ts time.Time) (*entity.UniswapV4PoolState, error) {
	stateABI, err := poolStateABI()
	if err != nil {
		return nil, err
	}

	state := &entity.UniswapV4PoolState{}
	reads := stateSnapshotReads(state, stateABI)
	if err := shared.RunSnapshotReads(ctx, mc, pool, blockHash, reads); err != nil {
		return nil, fmt.Errorf("snapshotting pool %s state: %w", pool.PoolIDHash, err)
	}

	state.PoolID = pool.ID
	state.BlockNumber = blockNumber
	state.BlockVersion = version
	state.BlockTimestamp = ts

	if err := state.Validate(); err != nil {
		return nil, fmt.Errorf("validating pool %s state snapshot: %w", pool.PoolIDHash, err)
	}
	return state, nil
}

// stateSnapshotReads describes the three-call state batch as self-contained
// pack/decode units, each closing over the state being built. Keeping every
// read's Pack next to its Decode means the call order below is the only place
// that determines wire order.
func stateSnapshotReads(state *entity.UniswapV4PoolState, stateABI *abi.ABI) []shared.SnapshotRead[RegisteredPool] {
	stateViewRead := func(method string, decode func(pool RegisteredPool, res outbound.Result) error) shared.SnapshotRead[RegisteredPool] {
		return shared.SnapshotRead[RegisteredPool]{
			Name: method,
			Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
				data, err := stateABI.Pack(method, pool.PoolIDHash)
				if err != nil {
					return nil, fmt.Errorf("packing %s(%s): %w", method, pool.PoolIDHash, err)
				}
				return []outbound.Call{{Target: pool.StateView, AllowFailure: false, CallData: data}}, nil
			},
			Decode: func(pool RegisteredPool, results []outbound.Result) error {
				return decode(pool, results[0])
			},
		}
	}

	return []shared.SnapshotRead[RegisteredPool]{
		stateViewRead("getSlot0", func(pool RegisteredPool, res outbound.Result) error {
			return decodeSlot0(pool, stateABI, res, state)
		}),
		stateViewRead("getLiquidity", func(pool RegisteredPool, res outbound.Result) error {
			liquidity, err := shared.UnpackUint(stateABI, "getLiquidity", res)
			if err != nil {
				return fmt.Errorf("pool %s getLiquidity(): %w", pool.PoolIDHash, err)
			}
			state.Liquidity = liquidity
			return nil
		}),
		stateViewRead("getFeeGrowthGlobals", func(pool RegisteredPool, res outbound.Result) error {
			return decodeFeeGrowthGlobals(pool, stateABI, res, state)
		}),
	}
}

func decodeSlot0(pool RegisteredPool, a *abi.ABI, res outbound.Result, state *entity.UniswapV4PoolState) error {
	out, err := unpackStateView(pool, a, "getSlot0", res, 4)
	if err != nil {
		return err
	}
	tick, err := int24Value("tick", out[1])
	if err != nil {
		return fmt.Errorf("pool %s getSlot0(): %w", pool.PoolIDHash, err)
	}
	protocolFee, err := uint24Value("protocolFee", out[2])
	if err != nil {
		return fmt.Errorf("pool %s getSlot0(): %w", pool.PoolIDHash, err)
	}
	lpFee, err := uint24Value("lpFee", out[3])
	if err != nil {
		return fmt.Errorf("pool %s getSlot0(): %w", pool.PoolIDHash, err)
	}
	state.SqrtPriceX96 = out[0]
	state.Tick = tick
	state.ProtocolFee = protocolFee
	state.LpFee = lpFee
	return nil
}

func decodeFeeGrowthGlobals(pool RegisteredPool, a *abi.ABI, res outbound.Result, state *entity.UniswapV4PoolState) error {
	out, err := unpackStateView(pool, a, "getFeeGrowthGlobals", res, 2)
	if err != nil {
		return err
	}
	state.FeeGrowthGlobal0X128 = out[0]
	state.FeeGrowthGlobal1X128 = out[1]
	return nil
}

func unpackStateView(pool RegisteredPool, a *abi.ABI, method string, res outbound.Result, want int) ([]*big.Int, error) {
	if !res.Success {
		return nil, fmt.Errorf("pool %s %s() reverted", pool.PoolIDHash, method)
	}
	out, err := a.Unpack(method, res.ReturnData)
	if err != nil {
		return nil, fmt.Errorf("unpacking pool %s %s(): %w", pool.PoolIDHash, method, err)
	}
	if len(out) != want {
		return nil, fmt.Errorf("pool %s %s() returned %d values, want %d", pool.PoolIDHash, method, len(out), want)
	}
	values := make([]*big.Int, want)
	for i, v := range out {
		bi, ok := v.(*big.Int)
		if !ok {
			return nil, fmt.Errorf("pool %s %s() value %d type = %T, want *big.Int", pool.PoolIDHash, method, i, v)
		}
		values[i] = bi
	}
	return values, nil
}

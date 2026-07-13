package uniswapv3indexer

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

// poolStateABIOnce parses stateViewMethodsJSON exactly once: SnapshotState
// runs per due pool per block, so re-parsing the JSON each call is pure waste.
var poolStateABIOnce = sync.OnceValues(func() (*abi.ABI, error) {
	parsed, err := abis.ParseABI(stateViewMethodsJSON)
	if err != nil {
		return nil, fmt.Errorf("parsing pool state ABI: %w", err)
	}
	return parsed, nil
})

// poolStateABI returns the ABI fragment for the pool's state-reading view
// methods (slot0, liquidity, feeGrowthGlobal0/1X128, protocolFees, observe).
// These are not events, so they live apart from PoolABI in abi.go, and apart
// from the tick-reading methods in tick.go (a distinct, single-responsibility
// read path per B7/B8 split).
func poolStateABI() (*abi.ABI, error) {
	return poolStateABIOnce()
}

// stateConstCallData holds the packed calldata for every state read whose
// arguments are pool-independent. slot0/liquidity/fee-growth/protocolFees take
// no arguments, and observe() always reads the same [twapWindowSecs, 0] window,
// so their selectors+args are identical for every pool at every block. Only
// balanceOf differs per pool (it takes pool.Address), so it stays packed inline.
type stateConstCallData struct {
	slot0                []byte
	liquidity            []byte
	feeGrowthGlobal0X128 []byte
	feeGrowthGlobal1X128 []byte
	protocolFees         []byte
	observe              []byte
}

// stateConstCallDataOnce packs the pool-independent state calldata exactly
// once. SnapshotState runs per due pool per block, so re-packing these constant
// blobs each call is pure waste (the byte slices are read-only after packing).
var stateConstCallDataOnce = sync.OnceValues(func() (stateConstCallData, error) {
	a, err := poolStateABI()
	if err != nil {
		return stateConstCallData{}, err
	}
	pack := func(method string, args ...any) ([]byte, error) {
		data, err := a.Pack(method, args...)
		if err != nil {
			return nil, fmt.Errorf("packing %s(): %w", method, err)
		}
		return data, nil
	}

	var c stateConstCallData
	for _, step := range []struct {
		dst  *[]byte
		name string
		args []any
	}{
		{&c.slot0, "slot0", nil},
		{&c.liquidity, "liquidity", nil},
		{&c.feeGrowthGlobal0X128, "feeGrowthGlobal0X128", nil},
		{&c.feeGrowthGlobal1X128, "feeGrowthGlobal1X128", nil},
		{&c.protocolFees, "protocolFees", nil},
		{&c.observe, "observe", []any{[]uint32{uint32(twapWindowSecs), 0}}},
	} {
		data, err := pack(step.name, step.args...)
		if err != nil {
			return stateConstCallData{}, err
		}
		*step.dst = data
	}
	return c, nil
})

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
	erc20, err := abis.GetERC20ABI()
	if err != nil {
		return nil, err
	}
	constCalls, err := stateConstCallDataOnce()
	if err != nil {
		return nil, err
	}

	state := &entity.UniswapV3PoolState{}
	reads := stateSnapshotReads(state, stateABI, erc20, constCalls)
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
//
// The pool-independent reads (slot0/liquidity/fee-growth/protocolFees/observe)
// return the pre-packed constCalls blobs rather than re-packing per pool per
// block; only balanceOf packs inline, since its argument is the pool address.
func stateSnapshotReads(state *entity.UniswapV3PoolState, stateABI, erc20 *abi.ABI, constCalls stateConstCallData) []shared.SnapshotRead[RegisteredPool] {
	// poolConstRead builds a CORE read (allowFailure=false; a revert fails the
	// snapshot) targeting the pool's own address with pre-packed,
	// pool-independent calldata.
	poolConstRead := func(name string, callData []byte, decode func(pool RegisteredPool, res outbound.Result) error) shared.SnapshotRead[RegisteredPool] {
		return shared.SnapshotRead[RegisteredPool]{
			Name: name,
			Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
				return []outbound.Call{{Target: pool.Address, AllowFailure: false, CallData: callData}}, nil
			},
			Decode: func(pool RegisteredPool, results []outbound.Result) error {
				return decode(pool, results[0])
			},
		}
	}

	unpackUintRead := func(name, method string, callData []byte, assign func(*entity.UniswapV3PoolState, *big.Int)) shared.SnapshotRead[RegisteredPool] {
		return poolConstRead(name, callData, func(pool RegisteredPool, res outbound.Result) error {
			v, err := shared.UnpackUint(stateABI, method, res)
			if err != nil {
				return fmt.Errorf("pool %s %s(): %w", pool.Address, method, err)
			}
			assign(state, v)
			return nil
		})
	}

	// balanceOf's account argument is the pool's own address, so its calldata
	// varies per pool but not per read: pack it once and reuse for both tokens.
	balanceRead := func(name string, target func(pool RegisteredPool) common.Address, assign func(*entity.UniswapV3PoolState, *big.Int)) shared.SnapshotRead[RegisteredPool] {
		return shared.SnapshotRead[RegisteredPool]{
			Name: name,
			Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
				data, err := erc20.Pack("balanceOf", pool.Address)
				if err != nil {
					return nil, fmt.Errorf("packing %s: %w", name, err)
				}
				return []outbound.Call{{Target: target(pool), AllowFailure: false, CallData: data}}, nil
			},
			Decode: func(pool RegisteredPool, results []outbound.Result) error {
				balance, err := shared.UnpackUint(erc20, "balanceOf", results[0])
				if err != nil {
					return fmt.Errorf("pool %s %s: %w", pool.Address, name, err)
				}
				assign(state, balance)
				return nil
			},
		}
	}

	return []shared.SnapshotRead[RegisteredPool]{
		poolConstRead("slot0", constCalls.slot0, func(pool RegisteredPool, res outbound.Result) error {
			return decodeSlot0(pool, stateABI, res, state)
		}),
		unpackUintRead("liquidity", "liquidity", constCalls.liquidity, func(s *entity.UniswapV3PoolState, v *big.Int) { s.Liquidity = v }),
		unpackUintRead("feeGrowthGlobal0X128", "feeGrowthGlobal0X128", constCalls.feeGrowthGlobal0X128, func(s *entity.UniswapV3PoolState, v *big.Int) { s.FeeGrowthGlobal0X128 = v }),
		unpackUintRead("feeGrowthGlobal1X128", "feeGrowthGlobal1X128", constCalls.feeGrowthGlobal1X128, func(s *entity.UniswapV3PoolState, v *big.Int) { s.FeeGrowthGlobal1X128 = v }),
		poolConstRead("protocolFees", constCalls.protocolFees, func(pool RegisteredPool, res outbound.Result) error {
			return decodeProtocolFees(pool, stateABI, res, state)
		}),
		balanceRead("balanceOf(pool) for token0", func(pool RegisteredPool) common.Address { return pool.Token0 }, func(s *entity.UniswapV3PoolState, v *big.Int) { s.Balance0 = v }),
		balanceRead("balanceOf(pool) for token1", func(pool RegisteredPool) common.Address { return pool.Token1 }, func(s *entity.UniswapV3PoolState, v *big.Int) { s.Balance1 = v }),
		{
			Name: "observe",
			Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
				return []outbound.Call{{Target: pool.Address, AllowFailure: true, CallData: constCalls.observe}}, nil
			},
			Decode: func(pool RegisteredPool, results []outbound.Result) error {
				if err := decodeTwap(stateABI, results[0], state); err != nil {
					return fmt.Errorf("pool %s observe(): %w", pool.Address, err)
				}
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
// TWAP tick over twapWindowSecs. observe reverts with reason `OLD` when the
// pool's observation cardinality doesn't cover the window; that is the only
// legitimate, expected revert (not a data-quality error), so it alone leaves
// TwapTick/TwapWindowSecs nil and returns no error.
//
// Any other revert — a different reason, an opaque/empty payload, or a Panic —
// is a data-quality signal (registry row pointing at a non-V3 contract, or an
// RPC bug) and returns an error: mapping it to a nil TWAP would produce a NULL
// twap_tick indistinguishable from a legitimate OLD, and the migration COMMENT
// documents twap_tick NULL to mean specifically OLD.
//
// A SUCCESSFUL call whose payload cannot be decoded (unpack failure, wrong
// arity, wrong element type/length) is treated the same way: it also returns an
// error so the snapshot fails loud.
func decodeTwap(a *abi.ABI, res outbound.Result, state *entity.UniswapV3PoolState) error {
	if !res.Success {
		// observe() is packed AllowFailure=true, so a revert lands here rather
		// than failing the multicall. Only the documented `OLD` reason (young pool
		// whose observation cardinality can't cover the window) is a legitimate
		// nil-TWAP; any other reason, an opaque/empty revert, or a Panic is a
		// data-quality signal that must fail the snapshot instead of silently
		// becoming a NULL twap_tick.
		reason, err := abi.UnpackRevert(res.ReturnData)
		if err != nil {
			return fmt.Errorf("observe() reverted with undecodable revert data (len %d): %w", len(res.ReturnData), err)
		}
		if reason == "OLD" {
			return nil
		}
		return fmt.Errorf("observe() reverted with unexpected reason %q, want OLD", reason)
	}
	out, err := a.Unpack("observe", res.ReturnData)
	if err != nil {
		return fmt.Errorf("unpacking observe(): %w", err)
	}
	if len(out) != 2 {
		return fmt.Errorf("observe() returned %d values, want 2", len(out))
	}
	tickCumulatives, ok := out[0].([]*big.Int)
	if !ok {
		return fmt.Errorf("observe() tickCumulatives type = %T, want []*big.Int", out[0])
	}
	if len(tickCumulatives) != 2 {
		return fmt.Errorf("observe() tickCumulatives has %d elements, want 2", len(tickCumulatives))
	}

	delta := new(big.Int).Sub(tickCumulatives[1], tickCumulatives[0])
	tick := int(new(big.Int).Div(delta, big.NewInt(twapWindowSecs)).Int64())
	window := twapWindowSecs
	state.TwapTick = &tick
	state.TwapWindowSecs = &window
	return nil
}

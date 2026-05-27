package uniswap_v3_dex

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// blockchainService bundles the ABIs and the multicaller used by the worker.
type blockchainService struct {
	multicaller outbound.Multicaller
	poolRead    *abi.ABI
	nfpmRead    *abi.ABI
}

func newBlockchainService(mc outbound.Multicaller) (*blockchainService, error) {
	poolRead, err := abis.GetUniswapV3PoolReadABI()
	if err != nil {
		return nil, fmt.Errorf("loading pool read ABI: %w", err)
	}
	nfpmRead, err := abis.GetUniswapV3NFPMReadABI()
	if err != nil {
		return nil, fmt.Errorf("loading NFPM read ABI: %w", err)
	}
	return &blockchainService{
		multicaller: mc,
		poolRead:    poolRead,
		nfpmRead:    nfpmRead,
	}, nil
}

// readPoolState issues the event-triggered pool multicall:
//
//	slot0() + liquidity() + observe([0]) + balanceOf(token0) + balanceOf(token1).
//
// One round-trip; observe([0]) is required on every state row so TWAP queries
// can self-join two rows at chosen timestamps (plan §12.4 #12). The
// balanceOf reads target the pool's own ERC-20 balance per token; we treat the
// pool as the `account` parameter because Uniswap V3 pools hold token balances
// natively, and a missing token registry entry would surface as a multicall
// revert (AllowFailure = true).
func (b *blockchainService) readPoolState(ctx context.Context, pool *entity.UniswapV3Pool, token0Addr, token1Addr common.Address, blockNumber int64) (*poolMulticallResult, error) {
	calls, err := b.buildPoolStateCalls(pool, token0Addr, token1Addr)
	if err != nil {
		return nil, err
	}
	results, err := b.multicaller.Execute(ctx, calls, big.NewInt(blockNumber))
	if err != nil {
		return nil, fmt.Errorf("multicall pool state at block %d: %w", blockNumber, err)
	}
	if len(results) != len(calls) {
		return nil, fmt.Errorf("pool multicall returned %d results, expected %d", len(results), len(calls))
	}
	return b.decodePoolState(results)
}

func (b *blockchainService) buildPoolStateCalls(pool *entity.UniswapV3Pool, token0Addr, token1Addr common.Address) ([]outbound.Call, error) {
	slotData, err := b.poolRead.Pack("slot0")
	if err != nil {
		return nil, fmt.Errorf("packing slot0: %w", err)
	}
	liqData, err := b.poolRead.Pack("liquidity")
	if err != nil {
		return nil, fmt.Errorf("packing liquidity: %w", err)
	}
	// observe([0]) — fetches the cumulatives at the current block. uint32[].
	obsData, err := b.poolRead.Pack("observe", []uint32{0})
	if err != nil {
		return nil, fmt.Errorf("packing observe: %w", err)
	}
	bal0Data, err := b.poolRead.Pack("balanceOf", token0Addr)
	if err != nil {
		return nil, fmt.Errorf("packing balanceOf(token0): %w", err)
	}
	bal1Data, err := b.poolRead.Pack("balanceOf", token1Addr)
	if err != nil {
		return nil, fmt.Errorf("packing balanceOf(token1): %w", err)
	}
	return []outbound.Call{
		{Target: pool.Address, CallData: slotData},
		{Target: pool.Address, CallData: liqData},
		// observe can revert if cardinality not initialised — allow failure but
		// still expect callers to inspect the captured cumulatives.
		{Target: pool.Address, CallData: obsData, AllowFailure: true},
		{Target: token0Addr, CallData: bal0Data, AllowFailure: true},
		{Target: token1Addr, CallData: bal1Data, AllowFailure: true},
	}, nil
}

func (b *blockchainService) decodePoolState(results []outbound.Result) (*poolMulticallResult, error) {
	out := &poolMulticallResult{}

	unpacked, err := b.poolRead.Unpack("slot0", results[0].ReturnData)
	if err != nil {
		return nil, fmt.Errorf("decoding slot0: %w", err)
	}
	if len(unpacked) != 7 {
		return nil, fmt.Errorf("slot0 returned %d values, want 7", len(unpacked))
	}
	out.SqrtPriceX96 = unpacked[0].(*big.Int)
	out.Tick = int32(unpacked[1].(*big.Int).Int64())
	out.ObservationIndex = int32(unpacked[2].(uint16))
	out.ObservationCardinality = int32(unpacked[3].(uint16))
	out.ObservationCardinalityNext = int32(unpacked[4].(uint16))
	out.FeeProtocol = int32(unpacked[5].(uint8))
	out.Unlocked = unpacked[6].(bool)

	liq, err := unpackUint(b.poolRead, "liquidity", results[1])
	if err != nil {
		return nil, fmt.Errorf("decoding liquidity: %w", err)
	}
	out.Liquidity = liq

	if results[2].Success {
		unpackedObs, err := b.poolRead.Unpack("observe", results[2].ReturnData)
		if err != nil {
			return nil, fmt.Errorf("decoding observe: %w", err)
		}
		// observe returns (int56[] tickCumulatives, uint160[] secondsPerLiquidityCumulativeX128s)
		ticks, ok := unpackedObs[0].([]*big.Int)
		if !ok || len(ticks) == 0 {
			return nil, fmt.Errorf("observe tickCumulatives malformed")
		}
		secs, ok := unpackedObs[1].([]*big.Int)
		if !ok || len(secs) == 0 {
			return nil, fmt.Errorf("observe secsPerLiquidity malformed")
		}
		out.TickCumulative = ticks[0]
		out.SecsPerLiquidityCumulativeX128 = secs[0]
	}

	if results[3].Success {
		if v, err := unpackUint(b.poolRead, "balanceOf", results[3]); err == nil {
			out.Balance0 = v
		}
	}
	if results[4].Success {
		if v, err := unpackUint(b.poolRead, "balanceOf", results[4]); err == nil {
			out.Balance1 = v
		}
	}
	return out, nil
}

// readPoolStatic loads token0/token1/fee/tickSpacing for an unknown pool. Used
// for NFPM position discovery — `positions(tokenId)` returns token addresses
// and fee, but we also need the pool address (computed externally) and tick
// spacing. The worker uses positions() output to look up by token0/token1/fee.
func (b *blockchainService) readPoolStatic(ctx context.Context, poolAddr common.Address, blockNumber int64) (token0, token1 common.Address, fee int32, err error) {
	t0Data, err := b.poolRead.Pack("token0")
	if err != nil {
		return common.Address{}, common.Address{}, 0, fmt.Errorf("packing token0: %w", err)
	}
	t1Data, err := b.poolRead.Pack("token1")
	if err != nil {
		return common.Address{}, common.Address{}, 0, fmt.Errorf("packing token1: %w", err)
	}
	feeData, err := b.poolRead.Pack("fee")
	if err != nil {
		return common.Address{}, common.Address{}, 0, fmt.Errorf("packing fee: %w", err)
	}
	calls := []outbound.Call{
		{Target: poolAddr, CallData: t0Data, AllowFailure: true},
		{Target: poolAddr, CallData: t1Data, AllowFailure: true},
		{Target: poolAddr, CallData: feeData, AllowFailure: true},
	}
	results, err := b.multicaller.Execute(ctx, calls, big.NewInt(blockNumber))
	if err != nil {
		return common.Address{}, common.Address{}, 0, fmt.Errorf("multicall pool static at block %d: %w", blockNumber, err)
	}
	if len(results) != 3 || !results[0].Success || !results[1].Success || !results[2].Success {
		return common.Address{}, common.Address{}, 0, fmt.Errorf("pool static read reverted for %s", poolAddr.Hex())
	}
	t0u, err := b.poolRead.Unpack("token0", results[0].ReturnData)
	if err != nil {
		return common.Address{}, common.Address{}, 0, fmt.Errorf("decoding token0: %w", err)
	}
	t1u, err := b.poolRead.Unpack("token1", results[1].ReturnData)
	if err != nil {
		return common.Address{}, common.Address{}, 0, fmt.Errorf("decoding token1: %w", err)
	}
	fu, err := b.poolRead.Unpack("fee", results[2].ReturnData)
	if err != nil {
		return common.Address{}, common.Address{}, 0, fmt.Errorf("decoding fee: %w", err)
	}
	return t0u[0].(common.Address), t1u[0].(common.Address), int32(fu[0].(*big.Int).Int64()), nil
}

// readNFPMPosition reads NFPM.positions(tokenId) and returns the typed result.
// Used both for cold-path position discovery (IncreaseLiquidity on an unknown
// tokenId) and for per-event state row population.
func (b *blockchainService) readNFPMPosition(ctx context.Context, nfpm common.Address, tokenID *big.Int, blockNumber int64) (*nfpmPositionResult, error) {
	data, err := b.nfpmRead.Pack("positions", tokenID)
	if err != nil {
		return nil, fmt.Errorf("packing positions(%s): %w", tokenID, err)
	}
	results, err := b.multicaller.Execute(ctx, []outbound.Call{{Target: nfpm, CallData: data, AllowFailure: true}}, big.NewInt(blockNumber))
	if err != nil {
		return nil, fmt.Errorf("multicall positions(%s): %w", tokenID, err)
	}
	if len(results) != 1 || !results[0].Success {
		return nil, fmt.Errorf("positions(%s) reverted", tokenID)
	}
	unpacked, err := b.nfpmRead.Unpack("positions", results[0].ReturnData)
	if err != nil {
		return nil, fmt.Errorf("decoding positions(%s): %w", tokenID, err)
	}
	if len(unpacked) != 12 {
		return nil, fmt.Errorf("positions(%s) returned %d values, want 12", tokenID, len(unpacked))
	}
	return &nfpmPositionResult{
		// unpacked[0]: nonce (uint96)        -- not used
		// unpacked[1]: operator (address)    -- not used
		Token0:                   unpacked[2].(common.Address),
		Token1:                   unpacked[3].(common.Address),
		Fee:                      int32(unpacked[4].(*big.Int).Int64()),
		TickLower:                int32(unpacked[5].(*big.Int).Int64()),
		TickUpper:                int32(unpacked[6].(*big.Int).Int64()),
		Liquidity:                unpacked[7].(*big.Int),
		FeeGrowthInside0LastX128: unpacked[8].(*big.Int),
		FeeGrowthInside1LastX128: unpacked[9].(*big.Int),
		TokensOwed0:              unpacked[10].(*big.Int),
		TokensOwed1:              unpacked[11].(*big.Int),
	}, nil
}

// unpackUint decodes a single uint-returning view method's result.
func unpackUint(a *abi.ABI, method string, r outbound.Result) (*big.Int, error) {
	if !r.Success {
		return nil, fmt.Errorf("%s reverted", method)
	}
	unpacked, err := a.Unpack(method, r.ReturnData)
	if err != nil {
		return nil, err
	}
	if len(unpacked) == 0 {
		return nil, fmt.Errorf("%s returned no values", method)
	}
	switch v := unpacked[0].(type) {
	case *big.Int:
		return v, nil
	}
	return nil, fmt.Errorf("%s returned %T, want *big.Int", method, unpacked[0])
}

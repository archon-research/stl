package uniswapv3

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

const nftManagerABIJSON = `[
	{"name":"balanceOf","type":"function","inputs":[{"name":"owner","type":"address"}],"outputs":[{"name":"","type":"uint256"}]},
	{"name":"tokenOfOwnerByIndex","type":"function","inputs":[{"name":"owner","type":"address"},{"name":"index","type":"uint256"}],"outputs":[{"name":"","type":"uint256"}]},
	{"name":"positions","type":"function","inputs":[{"name":"tokenId","type":"uint256"}],"outputs":[
		{"name":"nonce","type":"uint96"},
		{"name":"operator","type":"address"},
		{"name":"token0","type":"address"},
		{"name":"token1","type":"address"},
		{"name":"fee","type":"uint24"},
		{"name":"tickLower","type":"int24"},
		{"name":"tickUpper","type":"int24"},
		{"name":"liquidity","type":"uint128"},
		{"name":"feeGrowthInside0LastX128","type":"uint256"},
		{"name":"feeGrowthInside1LastX128","type":"uint256"},
		{"name":"tokensOwed0","type":"uint128"},
		{"name":"tokensOwed1","type":"uint128"}
	]}
]`

const poolABIJSON = `[
	{"name":"slot0","type":"function","inputs":[],"outputs":[
		{"name":"sqrtPriceX96","type":"uint160"},
		{"name":"tick","type":"int24"},
		{"name":"observationIndex","type":"uint16"},
		{"name":"observationCardinality","type":"uint16"},
		{"name":"observationCardinalityNext","type":"uint16"},
		{"name":"feeProtocol","type":"uint8"},
		{"name":"unlocked","type":"bool"}
	]},
	{"name":"token0","type":"function","inputs":[],"outputs":[{"name":"","type":"address"}]},
	{"name":"token1","type":"function","inputs":[],"outputs":[{"name":"","type":"address"}]},
	{"name":"fee","type":"function","inputs":[],"outputs":[{"name":"","type":"uint24"}]}
]`

// Reader reads Uniswap V3 NFT-based positions from the chain.
//
// Flow (4 multicall rounds):
//  1. balanceOf on NonfungiblePositionManager (mainnet: 0xC36442b4a4522E871399CD717aBDD847Ab11FE88) → NFT count per wallet
//  2. tokenOfOwnerByIndex → token IDs for each wallet
//  3. positions(tokenId) → liquidity, ticks, token0/token1 per NFT
//  4. slot0() + token0() + token1() + fee() on each pool → current sqrtPriceX96 and pool identity
//
// Token amounts are computed using Uniswap V3 tick math from the position's
// liquidity and tick range relative to the pool's current price.
//
// Every sub-call is issued expecting success (AllowFailure only keeps the
// batch from reverting wholesale), so a failed or undecodable result fails
// the whole read so the caller can retry it; defaulting it to zero or
// skipping it would hand the caller a partial snapshot that looks healthy.
type Reader struct {
	multicaller   outbound.Multicaller
	nftManagerABI abi.ABI
	poolABI       abi.ABI
}

// NewReader creates a new Uniswap V3 Reader.
func NewReader(multicaller outbound.Multicaller) (*Reader, error) {
	nftABI, err := abi.JSON(strings.NewReader(nftManagerABIJSON))
	if err != nil {
		return nil, fmt.Errorf("parse nft manager abi: %w", err)
	}

	pABI, err := abi.JSON(strings.NewReader(poolABIJSON))
	if err != nil {
		return nil, fmt.Errorf("parse pool abi: %w", err)
	}

	return &Reader{
		multicaller:   multicaller,
		nftManagerABI: nftABI,
		poolABI:       pABI,
	}, nil
}

// GetPositions reads all NFT positions held by the given wallets from the
// NonfungiblePositionManager, pinned to blockHash rather than a block number:
// after a reorg an archive node answers eth_call-by-number with the new
// canonical state, which can silently disagree with the reorged (older-version)
// data this read is being made for. Returns a map of wallet → positions.
func (m *Reader) GetPositions(
	ctx context.Context,
	wallets []common.Address,
	nftManager common.Address,
	blockHash common.Hash,
) (map[common.Address][]Position, error) {
	// Round 1: Get NFT count per wallet.
	counts, err := m.fetchNFTCounts(ctx, wallets, nftManager, blockHash)
	if err != nil {
		return nil, fmt.Errorf("fetch NFT counts: %w", err)
	}

	// Round 2: Get token IDs for each wallet.
	tokenIDs, err := m.fetchTokenIDs(ctx, wallets, counts, nftManager, blockHash)
	if err != nil {
		return nil, fmt.Errorf("fetch token IDs: %w", err)
	}

	// Flatten for batch positions lookup.
	var allTokenIDs []*big.Int
	tokenIDToWallet := make(map[string]common.Address)
	for wallet, ids := range tokenIDs {
		for _, id := range ids {
			allTokenIDs = append(allTokenIDs, id)
			tokenIDToWallet[id.String()] = wallet
		}
	}

	if len(allTokenIDs) == 0 {
		return make(map[common.Address][]Position), nil
	}

	// Round 3: Get position details.
	posMap, err := m.fetchPositions(ctx, allTokenIDs, nftManager, blockHash)
	if err != nil {
		return nil, fmt.Errorf("fetch positions: %w", err)
	}

	// Group by wallet.
	result := make(map[common.Address][]Position, len(wallets))
	for idStr, pos := range posMap {
		wallet := tokenIDToWallet[idStr]
		result[wallet] = append(result[wallet], *pos)
	}

	return result, nil
}

// poolStateMethods are the per-pool reads GetPoolStates batches, in call
// order; parsePoolState decodes results by the same indices.
var poolStateMethods = []string{"slot0", "token0", "token1", "fee"}

// GetPoolStates reads slot0, token0, token1, and fee for each pool in a
// single multicall pinned to blockHash (see GetPositions for why).
func (m *Reader) GetPoolStates(
	ctx context.Context,
	pools []common.Address,
	blockHash common.Hash,
) (map[common.Address]*PoolState, error) {
	calls := make([]outbound.Call, 0, len(pools)*len(poolStateMethods))
	for _, pool := range pools {
		for _, method := range poolStateMethods {
			data, err := m.poolABI.Pack(method)
			if err != nil {
				return nil, fmt.Errorf("pack %s: %w", method, err)
			}
			calls = append(calls, outbound.Call{Target: pool, AllowFailure: true, CallData: data})
		}
	}

	results, err := m.multicaller.ExecuteAtHash(ctx, calls, blockHash)
	if err != nil {
		return nil, fmt.Errorf("multicall pool state: %w", err)
	}
	if len(results) != len(calls) {
		return nil, fmt.Errorf("pool state multicall returned %d results for %d calls", len(results), len(calls))
	}

	states := make(map[common.Address]*PoolState, len(pools))
	for i, pool := range pools {
		state, err := m.parsePoolState(pool, results[i*len(poolStateMethods):(i+1)*len(poolStateMethods)])
		if err != nil {
			return nil, err
		}
		states[pool] = state
	}

	return states, nil
}

// parsePoolState decodes one pool's slice of results, ordered as
// poolStateMethods.
func (m *Reader) parsePoolState(pool common.Address, results []outbound.Result) (*PoolState, error) {
	for j, method := range poolStateMethods {
		if !results[j].Success {
			return nil, fmt.Errorf("%s sub-call failed for pool %s", method, pool.Hex())
		}
	}

	slot0Out, err := m.poolABI.Unpack("slot0", results[0].ReturnData)
	if err != nil {
		return nil, fmt.Errorf("unpack slot0 for pool %s: %w", pool.Hex(), err)
	}
	sqrtPriceX96, okPrice := slot0Out[0].(*big.Int)
	tickBig, okTick := slot0Out[1].(*big.Int)
	if !okPrice || !okTick {
		return nil, fmt.Errorf("slot0 for pool %s returned unexpected output types", pool.Hex())
	}

	token0, err := m.unpackPoolAddress("token0", pool, results[1].ReturnData)
	if err != nil {
		return nil, err
	}
	token1, err := m.unpackPoolAddress("token1", pool, results[2].ReturnData)
	if err != nil {
		return nil, err
	}

	feeOut, err := m.poolABI.Unpack("fee", results[3].ReturnData)
	if err != nil {
		return nil, fmt.Errorf("unpack fee for pool %s: %w", pool.Hex(), err)
	}
	fee, okFee := feeOut[0].(*big.Int)
	if !okFee {
		return nil, fmt.Errorf("fee for pool %s returned %T, want *big.Int", pool.Hex(), feeOut[0])
	}

	return &PoolState{
		SqrtPriceX96: sqrtPriceX96,
		Tick:         int(tickBig.Int64()),
		Token0:       token0,
		Token1:       token1,
		Fee:          fee,
	}, nil
}

// unpackPoolAddress decodes a single-address pool getter result.
func (m *Reader) unpackPoolAddress(method string, pool common.Address, data []byte) (common.Address, error) {
	out, err := m.poolABI.Unpack(method, data)
	if err != nil {
		return common.Address{}, fmt.Errorf("unpack %s for pool %s: %w", method, pool.Hex(), err)
	}
	addr, ok := out[0].(common.Address)
	if !ok {
		return common.Address{}, fmt.Errorf("%s for pool %s returned %T, want address", method, pool.Hex(), out[0])
	}
	return addr, nil
}

// ── Multicall helpers ──

// fetchNFTCounts reads balanceOf for each wallet on the NonfungiblePositionManager.
func (m *Reader) fetchNFTCounts(
	ctx context.Context,
	wallets []common.Address,
	nftManager common.Address,
	blockHash common.Hash,
) (map[common.Address]int, error) {
	calls := make([]outbound.Call, len(wallets))
	for i, w := range wallets {
		data, err := m.nftManagerABI.Pack("balanceOf", w)
		if err != nil {
			return nil, fmt.Errorf("pack balanceOf: %w", err)
		}
		calls[i] = outbound.Call{
			Target:       nftManager,
			AllowFailure: true,
			CallData:     data,
		}
	}

	results, err := m.multicaller.ExecuteAtHash(ctx, calls, blockHash)
	if err != nil {
		return nil, fmt.Errorf("multicall balanceOf: %w", err)
	}
	if len(results) != len(calls) {
		return nil, fmt.Errorf("balanceOf multicall returned %d results for %d calls", len(results), len(calls))
	}

	counts := make(map[common.Address]int, len(wallets))
	for i, w := range wallets {
		if !results[i].Success {
			return nil, fmt.Errorf("balanceOf(%s) sub-call failed on %s", w.Hex(), nftManager.Hex())
		}

		out, err := m.nftManagerABI.Unpack("balanceOf", results[i].ReturnData)
		if err != nil {
			return nil, fmt.Errorf("unpack balanceOf for wallet %s: %w", w.Hex(), err)
		}

		count, ok := out[0].(*big.Int)
		if !ok {
			return nil, fmt.Errorf("balanceOf for wallet %s returned %T, want *big.Int", w.Hex(), out[0])
		}
		if !count.IsInt64() {
			return nil, fmt.Errorf("balanceOf for wallet %s returned %s, outside int64 range", w.Hex(), count)
		}
		counts[w] = int(count.Int64())
	}

	return counts, nil
}

// fetchTokenIDs reads tokenOfOwnerByIndex for each wallet+index pair.
func (m *Reader) fetchTokenIDs(
	ctx context.Context,
	wallets []common.Address,
	counts map[common.Address]int,
	nftManager common.Address,
	blockHash common.Hash,
) (map[common.Address][]*big.Int, error) {
	type callRef struct {
		wallet common.Address
		index  int
	}
	var refs []callRef
	var calls []outbound.Call

	for _, w := range wallets {
		count := counts[w]
		for i := range count {
			data, err := m.nftManagerABI.Pack("tokenOfOwnerByIndex", w, big.NewInt(int64(i)))
			if err != nil {
				return nil, fmt.Errorf("pack tokenOfOwnerByIndex: %w", err)
			}
			calls = append(calls, outbound.Call{
				Target:       nftManager,
				AllowFailure: true,
				CallData:     data,
			})
			refs = append(refs, callRef{wallet: w, index: i})
		}
	}

	if len(calls) == 0 {
		return make(map[common.Address][]*big.Int), nil
	}

	results, err := m.multicaller.ExecuteAtHash(ctx, calls, blockHash)
	if err != nil {
		return nil, fmt.Errorf("multicall tokenOfOwnerByIndex: %w", err)
	}
	if len(results) != len(calls) {
		return nil, fmt.Errorf("tokenOfOwnerByIndex multicall returned %d results for %d calls", len(results), len(calls))
	}

	tokenIDs := make(map[common.Address][]*big.Int)
	for i, ref := range refs {
		if !results[i].Success {
			return nil, fmt.Errorf("tokenOfOwnerByIndex(%s, %d) sub-call failed", ref.wallet.Hex(), ref.index)
		}

		out, err := m.nftManagerABI.Unpack("tokenOfOwnerByIndex", results[i].ReturnData)
		if err != nil {
			return nil, fmt.Errorf("unpack tokenOfOwnerByIndex for wallet %s index %d: %w", ref.wallet.Hex(), ref.index, err)
		}

		tokenID, ok := out[0].(*big.Int)
		if !ok {
			return nil, fmt.Errorf("tokenOfOwnerByIndex for wallet %s index %d returned %T, want *big.Int", ref.wallet.Hex(), ref.index, out[0])
		}
		tokenIDs[ref.wallet] = append(tokenIDs[ref.wallet], tokenID)
	}

	return tokenIDs, nil
}

// fetchPositions reads positions(tokenId) for each NFT token ID.
func (m *Reader) fetchPositions(
	ctx context.Context,
	tokenIDs []*big.Int,
	nftManager common.Address,
	blockHash common.Hash,
) (map[string]*Position, error) {
	calls := make([]outbound.Call, len(tokenIDs))
	for i, id := range tokenIDs {
		data, err := m.nftManagerABI.Pack("positions", id)
		if err != nil {
			return nil, fmt.Errorf("pack positions: %w", err)
		}
		calls[i] = outbound.Call{
			Target:       nftManager,
			AllowFailure: true,
			CallData:     data,
		}
	}

	results, err := m.multicaller.ExecuteAtHash(ctx, calls, blockHash)
	if err != nil {
		return nil, fmt.Errorf("multicall positions: %w", err)
	}
	if len(results) != len(calls) {
		return nil, fmt.Errorf("positions multicall returned %d results for %d calls", len(results), len(calls))
	}

	positions := make(map[string]*Position, len(tokenIDs))
	for i, id := range tokenIDs {
		if !results[i].Success {
			return nil, fmt.Errorf("positions(%s) sub-call failed", id)
		}

		out, err := m.nftManagerABI.Unpack("positions", results[i].ReturnData)
		if err != nil {
			return nil, fmt.Errorf("unpack positions for tokenId %s: %w", id, err)
		}

		// positions returns: nonce(0), operator(1), token0(2), token1(3), fee(4),
		//                    tickLower(5), tickUpper(6), liquidity(7), ...
		if len(out) < 8 {
			return nil, fmt.Errorf("positions for tokenId %s returned %d outputs, want at least 8", id, len(out))
		}

		token0, ok0 := out[2].(common.Address)
		token1, ok1 := out[3].(common.Address)
		fee, okFee := out[4].(*big.Int)
		tickLowerBig, okLower := out[5].(*big.Int)
		tickUpperBig, okUpper := out[6].(*big.Int)
		liquidity, okLiq := out[7].(*big.Int)
		if !ok0 || !ok1 || !okFee || !okLower || !okUpper || !okLiq {
			return nil, fmt.Errorf("positions for tokenId %s returned unexpected output types", id)
		}

		positions[id.String()] = &Position{
			TokenID:   id,
			Token0:    token0,
			Token1:    token1,
			Fee:       fee,
			TickLower: int(tickLowerBig.Int64()),
			TickUpper: int(tickUpperBig.Int64()),
			Liquidity: liquidity,
		}
	}

	return positions, nil
}

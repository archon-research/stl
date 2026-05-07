package uniswapv3

import (
	"context"
	"fmt"
	"log/slog"
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
	{"name":"token1","type":"function","inputs":[],"outputs":[{"name":"","type":"address"}]}
]`

// Reader reads Uniswap V3 NFT-based positions from the chain.
//
// Flow (4 multicall rounds):
//  1. balanceOf on NonfungiblePositionManager (mainnet: 0xC36442b4a4522E871399CD717aBDD847Ab11FE88) → NFT count per wallet
//  2. tokenOfOwnerByIndex → token IDs for each wallet
//  3. positions(tokenId) → liquidity, ticks, token0/token1 per NFT
//  4. slot0() + token0() + token1() on each pool → current sqrtPriceX96
//
// Token amounts are computed using Uniswap V3 tick math from the position's
// liquidity and tick range relative to the pool's current price.
type Reader struct {
	multicaller   outbound.Multicaller
	nftManagerABI abi.ABI
	poolABI       abi.ABI
	logger        *slog.Logger
}

// NewReader creates a new Uniswap V3 Reader.
func NewReader(multicaller outbound.Multicaller, logger *slog.Logger) (*Reader, error) {
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
		logger:        logger.With("component", "uniswapv3-reader"),
	}, nil
}

// GetPositions reads all NFT positions held by the given wallets from the
// NonfungiblePositionManager at the specified block. Returns a map of
// wallet → positions.
func (m *Reader) GetPositions(
	ctx context.Context,
	wallets []common.Address,
	nftManager common.Address,
	block *big.Int,
) (map[common.Address][]Position, error) {
	// Round 1: Get NFT count per wallet.
	counts, err := m.fetchNFTCounts(ctx, wallets, nftManager, block)
	if err != nil {
		return nil, fmt.Errorf("fetch NFT counts: %w", err)
	}

	// Round 2: Get token IDs for each wallet.
	tokenIDs, err := m.fetchTokenIDs(ctx, wallets, counts, nftManager, block)
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
	posMap, err := m.fetchPositions(ctx, allTokenIDs, nftManager, block)
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

// GetPoolStates reads slot0, token0, and token1 for each pool in a single
// multicall at the given block.
func (m *Reader) GetPoolStates(
	ctx context.Context,
	pools []common.Address,
	block *big.Int,
) (map[common.Address]*PoolState, error) {
	return m.fetchSlot0s(ctx, pools, block)
}

// ── Multicall helpers ──

// fetchNFTCounts reads balanceOf for each wallet on the NonfungiblePositionManager.
func (m *Reader) fetchNFTCounts(
	ctx context.Context,
	wallets []common.Address,
	nftManager common.Address,
	block *big.Int,
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

	results, err := m.multicaller.Execute(ctx, calls, block)
	if err != nil {
		return nil, fmt.Errorf("multicall balanceOf: %w", err)
	}

	counts := make(map[common.Address]int, len(wallets))
	for i, w := range wallets {
		if i >= len(results) || !results[i].Success {
			counts[w] = 0
			continue
		}

		out, err := m.nftManagerABI.Unpack("balanceOf", results[i].ReturnData)
		if err != nil {
			m.logger.Warn("unpack balanceOf failed", "wallet", w.Hex(), "error", err)
			counts[w] = 0
			continue
		}

		count, ok := out[0].(*big.Int)
		if !ok {
			counts[w] = 0
			continue
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
	block *big.Int,
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

	results, err := m.multicaller.Execute(ctx, calls, block)
	if err != nil {
		return nil, fmt.Errorf("multicall tokenOfOwnerByIndex: %w", err)
	}

	tokenIDs := make(map[common.Address][]*big.Int)
	for i, ref := range refs {
		if i >= len(results) || !results[i].Success {
			continue
		}

		out, err := m.nftManagerABI.Unpack("tokenOfOwnerByIndex", results[i].ReturnData)
		if err != nil {
			m.logger.Warn("unpack tokenOfOwnerByIndex failed", "wallet", ref.wallet.Hex(), "index", ref.index, "error", err)
			continue
		}

		tokenID, ok := out[0].(*big.Int)
		if !ok {
			continue
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
	block *big.Int,
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

	results, err := m.multicaller.Execute(ctx, calls, block)
	if err != nil {
		return nil, fmt.Errorf("multicall positions: %w", err)
	}

	positions := make(map[string]*Position, len(tokenIDs))
	for i, id := range tokenIDs {
		if i >= len(results) || !results[i].Success {
			continue
		}

		out, err := m.nftManagerABI.Unpack("positions", results[i].ReturnData)
		if err != nil {
			m.logger.Warn("unpack positions failed", "tokenId", id, "error", err)
			continue
		}

		// positions returns: nonce(0), operator(1), token0(2), token1(3), fee(4),
		//                    tickLower(5), tickUpper(6), liquidity(7), ...
		if len(out) < 8 {
			continue
		}

		token0, _ := out[2].(common.Address)
		token1, _ := out[3].(common.Address)
		fee, _ := out[4].(*big.Int)
		tickLowerBig, _ := out[5].(*big.Int)
		tickUpperBig, _ := out[6].(*big.Int)
		liquidity, _ := out[7].(*big.Int)

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

// fetchSlot0s reads slot0, token0, and token1 for each pool contract.
func (m *Reader) fetchSlot0s(
	ctx context.Context,
	pools []common.Address,
	block *big.Int,
) (map[common.Address]*PoolState, error) {
	// 3 calls per pool: slot0, token0, token1.
	calls := make([]outbound.Call, 0, len(pools)*3)
	for _, pool := range pools {
		slot0Data, err := m.poolABI.Pack("slot0")
		if err != nil {
			return nil, fmt.Errorf("pack slot0: %w", err)
		}
		token0Data, err := m.poolABI.Pack("token0")
		if err != nil {
			return nil, fmt.Errorf("pack token0: %w", err)
		}
		token1Data, err := m.poolABI.Pack("token1")
		if err != nil {
			return nil, fmt.Errorf("pack token1: %w", err)
		}

		calls = append(calls,
			outbound.Call{Target: pool, AllowFailure: true, CallData: slot0Data},
			outbound.Call{Target: pool, AllowFailure: true, CallData: token0Data},
			outbound.Call{Target: pool, AllowFailure: true, CallData: token1Data},
		)
	}

	results, err := m.multicaller.Execute(ctx, calls, block)
	if err != nil {
		return nil, fmt.Errorf("multicall slot0: %w", err)
	}

	states := make(map[common.Address]*PoolState, len(pools))
	for i, pool := range pools {
		baseIdx := i * 3

		if baseIdx+2 >= len(results) {
			continue
		}

		if !results[baseIdx].Success || !results[baseIdx+1].Success || !results[baseIdx+2].Success {
			m.logger.Warn("pool call failed", "pool", pool.Hex())
			continue
		}

		// Parse slot0.
		slot0Out, err := m.poolABI.Unpack("slot0", results[baseIdx].ReturnData)
		if err != nil {
			m.logger.Warn("unpack slot0 failed", "pool", pool.Hex(), "error", err)
			continue
		}

		sqrtPriceX96, _ := slot0Out[0].(*big.Int)
		tickBig, _ := slot0Out[1].(*big.Int)

		// Parse token0.
		token0Out, err := m.poolABI.Unpack("token0", results[baseIdx+1].ReturnData)
		if err != nil {
			continue
		}
		token0, _ := token0Out[0].(common.Address)

		// Parse token1.
		token1Out, err := m.poolABI.Unpack("token1", results[baseIdx+2].ReturnData)
		if err != nil {
			continue
		}
		token1, _ := token1Out[0].(common.Address)

		states[pool] = &PoolState{
			SqrtPriceX96: sqrtPriceX96,
			Tick:         int(tickBig.Int64()),
			Token0:       token0,
			Token1:       token1,
		}
	}

	return states, nil
}

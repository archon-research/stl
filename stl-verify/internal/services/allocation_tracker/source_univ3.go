package allocation_tracker

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

// Well-known Uniswap V3 NonfungiblePositionManager addresses per chain.
var uniV3PositionManagers = map[string]common.Address{
	"mainnet":     common.HexToAddress("0xC36442b4a4522E871399CD717aBDD847Ab11FE88"),
	"arbitrum":    common.HexToAddress("0xC36442b4a4522E871399CD717aBDD847Ab11FE88"),
	"optimism":    common.HexToAddress("0xC36442b4a4522E871399CD717aBDD847Ab11FE88"),
	"base":        common.HexToAddress("0x03a520b32C04BF3bEEf7BEb72E919cf822Ed34f1"),
	"avalanche-c": common.HexToAddress("0x655C406EBFa14EE2006250925e54ec43AD184f8B"),
	"monad":       common.HexToAddress("0xC36442b4a4522E871399CD717aBDD847Ab11FE88"), // placeholder — update when deployed
}

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

const poolSlot0ABIJSON = `[
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

// UniV3Source fetches Uniswap V3 NFT-based position balances.
// It reads positions from the NonfungiblePositionManager and computes
// underlying token amounts using the V3 tick math.
//
// Flow (4 multicall rounds):
//  1. balanceOf on NonfungiblePositionManager (mainnet: 0xC36442b4a4522E871399CD717aBDD847Ab11FE88) → NFT count per wallet
//  2. tokenOfOwnerByIndex → token IDs for each wallet
//  3. positions(tokenId) → liquidity, ticks, token0/token1 per NFT
//  4. slot0() + token0() + token1() on each pool (e.g. 0xbafead7c60ea473758ed6c6021505e8bbd7e8e5d) → current sqrtPriceX96
//
// Token amounts are computed using Uniswap V3 tick math from the position's
// liquidity and tick range relative to the pool's current price.
type UniV3Source struct {
	multicaller   outbound.Multicaller
	nftManagerABI abi.ABI
	poolABI       abi.ABI
	logger        *slog.Logger
}

// NewUniV3Source creates a new UniV3Source.
func NewUniV3Source(multicaller outbound.Multicaller, logger *slog.Logger) (*UniV3Source, error) {
	nftABI, err := abi.JSON(strings.NewReader(nftManagerABIJSON))
	if err != nil {
		return nil, fmt.Errorf("parse nft manager abi: %w", err)
	}

	poolABI, err := abi.JSON(strings.NewReader(poolSlot0ABIJSON))
	if err != nil {
		return nil, fmt.Errorf("parse pool abi: %w", err)
	}

	return &UniV3Source{
		multicaller:   multicaller,
		nftManagerABI: nftABI,
		poolABI:       poolABI,
		logger:        logger.With("component", "univ3-source"),
	}, nil
}

func (s *UniV3Source) Name() string { return "uni-v3" }

func (s *UniV3Source) Supports(tokenType, protocol string) bool {
	return tokenType == "uni_v3_pool" || tokenType == "uni_v3_lp"
}

func (s *UniV3Source) FetchBalances(
	ctx context.Context,
	entries []*TokenEntry,
	blockNumber int64,
) (map[EntryKey]*PositionBalance, error) {
	if len(entries) == 0 {
		return make(map[EntryKey]*PositionBalance), nil
	}

	block := big.NewInt(blockNumber)
	result := make(map[EntryKey]*PositionBalance, len(entries))

	// Group entries by chain to use the correct NonfungiblePositionManager.
	byChain := make(map[string][]*TokenEntry)
	for _, e := range entries {
		byChain[e.Chain] = append(byChain[e.Chain], e)
	}

	for chain, chainEntries := range byChain {
		nftManager, ok := uniV3PositionManagers[chain]
		if !ok {
			s.logger.Warn("no NonfungiblePositionManager address for chain", "chain", chain)
			continue
		}

		if err := s.fetchChainBalances(ctx, chainEntries, nftManager, block, result); err != nil {
			return nil, fmt.Errorf("fetch V3 balances for chain %s: %w", chain, err)
		}
	}

	return result, nil
}

func (s *UniV3Source) fetchChainBalances(
	ctx context.Context,
	entries []*TokenEntry,
	nftManager common.Address,
	block *big.Int,
	result map[EntryKey]*PositionBalance,
) error {
	// Deduplicate wallets — multiple entries may share the same proxy.
	walletSet := make(map[common.Address]bool)
	for _, e := range entries {
		walletSet[e.WalletAddress] = true
	}

	wallets := make([]common.Address, 0, len(walletSet))
	for w := range walletSet {
		wallets = append(wallets, w)
	}

	// ── Round 1: Get NFT count per wallet ──
	nftCounts, err := s.fetchNFTCounts(ctx, wallets, nftManager, block)
	if err != nil {
		return fmt.Errorf("fetch NFT counts: %w", err)
	}

	// ── Round 2: Get token IDs for each wallet ──
	tokenIDs, err := s.fetchTokenIDs(ctx, wallets, nftCounts, nftManager, block)
	if err != nil {
		return fmt.Errorf("fetch token IDs: %w", err)
	}

	// Flatten all token IDs for positions lookup.
	var allTokenIDs []*big.Int
	tokenIDToWallet := make(map[string]common.Address)
	for wallet, ids := range tokenIDs {
		for _, id := range ids {
			allTokenIDs = append(allTokenIDs, id)
			tokenIDToWallet[id.String()] = wallet
		}
	}

	if len(allTokenIDs) == 0 {
		return nil
	}

	// ── Round 3: Get positions + pool slot0 ──
	positions, err := s.fetchPositions(ctx, allTokenIDs, nftManager, block)
	if err != nil {
		return fmt.Errorf("fetch positions: %w", err)
	}

	// Collect unique pools we need slot0 from.
	type poolKey struct {
		token0 common.Address
		token1 common.Address
		fee    *big.Int
	}
	poolAddresses := make(map[common.Address]bool)
	for _, e := range entries {
		poolAddresses[e.ContractAddress] = true
	}

	pools := make([]common.Address, 0, len(poolAddresses))
	for p := range poolAddresses {
		pools = append(pools, p)
	}

	slot0s, err := s.fetchSlot0s(ctx, pools, block)
	if err != nil {
		return fmt.Errorf("fetch slot0s: %w", err)
	}

	// ── Compute balances ──
	// For each entry (pool + wallet pair), find matching NFT positions,
	// compute amounts, and sum them.
	for _, entry := range entries {
		wallet := entry.WalletAddress
		ids, ok := tokenIDs[wallet]
		if !ok || len(ids) == 0 {
			continue
		}

		slot0, ok := slot0s[entry.ContractAddress]
		if !ok {
			s.logger.Warn("missing slot0 for pool", "pool", entry.ContractAddress.Hex())
			continue
		}

		// Get pool's token0 and token1 for matching.
		poolToken0 := slot0.token0
		poolToken1 := slot0.token1

		var totalAmount0, totalAmount1 big.Int
		matched := false

		for _, id := range ids {
			pos, ok := positions[id.String()]
			if !ok {
				continue
			}

			// Match position to this pool by token0/token1 pair.
			if pos.token0 != poolToken0 || pos.token1 != poolToken1 {
				continue
			}

			if pos.liquidity.Sign() == 0 {
				continue
			}

			matched = true
			amount0, amount1 := computePositionAmounts(
				slot0.sqrtPriceX96,
				pos.tickLower,
				pos.tickUpper,
				pos.liquidity,
			)

			totalAmount0.Add(&totalAmount0, amount0)
			totalAmount1.Add(&totalAmount1, amount1)

			s.logger.Debug("computed V3 position amounts",
				"tokenId", id,
				"wallet", wallet.Hex(),
				"pool", entry.ContractAddress.Hex(),
				"liquidity", pos.liquidity,
				"amount0", amount0,
				"amount1", amount1,
			)
		}

		if !matched {
			continue
		}

		// Determine balance in terms of the asset address.
		// If asset matches token0, use amount0. If token1, use amount1.
		// If no asset specified or neither matches, sum both (valid for stablecoin pairs).
		var balance *big.Int
		if entry.AssetAddress != nil {
			if *entry.AssetAddress == poolToken0 {
				balance = new(big.Int).Set(&totalAmount0)
			} else if *entry.AssetAddress == poolToken1 {
				balance = new(big.Int).Set(&totalAmount1)
			} else {
				// Asset doesn't match either token — sum both as approximation.
				balance = new(big.Int).Add(&totalAmount0, &totalAmount1)
			}
		} else {
			balance = new(big.Int).Add(&totalAmount0, &totalAmount1)
		}

		// ScaledBalance holds the total of both tokens (full position value).
		scaled := new(big.Int).Add(new(big.Int).Set(&totalAmount0), new(big.Int).Set(&totalAmount1))

		result[entry.Key()] = &PositionBalance{
			Balance:       balance,
			ScaledBalance: scaled,
		}
	}

	return nil
}

// ── Multicall helpers ──

func (s *UniV3Source) fetchNFTCounts(
	ctx context.Context,
	wallets []common.Address,
	nftManager common.Address,
	block *big.Int,
) (map[common.Address]int, error) {
	calls := make([]outbound.Call, len(wallets))
	for i, w := range wallets {
		data, err := s.nftManagerABI.Pack("balanceOf", w)
		if err != nil {
			return nil, fmt.Errorf("pack balanceOf: %w", err)
		}
		calls[i] = outbound.Call{
			Target:       nftManager,
			AllowFailure: true,
			CallData:     data,
		}
	}

	results, err := s.multicaller.Execute(ctx, calls, block)
	if err != nil {
		return nil, fmt.Errorf("multicall balanceOf: %w", err)
	}

	counts := make(map[common.Address]int, len(wallets))
	for i, w := range wallets {
		if i >= len(results) || !results[i].Success {
			counts[w] = 0
			continue
		}

		out, err := s.nftManagerABI.Unpack("balanceOf", results[i].ReturnData)
		if err != nil {
			s.logger.Warn("unpack balanceOf failed", "wallet", w.Hex(), "error", err)
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

func (s *UniV3Source) fetchTokenIDs(
	ctx context.Context,
	wallets []common.Address,
	counts map[common.Address]int,
	nftManager common.Address,
	block *big.Int,
) (map[common.Address][]*big.Int, error) {
	// Build calls for tokenOfOwnerByIndex.
	type callRef struct {
		wallet common.Address
		index  int
	}
	var refs []callRef
	var calls []outbound.Call

	for _, w := range wallets {
		count := counts[w]
		for i := range count {
			data, err := s.nftManagerABI.Pack("tokenOfOwnerByIndex", w, big.NewInt(int64(i)))
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

	results, err := s.multicaller.Execute(ctx, calls, block)
	if err != nil {
		return nil, fmt.Errorf("multicall tokenOfOwnerByIndex: %w", err)
	}

	tokenIDs := make(map[common.Address][]*big.Int)
	for i, ref := range refs {
		if i >= len(results) || !results[i].Success {
			continue
		}

		out, err := s.nftManagerABI.Unpack("tokenOfOwnerByIndex", results[i].ReturnData)
		if err != nil {
			s.logger.Warn("unpack tokenOfOwnerByIndex failed", "wallet", ref.wallet.Hex(), "index", ref.index, "error", err)
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

type v3Position struct {
	token0    common.Address
	token1    common.Address
	fee       *big.Int
	tickLower int
	tickUpper int
	liquidity *big.Int
}

func (s *UniV3Source) fetchPositions(
	ctx context.Context,
	tokenIDs []*big.Int,
	nftManager common.Address,
	block *big.Int,
) (map[string]*v3Position, error) {
	calls := make([]outbound.Call, len(tokenIDs))
	for i, id := range tokenIDs {
		data, err := s.nftManagerABI.Pack("positions", id)
		if err != nil {
			return nil, fmt.Errorf("pack positions: %w", err)
		}
		calls[i] = outbound.Call{
			Target:       nftManager,
			AllowFailure: true,
			CallData:     data,
		}
	}

	results, err := s.multicaller.Execute(ctx, calls, block)
	if err != nil {
		return nil, fmt.Errorf("multicall positions: %w", err)
	}

	positions := make(map[string]*v3Position, len(tokenIDs))
	for i, id := range tokenIDs {
		if i >= len(results) || !results[i].Success {
			continue
		}

		out, err := s.nftManagerABI.Unpack("positions", results[i].ReturnData)
		if err != nil {
			s.logger.Warn("unpack positions failed", "tokenId", id, "error", err)
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

		positions[id.String()] = &v3Position{
			token0:    token0,
			token1:    token1,
			fee:       fee,
			tickLower: int(tickLowerBig.Int64()),
			tickUpper: int(tickUpperBig.Int64()),
			liquidity: liquidity,
		}
	}

	return positions, nil
}

type poolSlot0 struct {
	sqrtPriceX96 *big.Int
	tick         int
	token0       common.Address
	token1       common.Address
}

func (s *UniV3Source) fetchSlot0s(
	ctx context.Context,
	pools []common.Address,
	block *big.Int,
) (map[common.Address]*poolSlot0, error) {
	// 3 calls per pool: slot0, token0, token1.
	calls := make([]outbound.Call, 0, len(pools)*3)
	for _, pool := range pools {
		slot0Data, err := s.poolABI.Pack("slot0")
		if err != nil {
			return nil, fmt.Errorf("pack slot0: %w", err)
		}
		token0Data, err := s.poolABI.Pack("token0")
		if err != nil {
			return nil, fmt.Errorf("pack token0: %w", err)
		}
		token1Data, err := s.poolABI.Pack("token1")
		if err != nil {
			return nil, fmt.Errorf("pack token1: %w", err)
		}

		calls = append(calls,
			outbound.Call{Target: pool, AllowFailure: true, CallData: slot0Data},
			outbound.Call{Target: pool, AllowFailure: true, CallData: token0Data},
			outbound.Call{Target: pool, AllowFailure: true, CallData: token1Data},
		)
	}

	results, err := s.multicaller.Execute(ctx, calls, block)
	if err != nil {
		return nil, fmt.Errorf("multicall slot0: %w", err)
	}

	slot0s := make(map[common.Address]*poolSlot0, len(pools))
	for i, pool := range pools {
		baseIdx := i * 3

		if baseIdx+2 >= len(results) {
			continue
		}

		if !results[baseIdx].Success || !results[baseIdx+1].Success || !results[baseIdx+2].Success {
			s.logger.Warn("pool call failed", "pool", pool.Hex())
			continue
		}

		// Parse slot0.
		slot0Out, err := s.poolABI.Unpack("slot0", results[baseIdx].ReturnData)
		if err != nil {
			s.logger.Warn("unpack slot0 failed", "pool", pool.Hex(), "error", err)
			continue
		}

		sqrtPriceX96, _ := slot0Out[0].(*big.Int)
		tickBig, _ := slot0Out[1].(*big.Int)

		// Parse token0.
		token0Out, err := s.poolABI.Unpack("token0", results[baseIdx+1].ReturnData)
		if err != nil {
			continue
		}
		token0, _ := token0Out[0].(common.Address)

		// Parse token1.
		token1Out, err := s.poolABI.Unpack("token1", results[baseIdx+2].ReturnData)
		if err != nil {
			continue
		}
		token1, _ := token1Out[0].(common.Address)

		slot0s[pool] = &poolSlot0{
			sqrtPriceX96: sqrtPriceX96,
			tick:         int(tickBig.Int64()),
			token0:       token0,
			token1:       token1,
		}
	}

	return slot0s, nil
}

package allocation_tracker

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/pkg/uniswapv3"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// UniV3Source fetches Uniswap V3 NFT-based position balances by delegating
// to the reusable uniswapv3.Reader for on-chain reads and tick math.
type UniV3Source struct {
	reader *uniswapv3.Reader
	logger *slog.Logger
}

// NewUniV3Source creates a new UniV3Source backed by a uniswapv3.Reader.
func NewUniV3Source(multicaller outbound.Multicaller, logger *slog.Logger) (*UniV3Source, error) {
	mgr, err := uniswapv3.NewReader(multicaller, logger)
	if err != nil {
		return nil, fmt.Errorf("create uniswapv3 reader: %w", err)
	}

	return &UniV3Source{
		reader: mgr,
		logger: logger.With("component", "univ3-source"),
	}, nil
}

// Name returns the source name.
func (s *UniV3Source) Name() string { return "uni-v3" }

// Supports returns true for uni_v3_pool and uni_v3_lp token types.
func (s *UniV3Source) Supports(tokenType, protocol string) bool {
	return tokenType == "uni_v3_pool" || tokenType == "uni_v3_lp"
}

// FetchBalances reads Uniswap V3 NFT positions for all entries and values
// each position fully (both sides) in the entry's hint asset.
func (s *UniV3Source) FetchBalances(
	ctx context.Context,
	entries []*TokenEntry,
	blockHash common.Hash,
) (*FetchResult, error) {
	result := NewFetchResult()
	if len(entries) == 0 {
		return result, nil
	}

	if err := validateUniV3Entries(entries); err != nil {
		return nil, err
	}

	// Group entries by chain to use the correct NonfungiblePositionManager.
	byChain := make(map[string][]*TokenEntry)
	for _, e := range entries {
		byChain[e.Chain] = append(byChain[e.Chain], e)
	}

	for chain, chainEntries := range byChain {
		nftManager, ok := uniswapv3.PositionManagers[chain]
		if !ok {
			// Config error of the same class as a missing hint asset: skipping
			// would silently drop the chain's positions on every block.
			return nil, fmt.Errorf(
				"no NonfungiblePositionManager registered for chain %s (%d uni_v3 entries)",
				chain, len(chainEntries),
			)
		}

		if err := s.fetchChainBalances(ctx, chainEntries, nftManager, blockHash, result.Balances); err != nil {
			return nil, fmt.Errorf("fetch V3 balances for chain %s: %w", chain, err)
		}
	}

	return result, nil
}

// validateUniV3Entries rejects entries with no hint asset up front: without
// one the position value has no denomination, and adding raw amount0 and
// amount1 across different tokens would be meaningless. A misconfigured entry
// fails the fetch immediately rather than only once a position appears.
func validateUniV3Entries(entries []*TokenEntry) error {
	for _, e := range entries {
		if e.AssetAddress == nil {
			return fmt.Errorf(
				"uni_v3 entry %s/%s has no asset address to denominate the position value in",
				e.ContractAddress.Hex(), e.WalletAddress.Hex(),
			)
		}
	}
	return nil
}

// fetchChainBalances handles all entries for a single chain.
func (s *UniV3Source) fetchChainBalances(
	ctx context.Context,
	entries []*TokenEntry,
	nftManager common.Address,
	blockHash common.Hash,
	result map[EntryKey]*PositionBalance,
) error {
	// Get all NFT positions for these wallets via the reader.
	walletPositions, err := s.reader.GetPositions(ctx, uniqueWallets(entries), nftManager, blockHash)
	if err != nil {
		return fmt.Errorf("get positions: %w", err)
	}

	poolStates, err := s.reader.GetPoolStates(ctx, uniquePools(entries), blockHash)
	if err != nil {
		return fmt.Errorf("get pool states: %w", err)
	}

	for _, entry := range entries {
		balance, ok, err := s.computeEntryBalance(entry, walletPositions, poolStates)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		result[entry.Key()] = balance
	}

	return nil
}

// uniqueWallets deduplicates entry wallets — multiple entries may share the
// same proxy.
func uniqueWallets(entries []*TokenEntry) []common.Address {
	seen := make(map[common.Address]bool, len(entries))
	wallets := make([]common.Address, 0, len(entries))
	for _, e := range entries {
		if !seen[e.WalletAddress] {
			seen[e.WalletAddress] = true
			wallets = append(wallets, e.WalletAddress)
		}
	}
	return wallets
}

// uniquePools deduplicates entry pool contracts.
func uniquePools(entries []*TokenEntry) []common.Address {
	seen := make(map[common.Address]bool, len(entries))
	pools := make([]common.Address, 0, len(entries))
	for _, e := range entries {
		if !seen[e.ContractAddress] {
			seen[e.ContractAddress] = true
			pools = append(pools, e.ContractAddress)
		}
	}
	return pools
}

// computeEntryBalance values one entry's matched V3 positions fully in the
// entry's hint asset. ok=false means the wallet holds no live position in the
// pool (closed or never opened): a structural absence, not an error.
func (s *UniV3Source) computeEntryBalance(
	entry *TokenEntry,
	walletPositions map[common.Address][]uniswapv3.Position,
	poolStates map[common.Address]*uniswapv3.PoolState,
) (*PositionBalance, bool, error) {
	positions, ok := walletPositions[entry.WalletAddress]
	if !ok || len(positions) == 0 {
		return nil, false, nil
	}

	// The slot0/token0/token1 read was issued for this pool, so an absent
	// state is a failed AllowFailure sub-call (or a non-pool address), never a
	// structural absence. Skipping would freeze the position at its previous
	// row; failing lets SQS redeliver the block.
	state, ok := poolStates[entry.ContractAddress]
	if !ok {
		return nil, false, fmt.Errorf(
			"pool state missing for %s (issued slot0/token0/token1 read failed) while wallet %s holds positions",
			entry.ContractAddress.Hex(), entry.WalletAddress.Hex(),
		)
	}

	total, matched := s.sumMatchedPositionAmounts(entry, positions, state)
	if !matched {
		return nil, false, nil
	}

	value, err := valueInHintAsset(entry, state, total)
	if err != nil {
		return nil, false, err
	}

	token0, token1 := state.Token0, state.Token1
	return &PositionBalance{
		Balance: value,
		// ScaledBalance stays nil: a V3 position has no share count in the
		// hint asset's decimals, and a raw amount0+amount1 sum mixes units.
		UnderlyingValue: new(big.Int).Set(value),
		PoolToken0:      &token0,
		PoolToken1:      &token1,
	}, true, nil
}

// sumMatchedPositionAmounts totals (amount0, amount1) over the wallet's live
// positions in the entry's pool. matched=false when none has liquidity.
func (s *UniV3Source) sumMatchedPositionAmounts(
	entry *TokenEntry,
	positions []uniswapv3.Position,
	state *uniswapv3.PoolState,
) (uniswapv3.PositionAmounts, bool) {
	total := uniswapv3.PositionAmounts{Amount0: new(big.Int), Amount1: new(big.Int)}
	matched := false

	for _, pos := range positions {
		// Match position to this pool by token0/token1 pair.
		if pos.Token0 != state.Token0 || pos.Token1 != state.Token1 {
			continue
		}

		if pos.Liquidity.Sign() == 0 {
			continue
		}

		matched = true
		amounts := uniswapv3.ComputePositionAmounts(
			state.SqrtPriceX96,
			pos.TickLower,
			pos.TickUpper,
			pos.Liquidity,
		)

		total.Amount0.Add(total.Amount0, amounts.Amount0)
		total.Amount1.Add(total.Amount1, amounts.Amount1)

		s.logger.Debug("computed V3 position amounts",
			"tokenId", pos.TokenID,
			"wallet", entry.WalletAddress.Hex(),
			"pool", entry.ContractAddress.Hex(),
			"liquidity", pos.Liquidity,
			"amount0", amounts.Amount0,
			"amount1", amounts.Amount1,
		)
	}

	return total, matched
}

// valueInHintAsset converts the summed position amounts into the entry's hint
// asset at the pool's own spot price (the sqrtPriceX96 read in the same
// hash-pinned multicall that produced the amounts).
//
// Pool spot, not an oracle price, is deliberate: the ingest layer records
// what happened on-chain and has no price-table access; the USD conversion
// happens in the API as underlying_value x the hint asset's oracle price. The
// position amounts themselves are already derived from the same sqrtPriceX96,
// so valuing with it adds no new trust assumption.
//
// The value covers principal (liquidity) only: uncollected fees are excluded
// because tokensOwed0/1 only reflect fees at the last poke, and computing
// live accruals needs feeGrowthInside state the reader does not fetch.
func valueInHintAsset(
	entry *TokenEntry,
	state *uniswapv3.PoolState,
	total uniswapv3.PositionAmounts,
) (*big.Int, error) {
	var value *big.Int
	var err error
	switch *entry.AssetAddress {
	case state.Token0:
		value, err = total.ValueInToken0(state.SqrtPriceX96)
	case state.Token1:
		value, err = total.ValueInToken1(state.SqrtPriceX96)
	default:
		return nil, fmt.Errorf(
			"uni_v3 hint asset %s matches neither side of pool %s (token0=%s, token1=%s); cannot denominate the position value",
			entry.AssetAddress.Hex(), entry.ContractAddress.Hex(),
			state.Token0.Hex(), state.Token1.Hex(),
		)
	}
	if err != nil {
		return nil, fmt.Errorf(
			"value uni_v3 position %s/%s in hint asset %s: %w",
			entry.ContractAddress.Hex(), entry.WalletAddress.Hex(), entry.AssetAddress.Hex(), err,
		)
	}
	return value, nil
}

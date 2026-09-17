package allocation_tracker

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/uniswapv3"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// UniV4Source values posm-managed Uniswap V4 LP positions from the
// already-indexed Postgres state (uniswap_v4_position, uniswap_v4_pool_state,
// uniswap_v4_position_nft_transfer) rather than a live chain read: V4 pools
// share one singleton PoolManager with no per-pool contract to eth_call, and
// the dex-indexer has already turned every touch into an append-on-change row
// keyed by block height — exactly the "value as of a point" read this source
// needs (VEC-829). The concentrated-liquidity math is shared with V3 via
// uniswapv3.ComputePositionAmounts: V4 positions carry the same
// (tickLower, tickUpper, liquidity) shape, just sourced from Postgres instead
// of an on-chain positions() call.
//
// A posm token id names no pool, so one entry aggregates every position the
// wallet holds across every pool on the chain into a single value in the
// entry's hint asset — unlike UniV3Source, which has one entry per pool
// contract. PoolToken0/PoolToken1 are left unset on the returned balance for
// the same reason: a single pair cannot name a position that may span pools
// with different pairs (see handler_prime_positions.go's univ4RowMeta).
type UniV4Source struct {
	reader     outbound.UniswapV4PositionValuationReader
	blockState outbound.BlockHashResolver
	logger     *slog.Logger
}

// NewUniV4Source creates a new UniV4Source backed by Postgres reads.
func NewUniV4Source(
	reader outbound.UniswapV4PositionValuationReader,
	blockState outbound.BlockHashResolver,
	logger *slog.Logger,
) *UniV4Source {
	return &UniV4Source{
		reader:     reader,
		blockState: blockState,
		logger:     logger.With("component", "univ4-source"),
	}
}

// Name returns the source name.
func (s *UniV4Source) Name() string { return "uni-v4" }

// Supports returns true for uni_v4_pool and uni_v4_lp token types.
func (s *UniV4Source) Supports(tokenType, protocol string) bool {
	return tokenType == "uni_v4_pool" || tokenType == "uni_v4_lp"
}

// FetchBalances values every entry's posm-managed V4 LP positions as of
// blockHash. The reads are all against Postgres, keyed by block number, so
// blockHash is resolved once against block_states up front rather than
// threaded through every query — see resolveBlockNumber for why that lookup
// is always safe here.
func (s *UniV4Source) FetchBalances(
	ctx context.Context,
	entries []*TokenEntry,
	blockHash common.Hash,
) (*FetchResult, error) {
	result := NewFetchResult()
	if len(entries) == 0 {
		return result, nil
	}

	if err := validateUniV4Entries(entries); err != nil {
		return nil, err
	}

	blockNumber, err := s.resolveBlockNumber(ctx, blockHash)
	if err != nil {
		return nil, err
	}

	byChain := make(map[string][]*TokenEntry)
	for _, e := range entries {
		byChain[e.Chain] = append(byChain[e.Chain], e)
	}

	for chain, chainEntries := range byChain {
		if err := s.fetchChainBalances(ctx, chain, chainEntries, blockNumber, result.Balances); err != nil {
			return nil, fmt.Errorf("fetch V4 balances for chain %s: %w", chain, err)
		}
	}

	return result, nil
}

// validateUniV4Entries rejects entries with no hint asset up front, mirroring
// UniV3Source: without one, a position's value has no denomination.
func validateUniV4Entries(entries []*TokenEntry) error {
	for _, e := range entries {
		if e.AssetAddress == nil {
			return fmt.Errorf(
				"uni_v4 entry %s/%s has no asset address to denominate the position value in",
				e.ContractAddress.Hex(), e.WalletAddress.Hex(),
			)
		}
	}
	return nil
}

// resolveBlockNumber looks blockHash up in block_states. The allocation
// tracker only ever calls a source with the block it is currently processing
// (a just-consumed live event or a sweep of the chain's current head), never
// an arbitrarily old replay, so the hash is always well within the 30-day
// window block_states retains (see docs/runbooks/vector-indexers.md).
func (s *UniV4Source) resolveBlockNumber(ctx context.Context, blockHash common.Hash) (int64, error) {
	state, err := s.blockState.GetBlockByHash(ctx, blockHash.Hex())
	if err != nil {
		return 0, fmt.Errorf("resolve block number for hash %s: %w", blockHash.Hex(), err)
	}
	if state == nil {
		return 0, fmt.Errorf(
			"block hash %s not found in block_states; cannot resolve a block number for the uni-v4 read",
			blockHash.Hex(),
		)
	}
	return state.Number, nil
}

// fetchChainBalances handles every entry for a single chain: it resolves the
// chain's pool registry once, then values each entry's aggregate V4 exposure
// against it. A chain with no registered pools has no posm position to find
// either (a pool is what a position lives in), so every entry gets an
// explicit zero without a wasted PositionManager lookup.
func (s *UniV4Source) fetchChainBalances(
	ctx context.Context,
	chain string,
	entries []*TokenEntry,
	blockNumber int64,
	result map[EntryKey]*PositionBalance,
) error {
	chainID, ok := chainIDForName(chain)
	if !ok {
		return fmt.Errorf("no chain ID registered for chain %q (%d uni_v4 entries)", chain, len(entries))
	}

	pools, err := s.reader.LoadPools(ctx, chainID)
	if err != nil {
		return fmt.Errorf("load pools: %w", err)
	}
	poolsByID := make(map[int64]outbound.UniswapV4PoolRow, len(pools))
	for _, p := range pools {
		poolsByID[p.ID] = p
	}

	// PositionManager is chain-level (every pool row on a chain carries the
	// same one), so any pool names it.
	var positionManager common.Address
	if len(pools) > 0 {
		positionManager = pools[0].PositionManager
	}

	for _, entry := range entries {
		value := new(big.Int)
		if len(pools) > 0 {
			value, err = s.entryValue(ctx, chainID, positionManager, entry, blockNumber, poolsByID)
			if err != nil {
				return err
			}
		}
		result[entry.Key()] = &PositionBalance{
			Balance:         value,
			UnderlyingValue: new(big.Int).Set(value),
		}
	}

	return nil
}

// entryValue sums a wallet's held positions in the entry's hint asset. No held
// tokens is a legitimate zero (never opened, or fully exited), like the
// empty-match case in UniV3Source: the API's latest-row read has no freshness
// cutoff, so skipping the entry would freeze the last positive row forever.
func (s *UniV4Source) entryValue(
	ctx context.Context,
	chainID int64,
	positionManager common.Address,
	entry *TokenEntry,
	blockNumber int64,
	poolsByID map[int64]outbound.UniswapV4PoolRow,
) (*big.Int, error) {
	tokenIDs, err := s.reader.HeldTokenIDsAtBlock(ctx, chainID, entry.WalletAddress, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("held token ids for wallet %s: %w", entry.WalletAddress.Hex(), err)
	}

	total := new(big.Int)
	for _, tokenID := range tokenIDs {
		value, err := s.positionValue(ctx, chainID, positionManager, entry, tokenID, blockNumber, poolsByID)
		if err != nil {
			return nil, err
		}
		total.Add(total, value)
	}
	return total, nil
}

// positionValue values one posm token: its pool, tick range and liquidity
// come from uniswap_v4_position, its price from uniswap_v4_pool_state. Both
// are invariant breaks if missing — the NFT transfer log already says wallet
// holds this token at blockNumber, so an absent position or pool state means
// the indexer has a coverage gap, not that the position never existed.
func (s *UniV4Source) positionValue(
	ctx context.Context,
	chainID int64,
	positionManager common.Address,
	entry *TokenEntry,
	tokenID *big.Int,
	blockNumber int64,
	poolsByID map[int64]outbound.UniswapV4PoolRow,
) (*big.Int, error) {
	position, err := s.reader.PositionForTokenAtBlock(ctx, chainID, positionManager, tokenID, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("posm position for token %s: %w", tokenID, err)
	}
	if position == nil {
		return nil, fmt.Errorf(
			"posm token %s held by %s has no indexed position at or before block %d",
			tokenID, entry.WalletAddress.Hex(), blockNumber,
		)
	}
	if position.Liquidity.Sign() == 0 {
		return new(big.Int), nil
	}

	pool, ok := poolsByID[position.PoolID]
	if !ok {
		return nil, fmt.Errorf(
			"posm token %s resolves to pool %d, absent from chain %d's current pool registry",
			tokenID, position.PoolID, chainID,
		)
	}

	sqrtPriceX96, err := s.reader.PoolStateAtBlock(ctx, position.PoolID, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("pool state for pool %d: %w", position.PoolID, err)
	}
	if sqrtPriceX96 == nil {
		return nil, fmt.Errorf(
			"pool %d (posm token %s) has no state snapshot at or before block %d",
			position.PoolID, tokenID, blockNumber,
		)
	}

	amounts := uniswapv3.ComputePositionAmounts(sqrtPriceX96, position.TickLower, position.TickUpper, position.Liquidity)
	value, err := valueInV4HintAsset(entry, pool, sqrtPriceX96, amounts)
	if err != nil {
		return nil, fmt.Errorf("value posm token %s: %w", tokenID, err)
	}

	s.logger.Debug("computed V4 position amounts",
		"tokenId", tokenID,
		"wallet", entry.WalletAddress.Hex(),
		"pool", position.PoolID,
		"liquidity", position.Liquidity,
		"amount0", amounts.Amount0,
		"amount1", amounts.Amount1,
	)
	return value, nil
}

// valueInV4HintAsset converts a position's amounts into the entry's hint
// asset at the pool's own spot price, exactly as UniV3Source's
// valueInHintAsset does for V3 (see its doc comment for why pool spot, not an
// oracle price, is the right source here too).
func valueInV4HintAsset(
	entry *TokenEntry,
	pool outbound.UniswapV4PoolRow,
	sqrtPriceX96 *big.Int,
	amounts uniswapv3.PositionAmounts,
) (*big.Int, error) {
	switch *entry.AssetAddress {
	case pool.Currency0:
		return amounts.ValueInToken0(sqrtPriceX96)
	case pool.Currency1:
		return amounts.ValueInToken1(sqrtPriceX96)
	default:
		return nil, fmt.Errorf(
			"hint asset %s matches neither side of pool %d (currency0=%s, currency1=%s); cannot denominate the position value",
			entry.AssetAddress.Hex(), pool.ID, pool.Currency0.Hex(), pool.Currency1.Hex(),
		)
	}
}

// chainIDForName reverse-looks-up entity.ChainIDToName; small enough (7
// entries) that a linear scan beats maintaining a second map in lockstep.
func chainIDForName(chain string) (int64, bool) {
	for id, name := range entity.ChainIDToName {
		if name == chain {
			return id, true
		}
	}
	return 0, false
}

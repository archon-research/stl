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

// FetchBalances reads Uniswap V3 NFT positions for all entries and computes
// underlying token amounts.
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
		nftManager, ok := uniswapv3.PositionManagers[chain]
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

// fetchChainBalances handles all entries for a single chain.
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

	// Get all NFT positions for these wallets via the reader.
	walletPositions, err := s.reader.GetPositions(ctx, wallets, nftManager, block)
	if err != nil {
		return fmt.Errorf("get positions: %w", err)
	}

	// Collect unique pools and get their current state.
	poolSet := make(map[common.Address]bool)
	for _, e := range entries {
		poolSet[e.ContractAddress] = true
	}

	pools := make([]common.Address, 0, len(poolSet))
	for p := range poolSet {
		pools = append(pools, p)
	}

	poolStates, err := s.reader.GetPoolStates(ctx, pools, block)
	if err != nil {
		return fmt.Errorf("get pool states: %w", err)
	}

	// Compute balances for each entry.
	for _, entry := range entries {
		positions, ok := walletPositions[entry.WalletAddress]
		if !ok || len(positions) == 0 {
			continue
		}

		state, ok := poolStates[entry.ContractAddress]
		if !ok {
			s.logger.Warn("missing pool state", "pool", entry.ContractAddress.Hex())
			continue
		}

		var totalAmount0, totalAmount1 big.Int
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

			totalAmount0.Add(&totalAmount0, amounts.Amount0)
			totalAmount1.Add(&totalAmount1, amounts.Amount1)

			s.logger.Debug("computed V3 position amounts",
				"tokenId", pos.TokenID,
				"wallet", entry.WalletAddress.Hex(),
				"pool", entry.ContractAddress.Hex(),
				"liquidity", pos.Liquidity,
				"amount0", amounts.Amount0,
				"amount1", amounts.Amount1,
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
			if *entry.AssetAddress == state.Token0 {
				balance = new(big.Int).Set(&totalAmount0)
			} else if *entry.AssetAddress == state.Token1 {
				balance = new(big.Int).Set(&totalAmount1)
			} else {
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

package outbound

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// UniswapV4PositionSnapshot is one posm-managed position's identity and
// liquidity as of a block: the pool it belongs to (a posm token id names no
// pool on its own) plus the tick range and liquidity uniswapv3.ComputePositionAmounts
// needs.
type UniswapV4PositionSnapshot struct {
	PoolID    int64
	TickLower int
	TickUpper int
	Liquidity *big.Int
}

// UniswapV4PositionValuationReader supplies the read paths the allocation
// tracker needs to value a posm-managed V4 LP position as of a historical
// block. uniswap_v4_position.owner is always the PositionManager contract for
// a posm-managed position (never the NFT holder), so the holder is resolved
// separately from the PositionManager's ERC-721 transfer log.
type UniswapV4PositionValuationReader interface {
	// LoadPools is UniswapV4Repository's; reused here for pool currency
	// identities and PositionManager address, both chain-level fields carried
	// on every pool row (see UniswapV4PoolRow), so no dedicated lookup is
	// needed when at least one pool is registered.
	LoadPools(ctx context.Context, chainID int64) ([]UniswapV4PoolRow, error)
	// HeldTokenIDsAtBlock returns the posm token IDs wallet held at or before
	// blockNumber: for every token ever transferred to wallet, the newest
	// uniswap_v4_position_nft_transfer row at or below blockNumber whose
	// to_address is still wallet. Ascending, deduplicated.
	HeldTokenIDsAtBlock(ctx context.Context, chainID int64, wallet common.Address, blockNumber int64) ([]*big.Int, error)
	// PositionForTokenAtBlock resolves the posm-managed position for tokenID
	// (salt = bytes32(tokenID), owner = positionManager) at or before
	// blockNumber, searching every pool registered on chainID since a posm
	// token id does not name its pool. Returns nil when the slot has no row at
	// or below blockNumber.
	PositionForTokenAtBlock(ctx context.Context, chainID int64, positionManager common.Address, tokenID *big.Int, blockNumber int64) (*UniswapV4PositionSnapshot, error)
	// PoolStateAtBlock returns the latest sqrtPriceX96 for poolID at or before
	// blockNumber, or nil when the pool has no state snapshot at or below
	// blockNumber.
	PoolStateAtBlock(ctx context.Context, poolID int64, blockNumber int64) (*big.Int, error)
}

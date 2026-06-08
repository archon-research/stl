package entity

import (
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// UniswapV3Position is the registry row for an NFPM-minted NFT position whose
// pool is one of our indexed `uniswap_v3_pool` rows. One row per
// (chain, NFPM, tokenId).
//
// TokenID is a uint256 NFT id, hence *big.Int. Owner moves on ERC-721
// Transfer; Burned flips true when liquidity decreases to 0 (the NFPM
// preserves the row but the position has no further state to track).
type UniswapV3Position struct {
	ID              int64
	ChainID         int64
	NFPMAddress     common.Address
	TokenID         *big.Int
	UniswapV3PoolID int64
	Owner           common.Address
	TickLower       int32
	TickUpper       int32
	Fee             int32
	CreatedAtBlock  int64
	Burned          bool
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

package curve_tracker

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// PoolConfig defines a Curve pool to track.
type PoolConfig struct {
	Address common.Address
	ChainID int64
	Name    string // Human-readable label (e.g. "sUSDSUSDT")
}

// PoolsForChainID filters the pool list by chain ID.
func PoolsForChainID(pools []PoolConfig, chainID int64) []PoolConfig {
	var result []PoolConfig
	for _, p := range pools {
		if p.ChainID == chainID {
			result = append(result, p)
		}
	}
	return result
}

// CoinBalance holds a single coin's balance within a pool snapshot.
type CoinBalance struct {
	Address  common.Address `json:"coin"`
	TokenID  int64          `json:"token_id"`
	Balance  string         `json:"balance"`
	Decimals int            `json:"decimals"`
	Symbol   string         `json:"symbol"`
}

// OraclePrice holds the oracle price of coin i+1 relative to coin 0.
type OraclePrice struct {
	Index int    `json:"index"`
	Price string `json:"price"`
}

// PoolSnapshot holds a complete snapshot of a Curve pool's state.
type PoolSnapshot struct {
	PoolAddress  common.Address
	ChainID      int64
	BlockNumber  int64
	NCoins       int
	CoinBalances []CoinBalance
	TotalSupply  *big.Int
	VirtualPrice *big.Int
	TvlUSD       *big.Float // nil if prices unavailable
	AmpFactor    int64
	Fee          *big.Int
	OraclePrices []OraclePrice
}

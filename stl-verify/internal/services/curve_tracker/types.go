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

	// APY from Curve API (nil if API call fails)
	FeeAPY    *float64
	CrvAPYMin *float64
	CrvAPYMax *float64
}

// APYData holds APY information from the Curve API for a single pool.
type APYData struct {
	FeeAPY    float64
	CrvAPYMin float64
	CrvAPYMax float64
}

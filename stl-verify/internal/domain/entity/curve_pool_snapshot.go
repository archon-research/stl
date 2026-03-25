package entity

import (
	"encoding/json"
	"fmt"
	"time"
)

// CurvePoolSnapshot represents a point-in-time state of a Curve pool.
type CurvePoolSnapshot struct {
	PoolAddress  []byte
	ChainID      int64
	BlockNumber  int64
	CoinBalances json.RawMessage // JSONB
	NCoins       int
	TotalSupply  string // NUMERIC as string
	VirtualPrice string // NUMERIC as string
	TvlUSD       *string
	AmpFactor    int
	Fee          string          // NUMERIC as string
	OraclePrices json.RawMessage // JSONB, nullable
	FeeAPY       *string
	CrvAPYMin    *string
	CrvAPYMax    *string
	SnapshotTime time.Time
}

func (s *CurvePoolSnapshot) Validate() error {
	if len(s.PoolAddress) == 0 {
		return fmt.Errorf("pool address is required")
	}
	if s.ChainID == 0 {
		return fmt.Errorf("chain ID is required")
	}
	if s.BlockNumber == 0 {
		return fmt.Errorf("block number is required")
	}
	if s.NCoins == 0 {
		return fmt.Errorf("n_coins is required")
	}
	return nil
}

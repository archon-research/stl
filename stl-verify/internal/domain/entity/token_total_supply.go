package entity

import (
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// TokenTotalSupply represents a snapshot of a pool's totalSupply (and optionally
// scaledTotalSupply for Aave-like aTokens) at a given block, ready for persistence.
type TokenTotalSupply struct {
	ChainID           int64
	TokenAddress      common.Address
	TokenSymbol       string
	TokenDecimals     int
	TotalSupply       *big.Int
	ScaledTotalSupply *big.Int
	BlockNumber       int64
	BlockVersion      int
	BlockTimestamp    time.Time
	Source            string // "event" | "sweep"
	CreatedAtBlock    int64
}

func (t *TokenTotalSupply) Validate() error {
	if t.ChainID == 0 {
		return fmt.Errorf("chain_id is required")
	}
	if t.TokenAddress == (common.Address{}) {
		return fmt.Errorf("token_address is required")
	}
	if t.TotalSupply == nil {
		return fmt.Errorf("total_supply is required")
	}
	if t.BlockNumber == 0 {
		return fmt.Errorf("block_number is required")
	}
	if t.BlockTimestamp.IsZero() {
		return fmt.Errorf("block_timestamp is required")
	}
	if t.Source != "event" && t.Source != "sweep" {
		return fmt.Errorf("source must be 'event' or 'sweep', got %q", t.Source)
	}
	return nil
}

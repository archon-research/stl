package entity

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// AllocationPosition represents a position snapshot ready for persistence.
type AllocationPosition struct {
	ChainID        int64
	TokenAddress   common.Address
	TokenSymbol    string
	TokenDecimals  int
	Star           string
	ProxyAddress   common.Address
	AssetDecimals  *int
	Balance        *big.Int
	ScaledBalance  *big.Int
	BlockNumber    int64
	BlockVersion   int
	TxHash         string
	LogIndex       int
	TxAmount       *big.Int
	Direction      string
	CreatedAtBlock int64
}

func (p *AllocationPosition) Validate() error {
	if p.ChainID == 0 {
		return fmt.Errorf("chain_id is required")
	}
	if p.TokenAddress == (common.Address{}) {
		return fmt.Errorf("token_address is required")
	}
	if p.ProxyAddress == (common.Address{}) {
		return fmt.Errorf("proxy_address is required")
	}
	if p.Balance == nil {
		return fmt.Errorf("balance is required")
	}
	if p.Direction == "" {
		return fmt.Errorf("direction is required")
	}
	if p.Direction != "in" && p.Direction != "out" && p.Direction != "sweep" {
		return fmt.Errorf("direction must be 'in', 'out', or 'sweep', got %q", p.Direction)
	}
	if p.Star == "" {
		return fmt.Errorf("star is required")
	}
	if p.BlockNumber == 0 {
		return fmt.Errorf("block_number is required")
	}
	return nil
}

package entity

import (
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// UnderlyingValuation carries a position's current value denominated in the
// underlying asset, read on-chain at the same pinned block as the balance.
// A nil valuation on the position means "not computable" and persists as NULL
// in both underlying columns - a single pointer makes a half-set pair
// unrepresentable. Value 0 is a genuinely empty position, never a failure
// sentinel (VEC-307).
type UnderlyingValuation struct {
	Value         *big.Int       // raw underlying-asset units (undecimalised)
	AssetAddress  common.Address // asset the value is denominated in
	AssetSymbol   string
	AssetDecimals int
}

// AllocationPosition represents a position snapshot ready for persistence.
type AllocationPosition struct {
	ChainID       int64
	TokenAddress  common.Address
	TokenSymbol   string
	TokenDecimals int
	PrimeID       int64
	ProxyAddress  common.Address
	Balance       *big.Int
	ScaledBalance *big.Int
	Underlying    *UnderlyingValuation
	BlockNumber   int64
	BlockVersion  int
	TxHash        string
	LogIndex      int
	TxAmount      *big.Int
	Direction     string
	// Both sides of the triggering Transfer as decoded; nil for a sweep, which
	// has no transfer. One of the two always equals ProxyAddress.
	FromAddress    *common.Address
	ToAddress      *common.Address
	CreatedAtBlock int64
	CreatedAt      time.Time // block timestamp — deterministic for hypertable dedup
}

func (p *AllocationPosition) Validate() error {
	if p.CreatedAt.IsZero() {
		return fmt.Errorf("created_at must be set explicitly (block timestamp)")
	}
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
	if err := p.validateTransferParties(); err != nil {
		return err
	}
	if p.PrimeID == 0 {
		return fmt.Errorf("prime_id is required")
	}
	if p.BlockNumber == 0 {
		return fmt.Errorf("block_number is required")
	}
	// created_at_block feeds the registry's LEAST() merge; a 0 ("unknown"
	// masquerading as genesis) would clobber the stored block.
	if p.CreatedAtBlock <= 0 {
		return fmt.Errorf("created_at_block must be positive, got %d", p.CreatedAtBlock)
	}
	if p.Underlying != nil {
		if p.Underlying.Value == nil {
			return fmt.Errorf("underlying valuation requires a value")
		}
		if p.Underlying.AssetAddress == (common.Address{}) {
			return fmt.Errorf("underlying valuation requires an asset address")
		}
		if p.Underlying.AssetDecimals < 0 {
			return fmt.Errorf("underlying asset decimals must be non-negative, got %d", p.Underlying.AssetDecimals)
		}
	}
	return nil
}

// validateTransferParties checks the two Transfer sides against the direction.
//
// NULL from_address/to_address mean "no transfer to read them from", so only a
// sweep may write them; a transfer-driven row without them would masquerade as a
// sweep. Mirrors the tx_hash rule in encodeTxHash.
//
// Storing both sides makes direction redundant with them, so the two can now
// contradict each other. A row saying "out" whose from_address is not the proxy
// is corrupt in a way no reader can detect, so the disagreement fails here
// rather than being persisted.
func (p *AllocationPosition) validateTransferParties() error {
	if p.Direction == "sweep" {
		return nil
	}
	if p.FromAddress == nil || p.ToAddress == nil {
		return fmt.Errorf(
			"from_address and to_address are required for a transfer-driven position (direction=%q)",
			p.Direction,
		)
	}

	proxySide := p.ToAddress
	if p.Direction == "out" {
		proxySide = p.FromAddress
	}
	if *proxySide != p.ProxyAddress {
		return fmt.Errorf(
			"direction=%q requires proxy_address %s on that side of the transfer, got from=%s to=%s",
			p.Direction, p.ProxyAddress, p.FromAddress, p.ToAddress,
		)
	}
	return nil
}

package entity

import (
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// Prime represents a registered prime agent vault tracked for debt.
type Prime struct {
	ID           int64
	Name         string
	VaultAddress common.Address
	CreatedAt    time.Time
}

// PrimeDebt is a single on-chain debt snapshot for a prime agent.
type PrimeDebt struct {
	PrimeID      int64
	PrimeName    string
	VaultAddress common.Address
	IlkName      string
	// DebtWad is the exact debt in wad units (art * rate / 1e27).
	// The value is an integer scaled by 1e18 (wad precision).
	DebtWad     *big.Int
	BlockNumber int64
	SyncedAt    time.Time
}

// Validate checks that the snapshot is well-formed before persistence.
func (d *PrimeDebt) Validate() error {
	if d.PrimeName == "" {
		return fmt.Errorf("prime name is required")
	}
	if d.VaultAddress == (common.Address{}) {
		return fmt.Errorf("vault address is required")
	}
	if d.IlkName == "" {
		return fmt.Errorf("ilk name is required")
	}
	if d.DebtWad == nil {
		return fmt.Errorf("debt wad is required")
	}
	if d.BlockNumber <= 0 {
		return fmt.Errorf("block number must be positive")
	}
	if d.SyncedAt.IsZero() {
		return fmt.Errorf("synced_at is required")
	}
	return nil
}

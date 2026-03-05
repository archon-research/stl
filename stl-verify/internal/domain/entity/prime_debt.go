package entity

import (
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// Prime represents a registered prime agent vault tracked for debt.
type Prime struct {
	ID           int64
	Name         string
	VaultAddress string
	CreatedAt    time.Time
}

// PrimeDebt is a single on-chain debt snapshot for a prime agent.
type PrimeDebt struct {
	PrimeID      int64
	PrimeName    string
	VaultAddress string
	IlkName      string
	// DebtWad is the exact debt in wad units (18 decimal places) as a decimal string.
	// Computed as art * rate / 1e27 using exact rational arithmetic.
	// Example: "2948696280.290761728641811098"
	DebtWad  string
	SyncedAt time.Time
}

// Validate checks that the snapshot is well-formed before persistence.
func (d *PrimeDebt) Validate() error {
	if d.PrimeName == "" {
		return fmt.Errorf("prime name is required")
	}
	if d.VaultAddress == "" {
		return fmt.Errorf("vault address is required")
	}
	if !common.IsHexAddress(d.VaultAddress) {
		return fmt.Errorf("vault address %q is not a valid hex address", d.VaultAddress)
	}
	if d.IlkName == "" {
		return fmt.Errorf("ilk name is required")
	}
	if d.DebtWad == "" {
		return fmt.Errorf("debt wad is required")
	}
	if d.SyncedAt.IsZero() {
		return fmt.Errorf("synced_at is required")
	}
	return nil
}

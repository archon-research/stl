package entity

import (
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// ray = 1e27 — the MakerDAO RAY unit used for cumulative rate.
var ray = new(big.Int).Exp(big.NewInt(10), big.NewInt(27), nil)

// Prime represents a registered prime agent vault tracked for debt.
type Prime struct {
	ID           int64
	Name         string
	VaultAddress common.Address
	CreatedAt    time.Time
}

// PrimeDebt is a single on-chain debt snapshot for a prime agent.
type PrimeDebt struct {
	PrimeID int64
	IlkName string
	// DebtWad is the exact debt in wad units (art * rate / 1e27).
	// The value is an integer scaled by 1e18 (wad precision).
	DebtWad     *big.Int
	BlockNumber int64
	SyncedAt    time.Time
}

// Validate checks that the snapshot is well-formed before persistence.
func (d *PrimeDebt) Validate() error {
	if d.PrimeID <= 0 {
		return fmt.Errorf("prime_id must be positive")
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

// ComputeDebtWad computes vault debt as a wad-scaled *big.Int (18 decimals).
//
// Formula: art (wad, 1e18) × rate (ray, 1e27) = rad (1e45) → ÷ 1e27 → wad (1e18).
//
// To recover a human-readable USDS amount, divide the result by 1e18.
func ComputeDebtWad(art, rate *big.Int) *big.Int {
	rad := new(big.Int).Mul(art, rate)
	return new(big.Int).Div(rad, ray)
}

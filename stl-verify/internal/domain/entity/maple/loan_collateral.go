package maple

import (
	"fmt"
	"math/big"
	"time"
)

// LoanCollateral is the collateral snapshot of an active loan at a sync
// cycle. Loans with null API collateral simply have no row. AssetAmount is in
// native asset decimals; AssetValueUSD is the per-unit USD price with
// 8 decimals (total USD = amount * price / 10^decimals / 10^8). Both are
// nullable in the API schema (plausibly during DepositPending) and kept nil
// so "collateral pending" stays distinguishable from "no collateral". The
// asset may be custodied off-chain (BTC, SOL), so it is identified by symbol
// only.
type LoanCollateral struct {
	LoanID           int64
	SyncedAt         time.Time
	AssetSymbol      string
	AssetAmount      *big.Int // nil when the API reports null
	AssetDecimals    int16
	AssetValueUSD    *big.Int // nil when the API reports null
	State            string   // 'Deposited' | 'DepositPending' | ... (may be empty)
	Custodian        string   // e.g. 'FORDEFI', 'ANCHORAGE' (may be empty)
	LiquidationLevel *big.Int // nil when absent
}

type LoanCollateralParams struct {
	LoanID           int64
	SyncedAt         time.Time
	AssetSymbol      string
	AssetAmount      *big.Int
	AssetDecimals    int16
	AssetValueUSD    *big.Int
	State            string
	Custodian        string
	LiquidationLevel *big.Int
}

// NewLoanCollateral creates a new LoanCollateral entity with validation.
func NewLoanCollateral(p LoanCollateralParams) (*LoanCollateral, error) {
	c := &LoanCollateral{
		LoanID:           p.LoanID,
		SyncedAt:         NormalizeSyncedAt(p.SyncedAt),
		AssetSymbol:      p.AssetSymbol,
		AssetAmount:      p.AssetAmount,
		AssetDecimals:    p.AssetDecimals,
		AssetValueUSD:    p.AssetValueUSD,
		State:            p.State,
		Custodian:        p.Custodian,
		LiquidationLevel: p.LiquidationLevel,
	}
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("NewLoanCollateral: %w", err)
	}
	return c, nil
}

// Validate checks that all fields have valid values.
func (c *LoanCollateral) Validate() error {
	if c.LoanID <= 0 {
		return fmt.Errorf("mapleLoanID must be positive, got %d", c.LoanID)
	}
	if c.SyncedAt.IsZero() {
		return fmt.Errorf("syncedAt must not be zero")
	}
	if c.AssetSymbol == "" {
		return fmt.Errorf("assetSymbol must not be empty")
	}
	if err := requireNonNegBigIntIfSet("assetAmount", c.AssetAmount); err != nil {
		return err
	}
	if c.AssetDecimals < 0 {
		return fmt.Errorf("assetDecimals must be non-negative, got %d", c.AssetDecimals)
	}
	if err := requireNonNegBigIntIfSet("assetValueUSD", c.AssetValueUSD); err != nil {
		return err
	}
	if err := requireNonNegBigIntIfSet("liquidationLevel", c.LiquidationLevel); err != nil {
		return err
	}
	return nil
}

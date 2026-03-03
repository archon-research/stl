package entity

import (
	"fmt"
	"math/big"
)

// MapleCollateral represents a Maple Finance borrower's collateral position at a specific block.
// Unlike the generic BorrowerCollateral entity, MapleCollateral uses a collateral asset symbol
// (e.g., "BTC", "SOL") instead of a token ID, because Maple collateral assets may not have
// on-chain contract addresses.
type MapleCollateral struct {
	ID                 int64
	UserID             int64
	ProtocolID         int64
	CollateralAsset    string   // e.g., "BTC", "XRP", "SOL"
	CollateralDecimals int      // decimal precision for the collateral asset
	Amount             *big.Int // current collateral amount in native decimals
	Custodian          string   // e.g., "FORDEFI", "ANCHORAGE"
	State              string   // e.g., "Deposited", "DepositPending"
	LiquidationLevel   *big.Int // liquidation trigger ratio (may be nil)
	BlockNumber        int64
	BlockVersion       int

	// LoanMeta fields - populated for internal Maple positions (loanMeta.type = "amm" or "strategy")
	// All fields are empty strings for external loans (where loanMeta is null in the API)
	LoanType          string // "amm", "strategy" for internal loans, empty for external loans
	LoanAssetSymbol   string // e.g., underlying asset symbol for internal positions
	LoanDexName       string // e.g., "Aerodrome" for AMM positions
	LoanLocation      string // location metadata
	LoanWalletAddress string // wallet address holding the position
	LoanWalletType    string // e.g., "BASE" for Base blockchain
}

// NewMapleCollateral creates a new MapleCollateral entity with validation.
func NewMapleCollateral(userID, protocolID int64, collateralAsset string, collateralDecimals int, amount *big.Int, custodian, state string, liquidationLevel *big.Int, blockNumber int64, blockVersion int) (*MapleCollateral, error) {
	c := &MapleCollateral{
		UserID:             userID,
		ProtocolID:         protocolID,
		CollateralAsset:    collateralAsset,
		CollateralDecimals: collateralDecimals,
		Amount:             amount,
		Custodian:          custodian,
		State:              state,
		LiquidationLevel:   liquidationLevel,
		BlockNumber:        blockNumber,
		BlockVersion:       blockVersion,
	}
	if err := c.validate(); err != nil {
		return nil, err
	}
	return c, nil
}

// validate checks that all fields have valid values.
func (c *MapleCollateral) validate() error {
	if c.UserID <= 0 {
		return fmt.Errorf("userID must be positive, got %d", c.UserID)
	}
	if c.ProtocolID <= 0 {
		return fmt.Errorf("protocolID must be positive, got %d", c.ProtocolID)
	}
	if c.CollateralAsset == "" {
		return fmt.Errorf("collateralAsset must not be empty")
	}
	if c.CollateralDecimals < 0 {
		return fmt.Errorf("collateralDecimals must be non-negative, got %d", c.CollateralDecimals)
	}
	if c.Amount == nil {
		return fmt.Errorf("amount must not be nil")
	}
	if c.Amount.Sign() < 0 {
		return fmt.Errorf("amount must be non-negative")
	}
	if c.BlockNumber <= 0 {
		return fmt.Errorf("blockNumber must be positive, got %d", c.BlockNumber)
	}
	if c.BlockVersion < 0 {
		return fmt.Errorf("blockVersion must be non-negative, got %d", c.BlockVersion)
	}
	return nil
}

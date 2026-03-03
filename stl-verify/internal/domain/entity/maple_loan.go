package entity

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
)

// MapleLoan represents a Maple Finance loan with its metadata at a specific block.
// The loan metadata (type, dexName, walletAddress, etc.) distinguishes internal Maple
// positions (type = "amm" or "strategy") from external loans (where metadata fields are empty).
type MapleLoan struct {
	ID           int64
	LoanAddress  common.Address // openTermLoan.id (contract address)
	ProtocolID   int64
	BlockNumber  int64
	BlockVersion int

	// Funding pool fields - every loan is funded by a specific pool
	PoolAddress       common.Address // funding pool contract address
	PoolName          string         // e.g., "Syrup USDC Pool"
	PoolAssetSymbol   string         // e.g., "USDC", "WBTC"
	PoolAssetDecimals int            // decimals for pool asset

	// LoanMeta fields - populated for internal Maple positions, empty for external loans
	LoanType          string // "amm", "strategy" for internal loans, empty for external
	LoanAssetSymbol   string // underlying asset symbol for internal positions
	LoanDexName       string // e.g., "Aerodrome", "Fluid" for AMM positions
	LoanLocation      string // location metadata
	LoanWalletAddress string // wallet address holding the position
	LoanWalletType    string // e.g., "EVM", "BASE" for blockchain type
}

// IsInternal returns true if this is an internal Maple loan (AMM or strategy position).
func (l *MapleLoan) IsInternal() bool {
	return l.LoanType == "amm" || l.LoanType == "strategy"
}

// NewMapleLoan creates a new MapleLoan entity with validation.
func NewMapleLoan(
	loanAddress common.Address,
	protocolID int64,
	blockNumber int64,
	blockVersion int,
	poolAddress common.Address,
	poolName string,
	poolAssetSymbol string,
	poolAssetDecimals int,
) (*MapleLoan, error) {
	loan := &MapleLoan{
		LoanAddress:       loanAddress,
		ProtocolID:        protocolID,
		BlockNumber:       blockNumber,
		BlockVersion:      blockVersion,
		PoolAddress:       poolAddress,
		PoolName:          poolName,
		PoolAssetSymbol:   poolAssetSymbol,
		PoolAssetDecimals: poolAssetDecimals,
	}
	if err := loan.validate(); err != nil {
		return nil, err
	}
	return loan, nil
}

// validate checks that all required fields have valid values.
func (l *MapleLoan) validate() error {
	if l.LoanAddress == (common.Address{}) {
		return fmt.Errorf("loanAddress must not be zero address")
	}
	if l.ProtocolID <= 0 {
		return fmt.Errorf("protocolID must be positive, got %d", l.ProtocolID)
	}
	if l.BlockNumber <= 0 {
		return fmt.Errorf("blockNumber must be positive, got %d", l.BlockNumber)
	}
	if l.BlockVersion < 0 {
		return fmt.Errorf("blockVersion must be non-negative, got %d", l.BlockVersion)
	}
	if l.PoolAddress == (common.Address{}) {
		return fmt.Errorf("poolAddress must not be zero address")
	}
	if l.PoolName == "" {
		return fmt.Errorf("poolName must not be empty")
	}
	if l.PoolAssetSymbol == "" {
		return fmt.Errorf("poolAssetSymbol must not be empty")
	}
	if l.PoolAssetDecimals < 0 {
		return fmt.Errorf("poolAssetDecimals must be non-negative, got %d", l.PoolAssetDecimals)
	}
	return nil
}

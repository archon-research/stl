package outbound

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// MapleClient defines the interface for querying the Maple Finance GraphQL API.
type MapleClient interface {
	// GetAllActiveLoansAtBlock returns all active open-term loans across all pools at a specific block number.
	GetAllActiveLoansAtBlock(ctx context.Context, blockNumber uint64) ([]MapleActiveLoan, error)
}


// MapleLoanCollateral represents the collateral for a single loan.
type MapleLoanCollateral struct {
	Asset            string   // symbol, e.g. "BTC", "USDC"
	AssetAmount      *big.Int // native decimals
	AssetValueUSD    *big.Int // 6 decimals
	Decimals         int
	State            string   // "Deposited" or "DepositPending"
	Custodian        string   // e.g. "FORDEFI", "ANCHORAGE"
	LiquidationLevel *big.Int // ratio for liquidation trigger
}

// MapleActiveLoan represents an active open-term loan with its funding pool info.
type MapleActiveLoan struct {
	LoanID            common.Address
	Borrower          common.Address
	State             string
	PrincipalOwed     *big.Int
	AcmRatio          *big.Int
	Collateral        MapleLoanCollateral
	PoolAddress       common.Address
	PoolName          string
	PoolAssetSymbol   string
	PoolAssetDecimals int
}

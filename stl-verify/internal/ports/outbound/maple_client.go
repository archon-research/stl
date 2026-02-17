package outbound

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// MapleClient defines the interface for querying the Maple Finance GraphQL API.
type MapleClient interface {
	// ListPools returns all Maple pool definitions.
	ListPools(ctx context.Context) ([]MaplePoolInfo, error)

	// GetPoolCollateral returns the collateral composition and TVL for a pool.
	GetPoolCollateral(ctx context.Context, poolAddress common.Address) (*MaplePoolData, error)

	// GetBorrowerCollateralAtBlock returns the active loans and their collateral for a pool at a specific block.
	GetBorrowerCollateralAtBlock(ctx context.Context, poolAddress common.Address, blockNumber uint64) ([]MapleBorrowerLoan, error)

	// GetAllActiveLoansAtBlock returns all active open-term loans across all pools at a specific block number.
	GetAllActiveLoansAtBlock(ctx context.Context, blockNumber uint64) ([]MapleActiveLoan, error)
}

// MaplePoolInfo represents a Maple pool definition.
type MaplePoolInfo struct {
	Address       common.Address
	Name          string
	AssetSymbol   string
	AssetDecimals int
}

// MaplePoolData contains a pool's TVL and collateral breakdown.
type MaplePoolData struct {
	TVL         *big.Int // 6-decimal USD value
	Collaterals []MapleCollateral
}

// MapleCollateral represents a single collateral asset in a pool.
type MapleCollateral struct {
	Asset         string   // symbol, e.g. "BTC", "XRP"
	AssetValueUSD *big.Int // 6-decimal USD value
	AssetDecimals int
}

// MapleBorrowerLoan represents an active loan with its collateral details.
type MapleBorrowerLoan struct {
	LoanID        common.Address
	Borrower      common.Address
	State         string
	PrincipalOwed *big.Int // 6 decimals
	AcmRatio      *big.Int // e.g. 1698389 = 169.84%
	Collateral    MapleLoanCollateral
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

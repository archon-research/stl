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

	// GetAccountPositions returns the user's pool positions (pool ID, name,
	// asset info, lending balance) for the given wallet address.
	GetAccountPositions(ctx context.Context, address common.Address) ([]MaplePoolPosition, error)

	// GetPoolCollateral returns the collateral composition and TVL for a pool.
	GetPoolCollateral(ctx context.Context, poolAddress common.Address) (*MaplePoolData, error)

	// GetBorrowerCollateralAtBlock returns the active loans and their collateral for a pool at a specific block.
	GetBorrowerCollateralAtBlock(ctx context.Context, poolAddress common.Address, blockNumber uint64) ([]MapleBorrowerLoan, error)
}

// MaplePoolInfo represents a Maple pool definition.
type MaplePoolInfo struct {
	Address       common.Address
	Name          string
	AssetSymbol   string
	AssetDecimals int
}

// MaplePoolPosition represents a user's position in a single Maple pool,
// as returned from the GraphQL API.
type MaplePoolPosition struct {
	PoolAddress    common.Address // pool contract address
	PoolName       string         // e.g. "Syrup USDC"
	AssetSymbol    string         // pool underlying asset symbol
	AssetDecimals  int            // pool underlying asset decimals
	LendingBalance *big.Int       // user's USD value in pool (6 decimals)
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

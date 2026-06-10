package outbound

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// MapleLoanMeta distinguishes internal vs external loans.
// Internal loans have Type = "amm" or "strategy"; external loans have nil LoanMeta.
type MapleLoanMeta struct {
	Type          string
	AssetSymbol   string
	DexName       string
	Location      string
	WalletAddress string // may be a non-EVM address (Base/Solana custody wallets); never hex-validated
	WalletType    string
}

// MapleLoanCollateral is the single (nullable upstream) collateral of a loan.
type MapleLoanCollateral struct {
	Asset            string   // symbol, e.g. "BTC", "USDC", "SOL"
	AssetAmount      *big.Int // native decimals
	AssetValueUSD    *big.Int // per-unit USD price, 8 decimals
	Decimals         int
	State            string   // "Deposited" | "DepositPending" | ...
	Custodian        string   // e.g. "FORDEFI", "ANCHORAGE"
	LiquidationLevel *big.Int // nil when absent
}

// MapleActiveLoan is one active Open Term Loan as reported by the Maple
// GraphQL API, flattened with its funding pool reference.
type MapleActiveLoan struct {
	LoanID            common.Address
	Borrower          common.Address
	State             string
	PrincipalOwed     *big.Int
	AcmRatio          *big.Int
	Collateral        *MapleLoanCollateral // nil when the API returns null
	LoanMeta          *MapleLoanMeta       // nil for external loans
	PoolAddress       common.Address
	PoolName          string
	PoolAssetSymbol   string
	PoolAssetDecimals int
}

// MaplePool is one PoolV2 lending pool with its current lending metrics.
type MaplePool struct {
	Address       common.Address
	Name          string
	AssetAddress  common.Address
	AssetSymbol   string
	AssetDecimals int
	IsSyrup       bool // syrupRouter != null
	TVL           *big.Int
	LiquidAssets  *big.Int // poolV2.assets
	CollateralUSD *big.Int
	PrincipalOut  *big.Int
	MonthlyAPY    *big.Int // 30 decimals
	SpotAPY       *big.Int // 30 decimals
}

// MapleSkyStrategy is one Sky strategy (internal Maple deployment of pool
// assets) with its current state.
type MapleSkyStrategy struct {
	Address            common.Address
	PoolAddress        common.Address
	State              string
	Version            int
	CurrentlyDeployed  *big.Int
	DepositedAssets    *big.Int
	WithdrawnAssets    *big.Int
	StrategyFeeRate    *big.Int // nil when absent
	TotalFeesCollected *big.Int // nil when absent
}

// MapleSyrupGlobals is Maple's protocol-wide Syrup aggregate snapshot.
type MapleSyrupGlobals struct {
	TVL             *big.Int
	APY             *big.Int // 30 decimals
	CollateralAPY   *big.Int // 30 decimals
	PoolAPY         *big.Int // 30 decimals
	DripsYieldBoost *big.Int // nil when absent
}

// MapleGraphQLClient is the outbound port for the Maple GraphQL API
// (https://api.maple.finance/v2/graphql). All methods query the latest state
// (no block argument) and paginate transparently; implementations must fail
// the whole call on any malformed row rather than skipping it.
type MapleGraphQLClient interface {
	// GetPools fetches all PoolV2 lending pools.
	GetPools(ctx context.Context) ([]MaplePool, error)

	// GetActiveLoans fetches all Open Term Loans with state Active.
	GetActiveLoans(ctx context.Context) ([]MapleActiveLoan, error)

	// GetSkyStrategies fetches all Sky strategies.
	GetSkyStrategies(ctx context.Context) ([]MapleSkyStrategy, error)

	// GetSyrupGlobals fetches the protocol-wide Syrup aggregates (singleton).
	GetSyrupGlobals(ctx context.Context) (*MapleSyrupGlobals, error)
}

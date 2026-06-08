package entity

import (
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// CurvePoolKind enumerates the on-chain implementation variants the worker
// supports. Matches the `pool_kind` CHECK constraint on the curve_pool table.
const (
	CurvePoolKindV1 = "v1" // Pre-NG (e.g. stETH-classic, 3pool).
	CurvePoolKindNG = "ng" // Stableswap-NG factory (e.g. stETH-ng).
)

// CurvePool is the registry row for a Curve pool we index. Coin slots are
// arrays in coin-index order; coin_addresses[i] / coin_token_ids[i] /
// coin_decimals[i] describe slot i. ETH placeholder (0xEee…EEEE) is stored in
// CoinAddresses as-is and mapped to WETH's token id in CoinTokenIDs.
type CurvePool struct {
	ID              int64
	ChainID         int64
	PoolKind        string
	Address         common.Address
	LPTokenAddress  *common.Address // NULL ⇒ LP token == Address (true for NG factory pools).
	Label           string
	NCoins          int16
	CoinAddresses   []common.Address
	CoinTokenIDs    []int64
	CoinDecimals    []int16
	DeploymentBlock *int64
	Enabled         bool
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

package entity

import (
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// UniswapV3Pool is the registry row for a Uniswap V3 pool we index. Pool
// composition is fixed two-token; Token0ID and Token1ID are scalar FKs into
// the shared `token` table (unlike the Curve registry which uses parallel
// arrays for variable N).
//
// FeeTier is the pool's static swap fee in pip units (1e6 = 100%), restricted
// to {100, 500, 3000, 10000}; TickSpacing is the canonical spacing for that
// tier set at factory deployment.
type UniswapV3Pool struct {
	ID              int64
	ChainID         int64
	Address         common.Address
	Token0ID        int64
	Token1ID        int64
	FeeTier         int32
	TickSpacing     int32
	Label           string
	DeploymentBlock *int64
	Enabled         bool
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

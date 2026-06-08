package entity

import (
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// CurveGauge is the registry row for a Curve liquidity gauge attached to a
// pool we index. At most one gauge per pool — see the `UNIQUE (curve_pool_id)`
// constraint on the curve_gauge table.
type CurveGauge struct {
	ID              int64
	CurvePoolID     int64
	ChainID         int64
	Address         common.Address
	DeploymentBlock *int64
	IsKilled        bool
	HasNoCRV        bool
	Enabled         bool
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

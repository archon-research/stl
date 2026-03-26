package outbound

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
)

// APYData holds APY information for a single Curve pool.
type APYData struct {
	FeeAPY    *float64
	CrvAPYMin *float64
	CrvAPYMax *float64
}

// CurveAPYFetcher retrieves APY data from the Curve Finance API.
type CurveAPYFetcher interface {
	FetchAPYs(ctx context.Context, chainName string, pools []common.Address) (map[common.Address]*APYData, error)
}

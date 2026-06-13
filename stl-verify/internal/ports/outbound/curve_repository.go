package outbound

import (
	"context"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

type CurveRepository interface {
	SaveSnapshots(ctx context.Context, snapshots []*entity.CurvePoolSnapshot) error
	LookupTokenID(ctx context.Context, chainID int64, address common.Address) (int64, error)
	// GetPreviousSnapshot returns the most recent virtual_price and snapshot_time
	// for a pool prior to the given block. Returns nil if no previous snapshot exists.
	GetPreviousSnapshot(ctx context.Context, poolAddress []byte, chainID int64) (virtualPrice string, snapshotTime time.Time, err error)
}

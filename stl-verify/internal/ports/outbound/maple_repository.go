package outbound

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/ethereum/go-ethereum/common"
)

// MapleRepository defines the interface for persisting Maple position and
// collateral snapshot data.
type MapleRepository interface {
	// UpsertPositions persists user positions. Conflict resolution on
	// (user_id, protocol_id, pool_address, snapshot_block).
	UpsertPositions(ctx context.Context, positions []*entity.MaplePosition) error

	// UpsertPoolCollateral persists pool collateral snapshots. Conflict
	// resolution on (pool_address, asset, snapshot_block).
	UpsertPoolCollateral(ctx context.Context, collaterals []*entity.MaplePoolCollateral) error

	// GetUsersWithMaplePositions returns users that have Maple protocol
	// associations (i.e. users we should check for Maple positions).
	GetUsersWithMaplePositions(ctx context.Context, protocolID int64) ([]MapleTrackedUser, error)
}

// MapleTrackedUser is a user that should be tracked for Maple positions.
type MapleTrackedUser struct {
	UserID  int64
	Address common.Address
}

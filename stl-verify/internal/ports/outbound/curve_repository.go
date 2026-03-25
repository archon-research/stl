package outbound

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/ethereum/go-ethereum/common"
)

type CurveRepository interface {
	SaveSnapshots(ctx context.Context, snapshots []*entity.CurvePoolSnapshot) error
	LookupTokenID(ctx context.Context, chainID int64, address common.Address) (int64, error)
}

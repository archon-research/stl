package outbound

import (
	"context"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

type UniswapRepository interface {
	SaveSnapshots(ctx context.Context, snapshots []*entity.UniswapPoolSnapshot) error
	LookupTokenID(ctx context.Context, chainID int64, address common.Address) (int64, error)
}

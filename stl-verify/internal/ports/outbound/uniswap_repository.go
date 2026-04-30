package outbound

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

type UniswapRepository interface {
	SaveSnapshots(ctx context.Context, snapshots []*entity.UniswapPoolSnapshot) error
}

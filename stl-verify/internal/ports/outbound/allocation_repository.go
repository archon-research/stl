package outbound

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// AllocationRepository persists allocation positions within an externally
// managed transaction. Callers obtain `tx` from a TxManager so this write can
// be coordinated with other repository writes (e.g. TokenTotalSupplyRepository)
// atomically.
type AllocationRepository interface {
	SavePositions(ctx context.Context, tx pgx.Tx, positions []*entity.AllocationPosition) error
}

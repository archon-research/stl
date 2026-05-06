package outbound

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// PrimeCapitalStackRepository persists prime capital stack snapshots.
type PrimeCapitalStackRepository interface {
	UpsertPrimeCapitalSnapshots(ctx context.Context, snapshots []entity.PrimeCapitalStackSnapshot) error
}

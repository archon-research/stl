package outbound

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// PrimeCapitalStackRepository persists prime capital stack snapshots.
type PrimeCapitalStackRepository interface {
	// SavePrimeCapitalSnapshots writes within the caller's transaction, so the
	// caller controls what else commits or rolls back with it.
	SavePrimeCapitalSnapshots(ctx context.Context, tx pgx.Tx, snapshots []entity.PrimeCapitalStackSnapshot) error
}

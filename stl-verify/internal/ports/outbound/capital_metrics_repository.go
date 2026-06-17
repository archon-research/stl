package outbound

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// CapitalMetricsRepository lists primes and persists capital-metrics snapshots.
type CapitalMetricsRepository interface {
	GetPrimes(ctx context.Context) ([]entity.Prime, error)
	SaveSnapshots(ctx context.Context, snapshots []*entity.CapitalMetricsSnapshot) error
}

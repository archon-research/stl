package outbound

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// PrimeRepository defines the interface for looking up prime agents.
type PrimeRepository interface {
	GetPrimeIDByName(ctx context.Context, name string) (int64, error)
	ListPrimes(ctx context.Context) ([]entity.Prime, error)
}

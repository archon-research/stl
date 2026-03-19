package outbound

import "context"

// PrimeRepository defines the interface for looking up prime agents.
type PrimeRepository interface {
	GetPrimeIDByName(ctx context.Context, name string) (int64, error)
}

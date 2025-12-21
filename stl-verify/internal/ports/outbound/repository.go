// Package outbound contains the secondary/outbound ports.
// These interfaces are implemented by infrastructure adapters.
package outbound

import "context"

// Repository defines the interface for data persistence.
// This is implemented by adapters in the infrastructure layer.
type Repository interface {
	// Add your repository methods here
	// Example:
	// Save(ctx context.Context, entity *entity.Verification) error
	// FindByID(ctx context.Context, id string) (*entity.Verification, error)
	HealthCheck(ctx context.Context) error
}

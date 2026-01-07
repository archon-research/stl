// repository.go provides an in-memory implementation of the Repository port.
//
// This is a minimal adapter for testing and development. It provides
// a basic health check and can be extended with additional data structures
// as needed for testing verification workflows.
//
// For production persistence, see the postgres adapter.
package memory

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that Repository implements outbound.Repository
var _ outbound.Repository = (*Repository)(nil)

// Repository is an in-memory implementation of the outbound.Repository port.
type Repository struct {
	// Add your data structures here
	// Example: items map[string]*entity.Verification
}

// NewRepository creates a new in-memory repository.
func NewRepository() *Repository {
	return &Repository{
		// Initialize data structures
	}
}

// HealthCheck verifies the repository is operational.
func (r *Repository) HealthCheck(ctx context.Context) error {
	// In-memory repository is always healthy
	return nil
}

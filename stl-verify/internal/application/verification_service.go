// Package application contains the implementation of use cases.
package application

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/ports/inbound"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that VerificationService implements inbound.VerificationService
var _ inbound.VerificationService = (*VerificationService)(nil)

// VerificationService implements the verification use cases.
type VerificationService struct {
	repo outbound.Repository
	// Add other outbound ports as needed (event publisher, external APIs, etc.)
}

// NewVerificationService creates a new verification service with the given dependencies.
func NewVerificationService(repo outbound.Repository) *VerificationService {
	return &VerificationService{
		repo: repo,
	}
}

// Ping verifies the service is running and dependencies are healthy.
func (s *VerificationService) Ping(ctx context.Context) error {
	return s.repo.HealthCheck(ctx)
}

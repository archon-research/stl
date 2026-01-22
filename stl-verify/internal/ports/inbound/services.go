// Package inbound contains the primary/inbound ports.
// These interfaces define the use cases that the application exposes.
package inbound

import "context"

// VerificationService defines the primary use cases for the verification domain.
// Inbound adapters (HTTP handlers, CLI, gRPC) call these methods.
type VerificationService interface {
	// Add your use case methods here
	// Example:
	// Verify(ctx context.Context, req VerifyRequest) (*VerifyResponse, error)
	Ping(ctx context.Context) error
}

// HealthChecker defines the interface for services that can report readiness and liveness.
// This enables health checking during rolling deployments, ensuring new instances
// are processing data before old ones are terminated.
//
// Implementations:
//   - LiveService: Ready after first block processed, healthy if blocks processed recently
type HealthChecker interface {
	// IsReady returns true when the service is ready to handle traffic.
	// For the watcher, this means at least one block has been successfully processed.
	// Used by ECS/Kubernetes readiness probes during rolling deployments.
	IsReady() bool

	// IsHealthy returns true when the service is operating normally.
	// For the watcher, this means blocks are being processed regularly (within 5 minutes).
	// Used by ECS/Kubernetes liveness probes to detect stuck services.
	IsHealthy() bool
}

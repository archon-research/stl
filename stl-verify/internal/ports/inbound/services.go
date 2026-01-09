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

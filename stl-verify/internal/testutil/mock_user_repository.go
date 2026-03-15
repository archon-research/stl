package testutil

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// MockUserRepository implements outbound.UserRepository for testing.
type MockUserRepository struct {
	GetOrCreateUserFn             func(ctx context.Context, tx pgx.Tx, user entity.User) (int64, error)
	GetOrCreateUserID             *int64
	GetOrCreateUserErr            error
	EnsureUserFn                  func(ctx context.Context, tx pgx.Tx, user entity.User) (int64, bool, error)
	EnsureUserID                  *int64
	EnsureUserCreated             *bool
	EnsureUserErr                 error
	UpsertUserProtocolMetadataFn  func(ctx context.Context, metadata []*entity.UserProtocolMetadata) error
	UpsertUserProtocolMetadataErr error
}

func (m *MockUserRepository) GetOrCreateUser(ctx context.Context, tx pgx.Tx, user entity.User) (int64, error) {
	if m.GetOrCreateUserFn != nil {
		return m.GetOrCreateUserFn(ctx, tx, user)
	}
	if m.GetOrCreateUserID != nil || m.GetOrCreateUserErr != nil {
		return valueOrDefault(m.GetOrCreateUserID, 1), m.GetOrCreateUserErr
	}
	return 1, nil
}

func (m *MockUserRepository) EnsureUser(ctx context.Context, tx pgx.Tx, user entity.User) (int64, bool, error) {
	if m.EnsureUserFn != nil {
		return m.EnsureUserFn(ctx, tx, user)
	}
	if m.EnsureUserID != nil || m.EnsureUserCreated != nil || m.EnsureUserErr != nil {
		return valueOrDefault(m.EnsureUserID, 1), valueOrDefault(m.EnsureUserCreated, true), m.EnsureUserErr
	}
	return 1, true, nil
}

func (m *MockUserRepository) UpsertUserProtocolMetadata(ctx context.Context, metadata []*entity.UserProtocolMetadata) error {
	if m.UpsertUserProtocolMetadataFn != nil {
		return m.UpsertUserProtocolMetadataFn(ctx, metadata)
	}
	if m.UpsertUserProtocolMetadataErr != nil {
		return m.UpsertUserProtocolMetadataErr
	}
	return nil
}

func valueOrDefault[T any](value *T, fallback T) T {
	if value != nil {
		return *value
	}
	return fallback
}

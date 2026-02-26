package testutil

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// MockUserRepository implements outbound.UserRepository for testing.
type MockUserRepository struct {
	GetOrCreateUserFn            func(ctx context.Context, tx pgx.Tx, user entity.User) (int64, error)
	UpsertUsersFn                func(ctx context.Context, users []*entity.User) error
	UpsertUserProtocolMetadataFn func(ctx context.Context, metadata []*entity.UserProtocolMetadata) error
}

func (m *MockUserRepository) GetOrCreateUser(ctx context.Context, tx pgx.Tx, user entity.User) (int64, error) {
	if m.GetOrCreateUserFn != nil {
		return m.GetOrCreateUserFn(ctx, tx, user)
	}
	return 1, nil
}

func (m *MockUserRepository) UpsertUsers(ctx context.Context, users []*entity.User) error {
	if m.UpsertUsersFn != nil {
		return m.UpsertUsersFn(ctx, users)
	}
	return nil
}

func (m *MockUserRepository) UpsertUserProtocolMetadata(ctx context.Context, metadata []*entity.UserProtocolMetadata) error {
	if m.UpsertUserProtocolMetadataFn != nil {
		return m.UpsertUserProtocolMetadataFn(ctx, metadata)
	}
	return nil
}

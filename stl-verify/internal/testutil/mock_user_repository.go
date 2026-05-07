package testutil

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// MockUserRepository implements outbound.UserRepository for testing.
type MockUserRepository struct {
	GetOrCreateUserFn            func(ctx context.Context, tx pgx.Tx, user entity.User) (int64, error)
	UpsertUserProtocolMetadataFn func(ctx context.Context, metadata []*entity.UserProtocolMetadata) error
}

func (m *MockUserRepository) GetOrCreateUser(ctx context.Context, tx pgx.Tx, user entity.User) (int64, error) {
	if m.GetOrCreateUserFn != nil {
		return m.GetOrCreateUserFn(ctx, tx, user)
	}
	return 1, nil
}

func (m *MockUserRepository) GetOrCreateUsers(ctx context.Context, tx pgx.Tx, users []entity.User) (map[common.Address]int64, error) {
	result := make(map[common.Address]int64, len(users))
	for i, u := range users {
		if m.GetOrCreateUserFn != nil {
			id, err := m.GetOrCreateUserFn(ctx, tx, u)
			if err != nil {
				return nil, err
			}
			result[u.Address] = id
		} else {
			result[u.Address] = int64(i + 1)
		}
	}
	return result, nil
}

func (m *MockUserRepository) UpsertUserProtocolMetadata(ctx context.Context, metadata []*entity.UserProtocolMetadata) error {
	if m.UpsertUserProtocolMetadataFn != nil {
		return m.UpsertUserProtocolMetadataFn(ctx, metadata)
	}
	return nil
}

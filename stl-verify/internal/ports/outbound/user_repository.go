package outbound

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// UserRepository defines the interface for user-related data persistence.
// This aggregate includes users and their protocol-specific metadata.
type UserRepository interface {
	// GetOrCreateUser retrieves a user by address, or creates it if it doesn't exist
	GetOrCreateUser(ctx context.Context, tx pgx.Tx, user entity.User) (int64, error)

	// EnsureUser upserts a user and reports whether the row was newly created.
	// Preserves the earliest first_seen_block identical to GetOrCreateUser.
	EnsureUser(ctx context.Context, tx pgx.Tx, user entity.User) (userID int64, created bool, err error)

	// UpsertUserProtocolMetadata upserts user protocol metadata records.
	// This stores protocol-specific data like health factors, LTV, etc.
	// Conflict resolution: ON CONFLICT (user_id, protocol_id) DO UPDATE
	UpsertUserProtocolMetadata(ctx context.Context, metadata []*entity.UserProtocolMetadata) error
}

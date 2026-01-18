package outbound

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// UserRepository defines the interface for user-related data persistence.
// This aggregate includes users and their protocol-specific metadata.
type UserRepository interface {
	// UpsertUsers upserts user records.
	// Conflict resolution: ON CONFLICT (chain_id, address) DO UPDATE
	// first_seen_block uses LEAST to keep the earliest block number.
	UpsertUsers(ctx context.Context, users []*entity.User) error

	// UpsertUserProtocolMetadata upserts user protocol metadata records.
	// This stores protocol-specific data like health factors, LTV, etc.
	// Conflict resolution: ON CONFLICT (user_id, protocol_id) DO UPDATE
	UpsertUserProtocolMetadata(ctx context.Context, metadata []*entity.UserProtocolMetadata) error
}

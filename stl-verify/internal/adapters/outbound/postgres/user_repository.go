package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that UserRepository implements outbound.UserRepository
var _ outbound.UserRepository = (*UserRepository)(nil)

// UserRepository is a PostgreSQL implementation of the outbound.UserRepository port.
type UserRepository struct {
	db     *sql.DB
	logger *slog.Logger
}

// NewUserRepository creates a new PostgreSQL User repository.
func NewUserRepository(db *sql.DB, logger *slog.Logger) *UserRepository {
	if logger == nil {
		logger = slog.Default()
	}
	return &UserRepository{db: db, logger: logger}
}

// UpsertUsers upserts user records.
func (r *UserRepository) UpsertUsers(ctx context.Context, users []*entity.User) error {
	if len(users) == 0 {
		return nil
	}

	const batchSize = 500
	for i := 0; i < len(users); i += batchSize {
		end := i + batchSize
		if end > len(users) {
			end = len(users)
		}
		batch := users[i:end]

		if err := r.upsertUserBatch(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

func (r *UserRepository) upsertUserBatch(ctx context.Context, users []*entity.User) error {
	if len(users) == 0 {
		return nil
	}

	var sb strings.Builder
	sb.WriteString(`
		INSERT INTO users (id, chain_id, address, first_seen_block, metadata, updated_at)
		VALUES `)

	args := make([]any, 0, len(users)*6)
	for i, user := range users {
		if i > 0 {
			sb.WriteString(", ")
		}
		baseIdx := i * 6
		sb.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, NOW())",
			baseIdx+1, baseIdx+2, baseIdx+3, baseIdx+4, baseIdx+5))

		metadata := marshalMetadata(user.Metadata)
		args = append(args, user.ID, user.ChainID, user.Address, user.FirstSeenBlock, metadata)
	}

	sb.WriteString(`
		ON CONFLICT (chain_id, address) DO UPDATE SET
			first_seen_block = LEAST(users.first_seen_block, EXCLUDED.first_seen_block),
			metadata = EXCLUDED.metadata,
			updated_at = NOW()
	`)

	_, err := r.db.ExecContext(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("failed to upsert user batch: %w", err)
	}
	return nil
}

// UpsertUserProtocolMetadata upserts user protocol metadata records.
func (r *UserRepository) UpsertUserProtocolMetadata(ctx context.Context, metadata []*entity.UserProtocolMetadata) error {
	if len(metadata) == 0 {
		return nil
	}

	const batchSize = 500
	for i := 0; i < len(metadata); i += batchSize {
		end := i + batchSize
		if end > len(metadata) {
			end = len(metadata)
		}
		batch := metadata[i:end]

		if err := r.upsertUserProtocolMetadataBatch(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

func (r *UserRepository) upsertUserProtocolMetadataBatch(ctx context.Context, metadata []*entity.UserProtocolMetadata) error {
	if len(metadata) == 0 {
		return nil
	}

	var sb strings.Builder
	sb.WriteString(`
		INSERT INTO user_protocol_metadata (id, user_id, protocol_id, metadata, updated_at)
		VALUES `)

	args := make([]any, 0, len(metadata)*4)
	for i, m := range metadata {
		if i > 0 {
			sb.WriteString(", ")
		}
		baseIdx := i * 4
		sb.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d, NOW())",
			baseIdx+1, baseIdx+2, baseIdx+3, baseIdx+4))

		metadataJSON := marshalMetadata(m.Metadata)

		args = append(args, m.ID, m.UserID, m.ProtocolID, metadataJSON)
	}

	sb.WriteString(`
		ON CONFLICT (user_id, protocol_id) DO UPDATE SET
			metadata = EXCLUDED.metadata,
			updated_at = NOW()
	`)

	_, err := r.db.ExecContext(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("failed to upsert user protocol metadata batch: %w", err)
	}
	return nil
}

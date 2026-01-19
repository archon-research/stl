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
	db        *sql.DB
	logger    *slog.Logger
	batchSize int
}

// NewUserRepository creates a new PostgreSQL User repository.
// If batchSize is <= 0, the default batch size from DefaultRepositoryConfig() is used.
// Returns an error if the database connection is nil.
//
// Note: This function does not verify that the database connection is alive.
// Use a separate health check or call db.Ping() if connection validation is needed.
func NewUserRepository(db *sql.DB, logger *slog.Logger, batchSize int) (*UserRepository, error) {
	if db == nil {
		return nil, fmt.Errorf("database connection cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if batchSize <= 0 {
		batchSize = DefaultRepositoryConfig().UserBatchSize
	}
	return &UserRepository{
		db:        db,
		logger:    logger,
		batchSize: batchSize,
	}, nil
}

// UpsertUsers upserts user records.
func (r *UserRepository) UpsertUsers(ctx context.Context, users []*entity.User) error {
	if len(users) == 0 {
		return nil
	}

	for i := 0; i < len(users); i += r.batchSize {
		end := i + r.batchSize
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
		INSERT INTO users (chain_id, address, first_seen_block, metadata, updated_at)
		VALUES `)

	args := make([]any, 0, len(users)*4)
	for i, user := range users {
		if i > 0 {
			sb.WriteString(", ")
		}
		baseIdx := i * 4
		sb.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d, NOW())",
			baseIdx+1, baseIdx+2, baseIdx+3, baseIdx+4))

		metadata, err := marshalMetadata(user.Metadata)
		if err != nil {
			return fmt.Errorf("failed to marshal user metadata for chain %d, address %x: %w", user.ChainID, user.Address, err)
		}
		args = append(args, user.ChainID, user.Address, user.FirstSeenBlock, metadata)
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

	for i := 0; i < len(metadata); i += r.batchSize {
		end := i + r.batchSize
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
		INSERT INTO user_protocol_metadata (user_id, protocol_id, metadata, updated_at)
		VALUES `)

	args := make([]any, 0, len(metadata)*3)
	for i, m := range metadata {
		if i > 0 {
			sb.WriteString(", ")
		}
		baseIdx := i * 3
		sb.WriteString(fmt.Sprintf("($%d, $%d, $%d, NOW())",
			baseIdx+1, baseIdx+2, baseIdx+3))

		metadataJSON, err := marshalMetadata(m.Metadata)
		if err != nil {
			return fmt.Errorf("failed to marshal user protocol metadata for user_id %d, protocol_id %d: %w", m.UserID, m.ProtocolID, err)
		}

		args = append(args, m.UserID, m.ProtocolID, metadataJSON)
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

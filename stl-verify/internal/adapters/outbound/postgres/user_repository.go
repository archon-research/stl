package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that UserRepository implements outbound.UserRepository
var _ outbound.UserRepository = (*UserRepository)(nil)

// UserRepository is a PostgreSQL implementation of the outbound.UserRepository port.
type UserRepository struct {
	pool      *pgxpool.Pool
	logger    *slog.Logger
	batchSize int
}

// NewUserRepository creates a new PostgreSQL User repository.
// If batchSize is <= 0, the default batch size from DefaultRepositoryConfig() is used.
// Returns an error if the database pool is nil.
//
// Note: This function does not verify that the database connection is alive.
// Use a separate health check or call pool.Ping() if connection validation is needed.
func NewUserRepository(pool *pgxpool.Pool, logger *slog.Logger, batchSize int) (*UserRepository, error) {
	if pool == nil {
		return nil, fmt.Errorf("database pool cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if batchSize <= 0 {
		batchSize = DefaultRepositoryConfig().UserBatchSize
	}
	return &UserRepository{
		pool:      pool,
		logger:    logger,
		batchSize: batchSize,
	}, nil
}

func (r *UserRepository) GetOrCreateUsers(ctx context.Context, tx pgx.Tx, users []entity.User) (map[common.Address]int64, error) {
	if len(users) == 0 {
		return make(map[common.Address]int64), nil
	}

	batch := &pgx.Batch{}
	for _, u := range users {
		batch.Queue(
			`INSERT INTO "user" (chain_id, address, first_seen_block, created_at, updated_at, metadata)
			 VALUES ($1, $2, $3, NOW(), NOW(), $4)
			 ON CONFLICT (chain_id, address) DO UPDATE SET
			     first_seen_block = LEAST("user".first_seen_block, EXCLUDED.first_seen_block),
			     updated_at = CASE
			         WHEN EXCLUDED.first_seen_block < "user".first_seen_block THEN NOW()
			         ELSE "user".updated_at
			     END
			 RETURNING id`,
			u.ChainID, u.Address.Bytes(), u.FirstSeenBlock, u.Metadata,
		)
	}

	br := tx.SendBatch(ctx, batch)
	defer br.Close()

	result := make(map[common.Address]int64, len(users))
	for i, u := range users {
		var id int64
		if err := br.QueryRow().Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to get or create user %d (%s): %w", i, u.Address.Hex(), err)
		}
		result[u.Address] = id
	}

	return result, nil
}

func (r *UserRepository) GetOrCreateUser(ctx context.Context, tx pgx.Tx, user entity.User) (int64, error) {
	var userID int64

	// Upsert: on conflict preserve the earliest first_seen_block via LEAST().
	// This is safe for concurrent workers processing different blocks for the same user —
	// whichever worker wins the INSERT race, the loser's LEAST() merge still produces
	// the correct minimum first_seen_block.
	err := tx.QueryRow(ctx,
		`INSERT INTO "user" (chain_id, address, first_seen_block, created_at, updated_at, metadata)
		 VALUES ($1, $2, $3, NOW(), NOW(), $4)
		 ON CONFLICT (chain_id, address) DO UPDATE SET
		     first_seen_block = LEAST("user".first_seen_block, EXCLUDED.first_seen_block),
		     updated_at = CASE
		         WHEN EXCLUDED.first_seen_block < "user".first_seen_block THEN NOW()
		         ELSE "user".updated_at
		     END
		 RETURNING id`,
		user.ChainID, user.Address.Bytes(), user.FirstSeenBlock, user.Metadata).Scan(&userID)
	if err != nil {
		return 0, fmt.Errorf("failed to get or create user: %w", err)
	}

	r.logger.Debug("user upserted", "address", user.Address.Hex(), "id", userID)
	return userID, nil
}

// UpsertUserProtocolMetadata upserts user protocol metadata records atomically atomically.
// All records are inserted in a single transaction - if any batch fails, all changes are rolled back.
func (r *UserRepository) UpsertUserProtocolMetadata(ctx context.Context, metadata []*entity.UserProtocolMetadata) error {
	if len(metadata) == 0 {
		return nil
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer rollback(ctx, tx, r.logger)

	for i := 0; i < len(metadata); i += r.batchSize {
		end := min(i+r.batchSize, len(metadata))
		batch := metadata[i:end]

		if err := r.upsertUserProtocolMetadataBatch(ctx, tx, batch); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

func (r *UserRepository) upsertUserProtocolMetadataBatch(ctx context.Context, tx pgx.Tx, metadata []*entity.UserProtocolMetadata) error {
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

	_, err := tx.Exec(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("failed to upsert user protocol metadata batch: %w", err)
	}
	return nil
}

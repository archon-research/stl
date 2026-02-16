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

// Compile-time check.
var _ outbound.MapleRepository = (*MapleRepository)(nil)

// MapleRepository is a PostgreSQL implementation of the outbound.MapleRepository port.
type MapleRepository struct {
	pool      *pgxpool.Pool
	logger    *slog.Logger
	batchSize int
}

// NewMapleRepository creates a new PostgreSQL Maple repository.
func NewMapleRepository(pool *pgxpool.Pool, logger *slog.Logger, batchSize int) (*MapleRepository, error) {
	if pool == nil {
		return nil, fmt.Errorf("database pool cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if batchSize <= 0 {
		batchSize = DefaultRepositoryConfig().PositionBatchSize
	}
	return &MapleRepository{
		pool:      pool,
		logger:    logger,
		batchSize: batchSize,
	}, nil
}

// UpsertPositions persists user positions with upsert semantics.
func (r *MapleRepository) UpsertPositions(ctx context.Context, positions []*entity.MaplePosition) error {
	if len(positions) == 0 {
		return nil
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer rollback(ctx, tx, r.logger)

	for start := 0; start < len(positions); start += r.batchSize {
		end := start + r.batchSize
		if end > len(positions) {
			end = len(positions)
		}
		batch := positions[start:end]

		if err := r.upsertPositionBatch(ctx, tx, batch); err != nil {
			return fmt.Errorf("upserting position batch: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}
	return nil
}

func (r *MapleRepository) upsertPositionBatch(ctx context.Context, tx pgx.Tx, batch []*entity.MaplePosition) error {
	var sb strings.Builder
	sb.WriteString(`INSERT INTO maple_position
		(user_id, protocol_id, pool_address, pool_name, asset_symbol, asset_decimals, lending_balance, snapshot_block, snapshot_time)
		VALUES `)

	args := make([]any, 0, len(batch)*9)
	for i, pos := range batch {
		if i > 0 {
			sb.WriteString(", ")
		}
		base := i * 9
		fmt.Fprintf(&sb, "($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9)

		balance, err := bigIntToNumeric(pos.LendingBalance)
		if err != nil {
			return fmt.Errorf("converting lending balance: %w", err)
		}

		args = append(args,
			pos.UserID,
			pos.ProtocolID,
			pos.PoolAddress.Bytes(),
			pos.PoolName,
			pos.AssetSymbol,
			pos.AssetDecimals,
			balance,
			pos.SnapshotBlock,
			pos.SnapshotTime,
		)
	}

	sb.WriteString(` ON CONFLICT (user_id, protocol_id, pool_address, snapshot_block)
		DO UPDATE SET
			pool_name = EXCLUDED.pool_name,
			asset_symbol = EXCLUDED.asset_symbol,
			asset_decimals = EXCLUDED.asset_decimals,
			lending_balance = EXCLUDED.lending_balance,
			snapshot_time = EXCLUDED.snapshot_time`)

	_, err := tx.Exec(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("executing position upsert: %w", err)
	}
	return nil
}

// UpsertPoolCollateral persists pool collateral snapshots with upsert semantics.
func (r *MapleRepository) UpsertPoolCollateral(ctx context.Context, collaterals []*entity.MaplePoolCollateral) error {
	if len(collaterals) == 0 {
		return nil
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer rollback(ctx, tx, r.logger)

	for start := 0; start < len(collaterals); start += r.batchSize {
		end := start + r.batchSize
		if end > len(collaterals) {
			end = len(collaterals)
		}
		batch := collaterals[start:end]

		if err := r.upsertCollateralBatch(ctx, tx, batch); err != nil {
			return fmt.Errorf("upserting collateral batch: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}
	return nil
}

func (r *MapleRepository) upsertCollateralBatch(ctx context.Context, tx pgx.Tx, batch []*entity.MaplePoolCollateral) error {
	var sb strings.Builder
	sb.WriteString(`INSERT INTO maple_pool_collateral
		(pool_address, pool_name, asset, asset_decimals, asset_value_usd, pool_tvl, snapshot_block, snapshot_time)
		VALUES `)

	args := make([]any, 0, len(batch)*8)
	for i, col := range batch {
		if i > 0 {
			sb.WriteString(", ")
		}
		base := i * 8
		fmt.Fprintf(&sb, "($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8)

		assetValue, err := bigIntToNumeric(col.AssetValueUSD)
		if err != nil {
			return fmt.Errorf("converting asset value: %w", err)
		}
		tvl, err := bigIntToNumeric(col.PoolTVL)
		if err != nil {
			return fmt.Errorf("converting pool TVL: %w", err)
		}

		args = append(args,
			col.PoolAddress.Bytes(),
			col.PoolName,
			col.Asset,
			col.AssetDecimals,
			assetValue,
			tvl,
			col.SnapshotBlock,
			col.SnapshotTime,
		)
	}

	sb.WriteString(` ON CONFLICT (pool_address, asset, snapshot_block)
		DO UPDATE SET
			pool_name = EXCLUDED.pool_name,
			asset_decimals = EXCLUDED.asset_decimals,
			asset_value_usd = EXCLUDED.asset_value_usd,
			pool_tvl = EXCLUDED.pool_tvl,
			snapshot_time = EXCLUDED.snapshot_time`)

	_, err := tx.Exec(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("executing collateral upsert: %w", err)
	}
	return nil
}

// GetUsersWithMaplePositions returns users associated with the Maple protocol.
// It looks up users that have user_protocol_metadata entries for the given protocol.
func (r *MapleRepository) GetUsersWithMaplePositions(ctx context.Context, protocolID int64) ([]outbound.MapleTrackedUser, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT u.id, u.address
		 FROM "user" u
		 JOIN user_protocol_metadata upm ON u.id = upm.user_id
		 WHERE upm.protocol_id = $1`,
		protocolID)
	if err != nil {
		return nil, fmt.Errorf("querying maple users: %w", err)
	}
	defer rows.Close()

	var users []outbound.MapleTrackedUser
	for rows.Next() {
		var user outbound.MapleTrackedUser
		var addrBytes []byte
		if err := rows.Scan(&user.UserID, &addrBytes); err != nil {
			return nil, fmt.Errorf("scanning maple user: %w", err)
		}
		user.Address = common.BytesToAddress(addrBytes)
		users = append(users, user)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating maple users: %w", err)
	}

	return users, nil
}

// RegisterUser creates a user and associates them with the Maple protocol.
// Returns the user ID.
func (r *MapleRepository) RegisterUser(ctx context.Context, chainID int64, address common.Address, protocolID int64) (int64, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("beginning transaction: %w", err)
	}
	defer rollback(ctx, tx, r.logger)

	// Upsert user.
	var userID int64
	err = tx.QueryRow(ctx,
		`INSERT INTO "user" (chain_id, address, first_seen_block, metadata)
		 VALUES ($1, $2, 1, '{}')
		 ON CONFLICT (chain_id, address) DO UPDATE SET updated_at = NOW()
		 RETURNING id`,
		chainID, address.Bytes()).Scan(&userID)
	if err != nil {
		return 0, fmt.Errorf("upserting user: %w", err)
	}

	// Upsert protocol metadata association.
	_, err = tx.Exec(ctx,
		`INSERT INTO user_protocol_metadata (user_id, protocol_id, metadata)
		 VALUES ($1, $2, '{}')
		 ON CONFLICT (user_id, protocol_id) DO NOTHING`,
		userID, protocolID)
	if err != nil {
		return 0, fmt.Errorf("upserting user protocol metadata: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("committing transaction: %w", err)
	}

	return userID, nil
}

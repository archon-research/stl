package postgres

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that MapleRepository implements outbound.MapleRepository.
var _ outbound.MapleRepository = (*MapleRepository)(nil)

// MapleRepository is a PostgreSQL implementation of the outbound.MapleRepository port.
type MapleRepository struct {
	pool    *pgxpool.Pool
	logger  *slog.Logger
	buildID buildregistry.BuildID
}

// NewMapleRepository creates a new PostgreSQL Maple repository.
func NewMapleRepository(pool *pgxpool.Pool, logger *slog.Logger, buildID buildregistry.BuildID) (*MapleRepository, error) {
	if pool == nil {
		return nil, fmt.Errorf("database pool cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &MapleRepository{
		pool:    pool,
		logger:  logger.With("component", "maple-repository"),
		buildID: buildID,
	}, nil
}

// GetAllVaults retrieves all known Syrup vaults for a chain, keyed by contract address.
func (r *MapleRepository) GetAllVaults(ctx context.Context, chainID int64) (map[common.Address]*entity.MapleVault, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, protocol_id, address, name, symbol, asset_token_id,
		        pool_address, vault_version, created_at_block
		   FROM maple_vault
		  WHERE chain_id = $1`, chainID)
	if err != nil {
		return nil, fmt.Errorf("querying maple vaults: %w", err)
	}
	defer rows.Close()

	vaults := make(map[common.Address]*entity.MapleVault)
	for rows.Next() {
		var (
			v      entity.MapleVault
			name   *string
			symbol *string
		)
		if err := rows.Scan(
			&v.ID, &v.ProtocolID, &v.Address, &name, &symbol,
			&v.AssetTokenID, &v.PoolAddress, &v.VaultVersion, &v.CreatedAtBlock,
		); err != nil {
			return nil, fmt.Errorf("scanning maple vault: %w", err)
		}
		if name != nil {
			v.Name = *name
		}
		if symbol != nil {
			v.Symbol = *symbol
		}
		v.ChainID = chainID
		vaults[common.BytesToAddress(v.Address)] = &v
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating maple vaults: %w", err)
	}
	return vaults, nil
}

// SaveVaultState saves a per-block Syrup vault snapshot inside an external transaction.
func (r *MapleRepository) SaveVaultState(ctx context.Context, tx pgx.Tx, state *entity.MapleVaultState) error {
	totalAssets, err := bigIntToNumeric(state.TotalAssets)
	if err != nil {
		return fmt.Errorf("converting total_assets: %w", err)
	}
	totalSupply, err := bigIntToNumeric(state.TotalSupply)
	if err != nil {
		return fmt.Errorf("converting total_supply: %w", err)
	}
	sharePrice, err := bigIntToNumeric(state.SharePrice)
	if err != nil {
		return fmt.Errorf("converting share_price: %w", err)
	}

	var underlyingUSD, syrupUSD *string
	if state.UnderlyingPriceUSD != nil {
		s := state.UnderlyingPriceUSD.String()
		underlyingUSD = &s
	}
	if state.SyrupPriceUSD != nil {
		s := state.SyrupPriceUSD.String()
		syrupUSD = &s
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO maple_vault_state (
			maple_vault_id, block_number, block_version, timestamp,
			total_assets, total_supply, share_price,
			underlying_price_usd, syrup_price_usd, build_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (maple_vault_id, block_number, block_version, processing_version, timestamp)
		DO NOTHING`,
		state.MapleVaultID, state.BlockNumber, state.BlockVersion, state.BlockTimestamp,
		totalAssets, totalSupply, sharePrice,
		underlyingUSD, syrupUSD, int(r.buildID),
	)
	if err != nil {
		return fmt.Errorf("saving maple vault state: %w", err)
	}
	return nil
}

// SaveVaultPositions writes per-user position snapshots for a single block in one batch.
func (r *MapleRepository) SaveVaultPositions(ctx context.Context, tx pgx.Tx, positions []*entity.MapleVaultPosition) error {
	if len(positions) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	const q = `
		INSERT INTO maple_vault_position (
			user_id, maple_vault_id, block_number, block_version, timestamp,
			shares, assets, build_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (user_id, maple_vault_id, block_number, block_version, processing_version, timestamp)
		DO NOTHING`

	for _, p := range positions {
		shares, err := bigIntToNumeric(p.Shares)
		if err != nil {
			return fmt.Errorf("converting shares for user %d: %w", p.UserID, err)
		}
		assets, err := bigIntToNumeric(p.Assets)
		if err != nil {
			return fmt.Errorf("converting assets for user %d: %w", p.UserID, err)
		}
		batch.Queue(q,
			p.UserID, p.MapleVaultID, p.BlockNumber, p.BlockVersion, p.BlockTimestamp,
			shares, assets, int(r.buildID),
		)
	}

	br := tx.SendBatch(ctx, batch)
	defer br.Close()
	for i := 0; i < batch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("batch insert maple_vault_position[%d]: %w", i, err)
		}
	}
	return nil
}

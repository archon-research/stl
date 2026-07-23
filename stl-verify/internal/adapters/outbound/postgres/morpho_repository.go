package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that MorphoRepository implements outbound.MorphoRepository.
var _ outbound.MorphoRepository = (*MorphoRepository)(nil)

// MorphoRepository is a PostgreSQL implementation of the outbound.MorphoRepository port.
type MorphoRepository struct {
	pool    *pgxpool.Pool
	logger  *slog.Logger
	buildID buildregistry.BuildID
}

// NewMorphoRepository creates a new PostgreSQL Morpho repository.
func NewMorphoRepository(pool *pgxpool.Pool, logger *slog.Logger, buildID buildregistry.BuildID) (*MorphoRepository, error) {
	if pool == nil {
		return nil, fmt.Errorf("database pool cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &MorphoRepository{
		pool:    pool,
		logger:  logger,
		buildID: buildID,
	}, nil
}

// GetOrCreateMarket retrieves or creates a Morpho Blue market.
func (r *MorphoRepository) GetOrCreateMarket(ctx context.Context, tx pgx.Tx, market *entity.MorphoMarket) (int64, error) {
	lltv, err := bigIntToNumeric(market.LLTV)
	if err != nil {
		return 0, fmt.Errorf("converting lltv: %w", err)
	}

	var id int64
	// The no-op SET is required so that DO UPDATE ... RETURNING id works on conflict.
	err = tx.QueryRow(ctx,
		`INSERT INTO morpho_market (chain_id, protocol_id, market_id, loan_token_id, collateral_token_id, oracle_address, irm_address, lltv, created_at_block)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		 ON CONFLICT (chain_id, market_id) DO UPDATE SET protocol_id = EXCLUDED.protocol_id
		 RETURNING id`,
		market.ChainID, market.ProtocolID, market.MarketID.Bytes(), market.LoanTokenID, market.CollateralTokenID,
		market.OracleAddress.Bytes(), market.IrmAddress.Bytes(), lltv, market.CreatedAtBlock,
	).Scan(&id)

	if err != nil {
		return 0, fmt.Errorf("upserting morpho market: %w", err)
	}
	return id, nil
}

// GetMarketByMarketID retrieves a market by its chain ID and 32-byte market ID hash.
func (r *MorphoRepository) GetMarketByMarketID(ctx context.Context, chainID int64, marketID common.Hash) (*entity.MorphoMarket, error) {
	var (
		lltvStr            string
		oracleAddressBytes []byte
		irmAddressBytes    []byte
		id                 int64
		protocolID         int64
		loanTokenID        int64
		collateralTokenID  int64
		createdAtBlock     int64
	)

	err := r.pool.QueryRow(ctx,
		`SELECT id, protocol_id, loan_token_id, collateral_token_id, oracle_address, irm_address, lltv, created_at_block
		 FROM morpho_market WHERE chain_id = $1 AND market_id = $2`,
		chainID, marketID.Bytes(),
	).Scan(&id, &protocolID, &loanTokenID, &collateralTokenID,
		&oracleAddressBytes, &irmAddressBytes, &lltvStr, &createdAtBlock)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("querying morpho market: %w", err)
	}

	lltv, err := numericToBigInt(lltvStr)
	if err != nil {
		return nil, fmt.Errorf("converting lltv: %w", err)
	}

	m := &entity.MorphoMarket{
		ID:                id,
		ChainID:           chainID,
		ProtocolID:        protocolID,
		MarketID:          marketID,
		LoanTokenID:       loanTokenID,
		CollateralTokenID: collateralTokenID,
		OracleAddress:     common.BytesToAddress(oracleAddressBytes),
		IrmAddress:        common.BytesToAddress(irmAddressBytes),
		LLTV:              lltv,
		CreatedAtBlock:    createdAtBlock,
	}
	return m, nil
}

// SaveMarketState saves a market state snapshot within an external transaction.
func (r *MorphoRepository) SaveMarketState(ctx context.Context, tx pgx.Tx, state *entity.MorphoMarketState) error {
	totalSupplyAssets, err := bigIntToNumeric(state.TotalSupplyAssets)
	if err != nil {
		return fmt.Errorf("converting total_supply_assets: %w", err)
	}
	totalSupplyShares, err := bigIntToNumeric(state.TotalSupplyShares)
	if err != nil {
		return fmt.Errorf("converting total_supply_shares: %w", err)
	}
	totalBorrowAssets, err := bigIntToNumeric(state.TotalBorrowAssets)
	if err != nil {
		return fmt.Errorf("converting total_borrow_assets: %w", err)
	}
	totalBorrowShares, err := bigIntToNumeric(state.TotalBorrowShares)
	if err != nil {
		return fmt.Errorf("converting total_borrow_shares: %w", err)
	}
	fee, err := bigIntToNumeric(state.Fee)
	if err != nil {
		return fmt.Errorf("converting fee: %w", err)
	}

	var prevBorrowRate, interestAccrued, feeShares *string
	if state.PrevBorrowRate != nil {
		s := state.PrevBorrowRate.String()
		prevBorrowRate = &s
	}
	if state.InterestAccrued != nil {
		s := state.InterestAccrued.String()
		interestAccrued = &s
	}
	if state.FeeShares != nil {
		s := state.FeeShares.String()
		feeShares = &s
	}

	// ON CONFLICT DO NOTHING: all events within one block yield the same on-chain
	// snapshot (eth_call reads end-of-block state), so the first insert captures
	// the correct state. Reorgs use a different block_version, so they insert cleanly.
	_, err = tx.Exec(ctx,
		`INSERT INTO morpho_market_state (morpho_market_id, block_number, block_version, timestamp, total_supply_assets, total_supply_shares, total_borrow_assets, total_borrow_shares, last_update, fee, prev_borrow_rate, interest_accrued, fee_shares, build_id)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
		 ON CONFLICT (morpho_market_id, block_number, block_version, processing_version, timestamp) DO NOTHING`,
		state.MorphoMarketID, state.BlockNumber, state.BlockVersion, state.BlockTimestamp,
		totalSupplyAssets, totalSupplyShares, totalBorrowAssets, totalBorrowShares,
		state.LastUpdate, fee, prevBorrowRate, interestAccrued, feeShares, int(r.buildID),
	)
	if err != nil {
		return fmt.Errorf("saving morpho market state: %w", err)
	}
	return nil
}

// SaveMarketPosition saves a user market position snapshot within an external transaction.
func (r *MorphoRepository) SaveMarketPosition(ctx context.Context, tx pgx.Tx, position *entity.MorphoMarketPosition) error {
	supplyShares, err := bigIntToNumeric(position.SupplyShares)
	if err != nil {
		return fmt.Errorf("converting supply_shares: %w", err)
	}
	borrowShares, err := bigIntToNumeric(position.BorrowShares)
	if err != nil {
		return fmt.Errorf("converting borrow_shares: %w", err)
	}
	collateral, err := bigIntToNumeric(position.Collateral)
	if err != nil {
		return fmt.Errorf("converting collateral: %w", err)
	}
	supplyAssets, err := bigIntToNumeric(position.SupplyAssets)
	if err != nil {
		return fmt.Errorf("converting supply_assets: %w", err)
	}
	borrowAssets, err := bigIntToNumeric(position.BorrowAssets)
	if err != nil {
		return fmt.Errorf("converting borrow_assets: %w", err)
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO morpho_market_position (user_id, morpho_market_id, block_number, block_version, timestamp, supply_shares, borrow_shares, collateral, supply_assets, borrow_assets, build_id)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		 ON CONFLICT (user_id, morpho_market_id, block_number, block_version, processing_version, timestamp) DO NOTHING`,
		position.UserID, position.MorphoMarketID, position.BlockNumber, position.BlockVersion, position.Timestamp,
		supplyShares, borrowShares, collateral, supplyAssets, borrowAssets, int(r.buildID),
	)
	if err != nil {
		return fmt.Errorf("saving morpho market position: %w", err)
	}
	return nil
}

// GetOrCreateVault retrieves or creates a MetaMorpho vault.
func (r *MorphoRepository) GetOrCreateVault(ctx context.Context, tx pgx.Tx, vault *entity.MorphoVault) (int64, error) {
	var id int64
	// The no-op SET is required so that DO UPDATE ... RETURNING id works on conflict.
	err := tx.QueryRow(ctx,
		`INSERT INTO morpho_vault (chain_id, protocol_id, address, name, symbol, asset_token_id, vault_version, created_at_block)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 ON CONFLICT (chain_id, address) DO UPDATE SET id = morpho_vault.id
		 RETURNING id`,
		vault.ChainID, vault.ProtocolID, vault.Address, vault.Name, vault.Symbol,
		vault.AssetTokenID, vault.VaultVersion, vault.CreatedAtBlock,
	).Scan(&id)

	if err != nil {
		return 0, fmt.Errorf("upserting morpho vault: %w", err)
	}
	return id, nil
}

// GetVaultByAddress retrieves a vault by its chain ID and contract address.
func (r *MorphoRepository) GetVaultByAddress(ctx context.Context, chainID int64, address common.Address) (*entity.MorphoVault, error) {
	var v entity.MorphoVault
	err := r.pool.QueryRow(ctx,
		`SELECT id, protocol_id, name, symbol, asset_token_id, vault_version, created_at_block
		 FROM morpho_vault WHERE chain_id = $1 AND address = $2`,
		chainID, address.Bytes(),
	).Scan(&v.ID, &v.ProtocolID, &v.Name, &v.Symbol, &v.AssetTokenID, &v.VaultVersion, &v.CreatedAtBlock)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("querying morpho vault: %w", err)
	}
	v.ChainID = chainID
	v.Address = address.Bytes()
	return &v, nil
}

// GetAllVaults retrieves all known vaults for a chain, keyed by contract address.
func (r *MorphoRepository) GetAllVaults(ctx context.Context, chainID int64) (map[common.Address]*entity.MorphoVault, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, protocol_id, address, name, symbol, asset_token_id, vault_version, created_at_block
		 FROM morpho_vault WHERE chain_id = $1`, chainID)
	if err != nil {
		return nil, fmt.Errorf("querying vaults: %w", err)
	}
	defer rows.Close()

	vaults := make(map[common.Address]*entity.MorphoVault)
	for rows.Next() {
		var v entity.MorphoVault
		if err := rows.Scan(&v.ID, &v.ProtocolID, &v.Address, &v.Name, &v.Symbol, &v.AssetTokenID, &v.VaultVersion, &v.CreatedAtBlock); err != nil {
			return nil, fmt.Errorf("scanning vault: %w", err)
		}
		v.ChainID = chainID
		vaults[common.BytesToAddress(v.Address)] = &v
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating vaults: %w", err)
	}
	return vaults, nil
}

// SaveVaultState saves a vault state snapshot within an external transaction.
func (r *MorphoRepository) SaveVaultState(ctx context.Context, tx pgx.Tx, state *entity.MorphoVaultState) error {
	totalAssets, err := bigIntToNumeric(state.TotalAssets)
	if err != nil {
		return fmt.Errorf("converting total_assets: %w", err)
	}
	totalShares, err := bigIntToNumeric(state.TotalShares)
	if err != nil {
		return fmt.Errorf("converting total_shares: %w", err)
	}

	var feeShares, newTotalAssets, previousTotalAssets, managementFeeShares *string
	if state.FeeShares != nil {
		s := state.FeeShares.String()
		feeShares = &s
	}
	if state.NewTotalAssets != nil {
		s := state.NewTotalAssets.String()
		newTotalAssets = &s
	}
	if state.PreviousTotalAssets != nil {
		s := state.PreviousTotalAssets.String()
		previousTotalAssets = &s
	}
	if state.ManagementFeeShares != nil {
		s := state.ManagementFeeShares.String()
		managementFeeShares = &s
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO morpho_vault_state (morpho_vault_id, block_number, block_version, timestamp, total_assets, total_shares, fee_shares, new_total_assets, previous_total_assets, management_fee_shares, build_id)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		 ON CONFLICT (morpho_vault_id, block_number, block_version, processing_version, timestamp) DO NOTHING`,
		state.MorphoVaultID, state.BlockNumber, state.BlockVersion, state.BlockTimestamp,
		totalAssets, totalShares, feeShares, newTotalAssets, previousTotalAssets, managementFeeShares, int(r.buildID),
	)
	if err != nil {
		return fmt.Errorf("saving morpho vault state: %w", err)
	}
	return nil
}

// SaveVaultPosition saves a user vault position snapshot within an external transaction.
func (r *MorphoRepository) SaveVaultPosition(ctx context.Context, tx pgx.Tx, position *entity.MorphoVaultPosition) error {
	shares, err := bigIntToNumeric(position.Shares)
	if err != nil {
		return fmt.Errorf("converting shares: %w", err)
	}
	assets, err := bigIntToNumeric(position.Assets)
	if err != nil {
		return fmt.Errorf("converting assets: %w", err)
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO morpho_vault_position (user_id, morpho_vault_id, block_number, block_version, timestamp, shares, assets, build_id)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 ON CONFLICT (user_id, morpho_vault_id, block_number, block_version, processing_version, timestamp) DO NOTHING`,
		position.UserID, position.MorphoVaultID, position.BlockNumber, position.BlockVersion, position.Timestamp,
		shares, assets, int(r.buildID),
	)
	if err != nil {
		return fmt.Errorf("saving morpho vault position: %w", err)
	}
	return nil
}

// GetOrCreateAdapter retrieves or creates a VaultV2 liquidity adapter registry
// row for (morpho_vault_id, address), converging late-arriving observations onto
// the incarnation whose lifetime window they belong to rather than duplicating.
//
// The candidate's added_at_block is matched against incarnations in three steps:
//
//  1. If an ACTIVE row (removed_at_block IS NULL) exists it is reused, its
//     added_at_block converging downward to LEAST(existing, candidate). This lets
//     the backfiller replay the TRUE AddAdapter@X for an adapter the live stream
//     lazily registered at first-seen block Y>X collapse onto one active row.
//  2. Otherwise, if a CLOSED incarnation covers the candidate (removed_at_block
//     >= candidate added_at_block), converge onto the earliest-closing such row
//     (UPDATE its added_at_block down). Without this, replaying the true
//     AddAdapter@W earlier than a live lazy-register+removal@X (W<X) would find no
//     active row and INSERT a second, spuriously-ACTIVE incarnation — resurrecting
//     a de-registered adapter into GetActiveAdaptersByVault / realAssets forever.
//  3. Only a candidate added strictly after every prior removal is a genuinely new
//     incarnation and is INSERTed. The UNIQUE key includes added_at_block, so the
//     ON CONFLICT no-op SET keeps a same-block backfill re-run idempotent.
func (r *MorphoRepository) GetOrCreateAdapter(ctx context.Context, tx pgx.Tx, adapter *entity.MorphoAdapter) (int64, error) {
	if err := adapter.Validate(); err != nil {
		return 0, fmt.Errorf("validating morpho adapter: %w", err)
	}

	// Serialize the "does an active row already exist?" read-then-write on the
	// block-free natural key (vault, address): ON CONFLICT alone cannot guard a
	// decision made before the insert (ADR-0002 §3). Without it two concurrent
	// live-vs-backfill writers could both observe no active row and insert two
	// active rows at different added_at_block. The key is deliberately block-free
	// so every writer of this adapter serializes on the same lock regardless of
	// the block it carries.
	lockKey := fmt.Sprintf("morpho_adapter|%d|%x", adapter.MorphoVaultID, adapter.Address)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, lockKey); err != nil {
		return 0, fmt.Errorf("locking %q: %w", lockKey, err)
	}

	var id int64
	err := tx.QueryRow(ctx,
		`UPDATE morpho_adapter
		 SET added_at_block = LEAST(added_at_block, $3)
		 WHERE morpho_vault_id = $1 AND address = $2 AND removed_at_block IS NULL
		 RETURNING id`,
		adapter.MorphoVaultID, adapter.Address, adapter.AddedAtBlock,
	).Scan(&id)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("converging active morpho adapter: %w", err)
	}

	// No active incarnation. If a CLOSED incarnation's window covers the candidate
	// (removed_at_block >= candidate added_at_block), the candidate is a late
	// observation of that same closed window — converge onto the earliest-closing
	// such row instead of inserting a spuriously-active duplicate.
	err = tx.QueryRow(ctx,
		`UPDATE morpho_adapter
		 SET added_at_block = LEAST(added_at_block, $3)
		 WHERE id = (
		     SELECT id FROM morpho_adapter
		     WHERE morpho_vault_id = $1 AND address = $2
		       AND removed_at_block IS NOT NULL AND removed_at_block >= $3
		     ORDER BY removed_at_block ASC
		     LIMIT 1
		 )
		 RETURNING id`,
		adapter.MorphoVaultID, adapter.Address, adapter.AddedAtBlock,
	).Scan(&id)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("converging closed morpho adapter incarnation: %w", err)
	}

	// Genuinely new incarnation (candidate added after every prior removal): insert
	// a new row. The ON CONFLICT no-op SET keeps a same-block replay (backfill
	// re-run of the same AddAdapter) idempotent.
	err = tx.QueryRow(ctx,
		`INSERT INTO morpho_adapter (morpho_vault_id, address, asset_token_id, adapter_type, added_at_block, removed_at_block)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (morpho_vault_id, address, added_at_block) DO UPDATE SET id = morpho_adapter.id
		 RETURNING id`,
		adapter.MorphoVaultID, adapter.Address, adapter.AssetTokenID, int16(adapter.AdapterType),
		adapter.AddedAtBlock, adapter.RemovedAtBlock,
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("upserting morpho adapter: %w", err)
	}
	return id, nil
}

// MarkAdapterRemoved records the block at which an adapter was de-registered.
func (r *MorphoRepository) MarkAdapterRemoved(ctx context.Context, tx pgx.Tx, morphoVaultID int64, address []byte, removedAtBlock int64) error {
	// The OR removed_at_block = $3 branch makes backfill replays idempotent: a
	// second removal at the same block re-matches the already-removed row.
	//
	// added_at_block <= $3 scopes the removal to the adapter incarnation that was
	// live at the removal block. Without it, replaying an old RemoveAdapter@X for
	// an adapter that was later re-added (added_at_block a2 > X) would match the
	// active re-added row via the removed_at_block IS NULL arm and wrongly close
	// it with a removal block earlier than its own registration.
	tag, err := tx.Exec(ctx,
		`UPDATE morpho_adapter SET removed_at_block = $3
		 WHERE morpho_vault_id = $1 AND address = $2 AND added_at_block <= $3
		   AND (removed_at_block IS NULL OR removed_at_block = $3)`,
		morphoVaultID, address, removedAtBlock,
	)
	if err != nil {
		return fmt.Errorf("marking morpho adapter removed: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("marking morpho adapter removed: no active adapter for vault %d address %x (unknown adapter or already removed at a different block)",
			morphoVaultID, address)
	}
	return nil
}

// GetActiveAdapter retrieves the active adapter for a vault and address, reading
// through the caller's transaction so an adapter added earlier in the same tx is
// visible (read-your-writes).
func (r *MorphoRepository) GetActiveAdapter(ctx context.Context, tx pgx.Tx, morphoVaultID int64, address []byte) (*entity.MorphoAdapter, error) {
	var a entity.MorphoAdapter
	err := tx.QueryRow(ctx,
		`SELECT id, asset_token_id, adapter_type, added_at_block, removed_at_block
		 FROM morpho_adapter
		 WHERE morpho_vault_id = $1 AND address = $2 AND removed_at_block IS NULL`,
		morphoVaultID, address,
	).Scan(&a.ID, &a.AssetTokenID, &a.AdapterType, &a.AddedAtBlock, &a.RemovedAtBlock)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("querying active morpho adapter: %w", err)
	}
	a.MorphoVaultID = morphoVaultID
	a.Address = address
	return &a, nil
}

// GetActiveAdaptersByVault retrieves all currently-active adapters for a vault.
func (r *MorphoRepository) GetActiveAdaptersByVault(ctx context.Context, morphoVaultID int64) ([]*entity.MorphoAdapter, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, address, asset_token_id, adapter_type, added_at_block, removed_at_block
		 FROM morpho_adapter
		 WHERE morpho_vault_id = $1 AND removed_at_block IS NULL
		 ORDER BY id`,
		morphoVaultID,
	)
	if err != nil {
		return nil, fmt.Errorf("querying active morpho adapters: %w", err)
	}
	defer rows.Close()

	var adapters []*entity.MorphoAdapter
	for rows.Next() {
		a := &entity.MorphoAdapter{MorphoVaultID: morphoVaultID}
		if err := rows.Scan(&a.ID, &a.Address, &a.AssetTokenID, &a.AdapterType, &a.AddedAtBlock, &a.RemovedAtBlock); err != nil {
			return nil, fmt.Errorf("scanning morpho adapter: %w", err)
		}
		adapters = append(adapters, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating morpho adapters: %w", err)
	}
	return adapters, nil
}

// SaveAdapterState saves an adapter realAssets() snapshot within an external transaction.
func (r *MorphoRepository) SaveAdapterState(ctx context.Context, tx pgx.Tx, state *entity.MorphoAdapterState) error {
	if err := state.Validate(); err != nil {
		return fmt.Errorf("validating morpho adapter state: %w", err)
	}

	realAssets, err := bigIntToNumeric(state.RealAssets)
	if err != nil {
		return fmt.Errorf("converting real_assets: %w", err)
	}

	// processing_version is assigned by the trigger; ON CONFLICT DO NOTHING
	// dedupes same-build retries (see SaveMarketState for the rationale).
	_, err = tx.Exec(ctx,
		`INSERT INTO morpho_adapter_state (morpho_adapter_id, block_number, block_version, timestamp, real_assets, build_id)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (morpho_adapter_id, block_number, block_version, timestamp, processing_version) DO NOTHING`,
		state.MorphoAdapterID, state.BlockNumber, state.BlockVersion, state.Timestamp, realAssets, int(r.buildID),
	)
	if err != nil {
		return fmt.Errorf("saving morpho adapter state: %w", err)
	}
	return nil
}

// SaveVaultCap saves a VaultV2 allocation-cap snapshot within an external transaction.
func (r *MorphoRepository) SaveVaultCap(ctx context.Context, tx pgx.Tx, vaultCap *entity.MorphoVaultCap) error {
	if err := vaultCap.Validate(); err != nil {
		return fmt.Errorf("validating morpho vault cap: %w", err)
	}

	absoluteCap, err := bigIntToNumeric(vaultCap.AbsoluteCap)
	if err != nil {
		return fmt.Errorf("converting absolute_cap: %w", err)
	}
	relativeCap, err := bigIntToNumeric(vaultCap.RelativeCap)
	if err != nil {
		return fmt.Errorf("converting relative_cap: %w", err)
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO morpho_vault_cap (morpho_vault_id, cap_id, id_data, absolute_cap, relative_cap, block_number, block_version, timestamp, build_id)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		 ON CONFLICT (morpho_vault_id, cap_id, block_number, block_version, timestamp, processing_version) DO NOTHING`,
		vaultCap.MorphoVaultID, vaultCap.CapID, vaultCap.IDData, absoluteCap, relativeCap,
		vaultCap.BlockNumber, vaultCap.BlockVersion, vaultCap.Timestamp, int(r.buildID),
	)
	if err != nil {
		return fmt.Errorf("saving morpho vault cap: %w", err)
	}
	return nil
}

// UpdateVaultFeeConfig applies a partial fee-configuration update to a vault.
func (r *MorphoRepository) UpdateVaultFeeConfig(ctx context.Context, tx pgx.Tx, morphoVaultID int64, update entity.MorphoVaultFeeUpdate) error {
	if err := update.Validate(); err != nil {
		return fmt.Errorf("validating vault fee update: %w", err)
	}

	// COALESCE keeps the stored value when a parameter is NULL, so each Set* fee
	// event updates only the field it carries without clobbering the others.
	tag, err := tx.Exec(ctx,
		`UPDATE morpho_vault SET
		     performance_fee           = COALESCE($2, performance_fee),
		     management_fee            = COALESCE($3, management_fee),
		     performance_fee_recipient = COALESCE($4, performance_fee_recipient),
		     management_fee_recipient  = COALESCE($5, management_fee_recipient)
		 WHERE id = $1`,
		morphoVaultID,
		optionalNumeric(update.PerformanceFee),
		optionalNumeric(update.ManagementFee),
		update.PerformanceFeeRecipient,
		update.ManagementFeeRecipient,
	)
	if err != nil {
		return fmt.Errorf("updating morpho vault fee config: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("updating morpho vault fee config: no vault with id %d", morphoVaultID)
	}
	return nil
}

// numericToBigInt converts a numeric string from Postgres to *big.Int.
func numericToBigInt(s string) (*big.Int, error) {
	n, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return nil, fmt.Errorf("invalid numeric string: %q", s)
	}
	return n, nil
}

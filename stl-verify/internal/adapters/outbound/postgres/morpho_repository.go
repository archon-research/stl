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

// GetOrCreateVault retrieves or creates a MetaMorpho vault, converging
// created_at_block downward to the earliest observation (LEAST), mirroring
// GetOrCreateToken. A vault first seen inside a narrowed backfill range — or on the
// live stream, long after deployment — records that block; without the merge, that
// wrong deploy block would persist forever because no later observation can correct
// it.
func (r *MorphoRepository) GetOrCreateVault(ctx context.Context, tx pgx.Tx, vault *entity.MorphoVault) (int64, error) {
	var id int64
	err := tx.QueryRow(ctx,
		`INSERT INTO morpho_vault (chain_id, protocol_id, address, name, symbol, asset_token_id, vault_version, created_at_block)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 ON CONFLICT (chain_id, address) DO UPDATE SET
		     created_at_block = LEAST(morpho_vault.created_at_block, EXCLUDED.created_at_block)
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
// The candidate's added_at_block is matched against incarnations in three ordered
// steps. The closed-window match MUST run before the active-row match: for a
// removed-then-re-added adapter a closed row and an active row coexist, and a
// backfilled add belonging to the earlier (closed) incarnation would otherwise
// match the active row first — pulling the re-added incarnation's added_at_block
// down into a prior window and leaving the closed row unconverged.
//
//  1. If a CLOSED incarnation strictly covers the candidate (removed_at_block >
//     candidate added_at_block), the candidate is a late observation of that closed
//     window: converge onto the earliest-closing such row (UPDATE its added_at_block
//     down). This keeps a backfilled AddAdapter@W replayed earlier than a live
//     lazy-register+removal@X (W<X) from INSERTing a second, spuriously-ACTIVE
//     incarnation — resurrecting a de-registered adapter into
//     GetActiveAdaptersByVault / realAssets forever — and keeps a re-added
//     adapter's active window intact when the backfilled add belongs to a prior,
//     already-closed incarnation. The comparison is STRICT because a governance
//     multicall can remove and re-add an adapter in ONE block: with `>=` that
//     re-add folded into the row just closed, leaving the adapter with no active row
//     on-DB while it is active on-chain.
//  2. Otherwise, if an ACTIVE row (removed_at_block IS NULL) exists it is reused,
//     its added_at_block converging downward to LEAST(existing, candidate). This
//     lets the backfiller replay the TRUE AddAdapter@X for an adapter the live
//     stream lazily registered at first-seen block Y>X collapse onto one active row.
//  3. Only a candidate added at or after every prior removal is a genuinely new
//     incarnation and is INSERTed. The UNIQUE key includes added_at_block, so the
//     ON CONFLICT no-op SET keeps a same-block backfill re-run idempotent — and
//     folds a backfilled add landing exactly on a lazy register+removal block onto
//     that already-closed row instead of resurrecting it.
//
// Both convergence steps also curate adapter_type: a row recorded as Unknown (the
// sentinel written when the on-chain probe cannot classify an adapter) is upgraded
// when a replay supplies a real type, and a known type is never overwritten. This
// is what makes replay the curation path the column's schema comment promises.
func (r *MorphoRepository) GetOrCreateAdapter(ctx context.Context, tx pgx.Tx, adapter *entity.MorphoAdapter) (int64, error) {
	if err := adapter.Validate(); err != nil {
		return 0, fmt.Errorf("validating morpho adapter: %w", err)
	}

	if err := lockAdapterKey(ctx, tx, adapter.MorphoVaultID, adapter.Address); err != nil {
		return 0, err
	}

	// A CLOSED incarnation whose window strictly covers the candidate
	// (removed_at_block > candidate added_at_block) takes precedence over the active
	// row: the candidate is a late observation of that closed window — converge onto
	// the earliest-closing such row instead of matching the active row. Matching the
	// active row first would pull a re-added incarnation's added_at_block down into
	// this prior window and leave the closed row unconverged; it would also, when no
	// active row exists, INSERT a spuriously-active duplicate that resurrects a
	// de-registered adapter.
	var id int64
	err := tx.QueryRow(ctx,
		`UPDATE morpho_adapter
		 SET added_at_block = LEAST(added_at_block, $3),
		     adapter_type = CASE WHEN adapter_type = $4 AND $5 <> $4 THEN $5 ELSE adapter_type END
		 WHERE id = (
		     SELECT id FROM morpho_adapter
		     WHERE morpho_vault_id = $1 AND address = $2
		       AND removed_at_block IS NOT NULL AND removed_at_block > $3
		     ORDER BY removed_at_block ASC
		     LIMIT 1
		 )
		 RETURNING id`,
		adapter.MorphoVaultID, adapter.Address, adapter.AddedAtBlock,
		int16(entity.MorphoAdapterTypeUnknown), int16(adapter.AdapterType),
	).Scan(&id)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("converging closed morpho adapter incarnation: %w", err)
	}

	// No closed window covers the candidate. If an ACTIVE row exists reuse it, its
	// added_at_block converging downward to LEAST(existing, candidate). This lets the
	// backfiller replay the TRUE AddAdapter@X for an adapter the live stream lazily
	// registered at first-seen block Y>X collapse onto one active row.
	err = tx.QueryRow(ctx,
		`UPDATE morpho_adapter
		 SET added_at_block = LEAST(added_at_block, $3),
		     adapter_type = CASE WHEN adapter_type = $4 AND $5 <> $4 THEN $5 ELSE adapter_type END
		 WHERE morpho_vault_id = $1 AND address = $2 AND removed_at_block IS NULL
		 RETURNING id`,
		adapter.MorphoVaultID, adapter.Address, adapter.AddedAtBlock,
		int16(entity.MorphoAdapterTypeUnknown), int16(adapter.AdapterType),
	).Scan(&id)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("converging active morpho adapter: %w", err)
	}

	// Genuinely new incarnation (candidate added after every prior removal): insert
	// a new row. The ON CONFLICT no-op SET keeps a same-block replay (backfill
	// re-run of the same AddAdapter) idempotent.
	//
	// The advisory lock is what keeps a concurrent removal from dropping us here
	// with the adapter already de-registered (a removal committing between the
	// closed-window check and the active-row UPDATE would make EvalPlanQual re-check
	// that UPDATE against the new removed_at_block and match 0 rows). The partial
	// unique index uq_morpho_adapter_active is the structural backstop for that
	// invariant: an unlocked writer aborts here rather than resurrecting a removed
	// adapter as a second active row.
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

// lockAdapterKey serializes every writer of one adapter registry key on a
// per-transaction advisory lock. ON CONFLICT alone cannot guard a decision made
// before the insert (ADR-0002 §3): without this, two concurrent live-vs-backfill
// writers could both observe no active row and insert two active incarnations, and
// a removal committing mid-decision could make a registration fall through to an
// INSERT that resurrects a de-registered adapter. The key is deliberately
// block-free so registrations and removals of the same adapter serialize on the
// same lock regardless of the block they carry.
func lockAdapterKey(ctx context.Context, tx pgx.Tx, morphoVaultID int64, address []byte) error {
	lockKey := fmt.Sprintf("morpho_adapter|%d|%x", morphoVaultID, address)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, lockKey); err != nil {
		return fmt.Errorf("locking %q: %w", lockKey, err)
	}
	return nil
}

// MarkAdapterRemoved records the block at which an adapter was de-registered,
// closing the incarnation that was live at removedAtBlock.
//
// removed_at_block converges to the EARLIEST observation (LEAST), mirroring
// added_at_block's first-observed semantics: a reorg that re-lands the
// RemoveAdapter transaction one block over, or a backfill replaying it, then
// settles on the same value regardless of the order the observations arrive in.
// Before this converged, a relocated removal matched neither idempotency arm,
// affected 0 rows and hard-errored on every redelivery — poisoning the FIFO queue
// forever. A relocation is WARN-logged: it is rare enough to be worth an
// operator's attention.
//
// Residual: a removal that is reorged OUT entirely and never re-lands leaves the
// row closed at a block where nothing happened. Nothing self-heals that — the
// registry carries no lifecycle versioning (removed_at_block has no block_version),
// so no later observation can contradict the recorded close.
//
// Closing a row that owns morpho_adapter_state snapshots recorded AFTER the
// removal block is refused, because those snapshots would be stranded inside the
// closed window: window-filtered queries drop them, window-ignoring queries
// double-count them against the next incarnation. That is a hard error on purpose
// — poison-pilling the event beats silently corrupting adapter lifetimes.
func (r *MorphoRepository) MarkAdapterRemoved(ctx context.Context, tx pgx.Tx, morphoVaultID int64, address []byte, removedAtBlock int64) error {
	if err := lockAdapterKey(ctx, tx, morphoVaultID, address); err != nil {
		return err
	}

	id, recordedRemoval, err := r.incarnationLiveAt(ctx, tx, morphoVaultID, address, removedAtBlock)
	if err != nil {
		return fmt.Errorf("marking morpho adapter removed for vault %d address %x: %w", morphoVaultID, address, err)
	}

	closeAt := removedAtBlock
	if recordedRemoval != nil && *recordedRemoval != removedAtBlock {
		closeAt = min(*recordedRemoval, removedAtBlock)
		r.logger.Warn("VaultV2 adapter removal observed at a different block than recorded — converging to the earliest observation",
			"vault_id", morphoVaultID,
			"adapter", fmt.Sprintf("%x", address),
			"recorded_block", *recordedRemoval,
			"observed_block", removedAtBlock,
			"converged_block", closeAt)
	}

	if err := r.assertNoStateAfterRemoval(ctx, tx, id, closeAt); err != nil {
		return fmt.Errorf("marking morpho adapter removed for vault %d address %x: %w", morphoVaultID, address, err)
	}

	if _, err := tx.Exec(ctx, `UPDATE morpho_adapter SET removed_at_block = $2 WHERE id = $1`, id, closeAt); err != nil {
		return fmt.Errorf("marking morpho adapter removed: %w", err)
	}
	return nil
}

// incarnationLiveAt returns the id and recorded removal block of the adapter
// incarnation a removal at atBlock closes: the LATEST incarnation registered at or
// before that block. The added_at_block scope is what keeps a replayed old
// RemoveAdapter@X from closing an incarnation re-added after X with a removal block
// earlier than its own registration.
func (r *MorphoRepository) incarnationLiveAt(ctx context.Context, tx pgx.Tx, morphoVaultID int64, address []byte, atBlock int64) (int64, *int64, error) {
	var (
		id      int64
		removed *int64
	)
	err := tx.QueryRow(ctx,
		`SELECT id, removed_at_block FROM morpho_adapter
		 WHERE morpho_vault_id = $1 AND address = $2 AND added_at_block <= $3
		 ORDER BY added_at_block DESC
		 LIMIT 1`,
		morphoVaultID, address, atBlock,
	).Scan(&id, &removed)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil, fmt.Errorf("no adapter incarnation registered at or before block %d", atBlock)
	}
	if err != nil {
		return 0, nil, fmt.Errorf("looking up adapter incarnation live at block %d: %w", atBlock, err)
	}
	return id, removed, nil
}

// assertNoStateAfterRemoval refuses a close that would strand realAssets snapshots
// outside the incarnation's lifetime window. Snapshots taken IN the removal block
// are inside the window (the Deallocate log preceding the RemoveAdapter log in one
// governance transaction), so the comparison is strict.
func (r *MorphoRepository) assertNoStateAfterRemoval(ctx context.Context, tx pgx.Tx, adapterID, closeAt int64) error {
	var (
		orphaned int64
		latest   *int64
	)
	if err := tx.QueryRow(ctx,
		`SELECT count(*), max(block_number) FROM morpho_adapter_state
		 WHERE morpho_adapter_id = $1 AND block_number > $2`,
		adapterID, closeAt,
	).Scan(&orphaned, &latest); err != nil {
		return fmt.Errorf("checking adapter_state snapshots after block %d: %w", closeAt, err)
	}
	if orphaned == 0 {
		return nil
	}
	return fmt.Errorf("closing incarnation %d at block %d would orphan %d morpho_adapter_state row(s) recorded after it (latest block %d): those snapshots belong to a later incarnation of this adapter and must be re-homed onto it before the removal can be recorded",
		adapterID, closeAt, orphaned, *latest)
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

// SaveVaultFee saves a VaultV2 full fee-config snapshot within an external transaction.
func (r *MorphoRepository) SaveVaultFee(ctx context.Context, tx pgx.Tx, vaultFee *entity.MorphoVaultFee) error {
	if err := vaultFee.Validate(); err != nil {
		return fmt.Errorf("validating morpho vault fee: %w", err)
	}

	performanceFee, err := bigIntToNumeric(vaultFee.PerformanceFee)
	if err != nil {
		return fmt.Errorf("converting performance_fee: %w", err)
	}
	managementFee, err := bigIntToNumeric(vaultFee.ManagementFee)
	if err != nil {
		return fmt.Errorf("converting management_fee: %w", err)
	}

	// processing_version is assigned by the mvf trigger; ON CONFLICT DO NOTHING
	// dedupes same-build retries and same-block sibling fee events (see
	// SaveVaultCap for the rationale).
	_, err = tx.Exec(ctx,
		`INSERT INTO morpho_vault_fee (morpho_vault_id, performance_fee, management_fee, performance_fee_recipient, management_fee_recipient, block_number, block_version, timestamp, build_id)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		 ON CONFLICT (morpho_vault_id, block_number, block_version, timestamp, processing_version) DO NOTHING`,
		vaultFee.MorphoVaultID, performanceFee, managementFee,
		vaultFee.PerformanceFeeRecipient, vaultFee.ManagementFeeRecipient,
		vaultFee.BlockNumber, vaultFee.BlockVersion, vaultFee.Timestamp, int(r.buildID),
	)
	if err != nil {
		return fmt.Errorf("saving morpho vault fee: %w", err)
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

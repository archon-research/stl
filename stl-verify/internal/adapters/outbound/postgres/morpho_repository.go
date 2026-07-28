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

// maxRemovalRelocationDistance bounds how far — in EITHER direction — a re-observed
// RemoveAdapter can sit from the recorded removal block and still be that same
// on-chain removal, relocated by a reorg. Ethereum PoS finalises after 2 epochs — 64
// slots — which is the deepest a reorg can rewrite, and therefore the furthest a
// transaction can move; it is the same bound the block watcher's reorg detection
// walks back (live_data.LiveConfigDefaults().FinalityBlockCount, set to 64 in
// cmd/base/watcher). That value is a service-layer config field and this is an
// adapter, so it is restated here rather than imported inward-to-outward.
const maxRemovalRelocationDistance = 64

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

// GetOrCreateAdapter retrieves or creates a VaultV2 liquidity adapter registry row
// for (morpho_vault_id, address). The three-step matching contract it implements —
// and why the step order is load-bearing — is stated once on
// outbound.MorphoRepository.GetOrCreateAdapter; the comments below cover only what
// the SQL itself does not show.
//
// Residual: an add→remove→re-add sequence within ONE block collapses to the closed
// row. The removal closes the incarnation at that block, then the re-add matches
// neither the closed window (the comparison is strict, see below) nor an active row,
// so it reaches the INSERT — where UNIQUE (morpho_vault_id, address, added_at_block)
// folds it onto the row just closed, leaving the adapter de-registered on-DB while it
// is active on-chain. Representing it would need an incarnation sequence number in
// the key, which every reader would then have to carry. No such sequence has been
// observed on-chain (a curator would have to add, remove, and re-add one adapter in a
// single block), so the simpler key wins until one is.
func (r *MorphoRepository) GetOrCreateAdapter(ctx context.Context, tx pgx.Tx, adapter *entity.MorphoAdapter) (int64, error) {
	if err := adapter.Validate(); err != nil {
		return 0, fmt.Errorf("validating morpho adapter: %w", err)
	}

	if err := lockAdapterKey(ctx, tx, adapter.MorphoVaultID, adapter.Address); err != nil {
		return 0, err
	}

	// Step 1, the closed-window match, which must run BEFORE the active-row match
	// (port doc). The comparison is STRICT because a governance multicall can remove
	// and re-add an adapter in ONE block: with `>=` that re-add folds into the row it
	// just closed, leaving the adapter with no active row on-DB while it is active
	// on-chain. Ordering by removed_at_block ASC picks the earliest-closing covering
	// window, i.e. the incarnation the candidate actually belongs to.
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

	// Step 2: no closed window covers the candidate, so reuse the ACTIVE row.
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

	// Step 3: a genuinely new incarnation. The ON CONFLICT no-op SET makes a
	// same-block replay idempotent, and is also what folds the one-block
	// add→remove→re-add sequence onto the closed row (see the Residual above).
	//
	// The advisory lock is what keeps a concurrent removal from dropping us here with
	// the adapter already de-registered (a removal committing between the
	// closed-window check and the active-row UPDATE would make EvalPlanQual re-check
	// that UPDATE against the new removed_at_block and match 0 rows). The partial
	// unique index uq_morpho_adapter_active is the structural backstop: an unlocked
	// writer aborts here rather than resurrecting a removed adapter as a second
	// active row.
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

// CreateAdapterIncarnation inserts the given incarnation and nothing else. The
// contract — and why no convergence may leak in here — is on
// outbound.MorphoRepository.CreateAdapterIncarnation.
//
// The ON CONFLICT DO UPDATE deliberately touches neither removed_at_block nor
// adapter_type, so a conflicting row cannot have a closed lifetime reopened or a curated
// classification overwritten.
func (r *MorphoRepository) CreateAdapterIncarnation(ctx context.Context, tx pgx.Tx, adapter *entity.MorphoAdapter, removedAtBlock int64) (int64, error) {
	adapter.AddedAtBlock = removedAtBlock
	adapter.RemovedAtBlock = &removedAtBlock
	if err := adapter.Validate(); err != nil {
		return 0, fmt.Errorf("validating morpho adapter: %w", err)
	}
	if err := lockAdapterKey(ctx, tx, adapter.MorphoVaultID, adapter.Address); err != nil {
		return 0, err
	}

	var id int64
	err := tx.QueryRow(ctx,
		`INSERT INTO morpho_adapter (morpho_vault_id, address, asset_token_id, adapter_type, added_at_block, removed_at_block)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (morpho_vault_id, address, added_at_block) DO UPDATE SET id = morpho_adapter.id
		 RETURNING id`,
		adapter.MorphoVaultID, adapter.Address, adapter.AssetTokenID, int16(adapter.AdapterType),
		removedAtBlock, removedAtBlock,
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("inserting morpho adapter incarnation removed at block %d: %w", removedAtBlock, err)
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
// closing the incarnation that was live at removedAtBlock. The convergence contract
// this implements is stated once on outbound.MorphoRepository.MarkAdapterRemoved.
//
// Residual: a removal that is reorged OUT entirely and never re-lands leaves the
// row closed at a block where nothing happened. Nothing self-heals that — the
// registry carries no lifecycle versioning (removed_at_block has no block_version),
// so no later observation can contradict the recorded close.
func (r *MorphoRepository) MarkAdapterRemoved(ctx context.Context, tx pgx.Tx, morphoVaultID int64, address []byte, removedAtBlock int64) error {
	if err := lockAdapterKey(ctx, tx, morphoVaultID, address); err != nil {
		return err
	}
	if err := r.closeIncarnationLiveAt(ctx, tx, morphoVaultID, address, removedAtBlock); err != nil {
		return fmt.Errorf("marking morpho adapter removed for vault %d address %x: %w", morphoVaultID, address, err)
	}
	return nil
}

// closeIncarnationLiveAt resolves which incarnation a removal closes, which block it
// closes at, and whether closing there would strand snapshots, then writes it.
func (r *MorphoRepository) closeIncarnationLiveAt(ctx context.Context, tx pgx.Tx, morphoVaultID int64, address []byte, removedAtBlock int64) error {
	id, recordedRemoval, err := r.incarnationLiveAt(ctx, tx, morphoVaultID, address, removedAtBlock)
	if err != nil {
		return err
	}

	closeAt, err := r.convergedCloseBlock(morphoVaultID, address, recordedRemoval, removedAtBlock)
	if err != nil {
		return err
	}

	if narrowsLifetime(recordedRemoval, closeAt) {
		if err := r.assertNoStateAfterRemoval(ctx, tx, id, closeAt); err != nil {
			return err
		}
	}

	tag, err := tx.Exec(ctx, `UPDATE morpho_adapter SET removed_at_block = $2 WHERE id = $1`, id, closeAt)
	if err != nil {
		return fmt.Errorf("writing removed_at_block %d: %w", closeAt, err)
	}
	// Every guard above exists to protect this one write, and the beyond-window arm
	// refuses precisely because a no-op UPDATE would return success. The row was
	// selected in this transaction under the adapter's advisory lock, so 0 rows is
	// impossible today — assert it rather than let a future delete path turn the
	// protected write into a silent skip.
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("writing removed_at_block %d to incarnation %d affected %d rows, want 1", closeAt, id, tag.RowsAffected())
	}
	return nil
}

// narrowsLifetime reports whether writing closeAt shrinks the incarnation's recorded
// lifetime — the only case in which the orphan guard has a question to ask, since a close
// landing at or above the recorded one leaves removed_at_block unchanged. The port doc
// covers why re-asking on an unchanged write is harmful.
func narrowsLifetime(recordedRemoval *int64, closeAt int64) bool {
	return recordedRemoval == nil || closeAt < *recordedRemoval
}

// convergedCloseBlock decides the block a removal closes its incarnation at.
//
// An OPEN incarnation closes at the observed block. An already-closed one means the
// removal is being re-observed, and the DISTANCE — measured symmetrically — is what
// discriminates between the two causes. Within maxRemovalRelocationDistance either
// way it is that same removal, relocated by a reorg or replayed by the backfiller, so
// it converges to the earlier of the two observations — deterministic whichever order
// they arrive in — and WARNs, because a relocation is rare enough to be worth an
// operator's attention.
//
// Beyond that window in EITHER direction it errors; the port doc argues why the bound is
// symmetric. The mechanical detail on the above arm: converging would leave
// removed_at_block at the lower block, so a real de-registration would be discarded by a
// no-op UPDATE returning success.
func (r *MorphoRepository) convergedCloseBlock(morphoVaultID int64, address []byte, recordedRemoval *int64, removedAtBlock int64) (int64, error) {
	if recordedRemoval == nil {
		return removedAtBlock, nil
	}
	if distance := removedAtBlock - *recordedRemoval; distance > maxRemovalRelocationDistance || distance < -maxRemovalRelocationDistance {
		return 0, fmt.Errorf("removal at block %d is %d blocks from the close already recorded at %d, beyond the %d-block reorg window, so the two cannot be one removal a reorg relocated: the likely cause is a conflated incarnation — this adapter lived several add/remove lifetimes and one registry row now spans more than one of them, so neither close can be converged away without erasing a real de-registration. Repair the row manually, or replay this adapter's full lifecycle history from before its earliest AddAdapter through past its latest RemoveAdapter, then re-run the removal",
			removedAtBlock, distance, *recordedRemoval, maxRemovalRelocationDistance)
	}
	if *recordedRemoval == removedAtBlock {
		return removedAtBlock, nil
	}
	closeAt := min(*recordedRemoval, removedAtBlock)
	r.logger.Warn("VaultV2 adapter removal observed at a different block than recorded — converging to the earliest observation",
		"vault_id", morphoVaultID,
		"adapter", fmt.Sprintf("%x", address),
		"recorded_block", *recordedRemoval,
		"observed_block", removedAtBlock,
		"converged_block", closeAt)
	return closeAt, nil
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

// assertNoStateAfterRemoval refuses a lifetime-narrowing close when it would strand
// realAssets snapshots outside the window: window-filtered queries drop them,
// window-ignoring queries double-count them against the next incarnation. That is a hard
// error on purpose — poison-pilling the event beats silently corrupting adapter
// lifetimes. Snapshots taken IN the close block are inside the window (the Deallocate log
// preceding the RemoveAdapter log in one governance transaction), so the comparison is
// strict.
//
// It compares block_number ONLY, at every distance, and that is load-bearing rather than
// an omission. A dead-chain snapshot a relocating reorg left above the new close is
// indistinguishable here from a live snapshot owned by a later incarnation: morpho_adapter
// carries no block_version of its own, and the removal event's version cannot arbitrate
// either, because block_version is a per-block_number counter rather than a chain epoch —
// a canonical snapshot routinely carries a lower version than the removal's own block.
// Nor may the convergence arm be exempted: inside the reorg window the relocation bound
// permits the move, so this guard is the only thing left between a replay that conflated
// two nearby lifetimes and an erased de-registration
// (TestMarkAdapterRemoved_ConflatedIncarnationsInsideTheReorgWindowAreRefused pins it).
// Between refusing a correct removal and erasing a recorded one, the guard refuses: the
// first stalls one FIFO queue visibly, the second is undetectable.
//
// The refusal is therefore an operator-facing poison pill with two possible resolutions,
// and the message names both — the snapshots belong to a later incarnation and must be
// re-homed, or they are dead-chain residue and must be deleted. No code path does either;
// the incarnation-sequence key the port doc names as the deferred follow-up is what would
// let the repository tell them apart on its own.
//
// Shape note: morpho_adapter_state is a compressed + S3-tiered hypertable partitioned on
// timestamp, while this predicate filters block_number, so no chunk exclusion applies and
// the read touches every chunk of the adapter's history that this connection can see —
// tiered chunks are NOT visible unless timescaledb.enable_tiered_reads is on, which the
// worker pool does not set, so snapshots older than the tiering horizon are a blind spot.
// The membership probe is an EXISTS that can stop at the first stranded row instead of
// aggregating them all, and the count/latest the message needs is paid only on the
// refusal path.
func (r *MorphoRepository) assertNoStateAfterRemoval(ctx context.Context, tx pgx.Tx, adapterID, closeAt int64) error {
	var orphaned bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM morpho_adapter_state WHERE `+orphanedStateWhere+`)`,
		adapterID, closeAt,
	).Scan(&orphaned); err != nil {
		return fmt.Errorf("checking adapter_state snapshots after block %d: %w", closeAt, err)
	}
	if !orphaned {
		return nil
	}
	return r.orphanedStateError(ctx, tx, adapterID, closeAt)
}

// orphanedStateWhere matches the adapter_state rows a close at $2 would strand. Shared so
// the EXISTS probe and the counting query on the refusal path can never disagree about
// what an orphan is.
const orphanedStateWhere = `
	     morpho_adapter_id = $1
	     AND block_number > $2`

// orphanedStateError builds the refusal message for assertNoStateAfterRemoval,
// counting the stranded snapshots on this failure path only. The EXISTS probe already
// matched under the same predicate in the same transaction and nothing deletes
// morpho_adapter_state rows, so the count is at least one.
func (r *MorphoRepository) orphanedStateError(ctx context.Context, tx pgx.Tx, adapterID, closeAt int64) error {
	var (
		orphaned int64
		latest   int64
	)
	if err := tx.QueryRow(ctx,
		`SELECT count(*), COALESCE(max(block_number), 0) FROM morpho_adapter_state WHERE `+orphanedStateWhere,
		adapterID, closeAt,
	).Scan(&orphaned, &latest); err != nil {
		return fmt.Errorf("counting adapter_state snapshots after block %d: %w", closeAt, err)
	}
	return fmt.Errorf("closing incarnation %d at block %d would orphan %d morpho_adapter_state row(s) recorded after it (latest block %d): either they belong to a later incarnation of this adapter and must be re-homed onto it, or they are residue of a chain a reorg replaced and must be deleted — this cannot tell the two apart, so an operator must, by hand, before the removal can be recorded",
		adapterID, closeAt, orphaned, latest)
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

// GetAdapterIncarnationAt retrieves the incarnation whose recorded lifetime
// contains atBlock, reading through the caller's transaction (read-your-writes).
//
// The removed_at_block >= atBlock predicate is what distinguishes this from
// incarnationLiveAt, whose scope is only added_at_block <= atBlock: an incarnation
// that already CLOSED below atBlock does not cover it, so this returns nil and the
// caller registers the unobserved later incarnation instead of letting the removal
// converge onto — or mint a zero-length duplicate of — the earlier one.
func (r *MorphoRepository) GetAdapterIncarnationAt(ctx context.Context, tx pgx.Tx, morphoVaultID int64, address []byte, atBlock int64) (*entity.MorphoAdapter, error) {
	var a entity.MorphoAdapter
	err := tx.QueryRow(ctx,
		`SELECT id, asset_token_id, adapter_type, added_at_block, removed_at_block
		 FROM morpho_adapter
		 WHERE morpho_vault_id = $1 AND address = $2 AND added_at_block <= $3
		   AND (removed_at_block IS NULL OR removed_at_block >= $3)
		 ORDER BY added_at_block DESC
		 LIMIT 1`,
		morphoVaultID, address, atBlock,
	).Scan(&a.ID, &a.AssetTokenID, &a.AdapterType, &a.AddedAtBlock, &a.RemovedAtBlock)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("querying morpho adapter incarnation at block %d: %w", atBlock, err)
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

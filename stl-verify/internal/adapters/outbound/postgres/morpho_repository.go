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
		return 0, fmt.Errorf("registering morpho market: %w", err)
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
		return 0, fmt.Errorf("registering morpho vault: %w", err)
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

// ObserveAdapterMembership records one observation of whether an adapter belongs to a
// vault's adapter set. The contract — which observations are appended unconditionally and
// which only when they change the answer — is stated once on
// outbound.MorphoRepository.ObserveAdapterMembership; the comments here cover only what
// the SQL does not show.
//
// The two writes are deliberately different idioms. The identity row is an
// INSERT … ON CONFLICT DO NOTHING followed by a SELECT, never a no-op
// `DO UPDATE SET id = morpho_adapter.id`: the DO UPDATE arm requires UPDATE privilege and
// is refused at executor start whether or not a conflict fires, and UPDATE is revoked from
// the application role on this table (db/migrations/AGENTS.md, strict append-only). It is
// race-free under READ COMMITTED, which every morpho writer uses: DO NOTHING waits out a
// conflicting in-progress transaction, so the follow-up SELECT — taking a fresh statement
// snapshot — sees the committed row. The membership row is a pure append whose
// processing_version the mam trigger assigns, deduped by ON CONFLICT DO NOTHING.
func (r *MorphoRepository) ObserveAdapterMembership(ctx context.Context, tx pgx.Tx, obs *entity.MorphoAdapterObservation) (int64, bool, error) {
	if err := obs.Validate(); err != nil {
		return 0, false, fmt.Errorf("validating morpho adapter observation: %w", err)
	}

	adapterID, err := r.adapterIdentityID(ctx, tx, &obs.Identity)
	if err != nil {
		return 0, false, err
	}

	if !obs.Membership.ObservedVia.IsTransition() {
		appends, err := r.assertionAppends(ctx, tx, adapterID, &obs.Membership)
		if err != nil {
			return 0, false, err
		}
		if !appends {
			return adapterID, false, nil
		}
	}

	if obs.Membership.IsMember && obs.Membership.AdapterType == nil {
		return 0, false, fmt.Errorf("recording adapter %x as a member of vault %d at block %d: %w",
			obs.Identity.Address, obs.Identity.MorphoVaultID, obs.Membership.BlockNumber, outbound.ErrAdapterUnclassified)
	}

	appended, err := r.appendMembership(ctx, tx, adapterID, &obs.Membership)
	if err != nil {
		return 0, false, err
	}
	return adapterID, appended, nil
}

// adapterIdentityID returns the stable id of the (vault, address) identity row,
// creating it on first sight. Every column it writes is immutable, so a row that
// already exists is returned as-is and never converged — that is what keeps the id a
// morpho_adapter_state snapshot hangs off from ever moving.
func (r *MorphoRepository) adapterIdentityID(ctx context.Context, tx pgx.Tx, identity *entity.MorphoAdapterIdentity) (int64, error) {
	var id int64
	err := tx.QueryRow(ctx,
		`INSERT INTO morpho_adapter (morpho_vault_id, address, asset_token_id, build_id)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (morpho_vault_id, address) DO NOTHING
		 RETURNING id`,
		identity.MorphoVaultID, identity.Address, identity.AssetTokenID, int(r.buildID),
	).Scan(&id)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("registering morpho adapter: %w", err)
	}

	// Zero rows means the row was already there (ours or a concurrent writer's, which
	// DO NOTHING waited out); read it back.
	if err := tx.QueryRow(ctx,
		`SELECT id FROM morpho_adapter WHERE morpho_vault_id = $1 AND address = $2`,
		identity.MorphoVaultID, identity.Address,
	).Scan(&id); err != nil {
		return 0, fmt.Errorf("reading back morpho adapter %x of vault %d: %w",
			identity.Address, identity.MorphoVaultID, err)
	}
	return id, nil
}

// assertionAppends reports whether an assertion carries information the log does not
// already hold at its position — the one read-then-write decision left in this
// repository, and therefore the only path that takes the advisory lock.
//
// The lock is acquired BEFORE the decisive read, not just before the insert
// (ADR-0002 §3): two overlapping writers that both read "no observation yet" would
// otherwise each append an assertion for one on-chain fact, and ON CONFLICT cannot catch
// that because their log positions differ. A TRANSITION skips this entirely — it is an
// unconditional append with no decision to serialize.
func (r *MorphoRepository) assertionAppends(ctx context.Context, tx pgx.Tx, adapterID int64, m *entity.MorphoAdapterMembership) (bool, error) {
	if err := lockAdapterKey(ctx, tx, adapterID); err != nil {
		return false, err
	}
	known, err := r.membershipAt(ctx, tx, adapterID, m.Position())
	if err != nil {
		return false, err
	}
	return known == nil || *known != m.IsMember, nil
}

// membershipAt returns the answer the log already gives for an adapter at a block
// position — the latest observation at or below it — or nil when the log says nothing
// there yet.
func (r *MorphoRepository) membershipAt(ctx context.Context, tx pgx.Tx, adapterID int64, at entity.BlockPosition) (*bool, error) {
	var isMember bool
	err := tx.QueryRow(ctx,
		`SELECT is_member FROM morpho_adapter_membership
		 WHERE morpho_adapter_id = $1
		   AND (block_number, block_version, log_index) <= ($2, $3, $4)
		 ORDER BY block_number DESC, block_version DESC, log_index DESC, processing_version DESC
		 LIMIT 1`,
		adapterID, at.BlockNumber, at.BlockVersion, at.LogIndex,
	).Scan(&isMember)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading adapter %d membership as of block %d: %w", adapterID, at.BlockNumber, err)
	}
	return &isMember, nil
}

// appendMembership writes one observation and reports whether a row was actually added.
// processing_version is assigned by the mam trigger; ON CONFLICT DO NOTHING dedupes a
// same-build retry (see SaveMarketState for the rationale), so a redelivery or an exact
// replay converges on the same single row — and returns false, because the log gained
// nothing. The command tag is the only thing that distinguishes the two outcomes.
func (r *MorphoRepository) appendMembership(ctx context.Context, tx pgx.Tx, adapterID int64, m *entity.MorphoAdapterMembership) (bool, error) {
	var adapterType *int16
	if m.AdapterType != nil {
		t := int16(*m.AdapterType)
		adapterType = &t
	}
	tag, err := tx.Exec(ctx,
		`INSERT INTO morpho_adapter_membership
		     (morpho_adapter_id, block_number, block_version, log_index, timestamp,
		      is_member, adapter_type, observed_via, build_id)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		 ON CONFLICT (morpho_adapter_id, block_number, block_version, log_index, processing_version) DO NOTHING`,
		adapterID, m.BlockNumber, m.BlockVersion, m.LogIndex, m.Timestamp,
		m.IsMember, adapterType, string(m.ObservedVia), int(r.buildID),
	)
	if err != nil {
		return false, fmt.Errorf("recording adapter %d membership at block %d: %w", adapterID, m.BlockNumber, err)
	}
	return tag.RowsAffected() == 1, nil
}

// lockAdapterKey serializes the writers that make a decision about one adapter on a
// per-transaction advisory lock. ON CONFLICT alone cannot guard a decision made before the
// insert (ADR-0002 §3): without this, two concurrent live-vs-backfill writers could both
// read "the log says nothing here" and both append the same assertion. The identity row's
// id IS the (vault, address) key — it is created before the lock is taken, and creating it
// needs no lock of its own (ON CONFLICT DO NOTHING makes that race a no-op) — and the key
// is deliberately block-free, so every decision about one adapter serializes regardless of
// the block it carries. Only one key is ever held, so the sorted-order rule has nothing to
// order.
func lockAdapterKey(ctx context.Context, tx pgx.Tx, adapterID int64) error {
	lockKey := fmt.Sprintf("morpho_adapter|%d", adapterID)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, lockKey); err != nil {
		return fmt.Errorf("locking %q: %w", lockKey, err)
	}
	return nil
}

// latestMembershipLateral is the "current membership per adapter" join every read below
// shares: an index scan BACKWARD along the membership PK, stopping at the first row, so
// the cost is one index descent per adapter rather than a scan of its history.
const latestMembershipLateral = `
	     SELECT is_member, adapter_type, block_number, observed_via
	     FROM morpho_adapter_membership
	     WHERE morpho_adapter_id = a.id`

const latestMembershipOrder = `
	     ORDER BY block_number DESC, block_version DESC, log_index DESC, processing_version DESC
	     LIMIT 1`

// GetActiveAdapterAt returns the adapter for (vault, address) when the latest observation
// at or below a block position says it is a member, so a backfiller replaying a historical
// block is answered about that block rather than about the present. Reads committed state
// through the pool, which is what lets the caller run it before opening its write
// transaction.
//
// The row comparison stays an index-bounded backward scan on the PK prefix, so the block
// bound folds into the index condition rather than becoming a filter.
func (r *MorphoRepository) GetActiveAdapterAt(ctx context.Context, morphoVaultID int64, address []byte, at entity.BlockPosition) (*entity.MorphoAdapterMember, error) {
	row := r.pool.QueryRow(ctx,
		`SELECT a.id, a.asset_token_id, m.adapter_type, m.block_number, m.observed_via
		 FROM morpho_adapter a
		 JOIN LATERAL (`+latestMembershipLateral+`
		       AND (block_number, block_version, log_index) <= ($3, $4, $5)`+latestMembershipOrder+`
		 ) m ON TRUE
		 WHERE a.morpho_vault_id = $1 AND a.address = $2 AND m.is_member`,
		morphoVaultID, address, at.BlockNumber, at.BlockVersion, at.LogIndex)
	return scanAdapterMember(row, morphoVaultID, address)
}

// scanAdapterMember reads one row of the shared active-adapter projection. adapter_type is
// NOT NULL whenever is_member (the table's CHECK), so an active adapter always has one.
func scanAdapterMember(row pgx.Row, morphoVaultID int64, address []byte) (*entity.MorphoAdapterMember, error) {
	member := &entity.MorphoAdapterMember{
		MorphoAdapterIdentity: entity.MorphoAdapterIdentity{MorphoVaultID: morphoVaultID, Address: address},
	}
	err := row.Scan(&member.ID, &member.AssetTokenID, &member.AdapterType, &member.AsOfBlock, &member.ObservedVia)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("querying active morpho adapter: %w", err)
	}
	return member, nil
}

// GetActiveAdaptersByVaultAt returns every adapter the log calls a member of the vault's
// set AS OF a block position — the whole-set counterpart of GetActiveAdapterAt, sharing
// its LATERAL shape so the bound folds into the index condition and each adapter still
// costs one backward index descent.
//
// There is deliberately no unbounded variant. Its only caller diffs this answer against
// an enumeration pinned to a block, and an unbounded read would answer about the chain
// head instead: an adapter added above the pinned block would come back a member, be
// absent from the enumeration, and be recorded as removed at a block where it was not.
func (r *MorphoRepository) GetActiveAdaptersByVaultAt(ctx context.Context, morphoVaultID int64, at entity.BlockPosition) ([]*entity.MorphoAdapterMember, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT a.id, a.address, a.asset_token_id, m.adapter_type, m.block_number, m.observed_via
		 FROM morpho_adapter a
		 JOIN LATERAL (`+latestMembershipLateral+`
		       AND (block_number, block_version, log_index) <= ($2, $3, $4)`+latestMembershipOrder+`
		 ) m ON TRUE
		 WHERE a.morpho_vault_id = $1 AND m.is_member
		 ORDER BY a.id`,
		morphoVaultID, at.BlockNumber, at.BlockVersion, at.LogIndex,
	)
	if err != nil {
		return nil, fmt.Errorf("querying active morpho adapters: %w", err)
	}
	defer rows.Close()

	var adapters []*entity.MorphoAdapterMember
	for rows.Next() {
		a := &entity.MorphoAdapterMember{
			MorphoAdapterIdentity: entity.MorphoAdapterIdentity{MorphoVaultID: morphoVaultID},
		}
		if err := rows.Scan(&a.ID, &a.Address, &a.AssetTokenID, &a.AdapterType, &a.AsOfBlock, &a.ObservedVia); err != nil {
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

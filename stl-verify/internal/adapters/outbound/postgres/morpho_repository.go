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

// numericToBigInt converts a numeric string from Postgres to *big.Int.
func numericToBigInt(s string) (*big.Int, error) {
	n, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return nil, fmt.Errorf("invalid numeric string: %q", s)
	}
	return n, nil
}

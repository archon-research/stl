package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that MorphoRepository implements outbound.MorphoRepository.
var _ outbound.MorphoRepository = (*MorphoRepository)(nil)

// MorphoRepository is a PostgreSQL implementation of the outbound.MorphoRepository port.
type MorphoRepository struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// NewMorphoRepository creates a new PostgreSQL Morpho repository.
func NewMorphoRepository(pool *pgxpool.Pool, logger *slog.Logger) (*MorphoRepository, error) {
	if pool == nil {
		return nil, fmt.Errorf("database pool cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &MorphoRepository{
		pool:   pool,
		logger: logger,
	}, nil
}

// GetOrCreateMarket retrieves or creates a Morpho Blue market.
func (r *MorphoRepository) GetOrCreateMarket(ctx context.Context, tx pgx.Tx, market *entity.MorphoMarket) (int64, error) {
	lltv, err := bigIntToNumeric(market.LLTV)
	if err != nil {
		return 0, fmt.Errorf("converting lltv: %w", err)
	}

	var id int64
	err = tx.QueryRow(ctx,
		`INSERT INTO morpho_market (protocol_id, market_id, loan_token_id, collateral_token_id, oracle_address, irm_address, lltv, created_at_block)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 ON CONFLICT (protocol_id, market_id) DO UPDATE SET protocol_id = EXCLUDED.protocol_id
		 RETURNING id`,
		market.ProtocolID, market.MarketID, market.LoanTokenID, market.CollateralTokenID,
		market.OracleAddress, market.IrmAddress, lltv, market.CreatedAtBlock,
	).Scan(&id)

	if err != nil {
		return 0, fmt.Errorf("upserting morpho market: %w", err)
	}
	return id, nil
}

// GetMarketByMarketID retrieves a market by its 32-byte market ID hash.
func (r *MorphoRepository) GetMarketByMarketID(ctx context.Context, marketID []byte) (*entity.MorphoMarket, error) {
	var m entity.MorphoMarket
	var lltvStr string

	err := r.pool.QueryRow(ctx,
		`SELECT id, protocol_id, market_id, loan_token_id, collateral_token_id, oracle_address, irm_address, lltv, created_at_block
		 FROM morpho_market WHERE market_id = $1`,
		marketID,
	).Scan(&m.ID, &m.ProtocolID, &m.MarketID, &m.LoanTokenID, &m.CollateralTokenID,
		&m.OracleAddress, &m.IrmAddress, &lltvStr, &m.CreatedAtBlock)

	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("querying morpho market: %w", err)
	}

	m.LLTV = numericToBigInt(lltvStr)
	return &m, nil
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

	_, err = tx.Exec(ctx,
		`INSERT INTO morpho_market_state (morpho_market_id, block_number, block_version, total_supply_assets, total_supply_shares, total_borrow_assets, total_borrow_shares, last_update, fee, prev_borrow_rate, interest_accrued, fee_shares)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		 ON CONFLICT (morpho_market_id, block_number, block_version) DO UPDATE SET
			total_supply_assets = EXCLUDED.total_supply_assets,
			total_supply_shares = EXCLUDED.total_supply_shares,
			total_borrow_assets = EXCLUDED.total_borrow_assets,
			total_borrow_shares = EXCLUDED.total_borrow_shares,
			last_update = EXCLUDED.last_update,
			fee = EXCLUDED.fee,
			prev_borrow_rate = EXCLUDED.prev_borrow_rate,
			interest_accrued = EXCLUDED.interest_accrued,
			fee_shares = EXCLUDED.fee_shares`,
		state.MorphoMarketID, state.BlockNumber, state.BlockVersion,
		totalSupplyAssets, totalSupplyShares, totalBorrowAssets, totalBorrowShares,
		state.LastUpdate, fee, prevBorrowRate, interestAccrued, feeShares,
	)
	if err != nil {
		return fmt.Errorf("saving morpho market state: %w", err)
	}
	return nil
}

// SavePosition saves a user position snapshot within an external transaction.
func (r *MorphoRepository) SavePosition(ctx context.Context, tx pgx.Tx, position *entity.MorphoPosition) error {
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
		`INSERT INTO morpho_position (user_id, morpho_market_id, block_number, block_version, supply_shares, borrow_shares, collateral, supply_assets, borrow_assets, event_type, tx_hash)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		 ON CONFLICT (user_id, morpho_market_id, block_number, block_version) DO UPDATE SET
			supply_shares = EXCLUDED.supply_shares,
			borrow_shares = EXCLUDED.borrow_shares,
			collateral = EXCLUDED.collateral,
			supply_assets = EXCLUDED.supply_assets,
			borrow_assets = EXCLUDED.borrow_assets,
			event_type = EXCLUDED.event_type,
			tx_hash = EXCLUDED.tx_hash`,
		position.UserID, position.MorphoMarketID, position.BlockNumber, position.BlockVersion,
		supplyShares, borrowShares, collateral, supplyAssets, borrowAssets,
		string(position.EventType), position.TxHash,
	)
	if err != nil {
		return fmt.Errorf("saving morpho position: %w", err)
	}
	return nil
}

// GetOrCreateVault retrieves or creates a MetaMorpho vault.
func (r *MorphoRepository) GetOrCreateVault(ctx context.Context, tx pgx.Tx, vault *entity.MorphoVault) (int64, error) {
	var id int64
	err := tx.QueryRow(ctx,
		`INSERT INTO morpho_vault (protocol_id, address, name, symbol, asset_token_id, vault_version, created_at_block)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 ON CONFLICT (protocol_id, address) DO UPDATE SET protocol_id = EXCLUDED.protocol_id
		 RETURNING id`,
		vault.ProtocolID, vault.Address, vault.Name, vault.Symbol,
		vault.AssetTokenID, vault.VaultVersion, vault.CreatedAtBlock,
	).Scan(&id)

	if err != nil {
		return 0, fmt.Errorf("upserting morpho vault: %w", err)
	}
	return id, nil
}

// GetVaultByAddress retrieves a vault by its contract address.
func (r *MorphoRepository) GetVaultByAddress(ctx context.Context, address common.Address) (*entity.MorphoVault, error) {
	var v entity.MorphoVault
	err := r.pool.QueryRow(ctx,
		`SELECT id, protocol_id, address, name, symbol, asset_token_id, vault_version, created_at_block
		 FROM morpho_vault WHERE address = $1`,
		address.Bytes(),
	).Scan(&v.ID, &v.ProtocolID, &v.Address, &v.Name, &v.Symbol, &v.AssetTokenID, &v.VaultVersion, &v.CreatedAtBlock)

	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("querying morpho vault: %w", err)
	}
	return &v, nil
}

// GetAllVaultAddresses retrieves all known vault addresses.
func (r *MorphoRepository) GetAllVaultAddresses(ctx context.Context) ([]common.Address, error) {
	rows, err := r.pool.Query(ctx, `SELECT address FROM morpho_vault`)
	if err != nil {
		return nil, fmt.Errorf("querying vault addresses: %w", err)
	}
	defer rows.Close()

	var addresses []common.Address
	for rows.Next() {
		var addrBytes []byte
		if err := rows.Scan(&addrBytes); err != nil {
			return nil, fmt.Errorf("scanning vault address: %w", err)
		}
		addresses = append(addresses, common.BytesToAddress(addrBytes))
	}

	return addresses, rows.Err()
}

// SaveVaultState saves a vault state snapshot within an external transaction.
func (r *MorphoRepository) SaveVaultState(ctx context.Context, tx pgx.Tx, state *entity.MorphoVaultState) error {
	totalAssets, err := bigIntToNumeric(state.TotalAssets)
	if err != nil {
		return fmt.Errorf("converting total_assets: %w", err)
	}
	totalSupply, err := bigIntToNumeric(state.TotalSupply)
	if err != nil {
		return fmt.Errorf("converting total_supply: %w", err)
	}

	var feeShares, newTotalAssets *string
	if state.FeeShares != nil {
		s := state.FeeShares.String()
		feeShares = &s
	}
	if state.NewTotalAssets != nil {
		s := state.NewTotalAssets.String()
		newTotalAssets = &s
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO morpho_vault_state (morpho_vault_id, block_number, block_version, total_assets, total_supply, fee_shares, new_total_assets)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 ON CONFLICT (morpho_vault_id, block_number, block_version) DO UPDATE SET
			total_assets = EXCLUDED.total_assets,
			total_supply = EXCLUDED.total_supply,
			fee_shares = EXCLUDED.fee_shares,
			new_total_assets = EXCLUDED.new_total_assets`,
		state.MorphoVaultID, state.BlockNumber, state.BlockVersion,
		totalAssets, totalSupply, feeShares, newTotalAssets,
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
		`INSERT INTO morpho_vault_position (user_id, morpho_vault_id, block_number, block_version, shares, assets, event_type, tx_hash)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 ON CONFLICT (user_id, morpho_vault_id, block_number, block_version) DO UPDATE SET
			shares = EXCLUDED.shares,
			assets = EXCLUDED.assets,
			event_type = EXCLUDED.event_type,
			tx_hash = EXCLUDED.tx_hash`,
		position.UserID, position.MorphoVaultID, position.BlockNumber, position.BlockVersion,
		shares, assets, string(position.EventType), position.TxHash,
	)
	if err != nil {
		return fmt.Errorf("saving morpho vault position: %w", err)
	}
	return nil
}

// numericToBigInt converts a numeric string from Postgres to *big.Int.
func numericToBigInt(s string) *big.Int {
	n := new(big.Int)
	n.SetString(s, 10)
	return n
}

package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that CurvePoolRepository implements outbound.CurvePoolRepository.
var _ outbound.CurvePoolRepository = (*CurvePoolRepository)(nil)

// CurvePoolRepository is a PostgreSQL implementation of the
// outbound.CurvePoolRepository port. Writes flow through caller-supplied
// transactions so a single block's protocol_event, typed projection, and
// gauge/state rows commit atomically.
type CurvePoolRepository struct {
	pool    *pgxpool.Pool
	logger  *slog.Logger
	buildID buildregistry.BuildID
}

// NewCurvePoolRepository creates a new PostgreSQL Curve repository.
func NewCurvePoolRepository(pool *pgxpool.Pool, logger *slog.Logger, buildID buildregistry.BuildID) (*CurvePoolRepository, error) {
	if pool == nil {
		return nil, fmt.Errorf("database pool cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &CurvePoolRepository{
		pool:    pool,
		logger:  logger,
		buildID: buildID,
	}, nil
}

// ---------------------------------------------------------------------------
// Registry: curve_pool
// ---------------------------------------------------------------------------

// GetCurvePool retrieves a Curve pool registry row by (chain, address).
func (r *CurvePoolRepository) GetCurvePool(ctx context.Context, chainID int64, address common.Address) (*entity.CurvePool, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT id, chain_id, pool_kind, address, lp_token_address, label, n_coins,
		       coin_addresses, coin_token_ids, coin_decimals, deployment_block,
		       enabled, created_at, updated_at
		FROM curve_pool
		WHERE chain_id = $1 AND address = $2`,
		chainID, address.Bytes(),
	)
	p, err := scanCurvePool(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("querying curve pool: %w", err)
	}
	return p, nil
}

// ListEnabledCurvePools returns every enabled Curve pool on the given chain.
func (r *CurvePoolRepository) ListEnabledCurvePools(ctx context.Context, chainID int64) ([]*entity.CurvePool, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, chain_id, pool_kind, address, lp_token_address, label, n_coins,
		       coin_addresses, coin_token_ids, coin_decimals, deployment_block,
		       enabled, created_at, updated_at
		FROM curve_pool
		WHERE chain_id = $1 AND enabled = true
		ORDER BY id`,
		chainID,
	)
	if err != nil {
		return nil, fmt.Errorf("listing enabled curve pools: %w", err)
	}
	defer rows.Close()

	var out []*entity.CurvePool
	for rows.Next() {
		p, err := scanCurvePool(rows)
		if err != nil {
			return nil, fmt.Errorf("scanning curve pool: %w", err)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating curve pools: %w", err)
	}
	return out, nil
}

// scanRow is satisfied by both pgx.Row and pgx.Rows; lets one helper drive both
// GetCurvePool (single row) and ListEnabledCurvePools (multi-row iteration).
type scanRow interface {
	Scan(dest ...any) error
}

func scanCurvePool(row scanRow) (*entity.CurvePool, error) {
	var (
		p               entity.CurvePool
		addressBytes    []byte
		lpTokenBytes    []byte // pgx scans SQL NULL into a nil []byte.
		coinAddrBytes   [][]byte
		coinTokenIDs    []int64
		coinDecimals    []int16
		deploymentBlock pgtype.Int8
	)
	if err := row.Scan(
		&p.ID, &p.ChainID, &p.PoolKind, &addressBytes, &lpTokenBytes, &p.Label, &p.NCoins,
		&coinAddrBytes, &coinTokenIDs, &coinDecimals, &deploymentBlock,
		&p.Enabled, &p.CreatedAt, &p.UpdatedAt,
	); err != nil {
		return nil, err
	}
	p.Address = common.BytesToAddress(addressBytes)
	if lpTokenBytes != nil {
		lp := common.BytesToAddress(lpTokenBytes)
		p.LPTokenAddress = &lp
	}
	p.CoinAddresses = make([]common.Address, len(coinAddrBytes))
	for i, b := range coinAddrBytes {
		p.CoinAddresses[i] = common.BytesToAddress(b)
	}
	p.CoinTokenIDs = coinTokenIDs
	p.CoinDecimals = coinDecimals
	if deploymentBlock.Valid {
		v := deploymentBlock.Int64
		p.DeploymentBlock = &v
	}
	return &p, nil
}

// ---------------------------------------------------------------------------
// Registry: curve_gauge
// ---------------------------------------------------------------------------

// GetCurveGauge retrieves the gauge attached to the given pool, if any.
func (r *CurvePoolRepository) GetCurveGauge(ctx context.Context, curvePoolID int64) (*entity.CurveGauge, error) {
	var (
		g               entity.CurveGauge
		addressBytes    []byte
		deploymentBlock pgtype.Int8
	)
	err := r.pool.QueryRow(ctx, `
		SELECT id, curve_pool_id, chain_id, address, deployment_block,
		       is_killed, has_no_crv, enabled, created_at, updated_at
		FROM curve_gauge
		WHERE curve_pool_id = $1`,
		curvePoolID,
	).Scan(
		&g.ID, &g.CurvePoolID, &g.ChainID, &addressBytes, &deploymentBlock,
		&g.IsKilled, &g.HasNoCRV, &g.Enabled, &g.CreatedAt, &g.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("querying curve gauge: %w", err)
	}
	g.Address = common.BytesToAddress(addressBytes)
	if deploymentBlock.Valid {
		v := deploymentBlock.Int64
		g.DeploymentBlock = &v
	}
	return &g, nil
}

// UpsertCurveGauge inserts or updates a gauge registry row by (chain_id, address).
// The no-op SET ensures RETURNING id fires on conflict as well as on insert.
func (r *CurvePoolRepository) UpsertCurveGauge(ctx context.Context, tx pgx.Tx, gauge *entity.CurveGauge) (int64, error) {
	var deploymentBlock any
	if gauge.DeploymentBlock != nil {
		deploymentBlock = *gauge.DeploymentBlock
	}

	var id int64
	err := tx.QueryRow(ctx, `
		INSERT INTO curve_gauge (curve_pool_id, chain_id, address, deployment_block,
		                         is_killed, has_no_crv, enabled, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
		ON CONFLICT (chain_id, address) DO UPDATE SET
		    curve_pool_id    = EXCLUDED.curve_pool_id,
		    deployment_block = COALESCE(curve_gauge.deployment_block, EXCLUDED.deployment_block),
		    is_killed        = EXCLUDED.is_killed,
		    has_no_crv       = EXCLUDED.has_no_crv,
		    enabled          = EXCLUDED.enabled,
		    updated_at       = NOW()
		RETURNING id`,
		gauge.CurvePoolID, gauge.ChainID, gauge.Address.Bytes(), deploymentBlock,
		gauge.IsKilled, gauge.HasNoCRV, gauge.Enabled,
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("upserting curve gauge: %w", err)
	}
	return id, nil
}

// SetCurveGaugeKilled flips the is_killed flag for a gauge identified by
// (chain_id, address). No-op (no row updated) if the gauge isn't registered.
func (r *CurvePoolRepository) SetCurveGaugeKilled(ctx context.Context, tx pgx.Tx, chainID int64, address common.Address, isKilled bool) error {
	_, err := tx.Exec(ctx, `
		UPDATE curve_gauge
		SET is_killed = $3, updated_at = NOW()
		WHERE chain_id = $1 AND address = $2`,
		chainID, address.Bytes(), isKilled,
	)
	if err != nil {
		return fmt.Errorf("updating curve gauge is_killed: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Hypertables
// ---------------------------------------------------------------------------

// SaveCurvePoolState writes one row to curve_pool_state. See the
// per-INSERT comment below for the trigger / ON CONFLICT contract.
func (r *CurvePoolRepository) SaveCurvePoolState(ctx context.Context, tx pgx.Tx, state *entity.CurvePoolState) error {
	balances, err := bigIntsToNumericArray(state.Balances)
	if err != nil {
		return fmt.Errorf("converting balances: %w", err)
	}
	totalSupply := bigIntToNullableNumeric(state.TotalSupply)
	virtualPrice := bigIntToNullableNumeric(state.VirtualPrice)
	aFactor := bigIntToNullableNumeric(state.AFactor)
	fee := bigIntToNullableNumeric(state.Fee)
	priceOracle, err := bigIntsToNullableNumericArray(state.PriceOracle)
	if err != nil {
		return fmt.Errorf("converting price_oracle: %w", err)
	}
	lastPrice, err := bigIntsToNullableNumericArray(state.LastPrice)
	if err != nil {
		return fmt.Errorf("converting last_price: %w", err)
	}

	// processing_version is assigned by the assign_processing_version_*
	// BEFORE INSERT trigger per ADR-0002: same (natural key, build_id) →
	// reuses the existing processing_version; different build_id → MAX+1.
	// ON CONFLICT DO NOTHING on the PK is the second half of that contract:
	// same-build SQS retries reuse the version and the PK swallows the dup;
	// reprocessing under a new build_id lands at a fresh version slot.
	_, err = tx.Exec(ctx, `
		INSERT INTO curve_pool_state (
		    curve_pool_id, block_number, block_version, timestamp, source,
		    balances, total_supply, virtual_price, a_factor, fee,
		    price_oracle, last_price, build_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (curve_pool_id, block_number, block_version, processing_version, timestamp) DO NOTHING`,
		state.CurvePoolID, state.BlockNumber, state.BlockVersion, state.Timestamp, state.Source,
		balances, totalSupply, virtualPrice, aFactor, fee,
		priceOracle, lastPrice, int(r.buildID),
	)
	if err != nil {
		return fmt.Errorf("saving curve pool state: %w", err)
	}
	return nil
}

// SaveCurvePoolSwap writes one row to curve_pool_swap.
func (r *CurvePoolRepository) SaveCurvePoolSwap(ctx context.Context, tx pgx.Tx, swap *entity.CurvePoolSwap) error {
	tokensSold, err := bigIntToNumericRequired(swap.TokensSold, "tokens_sold")
	if err != nil {
		return err
	}
	tokensBought, err := bigIntToNumericRequired(swap.TokensBought, "tokens_bought")
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO curve_pool_swap (
		    curve_pool_id, block_number, block_version, timestamp,
		    tx_hash, log_index, buyer,
		    sold_id, tokens_sold, bought_id, tokens_bought, is_underlying, build_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (curve_pool_id, block_number, block_version, tx_hash, log_index, processing_version, timestamp) DO NOTHING`,
		swap.CurvePoolID, swap.BlockNumber, swap.BlockVersion, swap.Timestamp,
		swap.TxHash.Bytes(), swap.LogIndex, swap.Buyer.Bytes(),
		swap.SoldID, tokensSold, swap.BoughtID, tokensBought, swap.IsUnderlying, int(r.buildID),
	)
	if err != nil {
		return fmt.Errorf("saving curve pool swap: %w", err)
	}
	return nil
}

// SaveCurvePoolLiquidityEvent writes one row to curve_pool_liquidity_event.
func (r *CurvePoolRepository) SaveCurvePoolLiquidityEvent(ctx context.Context, tx pgx.Tx, evt *entity.CurvePoolLiquidityEvent) error {
	// The DB requires token_amounts NOT NULL even for RemoveLiquidityOne, where
	// the event carries no per-coin breakdown. Pass an empty NUMERIC[] there.
	tokenAmounts, err := bigIntsToNumericArray(evt.TokenAmounts)
	if err != nil {
		return fmt.Errorf("converting token_amounts: %w", err)
	}
	fees, err := bigIntsToNullableNumericArray(evt.Fees)
	if err != nil {
		return fmt.Errorf("converting fees: %w", err)
	}
	invariantD := bigIntToNullableNumeric(evt.InvariantD)
	tokenSupply := bigIntToNullableNumeric(evt.TokenSupply)
	tokenAmount := bigIntToNullableNumeric(evt.TokenAmount)
	coinAmount := bigIntToNullableNumeric(evt.CoinAmount)

	var coinIndex any
	if evt.CoinIndex != nil {
		coinIndex = *evt.CoinIndex
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO curve_pool_liquidity_event (
		    curve_pool_id, block_number, block_version, timestamp,
		    tx_hash, log_index, event_kind, provider,
		    token_amounts, fees, invariant_d, token_supply,
		    token_amount, coin_index, coin_amount, build_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
		ON CONFLICT (curve_pool_id, block_number, block_version, tx_hash, log_index, processing_version, timestamp) DO NOTHING`,
		evt.CurvePoolID, evt.BlockNumber, evt.BlockVersion, evt.Timestamp,
		evt.TxHash.Bytes(), evt.LogIndex, evt.EventKind, evt.Provider.Bytes(),
		tokenAmounts, fees, invariantD, tokenSupply,
		tokenAmount, coinIndex, coinAmount, int(r.buildID),
	)
	if err != nil {
		return fmt.Errorf("saving curve pool liquidity event: %w", err)
	}
	return nil
}

// SaveCurvePoolParameterEvent writes one row to curve_pool_parameter_event.
func (r *CurvePoolRepository) SaveCurvePoolParameterEvent(ctx context.Context, tx pgx.Tx, evt *entity.CurvePoolParameterEvent) error {
	oldA := bigIntToNullableNumeric(evt.OldA)
	newA := bigIntToNullableNumeric(evt.NewA)
	newFee := bigIntToNullableNumeric(evt.NewFee)
	newAdminFee := bigIntToNullableNumeric(evt.NewAdminFee)

	var initialTime, futureTime any
	if evt.InitialTime != nil {
		initialTime = *evt.InitialTime
	}
	if evt.FutureTime != nil {
		futureTime = *evt.FutureTime
	}

	var extra any
	if len(evt.Extra) > 0 {
		extra = []byte(evt.Extra)
	}

	_, err := tx.Exec(ctx, `
		INSERT INTO curve_pool_parameter_event (
		    curve_pool_id, block_number, block_version, timestamp,
		    tx_hash, log_index, event_kind,
		    old_a, new_a, initial_time, future_time,
		    new_fee, new_admin_fee, extra, build_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
		ON CONFLICT (curve_pool_id, block_number, block_version, tx_hash, log_index, processing_version, timestamp) DO NOTHING`,
		evt.CurvePoolID, evt.BlockNumber, evt.BlockVersion, evt.Timestamp,
		evt.TxHash.Bytes(), evt.LogIndex, evt.EventKind,
		oldA, newA, initialTime, futureTime,
		newFee, newAdminFee, extra, int(r.buildID),
	)
	if err != nil {
		return fmt.Errorf("saving curve pool parameter event: %w", err)
	}
	return nil
}

// SaveCurvePoolExchangeRate writes one row to curve_pool_exchange_rate. Dy
// is nullable (column has DROP NOT NULL after migration
// 20260523_120000): a nil Dy lands as SQL NULL, signalling the on-chain
// get_dy call reverted. Consumers filter with IS NOT NULL.
func (r *CurvePoolRepository) SaveCurvePoolExchangeRate(ctx context.Context, tx pgx.Tx, rate *entity.CurvePoolExchangeRate) error {
	dx, err := bigIntToNumericRequired(rate.Dx, "dx")
	if err != nil {
		return err
	}
	dy := bigIntToNullableNumeric(rate.Dy)

	_, err = tx.Exec(ctx, `
		INSERT INTO curve_pool_exchange_rate (
		    curve_pool_id, block_number, block_version, timestamp,
		    i, j, dx, dy, build_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (curve_pool_id, block_number, block_version, i, j, processing_version, timestamp) DO NOTHING`,
		rate.CurvePoolID, rate.BlockNumber, rate.BlockVersion, rate.Timestamp,
		rate.I, rate.J, dx, dy, int(r.buildID),
	)
	if err != nil {
		return fmt.Errorf("saving curve pool exchange rate: %w", err)
	}
	return nil
}

// SaveCurveGaugeState writes one row to curve_gauge_state. Optional reward
// arrays are passed through as parallel slices — the DB CHECK enforces that
// their lengths match RewardCount.
func (r *CurvePoolRepository) SaveCurveGaugeState(ctx context.Context, tx pgx.Tx, state *entity.CurveGaugeState) error {
	inflationRate := bigIntToNullableNumeric(state.InflationRate)
	workingSupply := bigIntToNullableNumeric(state.WorkingSupply)
	totalSupply := bigIntToNullableNumeric(state.TotalSupply)

	var isKilled, rewardCount any
	if state.IsKilled != nil {
		isKilled = *state.IsKilled
	}
	if state.RewardCount != nil {
		rewardCount = *state.RewardCount
	}

	var rewardTokens any
	if state.RewardTokens != nil {
		b := make([][]byte, len(state.RewardTokens))
		for i, a := range state.RewardTokens {
			b[i] = a.Bytes()
		}
		rewardTokens = b
	}
	rewardRates, err := bigIntsToNullableNumericArray(state.RewardRates)
	if err != nil {
		return fmt.Errorf("converting reward_rates: %w", err)
	}
	var rewardPeriodFinish any
	if state.RewardPeriodFinish != nil {
		rewardPeriodFinish = state.RewardPeriodFinish
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO curve_gauge_state (
		    curve_gauge_id, block_number, block_version, timestamp, source,
		    inflation_rate, working_supply, total_supply, is_killed, reward_count,
		    reward_tokens, reward_rates, reward_period_finish, build_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
		ON CONFLICT (curve_gauge_id, block_number, block_version, processing_version, timestamp) DO NOTHING`,
		state.CurveGaugeID, state.BlockNumber, state.BlockVersion, state.Timestamp, state.Source,
		inflationRate, workingSupply, totalSupply, isKilled, rewardCount,
		rewardTokens, rewardRates, rewardPeriodFinish, int(r.buildID),
	)
	if err != nil {
		return fmt.Errorf("saving curve gauge state: %w", err)
	}
	return nil
}

// SaveCurveUserLpPosition writes one row to curve_user_lp_position.
// lp_balance is nullable on the DB side (see migration
// 20260521_140000_curve_user_lp_position_nullable_balance.sql) — pos.LpBalance == nil
// is a valid input and means "delta only, no absolute balance recorded".
func (r *CurvePoolRepository) SaveCurveUserLpPosition(ctx context.Context, tx pgx.Tx, pos *entity.CurveUserLpPosition) error {
	lpBalance := bigIntToNullableNumeric(pos.LpBalance)
	delta, err := bigIntToNumericRequired(pos.Delta, "delta")
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO curve_user_lp_position (
		    curve_pool_id, user_address, block_number, block_version, timestamp,
		    tx_hash, log_index, lp_balance, delta, build_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (curve_pool_id, user_address, block_number, block_version, tx_hash, log_index, processing_version, timestamp) DO NOTHING`,
		pos.CurvePoolID, pos.UserAddress.Bytes(), pos.BlockNumber, pos.BlockVersion, pos.Timestamp,
		pos.TxHash.Bytes(), pos.LogIndex, lpBalance, delta, int(r.buildID),
	)
	if err != nil {
		return fmt.Errorf("saving curve user lp position: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// big.Int / NUMERIC helpers (Curve-only — generic helpers live in helpers.go).
// ---------------------------------------------------------------------------

// bigIntToNullableNumeric returns a pgtype.Numeric for a possibly-nil raw
// integer big.Int (Exp = 0; storing the integer value as-is).
func bigIntToNullableNumeric(b *big.Int) pgtype.Numeric {
	if b == nil {
		return pgtype.Numeric{}
	}
	return pgtype.Numeric{Int: new(big.Int).Set(b), Exp: 0, Valid: true}
}

// bigIntToNumericRequired returns a non-nullable pgtype.Numeric, erroring with
// the column name on nil — the caller's column has NOT NULL on it.
func bigIntToNumericRequired(b *big.Int, column string) (pgtype.Numeric, error) {
	if b == nil {
		return pgtype.Numeric{}, fmt.Errorf("%s must not be nil", column)
	}
	return pgtype.Numeric{Int: new(big.Int).Set(b), Exp: 0, Valid: true}, nil
}

// bigIntsToNumericArray converts a slice of *big.Int to a slice of
// pgtype.Numeric for NUMERIC[] columns where the column is NOT NULL. Nil
// entries in the slice are an error.
func bigIntsToNumericArray(bs []*big.Int) ([]pgtype.Numeric, error) {
	out := make([]pgtype.Numeric, len(bs))
	for i, b := range bs {
		if b == nil {
			return nil, fmt.Errorf("element %d must not be nil", i)
		}
		out[i] = pgtype.Numeric{Int: new(big.Int).Set(b), Exp: 0, Valid: true}
	}
	return out, nil
}

// bigIntsToNullableNumericArray converts a slice to a NUMERIC[] payload,
// returning nil so pgx serialises a SQL NULL when the input slice is nil.
// An empty (non-nil) slice yields an empty array, not NULL.
func bigIntsToNullableNumericArray(bs []*big.Int) (any, error) {
	if bs == nil {
		return nil, nil
	}
	return bigIntsToNumericArray(bs)
}

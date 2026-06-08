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

// Compile-time check that UniswapV3PoolRepository implements
// outbound.UniswapV3PoolRepository.
var _ outbound.UniswapV3PoolRepository = (*UniswapV3PoolRepository)(nil)

// UniswapV3PoolRepository is a PostgreSQL implementation of the
// outbound.UniswapV3PoolRepository port. Writes flow through caller-supplied
// transactions so a single block's typed projections, raw protocol_event row,
// and position/state writes commit atomically.
type UniswapV3PoolRepository struct {
	pool    *pgxpool.Pool
	logger  *slog.Logger
	buildID buildregistry.BuildID
}

// NewUniswapV3PoolRepository creates a new PostgreSQL Uniswap V3 repository.
func NewUniswapV3PoolRepository(pool *pgxpool.Pool, logger *slog.Logger, buildID buildregistry.BuildID) (*UniswapV3PoolRepository, error) {
	if pool == nil {
		return nil, fmt.Errorf("database pool cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &UniswapV3PoolRepository{
		pool:    pool,
		logger:  logger,
		buildID: buildID,
	}, nil
}

// ---------------------------------------------------------------------------
// Registry: uniswap_v3_pool
// ---------------------------------------------------------------------------

// GetUniswapV3Pool retrieves a Uniswap V3 pool registry row by (chain, address).
func (r *UniswapV3PoolRepository) GetUniswapV3Pool(ctx context.Context, chainID int64, address common.Address) (*entity.UniswapV3Pool, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT id, chain_id, address, token0_id, token1_id, fee_tier, tick_spacing,
		       label, deployment_block, enabled, created_at, updated_at
		FROM uniswap_v3_pool
		WHERE chain_id = $1 AND address = $2`,
		chainID, address.Bytes(),
	)
	p, err := scanUniswapV3Pool(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("querying uniswap v3 pool: %w", err)
	}
	return p, nil
}

// ListEnabledUniswapV3Pools returns every enabled Uniswap V3 pool on the given chain.
func (r *UniswapV3PoolRepository) ListEnabledUniswapV3Pools(ctx context.Context, chainID int64) ([]*entity.UniswapV3Pool, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, chain_id, address, token0_id, token1_id, fee_tier, tick_spacing,
		       label, deployment_block, enabled, created_at, updated_at
		FROM uniswap_v3_pool
		WHERE chain_id = $1 AND enabled = true
		ORDER BY id`,
		chainID,
	)
	if err != nil {
		return nil, fmt.Errorf("listing enabled uniswap v3 pools: %w", err)
	}
	defer rows.Close()

	var out []*entity.UniswapV3Pool
	for rows.Next() {
		p, err := scanUniswapV3Pool(rows)
		if err != nil {
			return nil, fmt.Errorf("scanning uniswap v3 pool: %w", err)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating uniswap v3 pools: %w", err)
	}
	return out, nil
}

func scanUniswapV3Pool(row scanRow) (*entity.UniswapV3Pool, error) {
	var (
		p               entity.UniswapV3Pool
		addressBytes    []byte
		deploymentBlock pgtype.Int8
	)
	if err := row.Scan(
		&p.ID, &p.ChainID, &addressBytes, &p.Token0ID, &p.Token1ID,
		&p.FeeTier, &p.TickSpacing, &p.Label, &deploymentBlock,
		&p.Enabled, &p.CreatedAt, &p.UpdatedAt,
	); err != nil {
		return nil, err
	}
	p.Address = common.BytesToAddress(addressBytes)
	if deploymentBlock.Valid {
		v := deploymentBlock.Int64
		p.DeploymentBlock = &v
	}
	return &p, nil
}

// ---------------------------------------------------------------------------
// Registry: uniswap_v3_position
// ---------------------------------------------------------------------------

// GetUniswapV3Position retrieves a position registry row by (chain, NFPM, tokenId).
func (r *UniswapV3PoolRepository) GetUniswapV3Position(ctx context.Context, chainID int64, nfpm common.Address, tokenID *big.Int) (*entity.UniswapV3Position, error) {
	if tokenID == nil {
		return nil, fmt.Errorf("tokenID must not be nil")
	}
	tokenIDNumeric := pgtype.Numeric{Int: new(big.Int).Set(tokenID), Exp: 0, Valid: true}

	var (
		p          entity.UniswapV3Position
		nfpmBytes  []byte
		tokenIDOut pgtype.Numeric
		ownerBytes []byte
	)
	err := r.pool.QueryRow(ctx, `
		SELECT id, chain_id, nfpm_address, token_id, uniswap_v3_pool_id, owner,
		       tick_lower, tick_upper, fee, created_at_block, burned,
		       created_at, updated_at
		FROM uniswap_v3_position
		WHERE chain_id = $1 AND nfpm_address = $2 AND token_id = $3`,
		chainID, nfpm.Bytes(), tokenIDNumeric,
	).Scan(
		&p.ID, &p.ChainID, &nfpmBytes, &tokenIDOut, &p.UniswapV3PoolID, &ownerBytes,
		&p.TickLower, &p.TickUpper, &p.Fee, &p.CreatedAtBlock, &p.Burned,
		&p.CreatedAt, &p.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("querying uniswap v3 position: %w", err)
	}
	p.NFPMAddress = common.BytesToAddress(nfpmBytes)
	p.Owner = common.BytesToAddress(ownerBytes)
	if tokenIDOut.Valid && tokenIDOut.Int != nil {
		p.TokenID = new(big.Int).Set(tokenIDOut.Int)
	}
	return &p, nil
}

// UpsertUniswapV3Position inserts or updates a position registry row by
// (chain_id, nfpm_address, token_id). The conflict path only refreshes the
// mutable owner / burned columns plus updated_at — the static pool / tick /
// fee / created_at_block columns are NOT overwritten, since they are
// immutable from the chain's perspective.
func (r *UniswapV3PoolRepository) UpsertUniswapV3Position(ctx context.Context, tx pgx.Tx, pos *entity.UniswapV3Position) (int64, error) {
	if pos.TokenID == nil {
		return 0, fmt.Errorf("token_id must not be nil")
	}
	tokenID := pgtype.Numeric{Int: new(big.Int).Set(pos.TokenID), Exp: 0, Valid: true}

	var id int64
	err := tx.QueryRow(ctx, `
		INSERT INTO uniswap_v3_position (
		    chain_id, nfpm_address, token_id, uniswap_v3_pool_id, owner,
		    tick_lower, tick_upper, fee, created_at_block, burned, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
		ON CONFLICT (chain_id, nfpm_address, token_id) DO UPDATE SET
		    owner      = EXCLUDED.owner,
		    burned     = EXCLUDED.burned,
		    updated_at = NOW()
		RETURNING id`,
		pos.ChainID, pos.NFPMAddress.Bytes(), tokenID, pos.UniswapV3PoolID, pos.Owner.Bytes(),
		pos.TickLower, pos.TickUpper, pos.Fee, pos.CreatedAtBlock, pos.Burned,
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("upserting uniswap v3 position: %w", err)
	}
	return id, nil
}

// SetUniswapV3PositionOwner updates the owner of a registered position. No-op
// (no row updated) if the position id does not exist — the caller treats that
// as "Transfer for a position we don't track" and ignores it.
func (r *UniswapV3PoolRepository) SetUniswapV3PositionOwner(ctx context.Context, tx pgx.Tx, positionID int64, owner common.Address) error {
	_, err := tx.Exec(ctx, `
		UPDATE uniswap_v3_position
		SET owner = $2, updated_at = NOW()
		WHERE id = $1`,
		positionID, owner.Bytes(),
	)
	if err != nil {
		return fmt.Errorf("updating uniswap v3 position owner: %w", err)
	}
	return nil
}

// SetUniswapV3PositionBurned flips the burned flag to true for the given
// position. Called when DecreaseLiquidity drops the position's liquidity to 0.
func (r *UniswapV3PoolRepository) SetUniswapV3PositionBurned(ctx context.Context, tx pgx.Tx, positionID int64) error {
	_, err := tx.Exec(ctx, `
		UPDATE uniswap_v3_position
		SET burned = true, updated_at = NOW()
		WHERE id = $1`,
		positionID,
	)
	if err != nil {
		return fmt.Errorf("updating uniswap v3 position burned: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Hypertables
// ---------------------------------------------------------------------------

// SaveUniswapV3PoolState writes one row to uniswap_v3_pool_state. The BEFORE
// INSERT trigger fills processing_version per ADR-0002: same (natural key,
// build_id) reuses the existing version; different build_id bumps to MAX+1.
// ON CONFLICT DO NOTHING on the PK is the second half: same-build retries
// swallow silently, cross-build reprocessing lands at a fresh version slot.
//
// The uniswap_v3_pool_current head row is maintained entirely by
// trigger_refresh_uniswap_v3_pool_current on this INSERT; never write that
// table from Go (a second writer would race the trigger).
func (r *UniswapV3PoolRepository) SaveUniswapV3PoolState(ctx context.Context, tx pgx.Tx, state *entity.UniswapV3PoolState) error {
	sqrtPriceX96, err := bigIntToNumericRequired(state.SqrtPriceX96, "sqrt_price_x96")
	if err != nil {
		return err
	}
	liquidity, err := bigIntToNumericRequired(state.Liquidity, "liquidity")
	if err != nil {
		return err
	}
	tickCumulative := bigIntToNullableNumeric(state.TickCumulative)
	secsPerLiquidity := bigIntToNullableNumeric(state.SecsPerLiquidityCumulativeX128)
	balance0 := bigIntToNullableNumeric(state.Balance0)
	balance1 := bigIntToNullableNumeric(state.Balance1)

	observationIndex := nullableInt32(state.ObservationIndex)
	observationCardinality := nullableInt32(state.ObservationCardinality)
	observationCardinalityNext := nullableInt32(state.ObservationCardinalityNext)
	feeProtocol := nullableInt32(state.FeeProtocol)

	var unlocked any
	if state.Unlocked != nil {
		unlocked = *state.Unlocked
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO uniswap_v3_pool_state (
		    uniswap_v3_pool_id, block_number, block_version, timestamp, source,
		    sqrt_price_x96, tick, liquidity,
		    observation_index, observation_cardinality, observation_cardinality_next,
		    fee_protocol, unlocked,
		    tick_cumulative, secs_per_liquidity_cumulative_x128,
		    balance0, balance1, build_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
		ON CONFLICT (uniswap_v3_pool_id, block_number, block_version, processing_version, timestamp) DO NOTHING`,
		state.UniswapV3PoolID, state.BlockNumber, state.BlockVersion, state.Timestamp, state.Source,
		sqrtPriceX96, state.Tick, liquidity,
		observationIndex, observationCardinality, observationCardinalityNext,
		feeProtocol, unlocked,
		tickCumulative, secsPerLiquidity,
		balance0, balance1, int(r.buildID),
	)
	if err != nil {
		return fmt.Errorf("saving uniswap v3 pool state: %w", err)
	}
	return nil
}

// SaveUniswapV3PoolSwap writes one row to uniswap_v3_pool_swap.
func (r *UniswapV3PoolRepository) SaveUniswapV3PoolSwap(ctx context.Context, tx pgx.Tx, swap *entity.UniswapV3PoolSwap) error {
	amount0, err := bigIntToNumericRequired(swap.Amount0, "amount0")
	if err != nil {
		return err
	}
	amount1, err := bigIntToNumericRequired(swap.Amount1, "amount1")
	if err != nil {
		return err
	}
	sqrtPriceX96After, err := bigIntToNumericRequired(swap.SqrtPriceX96After, "sqrt_price_x96_after")
	if err != nil {
		return err
	}
	liquidityAfter, err := bigIntToNumericRequired(swap.LiquidityAfter, "liquidity_after")
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO uniswap_v3_pool_swap (
		    uniswap_v3_pool_id, block_number, block_version, timestamp,
		    tx_hash, log_index, sender, recipient,
		    amount0, amount1, sqrt_price_x96_after, liquidity_after, tick_after, build_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
		ON CONFLICT (uniswap_v3_pool_id, block_number, block_version, tx_hash, log_index, processing_version, timestamp) DO NOTHING`,
		swap.UniswapV3PoolID, swap.BlockNumber, swap.BlockVersion, swap.Timestamp,
		swap.TxHash.Bytes(), swap.LogIndex, swap.Sender.Bytes(), swap.Recipient.Bytes(),
		amount0, amount1, sqrtPriceX96After, liquidityAfter, swap.TickAfter, int(r.buildID),
	)
	if err != nil {
		return fmt.Errorf("saving uniswap v3 pool swap: %w", err)
	}
	return nil
}

// SaveUniswapV3PositionState writes one row to uniswap_v3_position_state.
func (r *UniswapV3PoolRepository) SaveUniswapV3PositionState(ctx context.Context, tx pgx.Tx, state *entity.UniswapV3PositionState) error {
	liquidity, err := bigIntToNumericRequired(state.Liquidity, "liquidity")
	if err != nil {
		return err
	}
	tokensOwed0, err := bigIntToNumericRequired(state.TokensOwed0, "tokens_owed0")
	if err != nil {
		return err
	}
	tokensOwed1, err := bigIntToNumericRequired(state.TokensOwed1, "tokens_owed1")
	if err != nil {
		return err
	}
	feeGrowth0, err := bigIntToNumericRequired(state.FeeGrowthInside0LastX128, "fee_growth_inside0_last_x128")
	if err != nil {
		return err
	}
	feeGrowth1, err := bigIntToNumericRequired(state.FeeGrowthInside1LastX128, "fee_growth_inside1_last_x128")
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO uniswap_v3_position_state (
		    uniswap_v3_position_id, block_number, block_version, timestamp, source,
		    liquidity, tokens_owed0, tokens_owed1,
		    fee_growth_inside0_last_x128, fee_growth_inside1_last_x128, build_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (uniswap_v3_position_id, block_number, block_version, processing_version, timestamp) DO NOTHING`,
		state.UniswapV3PositionID, state.BlockNumber, state.BlockVersion, state.Timestamp, state.Source,
		liquidity, tokensOwed0, tokensOwed1,
		feeGrowth0, feeGrowth1, int(r.buildID),
	)
	if err != nil {
		return fmt.Errorf("saving uniswap v3 position state: %w", err)
	}
	return nil
}

// SaveUniswapV3PoolLiquidityEvent writes one row to
// uniswap_v3_pool_liquidity_event. The DB CHECK constraint enforces the
// per-kind shape (Mint/Burn/Collect); this method passes the entity through
// faithfully and lets the constraint be the source of truth.
func (r *UniswapV3PoolRepository) SaveUniswapV3PoolLiquidityEvent(ctx context.Context, tx pgx.Tx, evt *entity.UniswapV3PoolLiquidityEvent) error {
	amount := bigIntToNullableNumeric(evt.Amount)
	amount0 := bigIntToNullableNumeric(evt.Amount0)
	amount1 := bigIntToNullableNumeric(evt.Amount1)

	var sender, recipient any
	if evt.Sender != nil {
		sender = evt.Sender.Bytes()
	}
	if evt.Recipient != nil {
		recipient = evt.Recipient.Bytes()
	}

	_, err := tx.Exec(ctx, `
		INSERT INTO uniswap_v3_pool_liquidity_event (
		    uniswap_v3_pool_id, block_number, block_version, timestamp,
		    tx_hash, log_index, event_kind, owner,
		    tick_lower, tick_upper, amount, amount0, amount1,
		    sender, recipient, build_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
		ON CONFLICT (uniswap_v3_pool_id, block_number, block_version, tx_hash, log_index, processing_version, timestamp) DO NOTHING`,
		evt.UniswapV3PoolID, evt.BlockNumber, evt.BlockVersion, evt.Timestamp,
		evt.TxHash.Bytes(), evt.LogIndex, evt.EventKind, evt.Owner.Bytes(),
		evt.TickLower, evt.TickUpper, amount, amount0, amount1,
		sender, recipient, int(r.buildID),
	)
	if err != nil {
		return fmt.Errorf("saving uniswap v3 pool liquidity event: %w", err)
	}
	return nil
}

// SaveUniswapV3PoolParameterEvent writes one row to
// uniswap_v3_pool_parameter_event. Only the columns relevant to the given
// EventKind should be populated by callers; everything else is passed
// through as SQL NULL.
func (r *UniswapV3PoolRepository) SaveUniswapV3PoolParameterEvent(ctx context.Context, tx pgx.Tx, evt *entity.UniswapV3PoolParameterEvent) error {
	sqrtPriceX96 := bigIntToNullableNumeric(evt.SqrtPriceX96)
	amount0 := bigIntToNullableNumeric(evt.Amount0)
	amount1 := bigIntToNullableNumeric(evt.Amount1)

	tick := nullableInt32(evt.Tick)
	observationCardinalityOld := nullableInt32(evt.ObservationCardinalityOld)
	observationCardinalityNew := nullableInt32(evt.ObservationCardinalityNew)
	feeProtocol0Old := nullableInt32(evt.FeeProtocol0Old)
	feeProtocol0New := nullableInt32(evt.FeeProtocol0New)
	feeProtocol1Old := nullableInt32(evt.FeeProtocol1Old)
	feeProtocol1New := nullableInt32(evt.FeeProtocol1New)

	var sender, recipient any
	if evt.Sender != nil {
		sender = evt.Sender.Bytes()
	}
	if evt.Recipient != nil {
		recipient = evt.Recipient.Bytes()
	}

	var extra any
	if len(evt.Extra) > 0 {
		extra = []byte(evt.Extra)
	}

	_, err := tx.Exec(ctx, `
		INSERT INTO uniswap_v3_pool_parameter_event (
		    uniswap_v3_pool_id, block_number, block_version, timestamp,
		    tx_hash, log_index, event_kind,
		    sqrt_price_x96, tick,
		    observation_cardinality_old, observation_cardinality_new,
		    fee_protocol0_old, fee_protocol0_new,
		    fee_protocol1_old, fee_protocol1_new,
		    amount0, amount1, sender, recipient,
		    extra, build_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
		ON CONFLICT (uniswap_v3_pool_id, block_number, block_version, tx_hash, log_index, processing_version, timestamp) DO NOTHING`,
		evt.UniswapV3PoolID, evt.BlockNumber, evt.BlockVersion, evt.Timestamp,
		evt.TxHash.Bytes(), evt.LogIndex, evt.EventKind,
		sqrtPriceX96, tick,
		observationCardinalityOld, observationCardinalityNew,
		feeProtocol0Old, feeProtocol0New,
		feeProtocol1Old, feeProtocol1New,
		amount0, amount1, sender, recipient,
		extra, int(r.buildID),
	)
	if err != nil {
		return fmt.Errorf("saving uniswap v3 pool parameter event: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Uniswap V3-only helpers (generic big.Int → NUMERIC helpers live alongside
// the Curve adapter in curve_pool_repository.go).
// ---------------------------------------------------------------------------

// nullableInt32 returns the int32's value as a driver `any` when present, or
// nil for a SQL NULL when the pointer is nil. Used for the many nullable INT
// columns on the Uniswap V3 parameter / state tables.
func nullableInt32(v *int32) any {
	if v == nil {
		return nil
	}
	return *v
}

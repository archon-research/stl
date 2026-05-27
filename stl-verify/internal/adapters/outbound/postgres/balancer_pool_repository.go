package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that BalancerPoolRepository implements
// outbound.BalancerPoolRepository.
var _ outbound.BalancerPoolRepository = (*BalancerPoolRepository)(nil)

// BalancerPoolRepository is a PostgreSQL implementation of the
// outbound.BalancerPoolRepository port. Writes flow through caller-supplied
// transactions so a single block's typed projections, raw protocol_event
// row, and per-token / state writes commit atomically.
type BalancerPoolRepository struct {
	pool    *pgxpool.Pool
	logger  *slog.Logger
	buildID buildregistry.BuildID
}

// NewBalancerPoolRepository creates a new PostgreSQL Balancer V2 repository.
func NewBalancerPoolRepository(pool *pgxpool.Pool, logger *slog.Logger, buildID buildregistry.BuildID) (*BalancerPoolRepository, error) {
	if pool == nil {
		return nil, fmt.Errorf("database pool cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &BalancerPoolRepository{
		pool:    pool,
		logger:  logger,
		buildID: buildID,
	}, nil
}

// ---------------------------------------------------------------------------
// Registry: balancer_pool
// ---------------------------------------------------------------------------

// GetBalancerPool retrieves a Balancer V2 pool registry row by (chain, address).
func (r *BalancerPoolRepository) GetBalancerPool(ctx context.Context, chainID int64, address common.Address) (*entity.BalancerPool, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT id, chain_id, pool_kind, address, pool_id, vault_address, label,
		       deployment_block, enabled, created_at, updated_at
		FROM balancer_pool
		WHERE chain_id = $1 AND address = $2`,
		chainID, address.Bytes(),
	)
	p, err := scanBalancerPool(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("querying balancer pool: %w", err)
	}
	return p, nil
}

// ListEnabledBalancerPools returns every enabled Balancer V2 pool on the given chain.
func (r *BalancerPoolRepository) ListEnabledBalancerPools(ctx context.Context, chainID int64) ([]*entity.BalancerPool, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, chain_id, pool_kind, address, pool_id, vault_address, label,
		       deployment_block, enabled, created_at, updated_at
		FROM balancer_pool
		WHERE chain_id = $1 AND enabled = true
		ORDER BY id`,
		chainID,
	)
	if err != nil {
		return nil, fmt.Errorf("listing enabled balancer pools: %w", err)
	}
	defer rows.Close()

	var out []*entity.BalancerPool
	for rows.Next() {
		p, err := scanBalancerPool(rows)
		if err != nil {
			return nil, fmt.Errorf("scanning balancer pool: %w", err)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating balancer pools: %w", err)
	}
	return out, nil
}

func scanBalancerPool(row scanRow) (*entity.BalancerPool, error) {
	var (
		p               entity.BalancerPool
		addressBytes    []byte
		poolIDBytes     []byte
		vaultBytes      []byte
		deploymentBlock pgtype.Int8
	)
	if err := row.Scan(
		&p.ID, &p.ChainID, &p.PoolKind, &addressBytes, &poolIDBytes, &vaultBytes, &p.Label,
		&deploymentBlock, &p.Enabled, &p.CreatedAt, &p.UpdatedAt,
	); err != nil {
		return nil, err
	}
	p.Address = common.BytesToAddress(addressBytes)
	p.PoolID = common.BytesToHash(poolIDBytes)
	p.VaultAddress = common.BytesToAddress(vaultBytes)
	if deploymentBlock.Valid {
		v := deploymentBlock.Int64
		p.DeploymentBlock = &v
	}
	return &p, nil
}

// ---------------------------------------------------------------------------
// Registry: balancer_pool_token
// ---------------------------------------------------------------------------

// ListBalancerPoolTokens returns the join-table rows for a pool in
// token_index order.
func (r *BalancerPoolRepository) ListBalancerPoolTokens(ctx context.Context, balancerPoolID int64) ([]*entity.BalancerPoolToken, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT balancer_pool_id, token_index, token_id, is_phantom, rate_provider,
		       created_at, updated_at
		FROM balancer_pool_token
		WHERE balancer_pool_id = $1
		ORDER BY token_index`,
		balancerPoolID,
	)
	if err != nil {
		return nil, fmt.Errorf("listing balancer pool tokens: %w", err)
	}
	defer rows.Close()

	var out []*entity.BalancerPoolToken
	for rows.Next() {
		var (
			t                entity.BalancerPoolToken
			rateProviderBlob []byte // pgx scans SQL NULL into a nil []byte.
		)
		if err := rows.Scan(
			&t.BalancerPoolID, &t.TokenIndex, &t.TokenID, &t.IsPhantom, &rateProviderBlob,
			&t.CreatedAt, &t.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scanning balancer pool token: %w", err)
		}
		if rateProviderBlob != nil {
			a := common.BytesToAddress(rateProviderBlob)
			t.RateProvider = &a
		}
		out = append(out, &t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating balancer pool tokens: %w", err)
	}
	return out, nil
}

// UpsertBalancerPoolToken inserts or updates a balancer_pool_token row by
// (balancer_pool_id, token_index). The conflict path refreshes the mutable
// token_id / is_phantom / rate_provider columns plus updated_at.
func (r *BalancerPoolRepository) UpsertBalancerPoolToken(ctx context.Context, tx pgx.Tx, t *entity.BalancerPoolToken) error {
	var rateProvider any
	if t.RateProvider != nil {
		rateProvider = t.RateProvider.Bytes()
	}

	_, err := tx.Exec(ctx, `
		INSERT INTO balancer_pool_token (
		    balancer_pool_id, token_index, token_id, is_phantom, rate_provider, updated_at
		) VALUES ($1, $2, $3, $4, $5, NOW())
		ON CONFLICT (balancer_pool_id, token_index) DO UPDATE SET
		    token_id      = EXCLUDED.token_id,
		    is_phantom    = EXCLUDED.is_phantom,
		    rate_provider = EXCLUDED.rate_provider,
		    updated_at    = NOW()`,
		t.BalancerPoolID, t.TokenIndex, t.TokenID, t.IsPhantom, rateProvider,
	)
	if err != nil {
		return fmt.Errorf("upserting balancer pool token: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Hypertables
// ---------------------------------------------------------------------------

// SaveBalancerPoolState writes one row to balancer_pool_state. The BEFORE
// INSERT trigger fills processing_version; no ON CONFLICT clause — replay
// safety is the trigger's job and a duplicate (same build, same key) will
// collide on the PK, which the caller treats as already-written.
func (r *BalancerPoolRepository) SaveBalancerPoolState(ctx context.Context, tx pgx.Tx, state *entity.BalancerPoolState) error {
	balances, err := bigIntsToNumericArray(state.Balances)
	if err != nil {
		return fmt.Errorf("converting balances: %w", err)
	}
	ampFactor := bigIntToNullableNumeric(state.AmpFactor)
	bptRate := bigIntToNullableNumeric(state.BptRate)
	actualSupply := bigIntToNullableNumeric(state.ActualSupply)
	totalSupply := bigIntToNullableNumeric(state.TotalSupply)
	tokenRates, err := bigIntsToNullableNumericArray(state.TokenRates)
	if err != nil {
		return fmt.Errorf("converting token_rates: %w", err)
	}
	scalingFactors, err := bigIntsToNullableNumericArray(state.ScalingFactors)
	if err != nil {
		return fmt.Errorf("converting scaling_factors: %w", err)
	}

	var lastChangeBlock any
	if state.LastChangeBlock != nil {
		lastChangeBlock = *state.LastChangeBlock
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO balancer_pool_state (
		    balancer_pool_id, block_number, block_version, timestamp, source,
		    balances, amp_factor, bpt_rate, actual_supply, total_supply,
		    token_rates, scaling_factors, last_change_block, build_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
		ON CONFLICT (balancer_pool_id, block_number, block_version, processing_version, timestamp) DO NOTHING`,
		state.BalancerPoolID, state.BlockNumber, state.BlockVersion, state.Timestamp, state.Source,
		balances, ampFactor, bptRate, actualSupply, totalSupply,
		tokenRates, scalingFactors, lastChangeBlock, int(r.buildID),
	)
	if err != nil {
		return fmt.Errorf("saving balancer pool state: %w", err)
	}
	return nil
}

// SaveBalancerPoolSwap writes one row to balancer_pool_swap.
func (r *BalancerPoolRepository) SaveBalancerPoolSwap(ctx context.Context, tx pgx.Tx, swap *entity.BalancerPoolSwap) error {
	amountIn, err := bigIntToNumericRequired(swap.AmountIn, "amount_in")
	if err != nil {
		return err
	}
	amountOut, err := bigIntToNumericRequired(swap.AmountOut, "amount_out")
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO balancer_pool_swap (
		    balancer_pool_id, block_number, block_version, timestamp,
		    tx_hash, log_index, token_in_idx, token_out_idx,
		    amount_in, amount_out, build_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (balancer_pool_id, block_number, block_version, tx_hash, log_index, processing_version, timestamp) DO NOTHING`,
		swap.BalancerPoolID, swap.BlockNumber, swap.BlockVersion, swap.Timestamp,
		swap.TxHash.Bytes(), swap.LogIndex, swap.TokenInIdx, swap.TokenOutIdx,
		amountIn, amountOut, int(r.buildID),
	)
	if err != nil {
		return fmt.Errorf("saving balancer pool swap: %w", err)
	}
	return nil
}

// SaveBalancerPoolLiquidityEvent writes one row to
// balancer_pool_liquidity_event.
func (r *BalancerPoolRepository) SaveBalancerPoolLiquidityEvent(ctx context.Context, tx pgx.Tx, evt *entity.BalancerPoolLiquidityEvent) error {
	deltas, err := bigIntsToNumericArray(evt.Deltas)
	if err != nil {
		return fmt.Errorf("converting deltas: %w", err)
	}
	protocolFeeAmounts, err := bigIntsToNullableNumericArray(evt.ProtocolFeeAmounts)
	if err != nil {
		return fmt.Errorf("converting protocol_fee_amounts: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO balancer_pool_liquidity_event (
		    balancer_pool_id, block_number, block_version, timestamp,
		    tx_hash, log_index, liquidity_provider,
		    deltas, protocol_fee_amounts, build_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (balancer_pool_id, block_number, block_version, tx_hash, log_index, processing_version, timestamp) DO NOTHING`,
		evt.BalancerPoolID, evt.BlockNumber, evt.BlockVersion, evt.Timestamp,
		evt.TxHash.Bytes(), evt.LogIndex, evt.LiquidityProvider.Bytes(),
		deltas, protocolFeeAmounts, int(r.buildID),
	)
	if err != nil {
		return fmt.Errorf("saving balancer pool liquidity event: %w", err)
	}
	return nil
}

// SaveBalancerPoolParameterEvent writes one row to
// balancer_pool_parameter_event. Only the columns relevant to the given
// EventKind should be populated by callers; everything else is passed
// through as SQL NULL.
func (r *BalancerPoolRepository) SaveBalancerPoolParameterEvent(ctx context.Context, tx pgx.Tx, evt *entity.BalancerPoolParameterEvent) error {
	startValue := bigIntToNullableNumeric(evt.StartValue)
	endValue := bigIntToNullableNumeric(evt.EndValue)
	currentValue := bigIntToNullableNumeric(evt.CurrentValue)
	rate := bigIntToNullableNumeric(evt.Rate)
	swapFeePercentage := bigIntToNullableNumeric(evt.SwapFeePercentage)

	var startTime, endTime any
	if evt.StartTime != nil {
		startTime = *evt.StartTime
	}
	if evt.EndTime != nil {
		endTime = *evt.EndTime
	}

	var tokenAddress, rateProvider any
	if evt.TokenAddress != nil {
		tokenAddress = evt.TokenAddress.Bytes()
	}
	if evt.RateProvider != nil {
		rateProvider = evt.RateProvider.Bytes()
	}

	cacheDuration := nullableInt32(evt.CacheDuration)

	var extra any
	if len(evt.Extra) > 0 {
		extra = []byte(evt.Extra)
	}

	_, err := tx.Exec(ctx, `
		INSERT INTO balancer_pool_parameter_event (
		    balancer_pool_id, block_number, block_version, timestamp,
		    tx_hash, log_index, event_kind,
		    start_value, end_value, start_time, end_time, current_value,
		    token_address, rate_provider, cache_duration, rate, swap_fee_percentage,
		    extra, build_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
		ON CONFLICT (balancer_pool_id, block_number, block_version, tx_hash, log_index, processing_version, timestamp) DO NOTHING`,
		evt.BalancerPoolID, evt.BlockNumber, evt.BlockVersion, evt.Timestamp,
		evt.TxHash.Bytes(), evt.LogIndex, evt.EventKind,
		startValue, endValue, startTime, endTime, currentValue,
		tokenAddress, rateProvider, cacheDuration, rate, swapFeePercentage,
		extra, int(r.buildID),
	)
	if err != nil {
		return fmt.Errorf("saving balancer pool parameter event: %w", err)
	}
	return nil
}

// SaveBalancerUserBptPosition writes one row to balancer_user_bpt_position.
func (r *BalancerPoolRepository) SaveBalancerUserBptPosition(ctx context.Context, tx pgx.Tx, pos *entity.BalancerUserBptPosition) error {
	bptBalance, err := bigIntToNumericRequired(pos.BptBalance, "bpt_balance")
	if err != nil {
		return err
	}
	delta, err := bigIntToNumericRequired(pos.Delta, "delta")
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO balancer_user_bpt_position (
		    balancer_pool_id, user_address, block_number, block_version, timestamp,
		    tx_hash, log_index, bpt_balance, delta, build_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (balancer_pool_id, user_address, block_number, block_version, tx_hash, log_index, processing_version, timestamp) DO NOTHING`,
		pos.BalancerPoolID, pos.UserAddress.Bytes(), pos.BlockNumber, pos.BlockVersion, pos.Timestamp,
		pos.TxHash.Bytes(), pos.LogIndex, bptBalance, delta, int(r.buildID),
	)
	if err != nil {
		return fmt.Errorf("saving balancer user bpt position: %w", err)
	}
	return nil
}

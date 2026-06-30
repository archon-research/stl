package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"sort"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that CurveRepository implements outbound.CurveRepository.
var _ outbound.CurveRepository = (*CurveRepository)(nil)

// CurveRepository is a PostgreSQL implementation of the outbound.CurveRepository port.
type CurveRepository struct {
	pool    *pgxpool.Pool
	logger  *slog.Logger
	buildID buildregistry.BuildID
}

// NewCurveRepository creates a new PostgreSQL Curve repository.
func NewCurveRepository(pool *pgxpool.Pool, logger *slog.Logger, buildID buildregistry.BuildID) (*CurveRepository, error) {
	if pool == nil {
		return nil, fmt.Errorf("database pool cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &CurveRepository{
		pool:    pool,
		logger:  logger,
		buildID: buildID,
	}, nil
}

// LoadPools returns all pools for the given chain with their coin decimals in coin_index order.
func (r *CurveRepository) LoadPools(ctx context.Context, chainID int64) ([]outbound.CurvePoolRow, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT cp.id, cp.protocol_id, cp.pool_address, cp.pool_kind, cp.n_coins, cp.deploy_block, cp.lp_token_address, t.decimals, cpc.precision
		 FROM curve_pool cp
		 JOIN curve_pool_coin cpc ON cpc.curve_pool_id = cp.id
		 JOIN token t ON t.id = cpc.token_id
		 WHERE cp.chain_id = $1
		 ORDER BY cp.id, cpc.coin_index`,
		chainID,
	)
	if err != nil {
		return nil, fmt.Errorf("querying curve pools: %w", err)
	}
	defer rows.Close()

	var result []outbound.CurvePoolRow
	index := make(map[int64]int) // pool id -> index in result

	for rows.Next() {
		var (
			poolID      int64
			protocolID  int64
			poolAddress []byte
			kind        string
			nCoins      int
			deployBlock *int64
			lpToken     []byte
			decimals    int
			precision   pgtype.Numeric
		)
		if err := rows.Scan(&poolID, &protocolID, &poolAddress, &kind, &nCoins, &deployBlock, &lpToken, &decimals, &precision); err != nil {
			return nil, fmt.Errorf("scanning curve pool row: %w", err)
		}

		idx, exists := index[poolID]
		if !exists {
			// A NULL deploy_block (pool registered before its deploy height was
			// backfilled) maps to 0 so LoadPools still returns the pool.
			var db int64
			if deployBlock != nil {
				db = *deployBlock
			}
			var lpAddr *common.Address
			if lpToken != nil {
				a := common.BytesToAddress(lpToken)
				lpAddr = &a
			}
			idx = len(result)
			index[poolID] = idx
			result = append(result, outbound.CurvePoolRow{
				ID:             poolID,
				ProtocolID:     protocolID,
				Address:        common.BytesToAddress(poolAddress),
				Kind:           kind,
				NCoins:         nCoins,
				DeployBlock:    db,
				LpTokenAddress: lpAddr,
			})
		}
		prec, convErr := NumericToNullableBigInt(precision)
		if convErr != nil {
			return nil, fmt.Errorf("converting precision for curve pool %d: %w", poolID, convErr)
		}
		result[idx].CoinDecimals = append(result[idx].CoinDecimals, decimals)
		result[idx].Precisions = append(result[idx].Precisions, prec)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating curve pools: %w", err)
	}
	return result, nil
}

// swapConverted holds pre-converted numeric values for a curve_swap insert.
type swapConverted struct {
	in           outbound.SwapInput
	tokensSold   any
	tokensBought any
}

// liquidityConverted holds pre-converted numeric values for a curve_liquidity_event insert.
type liquidityConverted struct {
	in           outbound.LiquidityInput
	tokenAmounts any
	fees         any
}

// stableConverted holds pre-converted numeric values for a curve_stableswap_state insert.
type stableConverted struct {
	s             *entity.CurveStableswapState
	balances      any
	vp            any
	ts            any
	a             any
	fee           any
	spotDy        any
	adminBalances any
	storedRates   any
	calcWithdraw  any
}

// cryptoConverted holds pre-converted numeric values for a curve_cryptoswap_state insert.
type cryptoConverted struct {
	s             *entity.CurveCryptoswapState
	balances      any
	vp            any
	ts            any
	a             any
	gamma         any
	fee           any
	priceScale    any
	priceOracle   any
	lastPrices    any
	spotDy        any
	adminBalances any
	getDx         any
	calcWithdraw  any
}

// SaveBlock persists all of a block's curve rows in one pgx.Batch within tx.
func (r *CurveRepository) SaveBlock(ctx context.Context, tx pgx.Tx, w outbound.BlockWrites) (stateRows int64, err error) {
	swaps, err := convertSwaps(w.Swaps)
	if err != nil {
		return 0, err
	}
	liqs, err := convertLiquidity(w.Liquidity)
	if err != nil {
		return 0, err
	}
	stables, err := convertStableStates(w.StableStates)
	if err != nil {
		return 0, err
	}
	cryptos, err := convertCryptoStates(w.CryptoStates)
	if err != nil {
		return 0, err
	}

	batch := &pgx.Batch{}
	if err := queueCurveBatch(batch, swaps, liqs, stables, cryptos, w.ParameterEvents, w.LpTokenEvents, r.buildID); err != nil {
		return 0, err
	}

	stateRows, err = sendCurveBatch(ctx, tx, batch, swaps, liqs, stables, cryptos, w.ParameterEvents, w.LpTokenEvents)
	if err != nil {
		return stateRows, err
	}

	// Config rows are written after the batch reader is fully closed: pgx forbids
	// issuing new queries on a connection while a batch result reader is open, and
	// each config insert depends on a prior read of the latest row for that pool
	// (a read-then-write race ON CONFLICT cannot guard, ADR-0002 §3). The pool
	// locks are acquired in sorted curve_pool_id order so concurrent SaveBlock
	// transactions never deadlock.
	if err := r.writeConfigs(ctx, tx, w.StableswapConfigs, w.CryptoswapConfigs); err != nil {
		return stateRows, err
	}

	return stateRows, nil
}

// convertSwaps pre-converts all numeric fields for swap inserts. Any conversion
// error is returned immediately so the caller gets a clear message without
// partially-queued state.
func convertSwaps(inputs []outbound.SwapInput) ([]swapConverted, error) {
	out := make([]swapConverted, 0, len(inputs))
	for i, in := range inputs {
		tokensSold, convErr := BigIntToNumericRequired(in.TokensSold, "tokens_sold")
		if convErr != nil {
			return nil, fmt.Errorf("swap %d converting tokens_sold: %w", i, convErr)
		}
		tokensBought, convErr := BigIntToNumericRequired(in.TokensBought, "tokens_bought")
		if convErr != nil {
			return nil, fmt.Errorf("swap %d converting tokens_bought: %w", i, convErr)
		}
		out = append(out, swapConverted{in: in, tokensSold: tokensSold, tokensBought: tokensBought})
	}
	return out, nil
}

// convertLiquidity pre-converts all numeric fields for liquidity event inserts.
func convertLiquidity(inputs []outbound.LiquidityInput) ([]liquidityConverted, error) {
	out := make([]liquidityConverted, 0, len(inputs))
	for i, in := range inputs {
		tokenAmounts, convErr := BigIntsToNumericArray(in.TokenAmounts)
		if convErr != nil {
			return nil, fmt.Errorf("liquidity %d converting token_amounts: %w", i, convErr)
		}
		fees, convErr := BigIntsToNullableNumericArray(in.Fees)
		if convErr != nil {
			return nil, fmt.Errorf("liquidity %d converting fees: %w", i, convErr)
		}
		out = append(out, liquidityConverted{in: in, tokenAmounts: tokenAmounts, fees: fees})
	}
	return out, nil
}

// convertStableStates pre-converts all numeric fields for stableswap state inserts.
func convertStableStates(states []*entity.CurveStableswapState) ([]stableConverted, error) {
	out := make([]stableConverted, 0, len(states))
	for i, s := range states {
		balances, convErr := BigIntsToNumericArray(s.Balances)
		if convErr != nil {
			return nil, fmt.Errorf("stableswap %d converting balances: %w", i, convErr)
		}
		spotDy, convErr := BigIntsToNumericArray(s.SpotDy)
		if convErr != nil {
			return nil, fmt.Errorf("stableswap %d converting spot_dy: %w", i, convErr)
		}
		vp, convErr := BigIntToNumericRequired(s.VirtualPrice, "virtual_price")
		if convErr != nil {
			return nil, fmt.Errorf("stableswap %d converting virtual_price: %w", i, convErr)
		}
		ts, convErr := BigIntToNumericRequired(s.TotalSupply, "total_supply")
		if convErr != nil {
			return nil, fmt.Errorf("stableswap %d converting total_supply: %w", i, convErr)
		}
		a, convErr := BigIntToNumericRequired(s.A, "a")
		if convErr != nil {
			return nil, fmt.Errorf("stableswap %d converting a: %w", i, convErr)
		}
		fee, convErr := BigIntToNumericRequired(s.Fee, "fee")
		if convErr != nil {
			return nil, fmt.Errorf("stableswap %d converting fee: %w", i, convErr)
		}
		adminBalances, convErr := BigIntsToNullableNumericArray(s.AdminBalances)
		if convErr != nil {
			return nil, fmt.Errorf("stableswap %d converting admin_balances: %w", i, convErr)
		}
		storedRates, convErr := BigIntsToNullableNumericArray(s.StoredRates)
		if convErr != nil {
			return nil, fmt.Errorf("stableswap %d converting stored_rates: %w", i, convErr)
		}
		calcWithdraw, convErr := BigIntsToNullableNumericArray(s.CalcWithdrawOneCoin)
		if convErr != nil {
			return nil, fmt.Errorf("stableswap %d converting calc_withdraw_one_coin: %w", i, convErr)
		}
		out = append(out, stableConverted{
			s: s, balances: balances, vp: vp, ts: ts, a: a, fee: fee, spotDy: spotDy,
			adminBalances: adminBalances, storedRates: storedRates, calcWithdraw: calcWithdraw,
		})
	}
	return out, nil
}

// convertCryptoStates pre-converts all numeric fields for cryptoswap state inserts.
func convertCryptoStates(states []*entity.CurveCryptoswapState) ([]cryptoConverted, error) {
	out := make([]cryptoConverted, 0, len(states))
	for i, s := range states {
		balances, convErr := BigIntsToNumericArray(s.Balances)
		if convErr != nil {
			return nil, fmt.Errorf("cryptoswap %d converting balances: %w", i, convErr)
		}
		vp, convErr := BigIntToNumericRequired(s.VirtualPrice, "virtual_price")
		if convErr != nil {
			return nil, fmt.Errorf("cryptoswap %d converting virtual_price: %w", i, convErr)
		}
		ts, convErr := BigIntToNumericRequired(s.TotalSupply, "total_supply")
		if convErr != nil {
			return nil, fmt.Errorf("cryptoswap %d converting total_supply: %w", i, convErr)
		}
		a, convErr := BigIntToNumericRequired(s.A, "a")
		if convErr != nil {
			return nil, fmt.Errorf("cryptoswap %d converting a: %w", i, convErr)
		}
		gamma, convErr := BigIntToNumericRequired(s.Gamma, "gamma")
		if convErr != nil {
			return nil, fmt.Errorf("cryptoswap %d converting gamma: %w", i, convErr)
		}
		fee, convErr := BigIntToNumericRequired(s.Fee, "fee")
		if convErr != nil {
			return nil, fmt.Errorf("cryptoswap %d converting fee: %w", i, convErr)
		}
		priceScale, convErr := BigIntsToNumericArray(s.PriceScale)
		if convErr != nil {
			return nil, fmt.Errorf("cryptoswap %d converting price_scale: %w", i, convErr)
		}
		priceOracle, convErr := BigIntsToNumericArray(s.PriceOracle)
		if convErr != nil {
			return nil, fmt.Errorf("cryptoswap %d converting price_oracle: %w", i, convErr)
		}
		lastPrices, convErr := BigIntsToNumericArray(s.LastPrices)
		if convErr != nil {
			return nil, fmt.Errorf("cryptoswap %d converting last_prices: %w", i, convErr)
		}
		spotDy, convErr := BigIntsToNumericArray(s.SpotDy)
		if convErr != nil {
			return nil, fmt.Errorf("cryptoswap %d converting spot_dy: %w", i, convErr)
		}
		adminBalances, convErr := BigIntsToNullableNumericArray(s.AdminBalances)
		if convErr != nil {
			return nil, fmt.Errorf("cryptoswap %d converting admin_balances: %w", i, convErr)
		}
		getDx, convErr := BigIntsToNullableNumericArray(s.GetDx)
		if convErr != nil {
			return nil, fmt.Errorf("cryptoswap %d converting get_dx: %w", i, convErr)
		}
		calcWithdraw, convErr := BigIntsToNullableNumericArray(s.CalcWithdrawOneCoin)
		if convErr != nil {
			return nil, fmt.Errorf("cryptoswap %d converting calc_withdraw_one_coin: %w", i, convErr)
		}
		out = append(out, cryptoConverted{
			s: s, balances: balances, vp: vp, ts: ts, a: a, gamma: gamma, fee: fee,
			priceScale: priceScale, priceOracle: priceOracle, lastPrices: lastPrices, spotDy: spotDy,
			adminBalances: adminBalances, getDx: getDx, calcWithdraw: calcWithdraw,
		})
	}
	return out, nil
}

// queueCurveBatch adds all converted rows to batch in the canonical order that
// sendCurveBatch expects to drain them: swaps, liquidity, stableswap states,
// cryptoswap states, parameter events, lp token events.
func queueCurveBatch(
	batch *pgx.Batch,
	swaps []swapConverted,
	liqs []liquidityConverted,
	stables []stableConverted,
	cryptos []cryptoConverted,
	parameterEvents []*entity.CurveParameterEvent,
	lpTokenEvents []*entity.CurveLpTokenEvent,
	buildID buildregistry.BuildID,
) error {
	for _, c := range swaps {
		batch.Queue(
			`INSERT INTO curve_swap
			   (curve_pool_id, block_number, block_version, block_timestamp,
			    tx_hash, log_index, buyer, sold_id, bought_id,
			    tokens_sold, tokens_bought, fee, is_underlying, build_id)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
			 ON CONFLICT (curve_pool_id, block_timestamp, block_number, block_version, log_index, processing_version) DO NOTHING`,
			c.in.CurvePoolID, c.in.BlockNumber, c.in.BlockVersion, c.in.BlockTimestamp,
			c.in.TxHash.Bytes(), c.in.LogIndex, c.in.Buyer.Bytes(), c.in.SoldID, c.in.BoughtID,
			c.tokensSold, c.tokensBought, BigIntToNullableNumeric(c.in.Fee), c.in.IsUnderlying, int(buildID),
		)
	}

	for _, c := range liqs {
		batch.Queue(
			`INSERT INTO curve_liquidity_event
			   (curve_pool_id, block_number, block_version, block_timestamp,
			    tx_hash, log_index, provider, kind, token_amounts,
			    coin_index, fees, invariant, token_supply, build_id)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
			 ON CONFLICT (curve_pool_id, block_timestamp, block_number, block_version, log_index, processing_version) DO NOTHING`,
			c.in.CurvePoolID, c.in.BlockNumber, c.in.BlockVersion, c.in.BlockTimestamp,
			c.in.TxHash.Bytes(), c.in.LogIndex, c.in.Provider.Bytes(), c.in.Kind, c.tokenAmounts,
			c.in.CoinIndex, c.fees, BigIntToNullableNumeric(c.in.Invariant), BigIntToNullableNumeric(c.in.TokenSupply),
			int(buildID),
		)
	}

	for _, c := range stables {
		batch.Queue(
			`INSERT INTO curve_stableswap_state
			   (curve_pool_id, block_number, block_version, block_timestamp,
			    balances, virtual_price, total_supply, a, fee, spot_dy,
			    last_price, price_oracle, a_precise, admin_balances, stored_rates,
			    ema_price, get_p, calc_token_amount, calc_withdraw_one_coin, build_id)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
			 ON CONFLICT (curve_pool_id, block_timestamp, block_number, block_version, processing_version) DO NOTHING`,
			c.s.CurvePoolID, c.s.BlockNumber, c.s.BlockVersion, c.s.Timestamp,
			c.balances, c.vp, c.ts, c.a, c.fee, c.spotDy,
			BigIntToNullableNumeric(c.s.LastPrice), BigIntToNullableNumeric(c.s.PriceOracle),
			BigIntToNullableNumeric(c.s.APrecise), c.adminBalances, c.storedRates,
			BigIntToNullableNumeric(c.s.EmaPrice), BigIntToNullableNumeric(c.s.GetP),
			BigIntToNullableNumeric(c.s.CalcTokenAmount), c.calcWithdraw, int(buildID),
		)
	}

	for _, c := range cryptos {
		batch.Queue(
			`INSERT INTO curve_cryptoswap_state
			   (curve_pool_id, block_number, block_version, block_timestamp,
			    balances, virtual_price, total_supply, a, gamma, fee,
			    d, xcp_profit, price_scale, price_oracle, last_prices, spot_dy,
			    admin_balances, lp_price, xcp_profit_a, last_prices_timestamp,
			    get_dx, calc_token_amount, calc_withdraw_one_coin, build_id)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24)
			 ON CONFLICT (curve_pool_id, block_timestamp, block_number, block_version, processing_version) DO NOTHING`,
			c.s.CurvePoolID, c.s.BlockNumber, c.s.BlockVersion, c.s.Timestamp,
			c.balances, c.vp, c.ts, c.a, c.gamma, c.fee,
			BigIntToNullableNumeric(c.s.D), BigIntToNullableNumeric(c.s.XcpProfit),
			c.priceScale, c.priceOracle, c.lastPrices, c.spotDy,
			c.adminBalances, BigIntToNullableNumeric(c.s.LpPrice), BigIntToNullableNumeric(c.s.XcpProfitA),
			c.s.LastPricesTimestamp, c.getDx, BigIntToNullableNumeric(c.s.CalcTokenAmount),
			c.calcWithdraw, int(buildID),
		)
	}

	for _, e := range parameterEvents {
		batch.Queue(
			`INSERT INTO curve_parameter_event
			   (curve_pool_id, block_number, block_version, block_timestamp,
			    tx_hash, log_index, event_name, params, build_id)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
			 ON CONFLICT (curve_pool_id, block_timestamp, block_number, block_version, log_index, processing_version) DO NOTHING`,
			e.CurvePoolID, e.BlockNumber, e.BlockVersion, e.Timestamp,
			e.TxHash.Bytes(), e.LogIndex, e.EventName, []byte(e.Params), int(buildID),
		)
	}

	for _, e := range lpTokenEvents {
		value, convErr := BigIntToNumericRequired(e.Value, "value")
		if convErr != nil {
			return fmt.Errorf("lp token event converting value: %w", convErr)
		}
		batch.Queue(
			`INSERT INTO curve_lp_token_event
			   (curve_pool_id, block_number, block_version, block_timestamp,
			    tx_hash, log_index, event_name, from_address, to_address, value, build_id)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
			 ON CONFLICT (curve_pool_id, block_timestamp, block_number, block_version, log_index, processing_version) DO NOTHING`,
			e.CurvePoolID, e.BlockNumber, e.BlockVersion, e.Timestamp,
			e.TxHash.Bytes(), e.LogIndex, e.EventName, e.From.Bytes(), e.To.Bytes(), value, int(buildID),
		)
	}

	return nil
}

// sendCurveBatch executes the queued batch and drains every result in queue
// order, returning the count of state rows inserted (only stableswap/cryptoswap
// states count). The batch reader is always closed before returning so the
// caller may issue further queries on tx.
func sendCurveBatch(
	ctx context.Context,
	tx pgx.Tx,
	batch *pgx.Batch,
	swaps []swapConverted,
	liqs []liquidityConverted,
	stables []stableConverted,
	cryptos []cryptoConverted,
	parameterEvents []*entity.CurveParameterEvent,
	lpTokenEvents []*entity.CurveLpTokenEvent,
) (stateRows int64, err error) {
	br := tx.SendBatch(ctx, batch)
	defer func() {
		if closeErr := br.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("closing curve SaveBlock batch: %w", closeErr)
		}
	}()

	for i := range swaps {
		if _, readErr := br.Exec(); readErr != nil {
			return stateRows, fmt.Errorf("batch swap %d: %w", i, readErr)
		}
	}

	for i := range liqs {
		if _, readErr := br.Exec(); readErr != nil {
			return stateRows, fmt.Errorf("batch liquidity %d: %w", i, readErr)
		}
	}

	for i := range stables {
		tag, readErr := br.Exec()
		if readErr != nil {
			return stateRows, fmt.Errorf("batch stableswap %d: %w", i, readErr)
		}
		stateRows += tag.RowsAffected()
	}

	for i := range cryptos {
		tag, readErr := br.Exec()
		if readErr != nil {
			return stateRows, fmt.Errorf("batch cryptoswap %d: %w", i, readErr)
		}
		stateRows += tag.RowsAffected()
	}

	for i := range parameterEvents {
		if _, readErr := br.Exec(); readErr != nil {
			return stateRows, fmt.Errorf("batch parameter event %d: %w", i, readErr)
		}
	}

	for i := range lpTokenEvents {
		if _, readErr := br.Exec(); readErr != nil {
			return stateRows, fmt.Errorf("batch lp token event %d: %w", i, readErr)
		}
	}

	return stateRows, nil
}

// writeConfigs persists the append-on-change config rows for both pool kinds.
// It first locks every affected pool (sorted ascending) so a SELECT-latest then
// INSERT decision is serialized against concurrent writers, then writes a new
// row only when no prior row exists or any field differs from the latest one.
func (r *CurveRepository) writeConfigs(
	ctx context.Context,
	tx pgx.Tx,
	stables []*entity.CurveStableswapConfig,
	cryptos []*entity.CurveCryptoswapConfig,
) error {
	if len(stables) == 0 && len(cryptos) == 0 {
		return nil
	}

	// Lock every affected pool in ascending id order before any read/insert so
	// concurrent block writers serialize deadlock-free (CLAUDE.md read-then-write
	// rule). This "curve_config|<id>" key is a distinct lock domain from the
	// triggers' "cssc|"/"ccsc|" keys on purpose: this guards the app-level
	// read-latest-then-insert decision, the trigger guards processing_version
	// assignment. They must not be harmonized.
	poolIDs := distinctSortedConfigPoolIDs(stables, cryptos)
	for _, poolID := range poolIDs {
		lockKey := fmt.Sprintf("curve_config|%d", poolID)
		if _, err := tx.Exec(ctx,
			`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
			lockKey,
		); err != nil {
			return fmt.Errorf("locking curve config pool %d: %w", poolID, err)
		}
	}

	for i, cfg := range stables {
		if err := r.writeStableswapConfig(ctx, tx, cfg); err != nil {
			return fmt.Errorf("stableswap config %d: %w", i, err)
		}
	}
	for i, cfg := range cryptos {
		if err := r.writeCryptoswapConfig(ctx, tx, cfg); err != nil {
			return fmt.Errorf("cryptoswap config %d: %w", i, err)
		}
	}
	return nil
}

func distinctSortedConfigPoolIDs(
	stables []*entity.CurveStableswapConfig,
	cryptos []*entity.CurveCryptoswapConfig,
) []int64 {
	seen := make(map[int64]struct{}, len(stables)+len(cryptos))
	for _, c := range stables {
		seen[c.CurvePoolID] = struct{}{}
	}
	for _, c := range cryptos {
		seen[c.CurvePoolID] = struct{}{}
	}
	ids := make([]int64, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (r *CurveRepository) writeStableswapConfig(ctx context.Context, tx pgx.Tx, cfg *entity.CurveStableswapConfig) error {
	var (
		initialA       pgtype.Numeric
		initialATime   int64
		futureA        pgtype.Numeric
		futureATime    int64
		adminFee       pgtype.Numeric
		futureFee      pgtype.Numeric
		futureAdminFee pgtype.Numeric
		maExpTime      *int64
		oracleMethod   pgtype.Numeric
	)
	err := tx.QueryRow(ctx,
		`SELECT initial_a, initial_a_time, future_a, future_a_time,
		        admin_fee, future_fee, future_admin_fee, ma_exp_time, oracle_method
		 FROM curve_stableswap_config
		 WHERE curve_pool_id = $1
		 ORDER BY block_number DESC, block_version DESC, processing_version DESC
		 LIMIT 1`,
		cfg.CurvePoolID,
	).Scan(&initialA, &initialATime, &futureA, &futureATime,
		&adminFee, &futureFee, &futureAdminFee, &maExpTime, &oracleMethod)

	switch {
	case err == nil:
		latest, convErr := toStableswapConfigValues(initialA, initialATime, futureA, futureATime,
			adminFee, futureFee, futureAdminFee, maExpTime, oracleMethod)
		if convErr != nil {
			return fmt.Errorf("reading latest stableswap config for pool %d: %w", cfg.CurvePoolID, convErr)
		}
		if stableswapConfigUnchanged(latest, cfg) {
			return nil
		}
	case errors.Is(err, pgx.ErrNoRows):
		// No prior row: fall through and insert the first one.
	default:
		return fmt.Errorf("querying latest stableswap config for pool %d: %w", cfg.CurvePoolID, err)
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO curve_stableswap_config
		   (curve_pool_id, block_number, block_version, block_timestamp,
		    initial_a, initial_a_time, future_a, future_a_time,
		    admin_fee, future_fee, future_admin_fee, ma_exp_time, oracle_method, build_id)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
		 ON CONFLICT (curve_pool_id, block_number, block_version, processing_version) DO NOTHING`,
		cfg.CurvePoolID, cfg.BlockNumber, cfg.BlockVersion, cfg.Timestamp,
		BigIntToNullableNumeric(cfg.InitialA), cfg.InitialATime,
		BigIntToNullableNumeric(cfg.FutureA), cfg.FutureATime,
		BigIntToNullableNumeric(cfg.AdminFee), BigIntToNullableNumeric(cfg.FutureFee),
		BigIntToNullableNumeric(cfg.FutureAdminFee), cfg.MaExpTime,
		BigIntToNullableNumeric(cfg.OracleMethod), int(r.buildID),
	); err != nil {
		return fmt.Errorf("inserting stableswap config for pool %d: %w", cfg.CurvePoolID, err)
	}
	return nil
}

type stableswapConfigValues struct {
	initialA       *big.Int
	initialATime   int64
	futureA        *big.Int
	futureATime    int64
	adminFee       *big.Int
	futureFee      *big.Int
	futureAdminFee *big.Int
	maExpTime      *int64
	oracleMethod   *big.Int
}

func toStableswapConfigValues(
	initialA pgtype.Numeric, initialATime int64, futureA pgtype.Numeric, futureATime int64,
	adminFee pgtype.Numeric, futureFee pgtype.Numeric, futureAdminFee pgtype.Numeric,
	maExpTime *int64, oracleMethod pgtype.Numeric,
) (stableswapConfigValues, error) {
	var v stableswapConfigValues
	var err error
	if v.initialA, err = NumericToNullableBigInt(initialA); err != nil {
		return v, fmt.Errorf("initial_a: %w", err)
	}
	if v.futureA, err = NumericToNullableBigInt(futureA); err != nil {
		return v, fmt.Errorf("future_a: %w", err)
	}
	if v.adminFee, err = NumericToNullableBigInt(adminFee); err != nil {
		return v, fmt.Errorf("admin_fee: %w", err)
	}
	if v.futureFee, err = NumericToNullableBigInt(futureFee); err != nil {
		return v, fmt.Errorf("future_fee: %w", err)
	}
	if v.futureAdminFee, err = NumericToNullableBigInt(futureAdminFee); err != nil {
		return v, fmt.Errorf("future_admin_fee: %w", err)
	}
	if v.oracleMethod, err = NumericToNullableBigInt(oracleMethod); err != nil {
		return v, fmt.Errorf("oracle_method: %w", err)
	}
	v.initialATime = initialATime
	v.futureATime = futureATime
	v.maExpTime = maExpTime
	return v, nil
}

func stableswapConfigUnchanged(latest stableswapConfigValues, cfg *entity.CurveStableswapConfig) bool {
	return bigIntEqual(latest.initialA, cfg.InitialA) &&
		latest.initialATime == cfg.InitialATime &&
		bigIntEqual(latest.futureA, cfg.FutureA) &&
		latest.futureATime == cfg.FutureATime &&
		bigIntEqual(latest.adminFee, cfg.AdminFee) &&
		bigIntEqual(latest.futureFee, cfg.FutureFee) &&
		bigIntEqual(latest.futureAdminFee, cfg.FutureAdminFee) &&
		int64PtrEqual(latest.maExpTime, cfg.MaExpTime) &&
		bigIntEqual(latest.oracleMethod, cfg.OracleMethod)
}

func (r *CurveRepository) writeCryptoswapConfig(ctx context.Context, tx pgx.Tx, cfg *entity.CurveCryptoswapConfig) error {
	var (
		initialAGamma      pgtype.Numeric
		futureAGamma       pgtype.Numeric
		initialAGammaTime  int64
		futureAGammaTime   int64
		midFee             pgtype.Numeric
		outFee             pgtype.Numeric
		feeGamma           pgtype.Numeric
		allowedExtraProfit pgtype.Numeric
		adjustmentStep     pgtype.Numeric
		maTime             pgtype.Numeric
		adminFee           pgtype.Numeric
	)
	err := tx.QueryRow(ctx,
		`SELECT initial_a_gamma, future_a_gamma, initial_a_gamma_time, future_a_gamma_time,
		        mid_fee, out_fee, fee_gamma, allowed_extra_profit, adjustment_step, ma_time, admin_fee
		 FROM curve_cryptoswap_config
		 WHERE curve_pool_id = $1
		 ORDER BY block_number DESC, block_version DESC, processing_version DESC
		 LIMIT 1`,
		cfg.CurvePoolID,
	).Scan(&initialAGamma, &futureAGamma, &initialAGammaTime, &futureAGammaTime,
		&midFee, &outFee, &feeGamma, &allowedExtraProfit, &adjustmentStep, &maTime, &adminFee)

	switch {
	case err == nil:
		latest, convErr := toCryptoswapConfigValues(initialAGamma, futureAGamma, initialAGammaTime, futureAGammaTime,
			midFee, outFee, feeGamma, allowedExtraProfit, adjustmentStep, maTime, adminFee)
		if convErr != nil {
			return fmt.Errorf("reading latest cryptoswap config for pool %d: %w", cfg.CurvePoolID, convErr)
		}
		if cryptoswapConfigUnchanged(latest, cfg) {
			return nil
		}
	case errors.Is(err, pgx.ErrNoRows):
		// No prior row: fall through and insert the first one.
	default:
		return fmt.Errorf("querying latest cryptoswap config for pool %d: %w", cfg.CurvePoolID, err)
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO curve_cryptoswap_config
		   (curve_pool_id, block_number, block_version, block_timestamp,
		    initial_a_gamma, future_a_gamma, initial_a_gamma_time, future_a_gamma_time,
		    mid_fee, out_fee, fee_gamma, allowed_extra_profit, adjustment_step, ma_time, admin_fee, build_id)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
		 ON CONFLICT (curve_pool_id, block_number, block_version, processing_version) DO NOTHING`,
		cfg.CurvePoolID, cfg.BlockNumber, cfg.BlockVersion, cfg.Timestamp,
		BigIntToNullableNumeric(cfg.InitialAGamma), BigIntToNullableNumeric(cfg.FutureAGamma),
		cfg.InitialAGammaTime, cfg.FutureAGammaTime,
		BigIntToNullableNumeric(cfg.MidFee), BigIntToNullableNumeric(cfg.OutFee),
		BigIntToNullableNumeric(cfg.FeeGamma), BigIntToNullableNumeric(cfg.AllowedExtraProfit),
		BigIntToNullableNumeric(cfg.AdjustmentStep), BigIntToNullableNumeric(cfg.MaTime),
		BigIntToNullableNumeric(cfg.AdminFee), int(r.buildID),
	); err != nil {
		return fmt.Errorf("inserting cryptoswap config for pool %d: %w", cfg.CurvePoolID, err)
	}
	return nil
}

type cryptoswapConfigValues struct {
	initialAGamma      *big.Int
	futureAGamma       *big.Int
	initialAGammaTime  int64
	futureAGammaTime   int64
	midFee             *big.Int
	outFee             *big.Int
	feeGamma           *big.Int
	allowedExtraProfit *big.Int
	adjustmentStep     *big.Int
	maTime             *big.Int
	adminFee           *big.Int
}

func toCryptoswapConfigValues(
	initialAGamma pgtype.Numeric, futureAGamma pgtype.Numeric, initialAGammaTime int64, futureAGammaTime int64,
	midFee pgtype.Numeric, outFee pgtype.Numeric, feeGamma pgtype.Numeric, allowedExtraProfit pgtype.Numeric,
	adjustmentStep pgtype.Numeric, maTime pgtype.Numeric, adminFee pgtype.Numeric,
) (cryptoswapConfigValues, error) {
	var v cryptoswapConfigValues
	var err error
	if v.initialAGamma, err = NumericToNullableBigInt(initialAGamma); err != nil {
		return v, fmt.Errorf("initial_a_gamma: %w", err)
	}
	if v.futureAGamma, err = NumericToNullableBigInt(futureAGamma); err != nil {
		return v, fmt.Errorf("future_a_gamma: %w", err)
	}
	if v.midFee, err = NumericToNullableBigInt(midFee); err != nil {
		return v, fmt.Errorf("mid_fee: %w", err)
	}
	if v.outFee, err = NumericToNullableBigInt(outFee); err != nil {
		return v, fmt.Errorf("out_fee: %w", err)
	}
	if v.feeGamma, err = NumericToNullableBigInt(feeGamma); err != nil {
		return v, fmt.Errorf("fee_gamma: %w", err)
	}
	if v.allowedExtraProfit, err = NumericToNullableBigInt(allowedExtraProfit); err != nil {
		return v, fmt.Errorf("allowed_extra_profit: %w", err)
	}
	if v.adjustmentStep, err = NumericToNullableBigInt(adjustmentStep); err != nil {
		return v, fmt.Errorf("adjustment_step: %w", err)
	}
	if v.maTime, err = NumericToNullableBigInt(maTime); err != nil {
		return v, fmt.Errorf("ma_time: %w", err)
	}
	if v.adminFee, err = NumericToNullableBigInt(adminFee); err != nil {
		return v, fmt.Errorf("admin_fee: %w", err)
	}
	v.initialAGammaTime = initialAGammaTime
	v.futureAGammaTime = futureAGammaTime
	return v, nil
}

func cryptoswapConfigUnchanged(latest cryptoswapConfigValues, cfg *entity.CurveCryptoswapConfig) bool {
	return bigIntEqual(latest.initialAGamma, cfg.InitialAGamma) &&
		bigIntEqual(latest.futureAGamma, cfg.FutureAGamma) &&
		latest.initialAGammaTime == cfg.InitialAGammaTime &&
		latest.futureAGammaTime == cfg.FutureAGammaTime &&
		bigIntEqual(latest.midFee, cfg.MidFee) &&
		bigIntEqual(latest.outFee, cfg.OutFee) &&
		bigIntEqual(latest.feeGamma, cfg.FeeGamma) &&
		bigIntEqual(latest.allowedExtraProfit, cfg.AllowedExtraProfit) &&
		bigIntEqual(latest.adjustmentStep, cfg.AdjustmentStep) &&
		bigIntEqual(latest.maTime, cfg.MaTime) &&
		bigIntEqual(latest.adminFee, cfg.AdminFee)
}

// bigIntEqual treats nil as a distinct value: nil == nil, nil != non-nil.
func bigIntEqual(a, b *big.Int) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Cmp(b) == 0
}

func int64PtrEqual(a, b *int64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

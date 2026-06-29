package postgres

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
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
		`SELECT cp.id, cp.pool_address, cp.pool_kind, cp.n_coins, cp.deploy_block, cp.lp_token_address, t.decimals
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
			poolAddress []byte
			kind        string
			nCoins      int
			deployBlock *int64
			lpToken     []byte
			decimals    int
		)
		if err := rows.Scan(&poolID, &poolAddress, &kind, &nCoins, &deployBlock, &lpToken, &decimals); err != nil {
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
				Address:        common.BytesToAddress(poolAddress),
				Kind:           kind,
				NCoins:         nCoins,
				DeployBlock:    db,
				LpTokenAddress: lpAddr,
			})
		}
		result[idx].CoinDecimals = append(result[idx].CoinDecimals, decimals)
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
	s        *entity.CurveStableswapState
	balances any
	vp       any
	ts       any
	a        any
	fee      any
	spotDy   any
}

// cryptoConverted holds pre-converted numeric values for a curve_cryptoswap_state insert.
type cryptoConverted struct {
	s           *entity.CurveCryptoswapState
	balances    any
	vp          any
	ts          any
	a           any
	gamma       any
	fee         any
	priceScale  any
	priceOracle any
	lastPrices  any
	spotDy      any
}

// SaveBlock persists all of a block's curve rows in one pgx.Batch within tx.
func (r *CurveRepository) SaveBlock(ctx context.Context, tx pgx.Tx, w outbound.BlockWrites) (stateRows int64, err error) {
	// Pre-convert all numeric values before touching the batch. Any conversion
	// error is returned immediately so the caller gets a clear message without
	// partially-queued state.
	swaps := make([]swapConverted, 0, len(w.Swaps))
	for i, in := range w.Swaps {
		tokensSold, convErr := BigIntToNumericRequired(in.TokensSold, "tokens_sold")
		if convErr != nil {
			return 0, fmt.Errorf("swap %d converting tokens_sold: %w", i, convErr)
		}
		tokensBought, convErr := BigIntToNumericRequired(in.TokensBought, "tokens_bought")
		if convErr != nil {
			return 0, fmt.Errorf("swap %d converting tokens_bought: %w", i, convErr)
		}
		swaps = append(swaps, swapConverted{in: in, tokensSold: tokensSold, tokensBought: tokensBought})
	}

	liqs := make([]liquidityConverted, 0, len(w.Liquidity))
	for i, in := range w.Liquidity {
		tokenAmounts, convErr := BigIntsToNumericArray(in.TokenAmounts)
		if convErr != nil {
			return 0, fmt.Errorf("liquidity %d converting token_amounts: %w", i, convErr)
		}
		fees, convErr := BigIntsToNullableNumericArray(in.Fees)
		if convErr != nil {
			return 0, fmt.Errorf("liquidity %d converting fees: %w", i, convErr)
		}
		liqs = append(liqs, liquidityConverted{in: in, tokenAmounts: tokenAmounts, fees: fees})
	}

	stables := make([]stableConverted, 0, len(w.StableStates))
	for i, s := range w.StableStates {
		balances, convErr := BigIntsToNumericArray(s.Balances)
		if convErr != nil {
			return 0, fmt.Errorf("stableswap %d converting balances: %w", i, convErr)
		}
		spotDy, convErr := BigIntsToNumericArray(s.SpotDy)
		if convErr != nil {
			return 0, fmt.Errorf("stableswap %d converting spot_dy: %w", i, convErr)
		}
		vp, convErr := BigIntToNumericRequired(s.VirtualPrice, "virtual_price")
		if convErr != nil {
			return 0, fmt.Errorf("stableswap %d converting virtual_price: %w", i, convErr)
		}
		ts, convErr := BigIntToNumericRequired(s.TotalSupply, "total_supply")
		if convErr != nil {
			return 0, fmt.Errorf("stableswap %d converting total_supply: %w", i, convErr)
		}
		a, convErr := BigIntToNumericRequired(s.A, "a")
		if convErr != nil {
			return 0, fmt.Errorf("stableswap %d converting a: %w", i, convErr)
		}
		fee, convErr := BigIntToNumericRequired(s.Fee, "fee")
		if convErr != nil {
			return 0, fmt.Errorf("stableswap %d converting fee: %w", i, convErr)
		}
		stables = append(stables, stableConverted{s: s, balances: balances, vp: vp, ts: ts, a: a, fee: fee, spotDy: spotDy})
	}

	cryptos := make([]cryptoConverted, 0, len(w.CryptoStates))
	for i, s := range w.CryptoStates {
		balances, convErr := BigIntsToNumericArray(s.Balances)
		if convErr != nil {
			return 0, fmt.Errorf("cryptoswap %d converting balances: %w", i, convErr)
		}
		vp, convErr := BigIntToNumericRequired(s.VirtualPrice, "virtual_price")
		if convErr != nil {
			return 0, fmt.Errorf("cryptoswap %d converting virtual_price: %w", i, convErr)
		}
		ts, convErr := BigIntToNumericRequired(s.TotalSupply, "total_supply")
		if convErr != nil {
			return 0, fmt.Errorf("cryptoswap %d converting total_supply: %w", i, convErr)
		}
		a, convErr := BigIntToNumericRequired(s.A, "a")
		if convErr != nil {
			return 0, fmt.Errorf("cryptoswap %d converting a: %w", i, convErr)
		}
		gamma, convErr := BigIntToNumericRequired(s.Gamma, "gamma")
		if convErr != nil {
			return 0, fmt.Errorf("cryptoswap %d converting gamma: %w", i, convErr)
		}
		fee, convErr := BigIntToNumericRequired(s.Fee, "fee")
		if convErr != nil {
			return 0, fmt.Errorf("cryptoswap %d converting fee: %w", i, convErr)
		}
		priceScale, convErr := BigIntsToNumericArray(s.PriceScale)
		if convErr != nil {
			return 0, fmt.Errorf("cryptoswap %d converting price_scale: %w", i, convErr)
		}
		priceOracle, convErr := BigIntsToNumericArray(s.PriceOracle)
		if convErr != nil {
			return 0, fmt.Errorf("cryptoswap %d converting price_oracle: %w", i, convErr)
		}
		lastPrices, convErr := BigIntsToNumericArray(s.LastPrices)
		if convErr != nil {
			return 0, fmt.Errorf("cryptoswap %d converting last_prices: %w", i, convErr)
		}
		spotDy, convErr := BigIntsToNumericArray(s.SpotDy)
		if convErr != nil {
			return 0, fmt.Errorf("cryptoswap %d converting spot_dy: %w", i, convErr)
		}
		cryptos = append(cryptos, cryptoConverted{
			s: s, balances: balances, vp: vp, ts: ts, a: a, gamma: gamma, fee: fee,
			priceScale: priceScale, priceOracle: priceOracle, lastPrices: lastPrices, spotDy: spotDy,
		})
	}

	batch := &pgx.Batch{}

	for _, c := range swaps {
		batch.Queue(
			`INSERT INTO curve_swap
			   (curve_pool_id, block_number, block_version, block_timestamp,
			    tx_hash, log_index, buyer, sold_id, bought_id,
			    tokens_sold, tokens_bought, fee, build_id)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
			 ON CONFLICT (curve_pool_id, block_timestamp, block_number, block_version, log_index, processing_version) DO NOTHING`,
			c.in.CurvePoolID, c.in.BlockNumber, c.in.BlockVersion, c.in.BlockTimestamp,
			c.in.TxHash.Bytes(), c.in.LogIndex, c.in.Buyer.Bytes(), c.in.SoldID, c.in.BoughtID,
			c.tokensSold, c.tokensBought, BigIntToNullableNumeric(c.in.Fee), int(r.buildID),
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
			int(r.buildID),
		)
	}

	for _, c := range stables {
		batch.Queue(
			`INSERT INTO curve_stableswap_state
			   (curve_pool_id, block_number, block_version, block_timestamp,
			    balances, virtual_price, total_supply, a, fee, spot_dy,
			    last_price, price_oracle, build_id)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
			 ON CONFLICT (curve_pool_id, block_timestamp, block_number, block_version, processing_version) DO NOTHING`,
			c.s.CurvePoolID, c.s.BlockNumber, c.s.BlockVersion, c.s.Timestamp,
			c.balances, c.vp, c.ts, c.a, c.fee, c.spotDy,
			BigIntToNullableNumeric(c.s.LastPrice), BigIntToNullableNumeric(c.s.PriceOracle), int(r.buildID),
		)
	}

	for _, c := range cryptos {
		batch.Queue(
			`INSERT INTO curve_cryptoswap_state
			   (curve_pool_id, block_number, block_version, block_timestamp,
			    balances, virtual_price, total_supply, a, gamma, fee,
			    d, xcp_profit, price_scale, price_oracle, last_prices, spot_dy, build_id)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
			 ON CONFLICT (curve_pool_id, block_timestamp, block_number, block_version, processing_version) DO NOTHING`,
			c.s.CurvePoolID, c.s.BlockNumber, c.s.BlockVersion, c.s.Timestamp,
			c.balances, c.vp, c.ts, c.a, c.gamma, c.fee,
			BigIntToNullableNumeric(c.s.D), BigIntToNullableNumeric(c.s.XcpProfit),
			c.priceScale, c.priceOracle, c.lastPrices, c.spotDy, int(r.buildID),
		)
	}

	br := tx.SendBatch(ctx, batch)
	defer func() {
		if closeErr := br.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()

	for i := range swaps {
		if _, readErr := br.Exec(); readErr != nil {
			err = fmt.Errorf("batch swap %d: %w", i, readErr)
			return stateRows, err
		}
	}

	for i := range liqs {
		if _, readErr := br.Exec(); readErr != nil {
			err = fmt.Errorf("batch liquidity %d: %w", i, readErr)
			return stateRows, err
		}
	}

	for i := range stables {
		tag, readErr := br.Exec()
		if readErr != nil {
			err = fmt.Errorf("batch stableswap %d: %w", i, readErr)
			return stateRows, err
		}
		stateRows += tag.RowsAffected()
	}

	for i := range cryptos {
		tag, readErr := br.Exec()
		if readErr != nil {
			err = fmt.Errorf("batch cryptoswap %d: %w", i, readErr)
			return stateRows, err
		}
		stateRows += tag.RowsAffected()
	}

	return stateRows, nil
}

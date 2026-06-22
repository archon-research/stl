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

// LoadPools returns all pools for the given chain with their coin token IDs in coin_index order.
func (r *CurveRepository) LoadPools(ctx context.Context, chainID int64) ([]outbound.CurvePoolRow, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT cp.id, cp.pool_address, cp.pool_kind, cp.n_coins, cp.deploy_block, cpc.token_id
		 FROM curve_pool cp
		 JOIN curve_pool_coin cpc ON cpc.curve_pool_id = cp.id
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
			deployBlock int64
			tokenID     int64
		)
		if err := rows.Scan(&poolID, &poolAddress, &kind, &nCoins, &deployBlock, &tokenID); err != nil {
			return nil, fmt.Errorf("scanning curve pool row: %w", err)
		}

		idx, exists := index[poolID]
		if !exists {
			idx = len(result)
			index[poolID] = idx
			result = append(result, outbound.CurvePoolRow{
				ID:          poolID,
				Address:     common.BytesToAddress(poolAddress),
				Kind:        kind,
				NCoins:      nCoins,
				DeployBlock: deployBlock,
			})
		}
		result[idx].CoinTokenIDs = append(result[idx].CoinTokenIDs, tokenID)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating curve pools: %w", err)
	}
	return result, nil
}

// SaveSwap persists a Curve exchange event within an external transaction.
func (r *CurveRepository) SaveSwap(ctx context.Context, tx pgx.Tx, in outbound.SwapInput) error {
	tokensSold, err := BigIntToNumericRequired(in.TokensSold, "tokens_sold")
	if err != nil {
		return err
	}
	tokensBought, err := BigIntToNumericRequired(in.TokensBought, "tokens_bought")
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO curve_swap
		   (curve_pool_id, block_number, block_version, block_timestamp,
		    tx_hash, log_index, buyer, sold_id, bought_id,
		    tokens_sold, tokens_bought, fee, build_id)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
		 ON CONFLICT (curve_pool_id, block_timestamp, block_number, block_version, log_index, processing_version) DO NOTHING`,
		in.CurvePoolID, in.BlockNumber, in.BlockVersion, in.BlockTimestamp,
		in.TxHash.Bytes(), in.LogIndex, in.Buyer.Bytes(), in.SoldID, in.BoughtID,
		tokensSold, tokensBought, BigIntToNullableNumeric(in.Fee), int(r.buildID),
	)
	if err != nil {
		return fmt.Errorf("saving curve swap: %w", err)
	}
	return nil
}

// SaveLiquidityEvent persists a Curve add/remove liquidity event within an external transaction.
func (r *CurveRepository) SaveLiquidityEvent(ctx context.Context, tx pgx.Tx, in outbound.LiquidityInput) error {
	tokenAmounts, err := BigIntsToNumericArray(in.TokenAmounts)
	if err != nil {
		return fmt.Errorf("converting token_amounts: %w", err)
	}
	fees, err := BigIntsToNullableNumericArray(in.Fees)
	if err != nil {
		return fmt.Errorf("converting fees: %w", err)
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO curve_liquidity_event
		   (curve_pool_id, block_number, block_version, block_timestamp,
		    tx_hash, log_index, provider, kind, token_amounts,
		    coin_index, fees, invariant, token_supply, build_id)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
		 ON CONFLICT (curve_pool_id, block_timestamp, block_number, block_version, log_index, processing_version) DO NOTHING`,
		in.CurvePoolID, in.BlockNumber, in.BlockVersion, in.BlockTimestamp,
		in.TxHash.Bytes(), in.LogIndex, in.Provider.Bytes(), in.Kind, tokenAmounts,
		in.CoinIndex, fees, BigIntToNullableNumeric(in.Invariant), BigIntToNullableNumeric(in.TokenSupply),
		int(r.buildID),
	)
	if err != nil {
		return fmt.Errorf("saving curve liquidity event: %w", err)
	}
	return nil
}

// SaveStableswapState persists a stableswap pool state snapshot within an external transaction.
func (r *CurveRepository) SaveStableswapState(ctx context.Context, tx pgx.Tx, s *entity.CurveStableswapState) error {
	balances, err := BigIntsToNumericArray(s.Balances)
	if err != nil {
		return fmt.Errorf("converting balances: %w", err)
	}
	spotDy, err := BigIntsToNumericArray(s.SpotDy)
	if err != nil {
		return fmt.Errorf("converting spot_dy: %w", err)
	}
	vp, err := BigIntToNumericRequired(s.VirtualPrice, "virtual_price")
	if err != nil {
		return err
	}
	ts, err := BigIntToNumericRequired(s.TotalSupply, "total_supply")
	if err != nil {
		return err
	}
	a, err := BigIntToNumericRequired(s.A, "a")
	if err != nil {
		return err
	}
	fee, err := BigIntToNumericRequired(s.Fee, "fee")
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO curve_stableswap_state
		   (curve_pool_id, block_number, block_version, block_timestamp,
		    balances, virtual_price, total_supply, a, fee, spot_dy,
		    last_price, price_oracle, build_id)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
		 ON CONFLICT (curve_pool_id, block_timestamp, block_number, block_version, processing_version) DO NOTHING`,
		s.CurvePoolID, s.BlockNumber, s.BlockVersion, s.Timestamp,
		balances, vp, ts, a, fee, spotDy,
		BigIntToNullableNumeric(s.LastPrice), BigIntToNullableNumeric(s.PriceOracle), int(r.buildID),
	)
	if err != nil {
		return fmt.Errorf("saving curve stableswap state: %w", err)
	}
	return nil
}

// SaveCryptoswapState persists a cryptoswap pool state snapshot within an external transaction.
func (r *CurveRepository) SaveCryptoswapState(ctx context.Context, tx pgx.Tx, s *entity.CurveCryptoswapState) error {
	balances, err := BigIntsToNumericArray(s.Balances)
	if err != nil {
		return fmt.Errorf("converting balances: %w", err)
	}
	vp, err := BigIntToNumericRequired(s.VirtualPrice, "virtual_price")
	if err != nil {
		return err
	}
	ts, err := BigIntToNumericRequired(s.TotalSupply, "total_supply")
	if err != nil {
		return err
	}
	a, err := BigIntToNumericRequired(s.A, "a")
	if err != nil {
		return err
	}
	gamma, err := BigIntToNumericRequired(s.Gamma, "gamma")
	if err != nil {
		return err
	}
	fee, err := BigIntToNumericRequired(s.Fee, "fee")
	if err != nil {
		return err
	}
	priceScale, err := BigIntsToNumericArray(s.PriceScale)
	if err != nil {
		return fmt.Errorf("converting price_scale: %w", err)
	}
	priceOracle, err := BigIntsToNumericArray(s.PriceOracle)
	if err != nil {
		return fmt.Errorf("converting price_oracle: %w", err)
	}
	lastPrices, err := BigIntsToNumericArray(s.LastPrices)
	if err != nil {
		return fmt.Errorf("converting last_prices: %w", err)
	}
	spotDy, err := BigIntsToNumericArray(s.SpotDy)
	if err != nil {
		return fmt.Errorf("converting spot_dy: %w", err)
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO curve_cryptoswap_state
		   (curve_pool_id, block_number, block_version, block_timestamp,
		    balances, virtual_price, total_supply, a, gamma, fee,
		    d, xcp_profit, price_scale, price_oracle, last_prices, spot_dy, build_id)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
		 ON CONFLICT (curve_pool_id, block_timestamp, block_number, block_version, processing_version) DO NOTHING`,
		s.CurvePoolID, s.BlockNumber, s.BlockVersion, s.Timestamp,
		balances, vp, ts, a, gamma, fee,
		BigIntToNullableNumeric(s.D), BigIntToNullableNumeric(s.XcpProfit),
		priceScale, priceOracle, lastPrices, spotDy, int(r.buildID),
	)
	if err != nil {
		return fmt.Errorf("saving curve cryptoswap state: %w", err)
	}
	return nil
}

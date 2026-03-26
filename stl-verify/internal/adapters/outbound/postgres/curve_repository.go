package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

var _ outbound.CurveRepository = (*CurveRepository)(nil)

type CurveRepository struct {
	pool   *pgxpool.Pool
	txm    *TxManager
	logger *slog.Logger
}

func NewCurveRepository(pool *pgxpool.Pool, txm *TxManager, logger *slog.Logger) *CurveRepository {
	return &CurveRepository{
		pool:   pool,
		txm:    txm,
		logger: logger.With("component", "curve-repository"),
	}
}

func (r *CurveRepository) SaveSnapshots(ctx context.Context, snapshots []*entity.CurvePoolSnapshot) error {
	if len(snapshots) == 0 {
		return nil
	}

	for i, s := range snapshots {
		if err := s.Validate(); err != nil {
			return fmt.Errorf("snapshot %d: %w", i, err)
		}
	}

	return r.txm.WithTransaction(ctx, func(tx pgx.Tx) error {
		batch := &pgx.Batch{}

		for _, s := range snapshots {
			batch.Queue(`
				INSERT INTO curve_pool_snapshot (
					pool_address, chain_id, block_number,
					coin_balances, n_coins,
					total_supply, virtual_price, tvl_usd,
					amp_factor, fee,
					oracle_prices,
					fee_apy, crv_apy_min, crv_apy_max,
					snapshot_time
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
				ON CONFLICT (pool_address, chain_id, block_number, snapshot_time)
				DO UPDATE SET
					coin_balances = EXCLUDED.coin_balances,
					n_coins       = EXCLUDED.n_coins,
					total_supply  = EXCLUDED.total_supply,
					virtual_price = EXCLUDED.virtual_price,
					tvl_usd       = EXCLUDED.tvl_usd,
					amp_factor    = EXCLUDED.amp_factor,
					fee           = EXCLUDED.fee,
					oracle_prices = EXCLUDED.oracle_prices,
					fee_apy       = EXCLUDED.fee_apy,
					crv_apy_min   = EXCLUDED.crv_apy_min,
					crv_apy_max   = EXCLUDED.crv_apy_max
			`,
				s.PoolAddress,
				s.ChainID,
				s.BlockNumber,
				s.CoinBalances,
				s.NCoins,
				s.TotalSupply,
				s.VirtualPrice,
				s.TvlUSD,
				s.AmpFactor,
				s.Fee,
				s.OraclePrices,
				s.FeeAPY,
				s.CrvAPYMin,
				s.CrvAPYMax,
				s.SnapshotTime,
			)
		}

		results := tx.SendBatch(ctx, batch)
		for i := range snapshots {
			if _, err := results.Exec(); err != nil {
				_ = results.Close()
				return fmt.Errorf("insert snapshot %d: %w", i, err)
			}
		}
		if err := results.Close(); err != nil {
			return fmt.Errorf("close batch: %w", err)
		}

		r.logger.Info("snapshots saved", "count", len(snapshots))
		return nil
	})
}

// LookupTokenID returns the token ID for a given chain + address.
// Returns outbound.ErrTokenNotFound if no matching token exists.
func (r *CurveRepository) LookupTokenID(ctx context.Context, chainID int64, address common.Address) (int64, error) {
	var id int64
	err := r.pool.QueryRow(ctx,
		`SELECT id FROM token WHERE chain_id = $1 AND address = $2`,
		chainID, address.Bytes(),
	).Scan(&id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, outbound.ErrTokenNotFound
		}
		return 0, fmt.Errorf("lookup token %s on chain %d: %w", address.Hex(), chainID, err)
	}
	return id, nil
}

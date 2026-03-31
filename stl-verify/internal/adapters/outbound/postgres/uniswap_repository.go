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

var _ outbound.UniswapRepository = (*UniswapRepository)(nil)

type UniswapRepository struct {
	pool   *pgxpool.Pool
	txm    *TxManager
	logger *slog.Logger
}

func NewUniswapRepository(pool *pgxpool.Pool, txm *TxManager, logger *slog.Logger) *UniswapRepository {
	return &UniswapRepository{
		pool:   pool,
		txm:    txm,
		logger: logger.With("component", "uniswap-repository"),
	}
}

func (r *UniswapRepository) SaveSnapshots(ctx context.Context, snapshots []*entity.UniswapPoolSnapshot) error {
	if len(snapshots) == 0 {
		return nil
	}

	for i, s := range snapshots {
		if s == nil {
			return fmt.Errorf("snapshot %d is nil", i)
		}
		if err := s.Validate(); err != nil {
			return fmt.Errorf("snapshot %d: %w", i, err)
		}
	}

	err := r.txm.WithTransaction(ctx, func(tx pgx.Tx) error {
		batch := &pgx.Batch{}

		for _, s := range snapshots {
			batch.Queue(`
				INSERT INTO uniswap_pool_snapshot (
					pool_address, chain_id, block_number, block_version,
					token0, token1, fee,
					sqrt_price_x96, current_tick, price,
					active_liquidity, tvl_usd,
					twap_tick, twap_price,
					fee_growth_global0_x128, fee_growth_global1_x128,
					snapshot_time
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
				ON CONFLICT DO NOTHING
			`,
				s.PoolAddress,
				s.ChainID,
				s.BlockNumber,
				s.BlockVersion,
				s.Token0,
				s.Token1,
				s.Fee,
				s.SqrtPriceX96,
				s.CurrentTick,
				s.Price,
				s.ActiveLiquidity,
				s.TvlUSD,
				s.TwapTick,
				s.TwapPrice,
				s.FeeGrowthGlobal0X128,
				s.FeeGrowthGlobal1X128,
				s.SnapshotTime,
			)
		}

		results := tx.SendBatch(ctx, batch)
		for i := range snapshots {
			if _, err := results.Exec(); err != nil {
				closeErr := results.Close()
				if closeErr != nil {
					return fmt.Errorf("insert snapshot %d: %w; close: %v", i, err, closeErr)
				}
				return fmt.Errorf("insert snapshot %d: %w", i, err)
			}
		}
		if err := results.Close(); err != nil {
			return fmt.Errorf("close batch: %w", err)
		}

		return nil
	})
	if err != nil {
		return err
	}

	r.logger.Info("snapshots saved", "count", len(snapshots))
	return nil
}

// LookupTokenID returns the token ID for a given chain + address.
// Returns outbound.ErrTokenNotFound if no matching token exists.
func (r *UniswapRepository) LookupTokenID(ctx context.Context, chainID int64, address common.Address) (int64, error) {
	var id int64
	err := r.pool.QueryRow(ctx,
		`SELECT id FROM token WHERE chain_id = $1 AND address = $2`,
		chainID, address.Bytes(),
	).Scan(&id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, fmt.Errorf("token %s on chain %d: %w", address.Hex(), chainID, outbound.ErrTokenNotFound)
		}
		return 0, fmt.Errorf("lookup token %s on chain %d: %w", address.Hex(), chainID, err)
	}
	return id, nil
}

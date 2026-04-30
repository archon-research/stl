package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

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
				INSERT INTO curve_pool_snapshot (
					pool_address, chain_id, block_number, block_version,
					coin_balances, n_coins,
					total_supply, virtual_price, tvl_usd,
					amp_factor, fee,
					oracle_prices, last_prices, exchange_rates,
					fee_apy,
					snapshot_time
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
				ON CONFLICT DO NOTHING
			`,
				s.PoolAddress,
				s.ChainID,
				s.BlockNumber,
				s.BlockVersion,
				s.CoinBalances,
				s.NCoins,
				s.TotalSupply,
				s.VirtualPrice,
				s.TvlUSD,
				s.AmpFactor,
				s.Fee,
				s.OraclePrices,
				s.LastPrices,
				s.ExchangeRates,
				s.FeeAPY,
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
func (r *CurveRepository) LookupTokenID(ctx context.Context, chainID int64, address common.Address) (int64, error) {
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

// GetPreviousSnapshot returns the most recent virtual_price and snapshot_time
// for a pool. Returns empty string if no previous snapshot exists.
func (r *CurveRepository) GetPreviousSnapshot(ctx context.Context, poolAddress []byte, chainID int64) (string, time.Time, error) {
	var virtualPrice string
	var snapshotTime time.Time
	err := r.pool.QueryRow(ctx,
		`SELECT virtual_price, snapshot_time FROM curve_pool_snapshot
		 WHERE pool_address = $1 AND chain_id = $2
		 ORDER BY snapshot_time DESC LIMIT 1`,
		poolAddress, chainID,
	).Scan(&virtualPrice, &snapshotTime)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", time.Time{}, nil
		}
		return "", time.Time{}, fmt.Errorf("get previous snapshot: %w", err)
	}
	return virtualPrice, snapshotTime, nil
}

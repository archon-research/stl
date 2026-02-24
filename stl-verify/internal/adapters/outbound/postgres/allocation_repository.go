package postgres

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

var _ outbound.AllocationRepository = (*AllocationRepository)(nil)

type AllocationRepository struct {
	pool      *pgxpool.Pool
	txm       *TxManager
	tokenRepo outbound.TokenRepository
	logger    *slog.Logger
}

type tokenCacheKey struct {
	ChainID int64
	Address common.Address
}

func NewAllocationRepository(
	pool *pgxpool.Pool,
	txm *TxManager,
	tokenRepo outbound.TokenRepository,
	logger *slog.Logger,
) *AllocationRepository {
	if logger == nil {
		logger = slog.Default()
	}
	return &AllocationRepository{
		pool:      pool,
		txm:       txm,
		tokenRepo: tokenRepo,
		logger:    logger,
	}
}

func (r *AllocationRepository) SavePositions(
	ctx context.Context,
	positions []*entity.AllocationPosition,
) error {
	if len(positions) == 0 {
		return nil
	}

	for i, pos := range positions {
		if err := pos.Validate(); err != nil {
			return fmt.Errorf("position %d: %w", i, err)
		}
	}

	return r.txm.WithTransaction(ctx, func(tx pgx.Tx) error {
		tokenIDs, err := r.resolveTokenIDs(ctx, tx, positions)
		if err != nil {
			return fmt.Errorf("resolve token IDs: %w", err)
		}

		for _, pos := range positions {
			key := tokenCacheKey{ChainID: pos.ChainID, Address: pos.TokenAddress}
			if _, ok := tokenIDs[key]; !ok {
				return fmt.Errorf(
					"token ID not resolved for chain=%d address=%s",
					pos.ChainID, pos.TokenAddress.Hex(),
				)
			}
		}

		batch := &pgx.Batch{}
		for _, pos := range positions {
			key := tokenCacheKey{ChainID: pos.ChainID, Address: pos.TokenAddress}
			tokenID := tokenIDs[key]

			query, args, err := r.buildInsertArgs(pos, tokenID)
			if err != nil {
				return fmt.Errorf(
					"build insert for chain=%d address=%s block=%d: %w",
					pos.ChainID, pos.TokenAddress.Hex(), pos.BlockNumber, err,
				)
			}
			batch.Queue(query, args...)
		}

		results := tx.SendBatch(ctx, batch)
		for i := range positions {
			if _, err := results.Exec(); err != nil {
				_ = results.Close()
				return fmt.Errorf(
					"insert position %d (chain=%d address=%s block=%d): %w",
					i, positions[i].ChainID,
					positions[i].TokenAddress.Hex(),
					positions[i].BlockNumber, err,
				)
			}
		}
		if err := results.Close(); err != nil {
			return fmt.Errorf("close batch: %w", err)
		}

		r.logger.Debug("positions saved", "inserted", len(positions))
		return nil
	})
}

func (r *AllocationRepository) buildInsertArgs(
	pos *entity.AllocationPosition,
	tokenID int64,
) (string, []interface{}, error) {
	balanceDecimals := pos.TokenDecimals
	if pos.AssetDecimals != nil {
		balanceDecimals = *pos.AssetDecimals
	}

	balanceStr := shared.FormatAmount(pos.Balance, balanceDecimals)

	var scaledStr *string
	if pos.ScaledBalance != nil {
		s := shared.FormatAmount(pos.ScaledBalance, pos.TokenDecimals)
		scaledStr = &s
	}

	txAmountStr := "0"
	if pos.TxAmount != nil {
		txAmountStr = shared.FormatAmount(pos.TxAmount, balanceDecimals)
	}

	txHashBytes, err := encodeTxHash(pos)
	if err != nil {
		return "", nil, fmt.Errorf("encode tx_hash: %w", err)
	}

	query := `
		INSERT INTO allocation_position (
			chain_id, token_id, star, proxy_address,
			balance, scaled_balance,
			block_number, block_version,
			tx_hash, log_index, tx_amount, direction
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (chain_id, token_id, proxy_address, block_number, block_version, tx_hash, log_index, direction)
		DO UPDATE SET
			balance = EXCLUDED.balance,
			scaled_balance = EXCLUDED.scaled_balance,
			tx_amount = EXCLUDED.tx_amount
	`

	args := []interface{}{
		pos.ChainID,
		tokenID,
		pos.Star,
		pos.ProxyAddress.Bytes(),
		balanceStr,
		scaledStr,
		pos.BlockNumber,
		pos.BlockVersion,
		txHashBytes,
		pos.LogIndex,
		txAmountStr,
		pos.Direction,
	}

	return query, args, nil
}

func encodeTxHash(pos *entity.AllocationPosition) ([]byte, error) {
	if pos.TxHash != "" {
		b := common.FromHex(pos.TxHash)
		if len(b) == 0 {
			return nil, fmt.Errorf("invalid hex tx_hash: %s", pos.TxHash)
		}
		return b, nil
	}

	input := fmt.Sprintf("sweep:%d:%d:%s:%s",
		pos.ChainID,
		pos.BlockNumber,
		pos.TokenAddress.Hex(),
		pos.ProxyAddress.Hex(),
	)
	h := sha256.Sum256([]byte(input))
	return h[:], nil
}

func (r *AllocationRepository) resolveTokenIDs(
	ctx context.Context,
	tx pgx.Tx,
	positions []*entity.AllocationPosition,
) (map[tokenCacheKey]int64, error) {
	result := make(map[tokenCacheKey]int64)
	toResolve := make(map[tokenCacheKey]*entity.AllocationPosition)

	for _, pos := range positions {
		key := tokenCacheKey{ChainID: pos.ChainID, Address: pos.TokenAddress}
		toResolve[key] = pos
	}

	for key, pos := range toResolve {
		tokenID, err := r.tokenRepo.GetOrCreateToken(
			ctx, tx,
			pos.ChainID,
			pos.TokenAddress,
			pos.TokenSymbol,
			pos.TokenDecimals,
			pos.CreatedAtBlock,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"GetOrCreateToken chain=%d address=%s: %w",
				pos.ChainID, pos.TokenAddress.Hex(), err,
			)
		}
		result[key] = tokenID
	}

	return result, nil
}

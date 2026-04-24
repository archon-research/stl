package postgres

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

var _ outbound.TokenTotalSupplyRepository = (*TokenTotalSupplyRepository)(nil)

// TokenTotalSupplyRepository persists per-block totalSupply observations for
// receipt tokens. processing_version is assigned by a DB trigger; the struct
// holds the build_id and passes it into every INSERT for idempotent replay.
type TokenTotalSupplyRepository struct {
	pool      *pgxpool.Pool
	txm       *TxManager
	tokenRepo outbound.TokenRepository
	logger    *slog.Logger
	buildID   buildregistry.BuildID
}

func NewTokenTotalSupplyRepository(
	pool *pgxpool.Pool,
	txm *TxManager,
	tokenRepo outbound.TokenRepository,
	logger *slog.Logger,
	buildID buildregistry.BuildID,
) *TokenTotalSupplyRepository {
	if logger == nil {
		logger = slog.Default()
	}
	return &TokenTotalSupplyRepository{
		pool:      pool,
		txm:       txm,
		tokenRepo: tokenRepo,
		logger:    logger,
		buildID:   buildID,
	}
}

func (r *TokenTotalSupplyRepository) SaveSuppliesTx(
	ctx context.Context,
	tx pgx.Tx,
	supplies []*entity.TokenTotalSupply,
) error {
	if len(supplies) == 0 {
		return nil
	}

	tokenIDs, err := r.resolveTokenIDs(ctx, tx, supplies)
	if err != nil {
		return fmt.Errorf("resolve token IDs: %w", err)
	}

	batch := &pgx.Batch{}
	for _, s := range supplies {
		key := tokenCacheKey{ChainID: s.ChainID, Address: s.TokenAddress}
		tokenID, ok := tokenIDs[key]
		if !ok {
			return fmt.Errorf(
				"token ID not resolved for chain=%d address=%s",
				s.ChainID, s.TokenAddress.Hex(),
			)
		}
		query, args := r.buildInsertArgs(s, tokenID)
		batch.Queue(query, args...)
	}

	results := tx.SendBatch(ctx, batch)
	for i := range supplies {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			return fmt.Errorf(
				"insert supply %d (chain=%d address=%s block=%d): %w",
				i, supplies[i].ChainID,
				supplies[i].TokenAddress.Hex(),
				supplies[i].BlockNumber, err,
			)
		}
	}
	if err := results.Close(); err != nil {
		return fmt.Errorf("close batch: %w", err)
	}

	r.logger.Debug("token total supplies saved", "inserted", len(supplies))
	return nil
}

func (r *TokenTotalSupplyRepository) buildInsertArgs(
	s *entity.TokenTotalSupply,
	tokenID int64,
) (string, []any) {
	totalSupply := toNumeric(s.TotalSupply, s.TokenDecimals)
	scaledTotalSupply := toNullableNumeric(s.ScaledTotalSupply, s.TokenDecimals)

	query := `
		INSERT INTO token_total_supply (
			chain_id, token_id,
			total_supply, scaled_total_supply,
			block_number, block_version, block_timestamp,
			source, build_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (chain_id, token_id, block_number, block_version, processing_version, block_timestamp) DO NOTHING
	`

	args := []any{
		s.ChainID,
		tokenID,
		totalSupply,
		scaledTotalSupply,
		s.BlockNumber,
		s.BlockVersion,
		s.BlockTimestamp,
		s.Source,
		int(r.buildID),
	}
	return query, args
}

func (r *TokenTotalSupplyRepository) resolveTokenIDs(
	ctx context.Context,
	tx pgx.Tx,
	supplies []*entity.TokenTotalSupply,
) (map[tokenCacheKey]int64, error) {
	result := make(map[tokenCacheKey]int64)

	for _, s := range supplies {
		key := tokenCacheKey{ChainID: s.ChainID, Address: s.TokenAddress}
		if _, exists := result[key]; exists {
			continue
		}
		tokenID, err := r.tokenRepo.GetOrCreateToken(
			ctx, tx,
			s.ChainID,
			s.TokenAddress,
			s.TokenSymbol,
			s.TokenDecimals,
			s.CreatedAtBlock,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"GetOrCreateToken chain=%d address=%s: %w",
				s.ChainID, s.TokenAddress.Hex(), err,
			)
		}
		result[key] = tokenID
	}

	return result, nil
}

package postgres

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"slices"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

var _ outbound.AllocationRepository = (*AllocationRepository)(nil)

type AllocationRepository struct {
	pool      *pgxpool.Pool
	txm       *TxManager
	tokenRepo outbound.TokenRepository
	logger    *slog.Logger
	buildID   buildregistry.BuildID
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
	buildID buildregistry.BuildID,
) *AllocationRepository {
	if logger == nil {
		logger = slog.Default()
	}
	return &AllocationRepository{
		pool:      pool,
		txm:       txm,
		tokenRepo: tokenRepo,
		logger:    logger,
		buildID:   buildID,
	}
}

// SavePositions persists positions within an externally managed transaction.
// Callers obtain `tx` from a TxManager so this write can be coordinated with
// other repository writes (e.g. TokenTotalSupplyRepository) atomically.
func (r *AllocationRepository) SavePositions(
	ctx context.Context,
	tx pgx.Tx,
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

	// Sort by allocation_position's natural key so the per-row advisory lock
	// in assign_processing_version_allocation_position is acquired in a
	// transaction-stable order. TokenAddress is a 1:1 stand-in for token_id
	// within ChainID — the absolute order doesn't matter, only that every
	// caller produces the same total order. See ADR-0002 §3.
	slices.SortFunc(positions, func(a, b *entity.AllocationPosition) int {
		return cmp.Or(
			cmp.Compare(a.ChainID, b.ChainID),
			bytes.Compare(a.TokenAddress.Bytes(), b.TokenAddress.Bytes()),
			cmp.Compare(a.PrimeID, b.PrimeID),
			bytes.Compare(a.ProxyAddress.Bytes(), b.ProxyAddress.Bytes()),
			cmp.Compare(a.BlockNumber, b.BlockNumber),
			cmp.Compare(a.BlockVersion, b.BlockVersion),
			cmp.Compare(a.TxHash, b.TxHash),
			cmp.Compare(a.LogIndex, b.LogIndex),
			cmp.Compare(a.Direction, b.Direction),
			a.CreatedAt.Compare(b.CreatedAt),
		)
	})

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
}

func (r *AllocationRepository) buildInsertArgs(
	pos *entity.AllocationPosition,
	tokenID int64,
) (string, []any, error) {
	balance := toNumeric(pos.Balance, pos.TokenDecimals)
	scaled := toNullableNumeric(pos.ScaledBalance, pos.TokenDecimals)
	txAmount := toNumeric(pos.TxAmount, pos.TokenDecimals)

	txHashBytes, err := encodeTxHash(pos)
	if err != nil {
		return "", nil, fmt.Errorf("encode tx_hash: %w", err)
	}

	query := `
		INSERT INTO allocation_position (
			chain_id, token_id, prime_id, proxy_address,
			balance, scaled_balance,
			block_number, block_version,
			tx_hash, log_index, tx_amount, direction, created_at, build_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
		ON CONFLICT (chain_id, token_id, prime_id, proxy_address, block_number, block_version, tx_hash, log_index, direction, processing_version, created_at) DO NOTHING
	`

	args := []any{
		pos.ChainID,
		tokenID,
		pos.PrimeID,
		pos.ProxyAddress.Bytes(),
		balance,
		scaled,
		pos.BlockNumber,
		pos.BlockVersion,
		txHashBytes,
		pos.LogIndex,
		txAmount,
		pos.Direction,
		pos.CreatedAt,
		int(r.buildID),
	}

	return query, args, nil
}

// encodeTxHash returns the on-chain transaction hash for a position, or the
// zero-hash sentinel for sweeps.
//
// Sweep (reconciliation) snapshots have no originating transaction. tx_hash is
// part of the primary key so it cannot be NULL, but the rest of the key
// — (chain_id, token_id, prime_id, proxy_address, block_number, block_version,
// log_index, direction, processing_version, created_at) — already uniquely
// identifies a sweep observation, so the hash carries no dedup information for
// them. We therefore store the zero hash as an explicit "no transaction"
// sentinel: no real transaction hash takes that value in practice (a collision
// is cryptographically negligible), and it no longer masquerades as a genuine
// on-chain tx the way the previous content-addressed synthetic hash did
// (VEC-340).
//
// The sentinel is only valid for sweeps. A transaction-driven position without
// a hash signals an upstream bug, so we reject it rather than silently persist
// it as "no transaction"; and a present hash must be a full 32-byte value
// (common.FromHex decodes truncated hex, which we must not store).
func encodeTxHash(pos *entity.AllocationPosition) ([]byte, error) {
	if pos.TxHash == "" {
		if pos.Direction != "sweep" {
			return nil, fmt.Errorf(
				"missing tx_hash for non-sweep position (direction=%q)",
				pos.Direction,
			)
		}
		return common.Hash{}.Bytes(), nil
	}

	b := common.FromHex(pos.TxHash)
	if len(b) != common.HashLength {
		return nil, fmt.Errorf(
			"tx_hash must be %d bytes, got %d: %s",
			common.HashLength, len(b), pos.TxHash,
		)
	}
	return b, nil
}

func (r *AllocationRepository) resolveTokenIDs(
	ctx context.Context,
	tx pgx.Tx,
	positions []*entity.AllocationPosition,
) (map[tokenCacheKey]int64, error) {
	result := make(map[tokenCacheKey]int64)

	for _, pos := range positions {
		key := tokenCacheKey{ChainID: pos.ChainID, Address: pos.TokenAddress}
		if _, exists := result[key]; exists {
			continue
		}

		tokenID, err := r.tokenRepo.GetOrCreateToken(
			ctx, tx,
			pos.ChainID,
			pos.TokenAddress,
			pos.TokenSymbol,
			pos.TokenDecimals,
			&pos.CreatedAtBlock,
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

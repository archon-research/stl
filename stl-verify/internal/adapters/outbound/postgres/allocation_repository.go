package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// Compile-time check
var _ outbound.AllocationRepository = (*AllocationRepository)(nil)

// AllocationRepository is a PostgreSQL implementation of outbound.AllocationRepository.
type AllocationRepository struct {
	pool      *pgxpool.Pool
	tokenRepo outbound.TokenRepository
	logger    *slog.Logger

	// In-memory token ID cache: (chainID, address) → token_id
	mu       sync.RWMutex
	tokenIDs map[tokenCacheKey]int64
}

type tokenCacheKey struct {
	ChainID int64
	Address common.Address
}

// NewAllocationRepository creates a new PostgreSQL allocation repository.
func NewAllocationRepository(pool *pgxpool.Pool, tokenRepo outbound.TokenRepository, logger *slog.Logger) *AllocationRepository {
	if logger == nil {
		logger = slog.Default()
	}
	return &AllocationRepository{
		pool:      pool,
		tokenRepo: tokenRepo,
		logger:    logger,
		tokenIDs:  make(map[tokenCacheKey]int64),
	}
}

// SavePositions persists a batch of allocation position snapshots.
func (r *AllocationRepository) SavePositions(ctx context.Context, positions []*outbound.AllocationPosition) error {
	if len(positions) == 0 {
		return nil
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if err := tx.Rollback(ctx); err != nil && err != pgx.ErrTxClosed {
			r.logger.Warn("rollback failed", "error", err)
		}
	}()

	// Resolve all token IDs within this transaction
	tokenIDs, err := r.resolveTokenIDs(ctx, tx, positions)
	if err != nil {
		return fmt.Errorf("resolve token IDs: %w", err)
	}

	// Batch insert with savepoints
	inserted := 0
	for i, pos := range positions {
		key := tokenCacheKey{ChainID: pos.ChainID, Address: pos.TokenAddress}
		tokenID, ok := tokenIDs[key]
		if !ok {
			r.logger.Warn("no token_id, skipping",
				"chain", pos.ChainID,
				"address", pos.TokenAddress.Hex())
			continue
		}

		sp := fmt.Sprintf("sp_insert_%d", i)
		if _, err := tx.Exec(ctx, "SAVEPOINT "+sp); err != nil {
			r.logger.Warn("savepoint failed", "error", err)
			continue
		}

		if err := r.insertPosition(ctx, tx, pos, tokenID); err != nil {
			if _, rbErr := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT "+sp); rbErr != nil {
				r.logger.Warn("savepoint rollback failed", "error", rbErr)
			}
			r.logger.Warn("insert failed",
				"address", pos.TokenAddress.Hex(),
				"error", err)
			continue
		}

		if _, err := tx.Exec(ctx, "RELEASE SAVEPOINT "+sp); err != nil {
			r.logger.Warn("savepoint release failed", "error", err)
		}
		inserted++
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	r.logger.Debug("positions saved", "inserted", inserted, "total", len(positions))
	return nil
}

func (r *AllocationRepository) insertPosition(ctx context.Context, tx pgx.Tx, pos *outbound.AllocationPosition, tokenID int64) error {
	// Balance and TxAmount are denominated in the underlying asset (e.g., USDC 6 decimals)
	// ScaledBalance is denominated in the vault token (e.g., morpho vault 18 decimals)
	balanceDecimals := pos.AssetDecimals
	if balanceDecimals == 0 {
		balanceDecimals = pos.TokenDecimals
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

	direction := pos.Direction
	if direction == "" {
		direction = "in"
	}

	txHash := pos.TxHash
	if txHash == "" {
		txHash = fmt.Sprintf("sweep:%d", pos.BlockNumber)
	}

	_, err := tx.Exec(ctx, `
		INSERT INTO allocation_position (
			chain_id, token_id, star, proxy_address,
			balance, scaled_balance,
			block_number, block_version,
			tx_hash, log_index, tx_amount, direction
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (chain_id, token_id, proxy_address, block_number, block_version, tx_hash, log_index, direction)
		DO UPDATE SET
			balance = EXCLUDED.balance,
			scaled_balance = EXCLUDED.scaled_balance
	`,
		pos.ChainID,
		tokenID,
		pos.Star,
		pos.ProxyAddress.Bytes(),
		balanceStr,
		scaledStr,
		pos.BlockNumber,
		pos.BlockVersion,
		common.FromHex(txHash),
		pos.LogIndex,
		txAmountStr,
		direction,
	)
	return err
}

func (r *AllocationRepository) resolveTokenIDs(ctx context.Context, tx pgx.Tx, positions []*outbound.AllocationPosition) (map[tokenCacheKey]int64, error) {
	result := make(map[tokenCacheKey]int64)
	var toResolve []*outbound.AllocationPosition

	// Check in-memory cache first
	r.mu.RLock()
	for _, pos := range positions {
		key := tokenCacheKey{ChainID: pos.ChainID, Address: pos.TokenAddress}
		if id, ok := r.tokenIDs[key]; ok {
			result[key] = id
		} else {
			toResolve = append(toResolve, pos)
		}
	}
	r.mu.RUnlock()

	if len(toResolve) == 0 {
		return result, nil
	}

	// Deduplicate
	seen := make(map[tokenCacheKey]bool)
	var unique []*outbound.AllocationPosition
	for _, pos := range toResolve {
		key := tokenCacheKey{ChainID: pos.ChainID, Address: pos.TokenAddress}
		if !seen[key] {
			seen[key] = true
			unique = append(unique, pos)
		}
	}

	// Resolve via GetOrCreateToken — use savepoints so one failure doesn't abort the tx
	for i, pos := range unique {
		key := tokenCacheKey{ChainID: pos.ChainID, Address: pos.TokenAddress}
		sp := fmt.Sprintf("sp_token_%d", i)

		// Create savepoint
		if _, err := tx.Exec(ctx, "SAVEPOINT "+sp); err != nil {
			r.logger.Warn("savepoint failed", "error", err)
			continue
		}

		tokenID, err := r.tokenRepo.GetOrCreateToken(
			ctx, tx,
			pos.ChainID,
			pos.TokenAddress,
			pos.TokenSymbol,
			pos.TokenDecimals,
			pos.CreatedAtBlock,
		)
		if err != nil {
			// Rollback to savepoint — tx stays usable
			if _, rbErr := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT "+sp); rbErr != nil {
				r.logger.Warn("savepoint rollback failed", "error", rbErr)
			}
			r.logger.Warn("GetOrCreateToken failed",
				"chain", pos.ChainID,
				"address", pos.TokenAddress.Hex(),
				"error", err)
			continue
		}

		// Release savepoint
		if _, err := tx.Exec(ctx, "RELEASE SAVEPOINT "+sp); err != nil {
			r.logger.Warn("savepoint release failed", "error", err)
		}

		result[key] = tokenID

		// Cache for future batches
		r.mu.Lock()
		r.tokenIDs[key] = tokenID
		r.mu.Unlock()
	}

	return result, nil
}

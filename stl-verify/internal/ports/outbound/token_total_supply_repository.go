package outbound

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// TokenTotalSupplyRepository persists pool totalSupply / scaledTotalSupply
// observations that the allocation indexer reads alongside balanceOf.
type TokenTotalSupplyRepository interface {
	// SaveSuppliesTx writes supply rows within an externally managed transaction.
	// Callers coordinate SaveSuppliesTx with AllocationRepository.SavePositionsTx
	// so both writes land atomically for a given block batch.
	SaveSuppliesTx(ctx context.Context, tx pgx.Tx, supplies []*entity.TokenTotalSupply) error
}

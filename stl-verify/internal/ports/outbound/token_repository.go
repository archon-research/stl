package outbound

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// TokenRepository defines the interface for token-related data persistence.
// This aggregate includes base tokens and their derivatives (receipt/debt tokens).
type TokenRepository interface {
	// UpsertTokens upserts base token records.
	// Conflict resolution: ON CONFLICT (chain_id, address) DO UPDATE
	UpsertTokens(ctx context.Context, tokens []*entity.Token) error

	// UpsertReceiptTokens upserts receipt token records (e.g., aTokens in Aave/SparkLend).
	// Conflict resolution: ON CONFLICT (protocol_id, underlying_token_id) DO UPDATE
	UpsertReceiptTokens(ctx context.Context, tokens []*entity.ReceiptToken) error

	// UpsertDebtTokens upserts debt token records (variable and stable debt tokens).
	// Conflict resolution: ON CONFLICT (protocol_id, underlying_token_id) DO UPDATE
	UpsertDebtTokens(ctx context.Context, tokens []*entity.DebtToken) error
}

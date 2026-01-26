package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/ethereum/go-ethereum/common"
)

// Compile-time check that TokenRepository implements outbound.TokenRepository
var _ outbound.TokenRepository = (*TokenRepository)(nil)

// TokenRepository is a PostgreSQL implementation of the outbound.TokenRepository port.
type TokenRepository struct {
	db        *sql.DB
	logger    *slog.Logger
	batchSize int
}

// NewTokenRepository creates a new PostgreSQL Token repository.
// If batchSize is <= 0, the default batch size from DefaultRepositoryConfig() is used.
// Returns an error if the database connection is nil.
//
// Note: This function does not verify that the database connection is alive.
// Use a separate health check or call db.Ping() if connection validation is needed.
func NewTokenRepository(db *sql.DB, logger *slog.Logger, batchSize int) (*TokenRepository, error) {
	if db == nil {
		return nil, fmt.Errorf("database connection cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if batchSize <= 0 {
		batchSize = DefaultRepositoryConfig().TokenBatchSize
	}
	return &TokenRepository{
		db:        db,
		logger:    logger,
		batchSize: batchSize,
	}, nil
}

// GetOrCreateTokenWithTX retrieves a token by address or creates it if it doesn't exist.
// This method participates in an external transaction.
func (r *TokenRepository) GetOrCreateTokenWithTX(ctx context.Context, tx *sql.Tx, chainID int64, address common.Address, symbol string, decimals int, createdAtBlock int64) (int64, error) {
	var tokenID int64

	err := tx.QueryRowContext(ctx,
		`SELECT id FROM token WHERE chain_id = $1 AND address = $2`,
		chainID, address.Bytes()).Scan(&tokenID)

	if err == sql.ErrNoRows {
		if symbol == "" {
			symbol = "UNKNOWN"
		}
		r.logger.Info("auto-creating token", "address", address.Hex(), "symbol", symbol, "decimals", decimals)
		err = tx.QueryRowContext(ctx,
			`INSERT INTO token (chain_id, address, symbol, decimals, created_at_block, metadata, updated_at)
			 VALUES ($1, $2, $3, $4, $5, '{}', NOW())
			 RETURNING id`,
			chainID, address.Bytes(), symbol, decimals, createdAtBlock).Scan(&tokenID)
		if err != nil {
			return 0, fmt.Errorf("failed to create token: %w", err)
		}
		r.logger.Debug("token created", "address", address.Hex(), "id", tokenID, "symbol", symbol, "decimals", decimals)
		return tokenID, nil
	} else if err != nil {
		return 0, fmt.Errorf("failed to get token: %w", err)
	}

	return tokenID, nil
}

// UpsertTokens upserts token records in batches atomically.
// All records are inserted in a single transaction - if any batch fails, all changes are rolled back.
func (r *TokenRepository) UpsertTokens(ctx context.Context, tokens []*entity.Token) error {
	if len(tokens) == 0 {
		return nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer rollback(tx, r.logger)

	for i := 0; i < len(tokens); i += r.batchSize {
		end := i + r.batchSize
		if end > len(tokens) {
			end = len(tokens)
		}
		batch := tokens[i:end]

		if err := r.upsertTokenBatch(ctx, tx, batch); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

func (r *TokenRepository) upsertTokenBatch(ctx context.Context, tx *sql.Tx, tokens []*entity.Token) error {
	if len(tokens) == 0 {
		return nil
	}

	var sb strings.Builder
	sb.WriteString(`
		INSERT INTO token (chain_id, address, symbol, decimals, created_at_block, metadata, updated_at)
		VALUES `)

	args := make([]any, 0, len(tokens)*6)
	for i, token := range tokens {
		if i > 0 {
			sb.WriteString(", ")
		}
		baseIdx := i * 6
		sb.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, NOW())",
			baseIdx+1, baseIdx+2, baseIdx+3, baseIdx+4, baseIdx+5, baseIdx+6))

		metadata, err := marshalMetadata(token.Metadata)
		if err != nil {
			return fmt.Errorf("failed to marshal token metadata for chain %d, address %x: %w", token.ChainID, token.Address, err)
		}
		args = append(args, token.ChainID, token.Address, token.Symbol, token.Decimals, token.CreatedAtBlock, metadata)
	}

	sb.WriteString(`
		ON CONFLICT (chain_id, address) DO UPDATE SET
			symbol = EXCLUDED.symbol,
			decimals = EXCLUDED.decimals,
			metadata = EXCLUDED.metadata,
			updated_at = NOW()
	`)

	_, err := tx.ExecContext(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("failed to upsert token batch: %w", err)
	}
	return nil
}

// UpsertReceiptTokens upserts receipt token records atomically atomically.
// All records are inserted in a single transaction - if any batch fails, all changes are rolled back.
func (r *TokenRepository) UpsertReceiptTokens(ctx context.Context, tokens []*entity.ReceiptToken) error {
	if len(tokens) == 0 {
		return nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer rollback(tx, r.logger)

	for i := 0; i < len(tokens); i += r.batchSize {
		end := i + r.batchSize
		if end > len(tokens) {
			end = len(tokens)
		}
		batch := tokens[i:end]

		if err := r.upsertReceiptTokenBatch(ctx, tx, batch); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

func (r *TokenRepository) upsertReceiptTokenBatch(ctx context.Context, tx *sql.Tx, tokens []*entity.ReceiptToken) error {
	if len(tokens) == 0 {
		return nil
	}

	var sb strings.Builder
	sb.WriteString(`
		INSERT INTO receipt_token (protocol_id, underlying_token_id, receipt_token_address, symbol, created_at_block, metadata, updated_at)
		VALUES `)

	args := make([]any, 0, len(tokens)*6)
	for i, token := range tokens {
		if i > 0 {
			sb.WriteString(", ")
		}
		baseIdx := i * 6
		sb.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, NOW())",
			baseIdx+1, baseIdx+2, baseIdx+3, baseIdx+4, baseIdx+5, baseIdx+6))

		metadata, err := marshalMetadata(token.Metadata)
		if err != nil {
			return fmt.Errorf("failed to marshal receipt token metadata for protocol %d, underlying %d: %w", token.ProtocolID, token.UnderlyingTokenID, err)
		}
		args = append(args, token.ProtocolID, token.UnderlyingTokenID, token.ReceiptTokenAddress, token.Symbol, token.CreatedAtBlock, metadata)
	}

	sb.WriteString(`
		ON CONFLICT (protocol_id, underlying_token_id) DO UPDATE SET
			receipt_token_address = EXCLUDED.receipt_token_address,
			symbol = EXCLUDED.symbol,
			metadata = EXCLUDED.metadata,
			updated_at = NOW()
	`)

	_, err := tx.ExecContext(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("failed to upsert receipt token batch: %w", err)
	}
	return nil
}

// UpsertDebtTokens upserts debt token records atomically atomically.
// All records are inserted in a single transaction - if any batch fails, all changes are rolled back.
func (r *TokenRepository) UpsertDebtTokens(ctx context.Context, tokens []*entity.DebtToken) error {
	if len(tokens) == 0 {
		return nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer rollback(tx, r.logger)

	for i := 0; i < len(tokens); i += r.batchSize {
		end := i + r.batchSize
		if end > len(tokens) {
			end = len(tokens)
		}
		batch := tokens[i:end]

		if err := r.upsertDebtTokenBatch(ctx, tx, batch); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

func (r *TokenRepository) upsertDebtTokenBatch(ctx context.Context, tx *sql.Tx, tokens []*entity.DebtToken) error {
	if len(tokens) == 0 {
		return nil
	}

	var sb strings.Builder
	sb.WriteString(`
		INSERT INTO debt_token (protocol_id, underlying_token_id, variable_debt_address, stable_debt_address, variable_symbol, stable_symbol, created_at_block, metadata, updated_at)
		VALUES `)

	args := make([]any, 0, len(tokens)*8)
	for i, token := range tokens {
		if i > 0 {
			sb.WriteString(", ")
		}
		baseIdx := i * 8
		sb.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, NOW())",
			baseIdx+1, baseIdx+2, baseIdx+3, baseIdx+4, baseIdx+5, baseIdx+6, baseIdx+7, baseIdx+8))

		metadata, err := marshalMetadata(token.Metadata)
		if err != nil {
			return fmt.Errorf("failed to marshal debt token metadata for protocol %d, underlying %d: %w", token.ProtocolID, token.UnderlyingTokenID, err)
		}
		args = append(args, token.ProtocolID, token.UnderlyingTokenID, token.VariableDebtAddress, token.StableDebtAddress, token.VariableSymbol, token.StableSymbol, token.CreatedAtBlock, metadata)
	}

	sb.WriteString(`
		ON CONFLICT (protocol_id, underlying_token_id) DO UPDATE SET
			variable_debt_address = EXCLUDED.variable_debt_address,
			stable_debt_address = EXCLUDED.stable_debt_address,
			variable_symbol = EXCLUDED.variable_symbol,
			stable_symbol = EXCLUDED.stable_symbol,
			metadata = EXCLUDED.metadata,
			updated_at = NOW()
	`)

	_, err := tx.ExecContext(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("failed to upsert debt token batch: %w", err)
	}
	return nil
}

package postgres

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that EventRepository implements outbound.EventRepository
var _ outbound.EventRepository = (*EventRepository)(nil)

// EventRepository is a PostgreSQL implementation of the outbound.EventRepository port.
type EventRepository struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// NewEventRepository creates a new PostgreSQL Event repository.
func NewEventRepository(pool *pgxpool.Pool, logger *slog.Logger) (*EventRepository, error) {
	if pool == nil {
		return nil, fmt.Errorf("database pool cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &EventRepository{
		pool:   pool,
		logger: logger,
	}, nil
}

// SaveEvent saves a single protocol event within an external transaction.
// Uses ON CONFLICT DO NOTHING — duplicate events are silently ignored.
func (r *EventRepository) SaveEvent(ctx context.Context, tx pgx.Tx, event *entity.ProtocolEvent) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO protocol_event (chain_id, protocol_id, block_number, block_version, tx_hash, log_index, contract_address, event_name, event_data)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		 ON CONFLICT (chain_id, block_number, block_version, tx_hash, log_index) DO NOTHING`,
		event.ChainID, event.ProtocolID, event.BlockNumber, event.BlockVersion,
		event.TxHash, event.LogIndex, event.ContractAddress, event.EventName, event.EventData)

	if err != nil {
		return fmt.Errorf("failed to save protocol event: %w", err)
	}
	return nil
}

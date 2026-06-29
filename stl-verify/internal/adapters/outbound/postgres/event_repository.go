package postgres

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that EventRepository implements outbound.EventRepository
var _ outbound.EventRepository = (*EventRepository)(nil)

// EventRepository is a PostgreSQL implementation of the outbound.EventRepository port.
type EventRepository struct {
	logger  *slog.Logger
	buildID buildregistry.BuildID
}

// NewEventRepository creates a new PostgreSQL Event repository.
func NewEventRepository(logger *slog.Logger, buildID buildregistry.BuildID) *EventRepository {
	if logger == nil {
		logger = slog.Default()
	}
	return &EventRepository{
		logger:  logger,
		buildID: buildID,
	}
}

// SaveEvent saves a single protocol event within an external transaction.
// Uses ON CONFLICT DO NOTHING -- duplicate events are silently ignored.
func (r *EventRepository) SaveEvent(ctx context.Context, tx pgx.Tx, event *entity.ProtocolEvent) error {
	if event == nil {
		return fmt.Errorf("event must not be nil")
	}

	_, err := tx.Exec(ctx,
		`INSERT INTO protocol_event (chain_id, protocol_id, block_number, block_version, tx_hash, log_index, contract_address, event_name, event_data, created_at, build_id)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		 ON CONFLICT (chain_id, block_number, block_version, tx_hash, log_index, processing_version, created_at) DO NOTHING`,
		event.ChainID, event.ProtocolID, event.BlockNumber, event.BlockVersion,
		event.TxHash, event.LogIndex, event.ContractAddress, event.EventName, event.EventData,
		event.CreatedAt, int(r.buildID))

	if err != nil {
		return fmt.Errorf("failed to save protocol event: %w", err)
	}
	return nil
}

// SaveBatch saves a slice of protocol events in a single pgx.Batch within tx.
// Empty slice returns nil without sending.
func (r *EventRepository) SaveBatch(ctx context.Context, tx pgx.Tx, evts []*entity.ProtocolEvent) (err error) {
	if len(evts) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, event := range evts {
		if event == nil {
			return fmt.Errorf("event must not be nil")
		}
		batch.Queue(
			`INSERT INTO protocol_event (chain_id, protocol_id, block_number, block_version, tx_hash, log_index, contract_address, event_name, event_data, created_at, build_id)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
			 ON CONFLICT (chain_id, block_number, block_version, tx_hash, log_index, processing_version, created_at) DO NOTHING`,
			event.ChainID, event.ProtocolID, event.BlockNumber, event.BlockVersion,
			event.TxHash, event.LogIndex, event.ContractAddress, event.EventName, event.EventData,
			event.CreatedAt, int(r.buildID),
		)
	}
	br := tx.SendBatch(ctx, batch)
	defer func() {
		if closeErr := br.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("closing protocol_event batch: %w", closeErr)
		}
	}()
	for i := range evts {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("batch protocol_event %d: %w", i, err)
		}
	}
	return nil
}

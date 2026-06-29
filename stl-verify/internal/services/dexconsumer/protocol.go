package dexconsumer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// ProtocolDescriptor identifies a DEX protocol in the protocol registry.
// GetOrCreateProtocol matches on (chain_id, Address); Name and ProtocolType are
// written only when the row is first created. Address must therefore match the
// row seeded by db/migrations/20260521_100000_create_dex_prereqs.sql — a wrong
// address silently creates a second, divergent protocol row.
type ProtocolDescriptor struct {
	Address      common.Address
	Name         string
	ProtocolType string
	DeployBlock  int64
}

// ResolveProtocolID gets (or seeds) the worker's protocol_id in its OWN committed
// transaction, so the returned id always references a committed row. Call it once
// at worker startup and reuse the id for the process lifetime.
//
// Do NOT resolve inside a per-block transaction: GetOrCreateProtocol writes (ON
// CONFLICT DO UPDATE), so resolving per event would amplify writes, and an id
// taken from a caller's uncommitted transaction would be left dangling (failing
// the protocol_event FK on every later block until restart) if that transaction
// rolled back.
func ResolveProtocolID(ctx context.Context, txManager outbound.TxManager, repo outbound.ProtocolRepository, desc ProtocolDescriptor, chainID int64) (int64, error) {
	var id int64
	err := txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		var err error
		id, err = repo.GetOrCreateProtocol(ctx, tx, chainID, desc.Address, desc.Name, desc.ProtocolType, desc.DeployBlock)
		if err != nil {
			return fmt.Errorf("getting %s protocol_id: %w", desc.Name, err)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return id, nil
}

// ProtocolEventInput is the protocol-agnostic shape of one decoded event the
// worker wants persisted to protocol_event. The worker marshals its decoded
// event into Payload and sets the identity fields (chain, block, tx, log index,
// event name, contract) from it.
type ProtocolEventInput struct {
	ContractAddress common.Address
	ChainID         int64
	BlockNumber     int64
	BlockVersion    int
	BlockTimestamp  time.Time
	TxHash          common.Hash
	LogIndex        uint
	EventName       string
	Payload         json.RawMessage
}

// ProtocolEventWriter persists decoded events to protocol_event under a fixed
// protocol_id (resolved once at worker startup via ResolveProtocolID).
type ProtocolEventWriter struct {
	protocolID int64
	eventRepo  outbound.EventRepository
}

func NewProtocolEventWriter(protocolID int64, eventRepo outbound.EventRepository) *ProtocolEventWriter {
	return &ProtocolEventWriter{protocolID: protocolID, eventRepo: eventRepo}
}

// Save builds a validated ProtocolEvent and persists it within tx. Persistence
// is ON CONFLICT DO NOTHING, so a redelivered block is idempotent.
func (w *ProtocolEventWriter) Save(ctx context.Context, tx pgx.Tx, in ProtocolEventInput) error {
	evt, err := entity.NewProtocolEvent(
		int(in.ChainID),
		w.protocolID,
		in.BlockNumber,
		in.BlockVersion,
		in.TxHash.Bytes(),
		int(in.LogIndex),
		in.ContractAddress.Bytes(),
		in.EventName,
		in.Payload,
		in.BlockTimestamp,
	)
	if err != nil {
		return fmt.Errorf("building protocol_event: %w", err)
	}
	if err := w.eventRepo.SaveEvent(ctx, tx, evt); err != nil {
		return fmt.Errorf("saving protocol_event: %w", err)
	}
	return nil
}

// SaveBatch builds validated ProtocolEvents for each input and persists them
// in a single batch within tx. Empty input returns nil.
func (w *ProtocolEventWriter) SaveBatch(ctx context.Context, tx pgx.Tx, ins []ProtocolEventInput) error {
	if len(ins) == 0 {
		return nil
	}
	evts := make([]*entity.ProtocolEvent, 0, len(ins))
	for _, in := range ins {
		evt, err := entity.NewProtocolEvent(
			int(in.ChainID),
			w.protocolID,
			in.BlockNumber,
			in.BlockVersion,
			in.TxHash.Bytes(),
			int(in.LogIndex),
			in.ContractAddress.Bytes(),
			in.EventName,
			in.Payload,
			in.BlockTimestamp,
		)
		if err != nil {
			return fmt.Errorf("building protocol_event: %w", err)
		}
		evts = append(evts, evt)
	}
	if err := w.eventRepo.SaveBatch(ctx, tx, evts); err != nil {
		return fmt.Errorf("saving protocol_events: %w", err)
	}
	return nil
}

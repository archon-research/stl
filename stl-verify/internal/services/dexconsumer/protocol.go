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
// protocol_id set at construction.
type ProtocolEventWriter struct {
	protocolID int64
	eventRepo  outbound.EventRepository
}

func NewProtocolEventWriter(protocolID int64, eventRepo outbound.EventRepository) *ProtocolEventWriter {
	return &ProtocolEventWriter{protocolID: protocolID, eventRepo: eventRepo}
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

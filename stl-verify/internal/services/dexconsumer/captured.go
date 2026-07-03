package dexconsumer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// AnonymousLogEventName names a capture-net entry for a log with zero topics
// (no topic0 to key off of). entity.ProtocolEvent.Validate rejects an empty
// EventName, which would abort SaveBatch and poison-stall the block on
// redelivery, so this sentinel must stay non-empty. Every DEX worker uses it
// for its zero-topic capture entries so protocol_event.event_name is never
// empty.
const AnonymousLogEventName = "anonymous"

// CapturedLog mirrors one log emitted by a watched pool: decoded when its topic0
// matched a known event, raw ({topics, data}) otherwise, so protocol_event stays
// a complete mirror of the on-chain log surface. Address is the log's emitting
// contract verbatim (the pool itself for most logs, but a separate LP-token
// contract for e.g. Curve pre-NG pools' Transfer/Approval); a captured entry
// carries only its emitting address, not pool identity.
type CapturedLog struct {
	Address   common.Address
	LogIndex  uint
	TxHash    common.Hash
	EventName string // decoded event name, or topic0 hex (or AnonymousLogEventName) when unrecognized — never empty
	Payload   json.RawMessage
}

// RawCapturedPayload builds the {topics, data} JSON payload for a log that is not
// ABI-decoded into a typed payload (unknown topic0, zero-topic logs, or
// word-sliced fixed-array events), so protocol_event stays a complete mirror of
// the on-chain log surface.
func RawCapturedPayload(log shared.Log) (json.RawMessage, error) {
	payload, err := json.Marshal(map[string]any{"topics": log.Topics, "data": log.Data})
	if err != nil {
		return nil, fmt.Errorf("marshalling captured log payload (log index %s): %w", log.LogIndex, err)
	}
	return payload, nil
}

// NewRawCapturedLog builds a CapturedLog holding the raw {topics, data} of a log
// whose topic0 did not match any known event (or which carries no topics at
// all). Callers must pass a non-empty eventName (topic0 hex, or
// AnonymousLogEventName for a zero-topic log); addr is the log's
// already-validated emitting contract.
func NewRawCapturedLog(addr common.Address, logIndex uint, txHash common.Hash, eventName string, log shared.Log) (CapturedLog, error) {
	payload, err := RawCapturedPayload(log)
	if err != nil {
		return CapturedLog{}, err
	}
	return CapturedLog{
		Address:   addr,
		LogIndex:  logIndex,
		TxHash:    txHash,
		EventName: eventName,
		Payload:   payload,
	}, nil
}

// ToProtocolEventInputs converts a block's captured logs into the
// protocol-agnostic ProtocolEventInputs the event writer persists, stamping each
// with the block's chain/height/version/timestamp identity. Empty input returns
// an empty slice.
func ToProtocolEventInputs(captured []CapturedLog, chainID, blockNumber int64, blockVersion int, blockTimestamp time.Time) []ProtocolEventInput {
	ins := make([]ProtocolEventInput, 0, len(captured))
	for _, c := range captured {
		ins = append(ins, ProtocolEventInput{
			ContractAddress: c.Address,
			ChainID:         chainID,
			BlockNumber:     blockNumber,
			BlockVersion:    blockVersion,
			BlockTimestamp:  blockTimestamp,
			TxHash:          c.TxHash,
			LogIndex:        c.LogIndex,
			EventName:       c.EventName,
			Payload:         c.Payload,
		})
	}
	return ins
}

// SaveBlockFunc saves a DEX worker's typed per-block writes within tx and returns
// the number of state rows actually inserted (may be zero on an idempotent
// ON CONFLICT DO NOTHING replay). It is the one type-specific seam PersistBlock
// takes, so dexconsumer stays free of any concrete BlockWrites type. Its error
// must already carry the worker's block context; PersistBlock does not re-wrap it.
type SaveBlockFunc func(ctx context.Context, tx pgx.Tx) (int64, error)

// PersistBlock saves the worker's typed block writes and its captured events in a
// single DB transaction: saveBlock and the event writer's SaveBatch share one
// pgx.Tx so both commit or both roll back together. Returns the state-row count
// saveBlock reported (zero on an idempotent replay).
func PersistBlock(ctx context.Context, txMgr outbound.TxManager, eventWriter *ProtocolEventWriter, saveBlock SaveBlockFunc, capturedIns []ProtocolEventInput, blockNumber int64) (int64, error) {
	var stateRows int64
	err := txMgr.WithTransaction(ctx, func(tx pgx.Tx) error {
		var txErr error
		stateRows, txErr = saveBlock(ctx, tx)
		if txErr != nil {
			return txErr
		}
		if txErr := eventWriter.SaveBatch(ctx, tx, capturedIns); txErr != nil {
			return fmt.Errorf("persisting captured events block %d: %w", blockNumber, txErr)
		}
		return nil
	})
	return stateRows, err
}

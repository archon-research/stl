package dexconsumer

import (
	"context"
	"encoding"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
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

// NewDecodedCapturedLog builds a CapturedLog for a log whose topic0 matched a
// known event, mirroring its decoded named fields (eventData, a shared.DecodeLog
// result) as the JSON payload. Every *big.Int — at the top level and nested
// inside slices/arrays (e.g. Curve NG uint256[] token_amounts/fees) — is
// stringified so uint256/int256 values keep full precision in JSONB rather than
// degrading to a lossy float in any number-parsing consumer. addr is the log's
// already-validated emitting contract; eventName must be non-empty.
func NewDecodedCapturedLog(addr common.Address, logIndex uint, txHash common.Hash, eventName string, eventData map[string]any) (CapturedLog, error) {
	payload, err := MarshalDecodedParams(eventData)
	if err != nil {
		return CapturedLog{}, fmt.Errorf("marshalling %s capture payload: %w", eventName, err)
	}
	return CapturedLog{
		Address:   addr,
		LogIndex:  logIndex,
		TxHash:    txHash,
		EventName: eventName,
		Payload:   payload,
	}, nil
}

// MarshalDecodedParams JSON-encodes a shared.DecodeLog result map, recursively
// stringifying every *big.Int (addresses and other scalars marshal as their
// natural JSON form) so uint256/int256/int24 values keep full precision in JSONB.
// Exported for typed-entity params (e.g. UniswapV3PoolEvent.Params) that share
// the capture net's lossless encoding.
func MarshalDecodedParams(data map[string]any) (json.RawMessage, error) {
	out := make(map[string]any, len(data))
	for k, v := range data {
		out[k] = stringifyBigInts(v)
	}
	payload, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("marshalling event params: %w", err)
	}
	return payload, nil
}

// stringifyBigInts returns v with every *big.Int (v itself, or elements of a
// slice/array however deeply nested) replaced by its decimal string; all other
// values are returned unchanged. ABI-decoded event fields are scalars, addresses,
// or fixed/dynamic uint256 arrays — no maps or structs — so slice/array recursion
// covers every big.Int the decode path can produce.
//
// Values that carry their own JSON encoding (common.Address / common.Hash are
// [N]byte arrays implementing encoding.TextMarshaler) are left intact so they
// still marshal as their hex text, not decomposed into a byte slice.
func stringifyBigInts(v any) any {
	if bi, ok := v.(*big.Int); ok {
		return bi.String()
	}
	if _, ok := v.(encoding.TextMarshaler); ok {
		return v
	}
	if _, ok := v.(json.Marshaler); ok {
		return v
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		out := make([]any, rv.Len())
		for i := range out {
			out[i] = stringifyBigInts(rv.Index(i).Interface())
		}
		return out
	}
	return v
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

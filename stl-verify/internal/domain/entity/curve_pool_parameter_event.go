package entity

import (
	"encoding/json"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// CurveParameterEventKind enumerates the value space for
// curve_pool_parameter_event.event_kind. Not enforced by a DB CHECK constraint
// (the column is open-text JSONB territory), but worth a constant for callers.
const (
	CurveParameterEventRampA     = "RampA"
	CurveParameterEventStopRampA = "StopRampA"
	CurveParameterEventNewFee    = "NewFee"
)

// CurvePoolParameterEvent is a per-event fact row for parameter-change
// emissions on a Curve pool — RampA, StopRampA, NewFee (and NG ApplyNewFee,
// which the worker normalises to event_kind = 'NewFee').
//
// Populated fields depend on EventKind; everything else stays nil. The Extra
// JSONB column carries any event-specific data that does not fit the columns
// above (e.g. NG offpeg_fee_multiplier).
type CurvePoolParameterEvent struct {
	CurvePoolID  int64
	BlockNumber  int64
	BlockVersion int32
	Timestamp    time.Time
	TxHash       common.Hash
	LogIndex     int32
	EventKind    string
	OldA         *big.Int
	NewA         *big.Int
	InitialTime  *time.Time
	FutureTime   *time.Time
	NewFee       *big.Int
	NewAdminFee  *big.Int
	Extra        json.RawMessage
}

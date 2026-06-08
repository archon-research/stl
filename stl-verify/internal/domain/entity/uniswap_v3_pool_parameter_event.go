package entity

import (
	"encoding/json"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// UniswapV3PoolParameterEventKind enumerates the values allowed in
// uniswap_v3_pool_parameter_event.event_kind. Matches the CHECK constraint
// on the table.
const (
	UniswapV3PoolParameterEventInitialize                         = "Initialize"
	UniswapV3PoolParameterEventIncreaseObservationCardinalityNext = "IncreaseObservationCardinalityNext"
	UniswapV3PoolParameterEventSetFeeProtocol                     = "SetFeeProtocol"
	UniswapV3PoolParameterEventCollectProtocol                    = "CollectProtocol"
)

// UniswapV3PoolParameterEvent is a per-event fact row for pool-level
// parameter / lifecycle emissions — Initialize, IncreaseObservationCardinality
// Next, SetFeeProtocol, CollectProtocol.
//
// Populated fields depend on EventKind; everything else stays nil. The Extra
// JSONB column carries any event-specific data that does not fit the
// dedicated columns above (forward-compat hook).
//
// Field shapes by EventKind:
//   - Initialize:                              SqrtPriceX96 + Tick.
//   - IncreaseObservationCardinalityNext:      ObservationCardinalityOld +
//     ObservationCardinalityNew.
//   - SetFeeProtocol:                          FeeProtocol{0,1}{Old,New}.
//   - CollectProtocol:                         Sender + Recipient +
//     Amount0 + Amount1.
type UniswapV3PoolParameterEvent struct {
	UniswapV3PoolID           int64
	BlockNumber               int64
	BlockVersion              int32
	Timestamp                 time.Time
	TxHash                    common.Hash
	LogIndex                  int32
	EventKind                 string
	SqrtPriceX96              *big.Int
	Tick                      *int32
	ObservationCardinalityOld *int32
	ObservationCardinalityNew *int32
	FeeProtocol0Old           *int32
	FeeProtocol0New           *int32
	FeeProtocol1Old           *int32
	FeeProtocol1New           *int32
	Amount0                   *big.Int
	Amount1                   *big.Int
	Sender                    *common.Address
	Recipient                 *common.Address
	Extra                     json.RawMessage
}

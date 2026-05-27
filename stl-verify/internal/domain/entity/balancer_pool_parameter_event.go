package entity

import (
	"encoding/json"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// BalancerParameterEventKind enumerates the values allowed in
// balancer_pool_parameter_event.event_kind. Matches the CHECK constraint on
// the table.
const (
	BalancerParameterEventAmpUpdateStarted         = "AmpUpdateStarted"
	BalancerParameterEventAmpUpdateStopped         = "AmpUpdateStopped"
	BalancerParameterEventTokenRateProviderSet     = "TokenRateProviderSet"
	BalancerParameterEventTokenRateCacheUpdated    = "TokenRateCacheUpdated"
	BalancerParameterEventSwapFeePercentageChanged = "SwapFeePercentageChanged"
	BalancerParameterEventPausedStateChanged       = "PausedStateChanged"
)

// BalancerPoolParameterEvent is a per-event fact row for parameter-change
// emissions on a Balancer V2 pool contract — amplification ramp scheduling,
// token-rate cache plumbing, swap-fee + pause governance.
//
// Populated fields depend on EventKind; everything else stays nil. The Extra
// JSONB column carries any event-specific data that does not fit the typed
// columns above (e.g. the `paused` boolean for PausedStateChanged).
type BalancerPoolParameterEvent struct {
	BalancerPoolID    int64
	BlockNumber       int64
	BlockVersion      int32
	Timestamp         time.Time
	TxHash            common.Hash
	LogIndex          int32
	EventKind         string
	StartValue        *big.Int
	EndValue          *big.Int
	CurrentValue      *big.Int
	StartTime         *time.Time
	EndTime           *time.Time
	TokenAddress      *common.Address
	RateProvider      *common.Address
	CacheDuration     *int32
	Rate              *big.Int
	SwapFeePercentage *big.Int
	Extra             json.RawMessage
}

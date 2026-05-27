package curve_dex

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// CurveEventCategory groups Curve event topics by where they were emitted from
// and which decoder ABI applies to them.
type CurveEventCategory int

const (
	CurveEventCategoryUnknown   CurveEventCategory = iota
	CurveEventCategoryPoolV1                       // V1 pool: stETH-classic, 3pool
	CurveEventCategoryPoolNG                       // NG factory pool: stETH-ng
	CurveEventCategoryLPToken                      // ERC-20 Transfer on a separate LP token contract
	CurveEventCategoryGauge                        // Curve liquidity gauge event
	CurveEventCategoryGaugeCtrl                    // GaugeController event (NewGauge/Kill/Killed)
)

// CurveEventName is the decoded Solidity event name. Used as protocol_event.event_name
// and to dispatch typed handling.
type CurveEventName string

const (
	EventTokenExchange            CurveEventName = "TokenExchange"
	EventTokenExchangeUnderlying  CurveEventName = "TokenExchangeUnderlying"
	EventAddLiquidity             CurveEventName = "AddLiquidity"
	EventRemoveLiquidity          CurveEventName = "RemoveLiquidity"
	EventRemoveLiquidityOne       CurveEventName = "RemoveLiquidityOne"
	EventRemoveLiquidityImbalance CurveEventName = "RemoveLiquidityImbalance"
	EventNewFee                   CurveEventName = "NewFee"      // V1
	EventApplyNewFee              CurveEventName = "ApplyNewFee" // NG (normalised to NewFee in DB)
	EventRampA                    CurveEventName = "RampA"
	EventStopRampA                CurveEventName = "StopRampA"
	EventTransfer                 CurveEventName = "Transfer" // ERC-20 on LP token
	EventGaugeDeposit             CurveEventName = "Deposit"
	EventGaugeWithdraw            CurveEventName = "Withdraw"
	EventUpdateLiquidityLimit     CurveEventName = "UpdateLiquidityLimit"
	EventNewGauge                 CurveEventName = "NewGauge"
	EventKillGauge                CurveEventName = "KillGauge"
	EventKilled                   CurveEventName = "Killed"
	EventUnkillGauge              CurveEventName = "UnkillGauge"
	EventUnkilled                 CurveEventName = "Unkilled"
)

// DecodedEvent carries an event the worker decoded from a log. The concrete
// shape lives in one of the typed structs below; Category and Name describe
// which one.
type DecodedEvent struct {
	Category CurveEventCategory
	Name     CurveEventName
	// Address is the log emitter (pool, LP token, gauge, or controller).
	Address common.Address
	LogIdx  int32
	TxHash  common.Hash

	Swap            *swapEvent
	Liquidity       *liquidityEvent
	Parameter       *parameterEvent
	Transfer        *transferEvent
	GaugeActivity   *gaugeActivityEvent
	GaugeController *gaugeControllerEvent
}

type swapEvent struct {
	Buyer        common.Address
	SoldID       int64
	TokensSold   *big.Int
	BoughtID     int64
	TokensBought *big.Int
	IsUnderlying bool
}

type liquidityEvent struct {
	Kind         string // entity.CurveLiquidityEvent* constant
	Provider     common.Address
	TokenAmounts []*big.Int
	Fees         []*big.Int
	Invariant    *big.Int
	TokenSupply  *big.Int
	TokenAmount  *big.Int
	CoinAmount   *big.Int
	CoinIndex    *int16 // NG RemoveLiquidityOne only
}

type parameterEvent struct {
	Kind        string // entity.CurveParameterEvent* constant
	OldA        *big.Int
	NewA        *big.Int
	InitialTime *big.Int
	FutureTime  *big.Int
	NewFee      *big.Int
	NewAdminFee *big.Int
	// Extra carries NG-only ApplyNewFee.offpeg_fee_multiplier, encoded as JSON.
	Extra map[string]any
}

type transferEvent struct {
	From  common.Address
	To    common.Address
	Value *big.Int
}

type gaugeActivityEvent struct {
	// Deposit/Withdraw expose provider+value; UpdateLiquidityLimit exposes
	// per-user working balance/supply pair. Fields that don't apply are nil.
	Provider        common.Address
	Value           *big.Int
	User            common.Address
	OriginalBalance *big.Int
	OriginalSupply  *big.Int
	WorkingBalance  *big.Int
	WorkingSupply   *big.Int
}

type gaugeControllerEvent struct {
	GaugeAddress common.Address
	GaugeType    *big.Int
	Weight       *big.Int
}

// poolMulticallResult bundles the typed outputs of a single event-triggered
// pool multicall (balances, virtual_price, A, fee, NG oracles, exchange-rate
// matrix, LP totalSupply).
type poolMulticallResult struct {
	Balances     []*big.Int
	VirtualPrice *big.Int
	AFactor      *big.Int
	Fee          *big.Int
	PriceOracle  []*big.Int // NG only; nil on V1
	LastPrice    []*big.Int // NG only; nil on V1
	TotalSupply  *big.Int
	ExchangeDy   []dyEntry
}

type dyEntry struct {
	I  int16
	J  int16
	Dx *big.Int
	Dy *big.Int
}

// gaugeMulticallResult bundles the typed outputs of a gauge multicall.
type gaugeMulticallResult struct {
	InflationRate      *big.Int
	WorkingSupply      *big.Int
	TotalSupply        *big.Int
	IsKilled           *bool
	RewardCount        *int32
	RewardTokens       []common.Address
	RewardRates        []*big.Int
	RewardPeriodFinish []*big.Int // unix seconds; converted to time.Time at write
}

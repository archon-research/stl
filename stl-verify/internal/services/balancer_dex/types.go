package balancer_dex

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// BalancerEventCategory groups Balancer V2 event topics by where they were
// emitted from and which decoder ABI applies to them.
type BalancerEventCategory int

const (
	BalancerEventCategoryUnknown BalancerEventCategory = iota
	BalancerEventCategoryVault                         // Vault: Swap, PoolBalanceChanged, PoolBalanceManaged
	BalancerEventCategoryPool                          // Pool contract: AmpUpdate*, TokenRate*, SwapFee*, Paused, Transfer
)

// BalancerEventName is the decoded Solidity event name. Used as
// protocol_event.event_name and to dispatch typed handling.
type BalancerEventName string

const (
	EventSwap                     BalancerEventName = "Swap"
	EventPoolBalanceChanged       BalancerEventName = "PoolBalanceChanged"
	EventPoolBalanceManaged       BalancerEventName = "PoolBalanceManaged"
	EventAmpUpdateStarted         BalancerEventName = "AmpUpdateStarted"
	EventAmpUpdateStopped         BalancerEventName = "AmpUpdateStopped"
	EventTokenRateProviderSet     BalancerEventName = "TokenRateProviderSet"
	EventTokenRateCacheUpdated    BalancerEventName = "TokenRateCacheUpdated"
	EventSwapFeePercentageChanged BalancerEventName = "SwapFeePercentageChanged"
	EventPausedStateChanged       BalancerEventName = "PausedStateChanged"
	EventTransfer                 BalancerEventName = "Transfer" // BPT ERC-20 on pool contract
)

// decodedEvent carries an event the worker decoded from a log. The concrete
// shape lives in one of the typed structs below; Category and Name describe
// which one.
type decodedEvent struct {
	Category BalancerEventCategory
	Name     BalancerEventName
	// Address is the log emitter (Vault for vault events, pool for pool events).
	Address common.Address
	LogIdx  int32
	TxHash  common.Hash
	// PoolID is set for Vault events (always present in Vault topics).
	PoolID common.Hash

	Swap           *swapEvent
	Liquidity      *liquidityEvent
	BalanceManaged *balanceManagedEvent
	Parameter      *parameterEvent
	Transfer       *transferEvent
}

type swapEvent struct {
	TokenIn   common.Address
	TokenOut  common.Address
	AmountIn  *big.Int
	AmountOut *big.Int
}

type liquidityEvent struct {
	LiquidityProvider  common.Address
	Tokens             []common.Address
	Deltas             []*big.Int
	ProtocolFeeAmounts []*big.Int
}

type balanceManagedEvent struct {
	AssetManager common.Address
	Token        common.Address
	CashDelta    *big.Int
	ManagedDelta *big.Int
}

type parameterEvent struct {
	Kind              string // entity.BalancerParameterEvent* constant
	StartValue        *big.Int
	EndValue          *big.Int
	StartTime         *big.Int
	EndTime           *big.Int
	CurrentValue      *big.Int
	TokenIndex        *int64
	RateProvider      *common.Address
	CacheDuration     *int32
	Rate              *big.Int
	SwapFeePercentage *big.Int
	Paused            *bool
	// Extra carries PausedStateChanged.paused (and anything else that does not
	// fit the typed columns), encoded as JSON at write time.
	Extra map[string]any
}

type transferEvent struct {
	From  common.Address
	To    common.Address
	Value *big.Int
}

// vaultTokensResult bundles the Vault.getPoolTokens(poolId) output.
type vaultTokensResult struct {
	Tokens          []common.Address
	Balances        []*big.Int
	LastChangeBlock *big.Int
}

// poolMulticallResult bundles the typed outputs of a single event-triggered
// multicall against the Vault + pool contract. Arrays are parallel to
// vaultTokensResult.Tokens / balancer_pool_token.token_index order.
type poolMulticallResult struct {
	Vault          *vaultTokensResult
	AmpFactor      *big.Int
	AmpIsUpdating  *bool
	BptRate        *big.Int
	ActualSupply   *big.Int // composable_stable only; nil otherwise
	TotalSupply    *big.Int
	ScalingFactors []*big.Int
	TokenRates     []*big.Int
	SwapFee        *big.Int
	Paused         *bool
}

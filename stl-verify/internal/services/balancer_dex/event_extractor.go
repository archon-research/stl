package balancer_dex

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// eventExtractor decodes Balancer V2 logs against the Vault and pool-contract
// (composable_stable) ABIs. Lookup is by topic[0].
type eventExtractor struct {
	// vault map keyed by topic[0]: Swap, PoolBalanceChanged, PoolBalanceManaged,
	// PoolRegistered.
	vault map[common.Hash]*abi.Event
	// pool map keyed by topic[0]: AmpUpdate*, TokenRate*, SwapFeePercentageChanged,
	// PausedStateChanged, Transfer.
	pool map[common.Hash]*abi.Event

	// Name lookups, separate so callers do not have to maintain a parallel map.
	vaultNames map[common.Hash]BalancerEventName
	poolNames  map[common.Hash]BalancerEventName
}

// newEventExtractor builds the topic registries for every Balancer V2 event
// the worker understands.
func newEventExtractor() (*eventExtractor, error) {
	e := &eventExtractor{
		vault:      make(map[common.Hash]*abi.Event),
		pool:       make(map[common.Hash]*abi.Event),
		vaultNames: make(map[common.Hash]BalancerEventName),
		poolNames:  make(map[common.Hash]BalancerEventName),
	}
	if err := e.load(); err != nil {
		return nil, err
	}
	return e, nil
}

func (e *eventExtractor) load() error {
	vaultABI, err := abis.GetBalancerV2VaultEventsABI()
	if err != nil {
		return fmt.Errorf("loading Balancer V2 vault events ABI: %w", err)
	}
	vaultMap := map[string]BalancerEventName{
		"Swap":               EventSwap,
		"PoolBalanceChanged": EventPoolBalanceChanged,
		"PoolBalanceManaged": EventPoolBalanceManaged,
	}
	for sol, n := range vaultMap {
		ev, ok := vaultABI.Events[sol]
		if !ok {
			return fmt.Errorf("vault event %s not in ABI", sol)
		}
		evCopy := ev
		e.vault[ev.ID] = &evCopy
		e.vaultNames[ev.ID] = n
	}
	// PoolRegistered is recognised but has no name mapping (audit only — the
	// worker does not yet auto-discover pools from this event, but accepting
	// the topic prevents the dispatcher from treating it as an unknown signal).
	if ev, ok := vaultABI.Events["PoolRegistered"]; ok {
		evCopy := ev
		e.vault[ev.ID] = &evCopy
	}

	poolABI, err := abis.GetBalancerV2ComposableStableEventsABI()
	if err != nil {
		return fmt.Errorf("loading Balancer V2 composable stable events ABI: %w", err)
	}
	poolMap := map[string]BalancerEventName{
		"AmpUpdateStarted":         EventAmpUpdateStarted,
		"AmpUpdateStopped":         EventAmpUpdateStopped,
		"TokenRateProviderSet":     EventTokenRateProviderSet,
		"TokenRateCacheUpdated":    EventTokenRateCacheUpdated,
		"SwapFeePercentageChanged": EventSwapFeePercentageChanged,
		"PausedStateChanged":       EventPausedStateChanged,
		"Transfer":                 EventTransfer,
	}
	for sol, n := range poolMap {
		ev, ok := poolABI.Events[sol]
		if !ok {
			return fmt.Errorf("pool event %s not in ABI", sol)
		}
		evCopy := ev
		e.pool[ev.ID] = &evCopy
		e.poolNames[ev.ID] = n
	}
	return nil
}

// vaultEvent returns the decoded Balancer V2 Vault event name for the log's
// topic[0], or "", false if no Vault ABI matches.
func (e *eventExtractor) vaultEvent(log shared.Log) (BalancerEventName, bool) {
	if len(log.Topics) == 0 {
		return "", false
	}
	n, ok := e.vaultNames[common.HexToHash(log.Topics[0])]
	return n, ok
}

// poolEvent returns the decoded Balancer V2 pool-contract event name for the
// log's topic[0], or "", false if no pool ABI matches.
func (e *eventExtractor) poolEvent(log shared.Log) (BalancerEventName, bool) {
	if len(log.Topics) == 0 {
		return "", false
	}
	n, ok := e.poolNames[common.HexToHash(log.Topics[0])]
	return n, ok
}

// extractVaultEvent decodes a Vault-emitted log. Returns nil, nil when the
// topic isn't a Vault event the worker handles.
func (e *eventExtractor) extractVaultEvent(log shared.Log) (*decodedEvent, error) {
	if len(log.Topics) == 0 {
		return nil, nil
	}
	topic0 := common.HexToHash(log.Topics[0])
	ev, ok := e.vault[topic0]
	if !ok {
		return nil, nil
	}
	name, hasName := e.vaultNames[topic0]
	if !hasName {
		// e.g. PoolRegistered — recognised but not projected.
		return nil, nil
	}

	data := make(map[string]any)
	if err := parseTopics(ev, log.Topics, data); err != nil {
		return nil, fmt.Errorf("decoding vault topics for %s: %w", name, err)
	}
	if err := parseData(ev, log.Data, data); err != nil {
		return nil, fmt.Errorf("decoding vault data for %s: %w", name, err)
	}

	out := &decodedEvent{
		Category: BalancerEventCategoryVault,
		Name:     name,
		Address:  common.HexToAddress(log.Address),
		TxHash:   common.HexToHash(log.TransactionHash),
	}
	logIdx, err := parseLogIndex(log.LogIndex)
	if err != nil {
		return nil, err
	}
	out.LogIdx = logIdx

	// All Vault events carry poolId as indexed topic[1].
	poolID, err := getHash(data, "poolId")
	if err != nil {
		return nil, fmt.Errorf("decoding poolId for %s: %w", name, err)
	}
	out.PoolID = poolID

	switch name {
	case EventSwap:
		s, err := extractSwap(data)
		if err != nil {
			return nil, fmt.Errorf("decoding Swap: %w", err)
		}
		out.Swap = s
	case EventPoolBalanceChanged:
		l, err := extractLiquidity(data)
		if err != nil {
			return nil, fmt.Errorf("decoding PoolBalanceChanged: %w", err)
		}
		out.Liquidity = l
	case EventPoolBalanceManaged:
		bm, err := extractBalanceManaged(data)
		if err != nil {
			return nil, fmt.Errorf("decoding PoolBalanceManaged: %w", err)
		}
		out.BalanceManaged = bm
	default:
		return nil, fmt.Errorf("unhandled vault event %s", name)
	}
	return out, nil
}

// extractPoolEvent decodes a pool-contract-emitted log. Returns nil, nil when
// the topic isn't a pool event the worker handles.
func (e *eventExtractor) extractPoolEvent(log shared.Log) (*decodedEvent, error) {
	if len(log.Topics) == 0 {
		return nil, nil
	}
	topic0 := common.HexToHash(log.Topics[0])
	ev, ok := e.pool[topic0]
	if !ok {
		return nil, nil
	}
	name := e.poolNames[topic0]

	data := make(map[string]any)
	if err := parseTopics(ev, log.Topics, data); err != nil {
		return nil, fmt.Errorf("decoding pool topics for %s: %w", name, err)
	}
	if err := parseData(ev, log.Data, data); err != nil {
		return nil, fmt.Errorf("decoding pool data for %s: %w", name, err)
	}

	out := &decodedEvent{
		Category: BalancerEventCategoryPool,
		Name:     name,
		Address:  common.HexToAddress(log.Address),
		TxHash:   common.HexToHash(log.TransactionHash),
	}
	logIdx, err := parseLogIndex(log.LogIndex)
	if err != nil {
		return nil, err
	}
	out.LogIdx = logIdx

	switch name {
	case EventAmpUpdateStarted:
		p, err := extractAmpUpdateStarted(data)
		if err != nil {
			return nil, fmt.Errorf("decoding AmpUpdateStarted: %w", err)
		}
		out.Parameter = p
	case EventAmpUpdateStopped:
		p, err := extractAmpUpdateStopped(data)
		if err != nil {
			return nil, fmt.Errorf("decoding AmpUpdateStopped: %w", err)
		}
		out.Parameter = p
	case EventTokenRateProviderSet:
		p, err := extractTokenRateProviderSet(data)
		if err != nil {
			return nil, fmt.Errorf("decoding TokenRateProviderSet: %w", err)
		}
		out.Parameter = p
	case EventTokenRateCacheUpdated:
		p, err := extractTokenRateCacheUpdated(data)
		if err != nil {
			return nil, fmt.Errorf("decoding TokenRateCacheUpdated: %w", err)
		}
		out.Parameter = p
	case EventSwapFeePercentageChanged:
		p, err := extractSwapFeePercentageChanged(data)
		if err != nil {
			return nil, fmt.Errorf("decoding SwapFeePercentageChanged: %w", err)
		}
		out.Parameter = p
	case EventPausedStateChanged:
		p, err := extractPausedStateChanged(data)
		if err != nil {
			return nil, fmt.Errorf("decoding PausedStateChanged: %w", err)
		}
		out.Parameter = p
	case EventTransfer:
		t, err := extractTransfer(data)
		if err != nil {
			return nil, fmt.Errorf("decoding Transfer: %w", err)
		}
		out.Transfer = t
	default:
		return nil, fmt.Errorf("unhandled pool event %s", name)
	}
	return out, nil
}

// -----------------------------------------------------------------------------
// Per-event extractors
// -----------------------------------------------------------------------------

func extractSwap(d map[string]any) (*swapEvent, error) {
	tokenIn, err := getAddress(d, "tokenIn")
	if err != nil {
		return nil, err
	}
	tokenOut, err := getAddress(d, "tokenOut")
	if err != nil {
		return nil, err
	}
	amountIn, err := getBigInt(d, "amountIn")
	if err != nil {
		return nil, err
	}
	amountOut, err := getBigInt(d, "amountOut")
	if err != nil {
		return nil, err
	}
	return &swapEvent{
		TokenIn:   tokenIn,
		TokenOut:  tokenOut,
		AmountIn:  amountIn,
		AmountOut: amountOut,
	}, nil
}

func extractLiquidity(d map[string]any) (*liquidityEvent, error) {
	lp, err := getAddress(d, "liquidityProvider")
	if err != nil {
		return nil, err
	}
	tokens, err := getAddressSlice(d, "tokens")
	if err != nil {
		return nil, err
	}
	deltas, err := getBigIntSlice(d, "deltas")
	if err != nil {
		return nil, err
	}
	fees, err := getBigIntSlice(d, "protocolFeeAmounts")
	if err != nil {
		return nil, err
	}
	return &liquidityEvent{
		LiquidityProvider:  lp,
		Tokens:             tokens,
		Deltas:             deltas,
		ProtocolFeeAmounts: fees,
	}, nil
}

func extractBalanceManaged(d map[string]any) (*balanceManagedEvent, error) {
	assetManager, err := getAddress(d, "assetManager")
	if err != nil {
		return nil, err
	}
	token, err := getAddress(d, "token")
	if err != nil {
		return nil, err
	}
	cashDelta, err := getBigInt(d, "cashDelta")
	if err != nil {
		return nil, err
	}
	managedDelta, err := getBigInt(d, "managedDelta")
	if err != nil {
		return nil, err
	}
	return &balanceManagedEvent{
		AssetManager: assetManager,
		Token:        token,
		CashDelta:    cashDelta,
		ManagedDelta: managedDelta,
	}, nil
}

func extractAmpUpdateStarted(d map[string]any) (*parameterEvent, error) {
	start, err := getBigInt(d, "startValue")
	if err != nil {
		return nil, err
	}
	end, err := getBigInt(d, "endValue")
	if err != nil {
		return nil, err
	}
	startTime, err := getBigInt(d, "startTime")
	if err != nil {
		return nil, err
	}
	endTime, err := getBigInt(d, "endTime")
	if err != nil {
		return nil, err
	}
	return &parameterEvent{
		Kind:       entity.BalancerParameterEventAmpUpdateStarted,
		StartValue: start,
		EndValue:   end,
		StartTime:  startTime,
		EndTime:    endTime,
	}, nil
}

func extractAmpUpdateStopped(d map[string]any) (*parameterEvent, error) {
	current, err := getBigInt(d, "currentValue")
	if err != nil {
		return nil, err
	}
	return &parameterEvent{
		Kind:         entity.BalancerParameterEventAmpUpdateStopped,
		CurrentValue: current,
	}, nil
}

func extractTokenRateProviderSet(d map[string]any) (*parameterEvent, error) {
	idx, err := getBigInt(d, "tokenIndex")
	if err != nil {
		return nil, err
	}
	provider, err := getAddress(d, "provider")
	if err != nil {
		return nil, err
	}
	cacheDuration, err := getBigInt(d, "cacheDuration")
	if err != nil {
		return nil, err
	}
	tokenIdx := idx.Int64()
	cd := int32(cacheDuration.Int64())
	return &parameterEvent{
		Kind:          entity.BalancerParameterEventTokenRateProviderSet,
		TokenIndex:    &tokenIdx,
		RateProvider:  &provider,
		CacheDuration: &cd,
	}, nil
}

func extractTokenRateCacheUpdated(d map[string]any) (*parameterEvent, error) {
	idx, err := getBigInt(d, "tokenIndex")
	if err != nil {
		return nil, err
	}
	rate, err := getBigInt(d, "rate")
	if err != nil {
		return nil, err
	}
	tokenIdx := idx.Int64()
	return &parameterEvent{
		Kind:       entity.BalancerParameterEventTokenRateCacheUpdated,
		TokenIndex: &tokenIdx,
		Rate:       rate,
	}, nil
}

func extractSwapFeePercentageChanged(d map[string]any) (*parameterEvent, error) {
	fee, err := getBigInt(d, "swapFeePercentage")
	if err != nil {
		return nil, err
	}
	return &parameterEvent{
		Kind:              entity.BalancerParameterEventSwapFeePercentageChanged,
		SwapFeePercentage: fee,
	}, nil
}

func extractPausedStateChanged(d map[string]any) (*parameterEvent, error) {
	p, err := getBool(d, "paused")
	if err != nil {
		return nil, err
	}
	return &parameterEvent{
		Kind:   entity.BalancerParameterEventPausedStateChanged,
		Paused: &p,
		Extra:  map[string]any{"paused": p},
	}, nil
}

func extractTransfer(d map[string]any) (*transferEvent, error) {
	from, err := getAddress(d, "from")
	if err != nil {
		return nil, err
	}
	to, err := getAddress(d, "to")
	if err != nil {
		return nil, err
	}
	value, err := getBigInt(d, "value")
	if err != nil {
		return nil, err
	}
	return &transferEvent{From: from, To: to, Value: value}, nil
}

// -----------------------------------------------------------------------------
// ABI helpers (mirrors curve_dex/event_extractor.go)
// -----------------------------------------------------------------------------

func parseTopics(event *abi.Event, topics []string, eventData map[string]any) error {
	var indexed abi.Arguments
	for _, arg := range event.Inputs {
		if arg.Indexed {
			indexed = append(indexed, arg)
		}
	}
	if len(indexed) == 0 {
		return nil
	}
	hashes := make([]common.Hash, 0, len(topics)-1)
	for i := 1; i < len(topics); i++ {
		hashes = append(hashes, common.HexToHash(topics[i]))
	}
	return abi.ParseTopicsIntoMap(eventData, indexed, hashes)
}

func parseData(event *abi.Event, data string, eventData map[string]any) error {
	var nonIndexed abi.Arguments
	for _, arg := range event.Inputs {
		if !arg.Indexed {
			nonIndexed = append(nonIndexed, arg)
		}
	}
	if len(nonIndexed) == 0 || len(data) <= 2 {
		return nil
	}
	raw := common.FromHex(data)
	return nonIndexed.UnpackIntoMap(eventData, raw)
}

func getAddress(d map[string]any, key string) (common.Address, error) {
	v, ok := d[key]
	if !ok {
		return common.Address{}, fmt.Errorf("missing field: %s", key)
	}
	addr, ok := v.(common.Address)
	if !ok {
		return common.Address{}, fmt.Errorf("invalid type for %s: %T", key, v)
	}
	return addr, nil
}

func getHash(d map[string]any, key string) (common.Hash, error) {
	v, ok := d[key]
	if !ok {
		return common.Hash{}, fmt.Errorf("missing field: %s", key)
	}
	switch t := v.(type) {
	case common.Hash:
		return t, nil
	case [32]byte:
		return common.Hash(t), nil
	default:
		return common.Hash{}, fmt.Errorf("invalid type for %s: %T", key, v)
	}
}

func getBigInt(d map[string]any, key string) (*big.Int, error) {
	v, ok := d[key]
	if !ok {
		return nil, fmt.Errorf("missing field: %s", key)
	}
	b, ok := v.(*big.Int)
	if !ok {
		return nil, fmt.Errorf("invalid type for %s: %T", key, v)
	}
	return b, nil
}

func getBigIntSlice(d map[string]any, key string) ([]*big.Int, error) {
	v, ok := d[key]
	if !ok {
		return nil, fmt.Errorf("missing field: %s", key)
	}
	s, ok := v.([]*big.Int)
	if !ok {
		return nil, fmt.Errorf("invalid type for %s: %T", key, v)
	}
	out := make([]*big.Int, len(s))
	for i, b := range s {
		out[i] = new(big.Int).Set(b)
	}
	return out, nil
}

func getAddressSlice(d map[string]any, key string) ([]common.Address, error) {
	v, ok := d[key]
	if !ok {
		return nil, fmt.Errorf("missing field: %s", key)
	}
	s, ok := v.([]common.Address)
	if !ok {
		return nil, fmt.Errorf("invalid type for %s: %T", key, v)
	}
	out := make([]common.Address, len(s))
	copy(out, s)
	return out, nil
}

func getBool(d map[string]any, key string) (bool, error) {
	v, ok := d[key]
	if !ok {
		return false, fmt.Errorf("missing field: %s", key)
	}
	b, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("invalid type for %s: %T", key, v)
	}
	return b, nil
}

func parseLogIndex(s string) (int32, error) {
	if s == "" {
		return 0, nil
	}
	v, ok := new(big.Int).SetString(s, 0)
	if !ok {
		return 0, fmt.Errorf("parsing log index %q", s)
	}
	return int32(v.Int64()), nil
}

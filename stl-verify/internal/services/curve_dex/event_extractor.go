package curve_dex

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// sigEntry binds a topic-hash to its decoded event metadata and category.
type sigEntry struct {
	event    *abi.Event
	category CurveEventCategory
	name     CurveEventName
}

// eventExtractor decodes Curve DEX logs against the V1 / NG / LP token / gauge /
// gauge-controller ABIs. Lookup is by topic[0]; the same topic hash may appear
// across pool kinds (e.g. RampA in V1 and NG share the signature), in which
// case the V1 entry "wins" — pool kind is disambiguated at decode time via the
// caller-provided category.
type eventExtractor struct {
	// poolV1 / poolNG hold per-name event metadata so we can decode against
	// the right ABI based on the source pool's pool_kind.
	poolV1 map[CurveEventName]*abi.Event
	poolNG map[CurveEventName]*abi.Event

	// lp / gauge / gaugeCtrl lookup by topic[0].
	lpToken    map[common.Hash]*abi.Event
	gauge      map[common.Hash]*sigEntry
	gaugeCtrl  map[common.Hash]*sigEntry
	poolTopics map[common.Hash]CurveEventName // first registration wins for shared topics
}

// newEventExtractor builds the topic registries for every Curve event the worker
// understands.
func newEventExtractor() (*eventExtractor, error) {
	e := &eventExtractor{
		poolV1:     make(map[CurveEventName]*abi.Event),
		poolNG:     make(map[CurveEventName]*abi.Event),
		lpToken:    make(map[common.Hash]*abi.Event),
		gauge:      make(map[common.Hash]*sigEntry),
		gaugeCtrl:  make(map[common.Hash]*sigEntry),
		poolTopics: make(map[common.Hash]CurveEventName),
	}
	if err := e.load(); err != nil {
		return nil, err
	}
	return e, nil
}

func (e *eventExtractor) load() error {
	v1ABI, err := abis.GetCurveStableswapV1EventsABI()
	if err != nil {
		return fmt.Errorf("loading Curve V1 events ABI: %w", err)
	}
	v1Names := []CurveEventName{
		EventTokenExchange, EventAddLiquidity, EventRemoveLiquidity,
		EventRemoveLiquidityOne, EventRemoveLiquidityImbalance,
		EventNewFee, EventRampA, EventStopRampA,
	}
	for _, n := range v1Names {
		ev, ok := v1ABI.Events[string(n)]
		if !ok {
			return fmt.Errorf("V1 event %s not in ABI", n)
		}
		evCopy := ev
		e.poolV1[n] = &evCopy
		if _, present := e.poolTopics[evCopy.ID]; !present {
			e.poolTopics[evCopy.ID] = n
		}
	}

	ngABI, err := abis.GetCurveStableswapNGEventsABI()
	if err != nil {
		return fmt.Errorf("loading Curve NG events ABI: %w", err)
	}
	ngNames := []CurveEventName{
		EventTokenExchange, EventTokenExchangeUnderlying,
		EventAddLiquidity, EventRemoveLiquidity,
		EventRemoveLiquidityOne, EventRemoveLiquidityImbalance,
		EventRampA, EventStopRampA, EventApplyNewFee,
	}
	for _, n := range ngNames {
		ev, ok := ngABI.Events[string(n)]
		if !ok {
			return fmt.Errorf("NG event %s not in ABI", n)
		}
		evCopy := ev
		e.poolNG[n] = &evCopy
		if _, present := e.poolTopics[evCopy.ID]; !present {
			e.poolTopics[evCopy.ID] = n
		}
	}

	lpABI, err := abis.GetCurveLPTokenEventsABI()
	if err != nil {
		return fmt.Errorf("loading Curve LP token ABI: %w", err)
	}
	tEv, ok := lpABI.Events["Transfer"]
	if !ok {
		return fmt.Errorf("LP token Transfer event not in ABI")
	}
	tEvCopy := tEv
	e.lpToken[tEv.ID] = &tEvCopy

	gAbi, err := abis.GetCurveGaugeEventsABI()
	if err != nil {
		return fmt.Errorf("loading Curve gauge events ABI: %w", err)
	}
	gaugeMap := map[string]CurveEventName{
		"Deposit":              EventGaugeDeposit,
		"Withdraw":             EventGaugeWithdraw,
		"UpdateLiquidityLimit": EventUpdateLiquidityLimit,
	}
	for sol, n := range gaugeMap {
		ev, ok := gAbi.Events[sol]
		if !ok {
			return fmt.Errorf("gauge event %s not in ABI", sol)
		}
		evCopy := ev
		e.gauge[ev.ID] = &sigEntry{event: &evCopy, category: CurveEventCategoryGauge, name: n}
	}

	gcAbi, err := abis.GetCurveGaugeControllerEventsABI()
	if err != nil {
		return fmt.Errorf("loading Curve gauge controller events ABI: %w", err)
	}
	gcMap := map[string]CurveEventName{
		"NewGauge":    EventNewGauge,
		"KillGauge":   EventKillGauge,
		"Killed":      EventKilled,
		"UnkillGauge": EventUnkillGauge,
		"Unkilled":    EventUnkilled,
	}
	for sol, n := range gcMap {
		ev, ok := gcAbi.Events[sol]
		if !ok {
			return fmt.Errorf("gauge controller event %s not in ABI", sol)
		}
		evCopy := ev
		e.gaugeCtrl[ev.ID] = &sigEntry{event: &evCopy, category: CurveEventCategoryGaugeCtrl, name: n}
	}

	return nil
}

// poolTopic returns the decoded Curve pool event name for the log's topic[0],
// or "", false if no pool ABI matches.
func (e *eventExtractor) poolTopic(log shared.Log) (CurveEventName, bool) {
	if len(log.Topics) == 0 {
		return "", false
	}
	n, ok := e.poolTopics[common.HexToHash(log.Topics[0])]
	return n, ok
}

// isLPTokenTransfer returns true if topic[0] matches the ERC-20 Transfer hash.
func (e *eventExtractor) isLPTokenTransfer(log shared.Log) bool {
	if len(log.Topics) == 0 {
		return false
	}
	_, ok := e.lpToken[common.HexToHash(log.Topics[0])]
	return ok
}

// gaugeSig returns the typed gauge event metadata for topic[0], or nil if no
// gauge ABI matches.
func (e *eventExtractor) gaugeSig(log shared.Log) *sigEntry {
	if len(log.Topics) == 0 {
		return nil
	}
	return e.gauge[common.HexToHash(log.Topics[0])]
}

// gaugeCtrlSig returns the typed GaugeController event metadata for
// topic[0], or nil if no controller ABI matches.
func (e *eventExtractor) gaugeCtrlSig(log shared.Log) *sigEntry {
	if len(log.Topics) == 0 {
		return nil
	}
	return e.gaugeCtrl[common.HexToHash(log.Topics[0])]
}

// extractPoolEvent decodes a pool-emitted log against the right ABI for the
// pool's kind (entity.CurvePoolKindV1 or KindNG). Returns nil, nil when the
// topic isn't a Curve pool event the worker handles.
func (e *eventExtractor) extractPoolEvent(log shared.Log, poolKind string) (*DecodedEvent, error) {
	name, ok := e.poolTopic(log)
	if !ok {
		return nil, nil
	}

	var src *abi.Event
	if poolKind == entity.CurvePoolKindNG {
		src = e.poolNG[name]
		if src == nil {
			src = e.poolV1[name] // RemoveLiquidityImbalance / etc. may share via V1.
		}
	} else {
		src = e.poolV1[name]
		if src == nil {
			src = e.poolNG[name]
		}
	}
	if src == nil {
		return nil, fmt.Errorf("no decoder for pool event %s (poolKind=%s)", name, poolKind)
	}

	data := make(map[string]any)
	if err := parseTopics(src, log.Topics, data); err != nil {
		return nil, fmt.Errorf("decoding topics for %s: %w", name, err)
	}
	if err := parseData(src, log.Data, data); err != nil {
		return nil, fmt.Errorf("decoding data for %s: %w", name, err)
	}

	out := &DecodedEvent{
		Category: CurveEventCategoryPoolV1,
		Name:     name,
		Address:  common.HexToAddress(log.Address),
		TxHash:   common.HexToHash(log.TransactionHash),
	}
	if poolKind == entity.CurvePoolKindNG {
		out.Category = CurveEventCategoryPoolNG
	}
	logIdx, err := parseLogIndex(log.LogIndex)
	if err != nil {
		return nil, err
	}
	out.LogIdx = logIdx

	switch name {
	case EventTokenExchange, EventTokenExchangeUnderlying:
		s, err := extractSwap(data, name == EventTokenExchangeUnderlying)
		if err != nil {
			return nil, fmt.Errorf("decoding %s: %w", name, err)
		}
		out.Swap = s
	case EventAddLiquidity, EventRemoveLiquidity, EventRemoveLiquidityImbalance:
		l, err := extractLiquidityArray(data, string(name))
		if err != nil {
			return nil, fmt.Errorf("decoding %s: %w", name, err)
		}
		out.Liquidity = l
	case EventRemoveLiquidityOne:
		l, err := extractLiquidityOne(data, poolKind == entity.CurvePoolKindNG)
		if err != nil {
			return nil, fmt.Errorf("decoding %s: %w", name, err)
		}
		out.Liquidity = l
	case EventNewFee:
		p, err := extractV1NewFee(data)
		if err != nil {
			return nil, fmt.Errorf("decoding NewFee: %w", err)
		}
		out.Parameter = p
	case EventApplyNewFee:
		p, err := extractNGApplyNewFee(data)
		if err != nil {
			return nil, fmt.Errorf("decoding ApplyNewFee: %w", err)
		}
		out.Parameter = p
	case EventRampA:
		p, err := extractRampA(data)
		if err != nil {
			return nil, fmt.Errorf("decoding RampA: %w", err)
		}
		out.Parameter = p
	case EventStopRampA:
		p, err := extractStopRampA(data)
		if err != nil {
			return nil, fmt.Errorf("decoding StopRampA: %w", err)
		}
		out.Parameter = p
	default:
		return nil, fmt.Errorf("unhandled pool event name %s", name)
	}
	return out, nil
}

// extractLPTokenTransfer decodes an ERC-20 Transfer log emitted by a Curve LP
// token contract (or by an NG pool's own address).
func (e *eventExtractor) extractLPTokenTransfer(log shared.Log) (*DecodedEvent, error) {
	if len(log.Topics) == 0 {
		return nil, fmt.Errorf("transfer log has no topics")
	}
	ev, ok := e.lpToken[common.HexToHash(log.Topics[0])]
	if !ok {
		return nil, fmt.Errorf("not a Transfer event")
	}
	data := make(map[string]any)
	if err := parseTopics(ev, log.Topics, data); err != nil {
		return nil, fmt.Errorf("decoding Transfer topics: %w", err)
	}
	if err := parseData(ev, log.Data, data); err != nil {
		return nil, fmt.Errorf("decoding Transfer data: %w", err)
	}
	from, err := getAddress(data, "from")
	if err != nil {
		return nil, err
	}
	to, err := getAddress(data, "to")
	if err != nil {
		return nil, err
	}
	val, err := getBigInt(data, "value")
	if err != nil {
		return nil, err
	}
	logIdx, err := parseLogIndex(log.LogIndex)
	if err != nil {
		return nil, err
	}
	return &DecodedEvent{
		Category: CurveEventCategoryLPToken,
		Name:     EventTransfer,
		Address:  common.HexToAddress(log.Address),
		LogIdx:   logIdx,
		TxHash:   common.HexToHash(log.TransactionHash),
		Transfer: &transferEvent{From: from, To: to, Value: val},
	}, nil
}

// extractGaugeEvent decodes a gauge-emitted log.
func (e *eventExtractor) extractGaugeEvent(log shared.Log) (*DecodedEvent, error) {
	entry := e.gaugeSig(log)
	if entry == nil {
		return nil, fmt.Errorf("not a gauge event")
	}
	data := make(map[string]any)
	if err := parseTopics(entry.event, log.Topics, data); err != nil {
		return nil, fmt.Errorf("decoding gauge topics: %w", err)
	}
	if err := parseData(entry.event, log.Data, data); err != nil {
		return nil, fmt.Errorf("decoding gauge data: %w", err)
	}
	logIdx, err := parseLogIndex(log.LogIndex)
	if err != nil {
		return nil, err
	}
	out := &DecodedEvent{
		Category: CurveEventCategoryGauge,
		Name:     entry.name,
		Address:  common.HexToAddress(log.Address),
		LogIdx:   logIdx,
		TxHash:   common.HexToHash(log.TransactionHash),
	}
	g := &gaugeActivityEvent{}
	switch entry.name {
	case EventGaugeDeposit, EventGaugeWithdraw:
		addr, err := getAddress(data, "provider")
		if err != nil {
			return nil, err
		}
		val, err := getBigInt(data, "value")
		if err != nil {
			return nil, err
		}
		g.Provider = addr
		g.Value = val
	case EventUpdateLiquidityLimit:
		u, err := getAddress(data, "user")
		if err != nil {
			return nil, err
		}
		ob, err := getBigInt(data, "original_balance")
		if err != nil {
			return nil, err
		}
		os_, err := getBigInt(data, "original_supply")
		if err != nil {
			return nil, err
		}
		wb, err := getBigInt(data, "working_balance")
		if err != nil {
			return nil, err
		}
		ws, err := getBigInt(data, "working_supply")
		if err != nil {
			return nil, err
		}
		g.User = u
		g.OriginalBalance = ob
		g.OriginalSupply = os_
		g.WorkingBalance = wb
		g.WorkingSupply = ws
	default:
		return nil, fmt.Errorf("unhandled gauge event %s", entry.name)
	}
	out.GaugeActivity = g
	return out, nil
}

// extractGaugeControllerEvent decodes a GaugeController log.
func (e *eventExtractor) extractGaugeControllerEvent(log shared.Log) (*DecodedEvent, error) {
	entry := e.gaugeCtrlSig(log)
	if entry == nil {
		return nil, fmt.Errorf("not a gauge controller event")
	}
	data := make(map[string]any)
	if err := parseTopics(entry.event, log.Topics, data); err != nil {
		return nil, fmt.Errorf("decoding gc topics: %w", err)
	}
	if err := parseData(entry.event, log.Data, data); err != nil {
		return nil, fmt.Errorf("decoding gc data: %w", err)
	}
	logIdx, err := parseLogIndex(log.LogIndex)
	if err != nil {
		return nil, err
	}
	out := &DecodedEvent{
		Category: CurveEventCategoryGaugeCtrl,
		Name:     entry.name,
		Address:  common.HexToAddress(log.Address),
		LogIdx:   logIdx,
		TxHash:   common.HexToHash(log.TransactionHash),
	}
	gc := &gaugeControllerEvent{}
	addr, err := getAddress(data, "addr")
	if err != nil {
		return nil, err
	}
	gc.GaugeAddress = addr
	if entry.name == EventNewGauge {
		if gt, err := getBigInt(data, "gauge_type"); err == nil {
			gc.GaugeType = gt
		}
		if w, err := getBigInt(data, "weight"); err == nil {
			gc.Weight = w
		}
	}
	out.GaugeController = gc
	return out, nil
}

// -----------------------------------------------------------------------------
// Per-event extractors
// -----------------------------------------------------------------------------

func extractSwap(d map[string]any, isUnderlying bool) (*swapEvent, error) {
	buyer, err := getAddress(d, "buyer")
	if err != nil {
		return nil, err
	}
	sold, err := getBigInt(d, "sold_id")
	if err != nil {
		return nil, err
	}
	ts, err := getBigInt(d, "tokens_sold")
	if err != nil {
		return nil, err
	}
	bought, err := getBigInt(d, "bought_id")
	if err != nil {
		return nil, err
	}
	tb, err := getBigInt(d, "tokens_bought")
	if err != nil {
		return nil, err
	}
	return &swapEvent{
		Buyer:        buyer,
		SoldID:       sold.Int64(),
		TokensSold:   ts,
		BoughtID:     bought.Int64(),
		TokensBought: tb,
		IsUnderlying: isUnderlying,
	}, nil
}

func extractLiquidityArray(d map[string]any, name string) (*liquidityEvent, error) {
	prov, err := getAddress(d, "provider")
	if err != nil {
		return nil, err
	}
	amounts, err := getBigIntSlice(d, "token_amounts")
	if err != nil {
		return nil, err
	}
	fees, err := getBigIntSlice(d, "fees")
	if err != nil {
		return nil, err
	}
	ts, err := getBigInt(d, "token_supply")
	if err != nil {
		return nil, err
	}
	out := &liquidityEvent{
		Kind:         name,
		Provider:     prov,
		TokenAmounts: amounts,
		Fees:         fees,
		TokenSupply:  ts,
	}
	// invariant is absent on RemoveLiquidity (only AddLiquidity / RemoveLiquidityImbalance carry it).
	if inv, err := getBigInt(d, "invariant"); err == nil {
		out.Invariant = inv
	}
	return out, nil
}

func extractLiquidityOne(d map[string]any, isNG bool) (*liquidityEvent, error) {
	prov, err := getAddress(d, "provider")
	if err != nil {
		return nil, err
	}
	tokenAmount, err := getBigInt(d, "token_amount")
	if err != nil {
		return nil, err
	}
	coinAmount, err := getBigInt(d, "coin_amount")
	if err != nil {
		return nil, err
	}
	out := &liquidityEvent{
		Kind:        entity.CurveLiquidityEventRemoveLiquidityOne,
		Provider:    prov,
		TokenAmount: tokenAmount,
		CoinAmount:  coinAmount,
		// token_amounts/fees stay nil — the LiquidityEvent schema requires
		// token_amounts NOT NULL on insert; the repo handles the empty case.
		TokenAmounts: []*big.Int{},
	}
	if isNG {
		if ci, err := getBigInt(d, "coin_index"); err == nil {
			v := int16(ci.Int64())
			out.CoinIndex = &v
		}
		if ts, err := getBigInt(d, "token_supply"); err == nil {
			out.TokenSupply = ts
		}
	}
	return out, nil
}

func extractV1NewFee(d map[string]any) (*parameterEvent, error) {
	fee, err := getBigInt(d, "fee")
	if err != nil {
		return nil, err
	}
	out := &parameterEvent{
		Kind:   entity.CurveParameterEventNewFee,
		NewFee: fee,
	}
	if af, err := getBigInt(d, "admin_fee"); err == nil {
		out.NewAdminFee = af
	}
	return out, nil
}

func extractNGApplyNewFee(d map[string]any) (*parameterEvent, error) {
	fee, err := getBigInt(d, "fee")
	if err != nil {
		return nil, err
	}
	out := &parameterEvent{
		Kind:   entity.CurveParameterEventNewFee,
		NewFee: fee,
	}
	if m, err := getBigInt(d, "offpeg_fee_multiplier"); err == nil {
		out.Extra = map[string]any{"offpeg_fee_multiplier": m.String()}
	}
	return out, nil
}

func extractRampA(d map[string]any) (*parameterEvent, error) {
	oldA, err := getBigInt(d, "old_A")
	if err != nil {
		return nil, err
	}
	newA, err := getBigInt(d, "new_A")
	if err != nil {
		return nil, err
	}
	init, err := getBigInt(d, "initial_time")
	if err != nil {
		return nil, err
	}
	future, err := getBigInt(d, "future_time")
	if err != nil {
		return nil, err
	}
	return &parameterEvent{
		Kind:        entity.CurveParameterEventRampA,
		OldA:        oldA,
		NewA:        newA,
		InitialTime: init,
		FutureTime:  future,
	}, nil
}

func extractStopRampA(d map[string]any) (*parameterEvent, error) {
	a, err := getBigInt(d, "A")
	if err != nil {
		return nil, err
	}
	t, err := getBigInt(d, "t")
	if err != nil {
		return nil, err
	}
	// StopRampA's (A, t) shape does not map onto RampA's
	// (old_a, new_a, initial_time, future_time) columns — there's no "old"
	// value (the ramp is being aborted) and no "future" target (the new A is
	// already in effect). Park the on-chain payload in `extra` so consumers
	// reading the typed columns see clean NULLs and can opt-in to the post-
	// stop state via `extra->>'stop_a'` / `extra->>'stopped_at_time'`.
	return &parameterEvent{
		Kind: entity.CurveParameterEventStopRampA,
		Extra: map[string]any{
			"stop_a":          a.String(),
			"stopped_at_time": t.String(),
		},
	}, nil
}

// -----------------------------------------------------------------------------
// ABI helpers (mirrors morpho_indexer/event_extractor.go)
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

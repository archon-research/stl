package uniswapv4indexer

import (
	"fmt"
	"maps"
	"math/big"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/services/dexconsumer"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

var eventsByID = sync.OnceValues(func() (map[common.Hash]*abi.Event, error) {
	poolManagerABI, err := PoolManagerABI()
	if err != nil {
		return nil, err
	}
	if err := assertRoutedEventsExist(poolManagerABI, poolKeyedEvents); err != nil {
		return nil, err
	}
	out := make(map[common.Hash]*abi.Event, len(poolManagerABI.Events))
	for _, ev := range poolManagerABI.Events {
		out[ev.ID] = &ev
	}
	return out, nil
})

// poolKeyedEvents carry the PoolId as their first indexed argument, so topics[1]
// resolves which pool they belong to.
var poolKeyedEvents = map[string]struct{}{
	"Initialize":         {},
	"ModifyLiquidity":    {},
	"Swap":               {},
	"Donate":             {},
	"ProtocolFeeUpdated": {},
}

// erc6909Events are the inherited claim-token events: keyed by currency id, not
// PoolId, so they cannot be attributed to a pool at all — dropped, not captured.
var erc6909Events = map[string]struct{}{
	"Transfer":    {},
	"Approval":    {},
	"OperatorSet": {},
}

var poolEventNames = map[string]entity.UniswapV4PoolEventName{
	"Initialize":         entity.UniswapV4PoolEventInitialize,
	"Donate":             entity.UniswapV4PoolEventDonate,
	"ProtocolFeeUpdated": entity.UniswapV4PoolEventProtocolFeeUpdated,
}

// A typo in a routing table would silently divert real logs into the
// unknown-topic0 capture net instead of their typed table.
func assertRoutedEventsExist(poolManagerABI *abi.ABI, keyed map[string]struct{}) error {
	tables := []struct {
		table string
		names []string
	}{
		{"poolKeyedEvents", slices.Sorted(maps.Keys(keyed))},
		{"erc6909Events", slices.Sorted(maps.Keys(erc6909Events))},
		{"poolEventNames", slices.Sorted(maps.Keys(poolEventNames))},
	}
	for _, t := range tables {
		for _, name := range t.names {
			if _, ok := poolManagerABI.Events[name]; !ok {
				return fmt.Errorf("%s routes %q, which the PoolManager ABI does not define", t.table, name)
			}
		}
	}
	return assertKeyedEventsDispatch(keyed)
}

func assertKeyedEventsDispatch(keyed map[string]struct{}) error {
	for _, name := range slices.Sorted(maps.Keys(keyed)) {
		if name == "Swap" || name == "ModifyLiquidity" {
			continue
		}
		if _, ok := poolEventNames[name]; !ok {
			return fmt.Errorf("poolKeyedEvents routes %q, which appendTypedEvent has no builder for", name)
		}
	}
	return nil
}

// DecodeEvents runs once per receipt against the whole registry, and returns the
// set of registered pool IDs that receipt touched.
func DecodeEvents(
	receipt shared.TransactionReceipt,
	poolsByID map[common.Hash]RegisteredPool,
	poolManager common.Address,
	blockNumber int64,
	version int,
	ts time.Time,
) (DecodedEvents, map[int64]bool, error) {
	events, err := eventsByID()
	if err != nil {
		return DecodedEvents{}, nil, fmt.Errorf("loading PoolManager ABI: %w", err)
	}

	d := &receiptDecoder{
		events:      events,
		poolsByID:   poolsByID,
		poolManager: poolManager,
		blockNumber: blockNumber,
		version:     version,
		ts:          ts,
		touched:     make(map[int64]bool),
	}
	for _, log := range receipt.Logs {
		if err := d.decodeLog(log); err != nil {
			return DecodedEvents{}, nil, err
		}
	}
	return d.out, d.touched, nil
}

type receiptDecoder struct {
	events      map[common.Hash]*abi.Event
	poolsByID   map[common.Hash]RegisteredPool
	poolManager common.Address
	blockNumber int64
	version     int
	ts          time.Time

	out     DecodedEvents
	touched map[int64]bool
}

type logSite struct {
	address  common.Address
	logIndex uint
	txHash   common.Hash
}

func (d *receiptDecoder) decodeLog(log shared.Log) error {
	if !common.IsHexAddress(log.Address) {
		return fmt.Errorf("invalid log address %q", log.Address)
	}
	addr := common.HexToAddress(log.Address)
	if !shared.LogBelongsTo(addr, d.poolManager) {
		return nil
	}

	if err := assertHexWords(log); err != nil {
		return err
	}
	logIndex, err := shared.ParseHexUint(log.LogIndex)
	if err != nil {
		return fmt.Errorf("parsing log index %q: %w", log.LogIndex, err)
	}
	site := logSite{address: addr, logIndex: logIndex, txHash: common.HexToHash(log.TransactionHash)}

	ev := d.knownEvent(log)
	if ev == nil {
		return d.captureRaw(log, site)
	}
	if _, skip := erc6909Events[ev.Name]; skip {
		return nil
	}
	if _, keyed := poolKeyedEvents[ev.Name]; keyed {
		return d.decodePoolKeyedLog(*ev, log, site)
	}
	_, err = d.decodeAndCapture(*ev, log, site)
	return err
}

func (d *receiptDecoder) knownEvent(log shared.Log) *abi.Event {
	if len(log.Topics) == 0 {
		return nil
	}
	return d.events[common.HexToHash(log.Topics[0])]
}

// A log for an untracked pool is dropped: the singleton emits for thousands of
// pools outside the registry, so this filter is deliberate, not a swallowed failure.
func (d *receiptDecoder) decodePoolKeyedLog(ev abi.Event, log shared.Log, site logSite) error {
	poolID, err := indexedPoolID(ev, log)
	if err != nil {
		return err
	}
	pool, tracked := d.poolsByID[poolID]
	if !tracked {
		return nil
	}

	data, err := d.decodeAndCapture(ev, log, site)
	if err != nil {
		return err
	}
	if err := d.appendTypedEvent(ev.Name, data, pool, site); err != nil {
		return fmt.Errorf("extracting %s: %w", ev.Name, err)
	}
	d.touched[pool.ID] = true
	return nil
}

// assertHexWords has already rejected a topic that is not a full 32-byte word.
func indexedPoolID(ev abi.Event, log shared.Log) (common.Hash, error) {
	if len(log.Topics) < 2 {
		return common.Hash{}, fmt.Errorf("%s log (index %s) carries no indexed pool id", ev.Name, log.LogIndex)
	}
	return common.HexToHash(log.Topics[1]), nil
}

// common.HexToHash left-pads a short value and truncates at the first non-hex
// character, so one corrupted character would silently become a registry miss or
// a wrong sender or transaction hash on a persisted row.
func assertHexWords(log shared.Log) error {
	if !isHexWord(log.TransactionHash) {
		return fmt.Errorf("log (index %s) transaction hash %q is not a 32-byte hex word", log.LogIndex, log.TransactionHash)
	}
	for i, topic := range log.Topics {
		if !isHexWord(topic) {
			return fmt.Errorf("log (index %s) topic %d %q is not a 32-byte hex word", log.LogIndex, i, topic)
		}
	}
	return nil
}

func isHexWord(value string) bool {
	return len(value) == 66 && strings.HasPrefix(value, "0x") && common.IsHexHash(value)
}

func (d *receiptDecoder) decodeAndCapture(ev abi.Event, log shared.Log, site logSite) (map[string]any, error) {
	data, err := shared.DecodeLog(ev, log)
	if err != nil {
		return nil, fmt.Errorf("decoding %s log (index %s): %w", ev.Name, log.LogIndex, err)
	}
	captured, err := dexconsumer.NewDecodedCapturedLog(site.address, site.logIndex, site.txHash, ev.Name, data)
	if err != nil {
		return nil, err
	}
	d.out.Captured = append(d.out.Captured, captured)
	return data, nil
}

// The PoolManager is non-upgradeable, so this net is expected to stay empty.
func (d *receiptDecoder) captureRaw(log shared.Log, site logSite) error {
	name := dexconsumer.AnonymousLogEventName
	if len(log.Topics) > 0 {
		name = common.HexToHash(log.Topics[0]).Hex()
	}
	captured, err := dexconsumer.NewRawCapturedLog(site.address, site.logIndex, site.txHash, name, log)
	if err != nil {
		return err
	}
	d.out.Captured = append(d.out.Captured, captured)
	return nil
}

func (d *receiptDecoder) appendTypedEvent(abiEventName string, data map[string]any, pool RegisteredPool, site logSite) error {
	switch abiEventName {
	case "Swap":
		swap, err := d.buildSwap(data, pool, site)
		if err != nil {
			return err
		}
		d.out.Swaps = append(d.out.Swaps, swap)

	case "ModifyLiquidity":
		liq, err := d.buildLiquidityEvent(data, pool, site)
		if err != nil {
			return err
		}
		d.out.LiquidityEvents = append(d.out.LiquidityEvents, liq)

	default:
		ev, err := d.buildPoolEvent(abiEventName, data, pool, site)
		if err != nil {
			return err
		}
		d.out.PoolEvents = append(d.out.PoolEvents, ev)
	}
	return nil
}

func (d *receiptDecoder) buildSwap(data map[string]any, pool RegisteredPool, site logSite) (*entity.UniswapV4Swap, error) {
	fields, err := bigIntFields(data, "amount0", "amount1", "sqrtPriceX96", "liquidity", "tick", "fee")
	if err != nil {
		return nil, err
	}
	sender, err := shared.GetAddrField(data, "sender")
	if err != nil {
		return nil, err
	}
	tick, err := int24Value("tick", fields["tick"])
	if err != nil {
		return nil, err
	}
	fee, err := uint24Value("fee", fields["fee"])
	if err != nil {
		return nil, err
	}

	swap := &entity.UniswapV4Swap{
		PoolID:         pool.ID,
		BlockNumber:    d.blockNumber,
		BlockVersion:   d.version,
		BlockTimestamp: d.ts,
		TxHash:         site.txHash,
		LogIndex:       int(site.logIndex),
		Sender:         sender,
		Amount0:        fields["amount0"],
		Amount1:        fields["amount1"],
		SqrtPriceX96:   fields["sqrtPriceX96"],
		Liquidity:      fields["liquidity"],
		Tick:           tick,
		Fee:            fee,
	}
	if err := swap.Validate(); err != nil {
		return nil, fmt.Errorf("validating Swap: %w", err)
	}
	return swap, nil
}

func (d *receiptDecoder) buildLiquidityEvent(data map[string]any, pool RegisteredPool, site logSite) (*entity.UniswapV4LiquidityEvent, error) {
	key, err := modifyLiquidityKey(data)
	if err != nil {
		return nil, err
	}
	liquidityDelta, err := shared.GetBigIntField(data, "liquidityDelta")
	if err != nil {
		return nil, err
	}

	e := &entity.UniswapV4LiquidityEvent{
		PoolID:         pool.ID,
		BlockNumber:    d.blockNumber,
		BlockVersion:   d.version,
		BlockTimestamp: d.ts,
		TxHash:         site.txHash,
		LogIndex:       int(site.logIndex),
		Sender:         key.Owner,
		TickLower:      key.TickLower,
		TickUpper:      key.TickUpper,
		LiquidityDelta: liquidityDelta,
		Salt:           key.Salt,
	}
	if err := e.Validate(); err != nil {
		return nil, fmt.Errorf("validating ModifyLiquidity: %w", err)
	}
	return e, nil
}

func (d *receiptDecoder) buildPoolEvent(abiEventName string, data map[string]any, pool RegisteredPool, site logSite) (*entity.UniswapV4PoolEvent, error) {
	name, ok := poolEventNames[abiEventName]
	if !ok {
		return nil, fmt.Errorf("unhandled pool event %s", abiEventName)
	}
	params, err := dexconsumer.MarshalDecodedParams(data)
	if err != nil {
		return nil, fmt.Errorf("marshalling %s params: %w", abiEventName, err)
	}

	ev := &entity.UniswapV4PoolEvent{
		PoolID:         pool.ID,
		BlockNumber:    d.blockNumber,
		BlockVersion:   d.version,
		BlockTimestamp: d.ts,
		TxHash:         site.txHash,
		LogIndex:       int(site.logIndex),
		EventName:      name,
		Params:         params,
	}
	if err := ev.Validate(); err != nil {
		return nil, fmt.Errorf("validating %s: %w", abiEventName, err)
	}
	return ev, nil
}

func bigIntFields(data map[string]any, keys ...string) (map[string]*big.Int, error) {
	out := make(map[string]*big.Int, len(keys))
	for _, key := range keys {
		v, err := shared.GetBigIntField(data, key)
		if err != nil {
			return nil, err
		}
		out[key] = v
	}
	return out, nil
}

package uniswapv4indexer

import (
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/services/dexconsumer"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// eventsByID returns the PoolManager ABI's events indexed by topic0 for O(1)
// log dispatch, built exactly once alongside the once-parsed ABI: like the ABI
// itself, this map is immutable and on the per-receipt hot path.
var eventsByID = sync.OnceValues(func() (map[common.Hash]*abi.Event, error) {
	poolManagerABI, err := PoolManagerABI()
	if err != nil {
		return nil, err
	}
	out := make(map[common.Hash]*abi.Event, len(poolManagerABI.Events))
	for _, ev := range poolManagerABI.Events {
		e := ev
		out[e.ID] = &e
	}
	return out, nil
})

// poolKeyedEvents are the PoolManager events whose first indexed argument is
// the PoolId, so a registry lookup on topics[1] decides which pool they belong
// to.
var poolKeyedEvents = map[string]struct{}{
	"Initialize":         {},
	"ModifyLiquidity":    {},
	"Swap":               {},
	"Donate":             {},
	"ProtocolFeeUpdated": {},
}

// erc6909Events are the claim-token events the PoolManager inherits from
// ERC6909. They are keyed by currency id, not PoolId, and are emitted whenever
// anyone mints, burns or transfers a claim on ANY currency the singleton holds
// — high-volume traffic that cannot be attributed to a pool at all. Capturing
// them would fill protocol_event with rows no V4 query can join, so they are
// dropped rather than mirrored.
var erc6909Events = map[string]struct{}{
	"Transfer":    {},
	"Approval":    {},
	"OperatorSet": {},
}

// poolEventNames maps an ABI event name to its persisted UniswapV4PoolEventName.
var poolEventNames = map[string]entity.UniswapV4PoolEventName{
	"Initialize":         entity.UniswapV4PoolEventInitialize,
	"Donate":             entity.UniswapV4PoolEventDonate,
	"ProtocolFeeUpdated": entity.UniswapV4PoolEventProtocolFeeUpdated,
}

// DecodeEvents extracts typed entities from a single transaction receipt,
// routing each PoolManager log by its PoolId. Unlike the per-pool V3 decoder it
// runs ONCE per receipt: V4's emitter is a singleton shared by every pool, so
// scanning the receipt per registered pool would be O(pools × logs) for the
// same result. It returns the set of registered pool IDs the receipt touched,
// which the caller feeds to dexconsumer.DueSet.
//
// Capture-net design: a log on the PoolManager is mirrored into Captured
// whether or not topic0 matched a known event, EXCEPT for logs belonging to
// pools outside the registry (the singleton emits for thousands of untracked
// pools) and for the ERC6909 claim-token set (see erc6909Events).
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

// receiptDecoder carries the registry and block identity every log decode
// needs, so the per-log helpers below take the log alone instead of repeating a
// seven-argument tail.
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

// logSite is one log's identity within its block.
type logSite struct {
	address  common.Address
	logIndex uint
	txHash   common.Hash
}

// decodeLog routes one log: anything not emitted by the PoolManager is ignored,
// pool-keyed events become typed entities, the singleton's governance events
// are captured only, ERC6909 traffic is skipped, and an unrecognised topic0 is
// captured raw so protocol_event keeps a complete mirror.
func (d *receiptDecoder) decodeLog(log shared.Log) error {
	if !common.IsHexAddress(log.Address) {
		return fmt.Errorf("invalid log address %q", log.Address)
	}
	addr := common.HexToAddress(log.Address)
	if !shared.LogBelongsTo(addr, d.poolManager) {
		return nil
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

// knownEvent resolves a log's topic0 against the PoolManager ABI, returning nil
// for a zero-topic log or an unrecognised topic0.
func (d *receiptDecoder) knownEvent(log shared.Log) *abi.Event {
	if len(log.Topics) == 0 {
		return nil
	}
	return d.events[common.HexToHash(log.Topics[0])]
}

// decodePoolKeyedLog resolves the log's PoolId against the registry and, for a
// tracked pool, appends both the typed entity and its capture-net mirror. A log
// for an untracked pool is dropped entirely — the deliberate high-volume filter
// this indexer depends on, not a swallowed failure.
func (d *receiptDecoder) decodePoolKeyedLog(ev abi.Event, log shared.Log, site logSite) error {
	if len(log.Topics) < 2 {
		return fmt.Errorf("%s log (index %s) carries no indexed pool id", ev.Name, log.LogIndex)
	}
	pool, tracked := d.poolsByID[common.HexToHash(log.Topics[1])]
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

// decodeAndCapture ABI-decodes a known event and mirrors it into the capture
// net, returning the decoded fields for any typed entity built from them.
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

// captureRaw mirrors a log the ABI cannot decode, named by its topic0 (or the
// anonymous sentinel when it has none).
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

// appendTypedEvent decodes data into the entity matching abiEventName and
// appends it to the correct slice.
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

	case "Initialize", "Donate", "ProtocolFeeUpdated":
		ev, err := d.buildPoolEvent(abiEventName, data, pool, site)
		if err != nil {
			return err
		}
		d.out.PoolEvents = append(d.out.PoolEvents, ev)

	default:
		return fmt.Errorf("unhandled pool-keyed event %s", abiEventName)
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
		Tick:           int(fields["tick"].Int64()),
		Fee:            int(fields["fee"].Int64()),
	}
	if err := swap.Validate(); err != nil {
		return nil, fmt.Errorf("validating Swap: %w", err)
	}
	return swap, nil
}

func (d *receiptDecoder) buildLiquidityEvent(data map[string]any, pool RegisteredPool, site logSite) (*entity.UniswapV4LiquidityEvent, error) {
	fields, err := bigIntFields(data, "tickLower", "tickUpper", "liquidityDelta")
	if err != nil {
		return nil, err
	}
	sender, err := shared.GetAddrField(data, "sender")
	if err != nil {
		return nil, err
	}
	salt, err := getHashField(data, "salt")
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
		Sender:         sender,
		TickLower:      int(fields["tickLower"].Int64()),
		TickUpper:      int(fields["tickUpper"].Int64()),
		LiquidityDelta: fields["liquidityDelta"],
		Salt:           salt,
	}
	if err := e.Validate(); err != nil {
		return nil, fmt.Errorf("validating ModifyLiquidity: %w", err)
	}
	return e, nil
}

// buildPoolEvent decodes a low-frequency pool event (Initialize, Donate,
// ProtocolFeeUpdated) into a UniswapV4PoolEvent, JSON-marshalling its decoded
// named fields as Params rather than promoting them to columns.
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

// bigIntFields reads several *big.Int fields at once so each entity builder
// stays a field mapping instead of a run of identical error checks. Every
// v4-core numeric event argument (int24/uint24/int128/int256/uint128/uint160)
// decodes to *big.Int: go-ethereum only returns native Go integers for the
// 8/16/32/64-bit widths, none of which appear in these events.
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

// getHashField reads key from a DecodeLog result map as a common.Hash.
// go-ethereum decodes a bytes32 argument — indexed or not — into a [32]byte
// array rather than any named type.
func getHashField(data map[string]any, key string) (common.Hash, error) {
	v, ok := data[key]
	if !ok {
		return common.Hash{}, fmt.Errorf("missing field: %s", key)
	}
	b, ok := v.([32]byte)
	if !ok {
		return common.Hash{}, fmt.Errorf("field %s: unexpected type %T", key, v)
	}
	return common.Hash(b), nil
}

package uniswapv3indexer

import (
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/services/dexconsumer"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// eventsByID returns the pool ABI's events indexed by topic0 for O(1) log
// dispatch, built exactly once alongside the once-parsed PoolABI: like the ABI
// itself, this map is immutable and on the per-receipt hot path, so rebuilding
// it per matched receipt is pure waste.
var eventsByID = sync.OnceValues(func() (map[common.Hash]*abi.Event, error) {
	poolABI, err := PoolABI()
	if err != nil {
		return nil, err
	}
	out := make(map[common.Hash]*abi.Event, len(poolABI.Events))
	for _, ev := range poolABI.Events {
		e := ev
		out[e.ID] = &e
	}
	return out, nil
})

// DecodeEvents extracts typed entities from a single transaction receipt for
// one Uniswap V3 pool.
//
// Capture-net design: every log on the pool address is appended to Captured
// so protocol_event stays a complete mirror of the on-chain log surface,
// whether or not topic0 matched a known event. Captured is always a superset
// of Swaps, LiquidityEvents, and PoolEvents.
func DecodeEvents(
	receipt shared.TransactionReceipt,
	pool RegisteredPool,
	chainID, blockNumber int64,
	version int,
	ts time.Time,
) (DecodedEvents, error) {
	byID, err := eventsByID()
	if err != nil {
		return DecodedEvents{}, fmt.Errorf("loading pool ABI: %w", err)
	}

	var result DecodedEvents
	for _, log := range receipt.Logs {
		if !common.IsHexAddress(log.Address) {
			return DecodedEvents{}, fmt.Errorf("invalid log address %q", log.Address)
		}
		addr := common.HexToAddress(log.Address)
		if !shared.LogBelongsTo(addr, pool.Address) {
			continue
		}

		logIndex, err := shared.ParseHexUint(log.LogIndex)
		if err != nil {
			return DecodedEvents{}, fmt.Errorf("parsing log index %q: %w", log.LogIndex, err)
		}
		txHash := common.HexToHash(log.TransactionHash)

		if len(log.Topics) == 0 {
			captured, err := dexconsumer.NewRawCapturedLog(addr, logIndex, txHash, dexconsumer.AnonymousLogEventName, log)
			if err != nil {
				return DecodedEvents{}, err
			}
			result.Captured = append(result.Captured, captured)
			continue
		}

		topic0 := common.HexToHash(log.Topics[0])
		ev, known := byID[topic0]
		if !known {
			captured, err := dexconsumer.NewRawCapturedLog(addr, logIndex, txHash, topic0.Hex(), log)
			if err != nil {
				return DecodedEvents{}, err
			}
			result.Captured = append(result.Captured, captured)
			continue
		}

		eventData, err := shared.DecodeLog(*ev, log)
		if err != nil {
			return DecodedEvents{}, fmt.Errorf("decoding %s log (index %s): %w", ev.Name, log.LogIndex, err)
		}

		if err := appendTypedEvent(&result, ev.Name, eventData, pool, blockNumber, version, ts, txHash, logIndex); err != nil {
			return DecodedEvents{}, fmt.Errorf("extracting %s: %w", ev.Name, err)
		}

		captured, err := dexconsumer.NewDecodedCapturedLog(addr, logIndex, txHash, ev.Name, eventData)
		if err != nil {
			return DecodedEvents{}, err
		}
		result.Captured = append(result.Captured, captured)
	}

	return result, nil
}

// appendTypedEvent decodes eventData into the entity matching abiEventName and
// appends it to the correct slice on result.
func appendTypedEvent(
	result *DecodedEvents,
	abiEventName string,
	data map[string]any,
	pool RegisteredPool,
	blockNumber int64,
	version int,
	ts time.Time,
	txHash common.Hash,
	logIndex uint,
) error {
	switch abiEventName {
	case "Swap":
		swap, err := buildSwap(data, pool, blockNumber, version, ts, txHash, logIndex)
		if err != nil {
			return err
		}
		result.Swaps = append(result.Swaps, swap)

	case "Mint", "Burn", "Collect":
		liq, err := buildLiquidityEvent(abiEventName, data, pool, blockNumber, version, ts, txHash, logIndex)
		if err != nil {
			return err
		}
		result.LiquidityEvents = append(result.LiquidityEvents, liq)

	case "Initialize", "Flash", "SetFeeProtocol", "CollectProtocol", "IncreaseObservationCardinalityNext":
		ev, err := buildPoolEvent(abiEventName, data, pool, blockNumber, version, ts, txHash, logIndex)
		if err != nil {
			return err
		}
		result.PoolEvents = append(result.PoolEvents, ev)

	default:
		return fmt.Errorf("unhandled pool event %s", abiEventName)
	}
	return nil
}

func buildSwap(
	data map[string]any,
	pool RegisteredPool,
	blockNumber int64,
	version int,
	ts time.Time,
	txHash common.Hash,
	logIndex uint,
) (*entity.UniswapV3Swap, error) {
	sender, err := shared.GetAddrField(data, "sender")
	if err != nil {
		return nil, err
	}
	recipient, err := shared.GetAddrField(data, "recipient")
	if err != nil {
		return nil, err
	}
	amount0, err := shared.GetBigIntField(data, "amount0")
	if err != nil {
		return nil, err
	}
	amount1, err := shared.GetBigIntField(data, "amount1")
	if err != nil {
		return nil, err
	}
	sqrtPriceX96, err := shared.GetBigIntField(data, "sqrtPriceX96")
	if err != nil {
		return nil, err
	}
	liquidity, err := shared.GetBigIntField(data, "liquidity")
	if err != nil {
		return nil, err
	}
	tick, err := shared.GetBigIntField(data, "tick")
	if err != nil {
		return nil, err
	}

	swap := &entity.UniswapV3Swap{
		PoolID:         pool.ID,
		BlockNumber:    blockNumber,
		BlockVersion:   version,
		BlockTimestamp: ts,
		TxHash:         txHash,
		LogIndex:       int(logIndex),
		Sender:         sender,
		Recipient:      recipient,
		Amount0:        amount0,
		Amount1:        amount1,
		SqrtPriceX96:   sqrtPriceX96,
		Liquidity:      liquidity,
		Tick:           int(tick.Int64()),
	}
	if err := swap.Validate(); err != nil {
		return nil, fmt.Errorf("validating Swap: %w", err)
	}
	return swap, nil
}

// buildLiquidityEvent decodes Mint, Burn, or Collect into a
// UniswapV3LiquidityEvent. Field presence (Sender/Recipient/Amount) mirrors
// the v3-core event shapes documented on entity.UniswapV3LiquidityEvent.Validate.
func buildLiquidityEvent(
	abiEventName string,
	data map[string]any,
	pool RegisteredPool,
	blockNumber int64,
	version int,
	ts time.Time,
	txHash common.Hash,
	logIndex uint,
) (*entity.UniswapV3LiquidityEvent, error) {
	owner, err := shared.GetAddrField(data, "owner")
	if err != nil {
		return nil, err
	}
	tickLower, err := shared.GetBigIntField(data, "tickLower")
	if err != nil {
		return nil, err
	}
	tickUpper, err := shared.GetBigIntField(data, "tickUpper")
	if err != nil {
		return nil, err
	}
	amount0, err := shared.GetBigIntField(data, "amount0")
	if err != nil {
		return nil, err
	}
	amount1, err := shared.GetBigIntField(data, "amount1")
	if err != nil {
		return nil, err
	}

	e := &entity.UniswapV3LiquidityEvent{
		PoolID:         pool.ID,
		BlockNumber:    blockNumber,
		BlockVersion:   version,
		BlockTimestamp: ts,
		TxHash:         txHash,
		LogIndex:       int(logIndex),
		Owner:          owner,
		TickLower:      int(tickLower.Int64()),
		TickUpper:      int(tickUpper.Int64()),
		Amount0:        amount0,
		Amount1:        amount1,
	}

	switch abiEventName {
	case "Mint":
		sender, err := shared.GetAddrField(data, "sender")
		if err != nil {
			return nil, err
		}
		amount, err := shared.GetBigIntField(data, "amount")
		if err != nil {
			return nil, err
		}
		e.EventName = entity.LiquidityEventMint
		e.Sender = &sender
		e.Amount = amount

	case "Burn":
		amount, err := shared.GetBigIntField(data, "amount")
		if err != nil {
			return nil, err
		}
		e.EventName = entity.LiquidityEventBurn
		e.Amount = amount

	case "Collect":
		recipient, err := shared.GetAddrField(data, "recipient")
		if err != nil {
			return nil, err
		}
		e.EventName = entity.LiquidityEventCollect
		e.Recipient = &recipient

	default:
		return nil, fmt.Errorf("unhandled liquidity event %s", abiEventName)
	}

	if err := e.Validate(); err != nil {
		return nil, fmt.Errorf("validating %s: %w", abiEventName, err)
	}
	return e, nil
}

// poolEventNames maps an ABI event name to its persisted PoolEventName.
var poolEventNames = map[string]entity.PoolEventName{
	"Initialize":                         entity.PoolEventInitialize,
	"Flash":                              entity.PoolEventFlash,
	"SetFeeProtocol":                     entity.PoolEventSetFeeProtocol,
	"CollectProtocol":                    entity.PoolEventCollectProtocol,
	"IncreaseObservationCardinalityNext": entity.PoolEventIncreaseObservationCardinalityNext,
}

// buildPoolEvent decodes a typed low-frequency pool event (Initialize, Flash,
// SetFeeProtocol, CollectProtocol, IncreaseObservationCardinalityNext) into a
// UniswapV3PoolEvent, JSON-marshalling its decoded named fields as Params.
func buildPoolEvent(
	abiEventName string,
	data map[string]any,
	pool RegisteredPool,
	blockNumber int64,
	version int,
	ts time.Time,
	txHash common.Hash,
	logIndex uint,
) (*entity.UniswapV3PoolEvent, error) {
	name, ok := poolEventNames[abiEventName]
	if !ok {
		return nil, fmt.Errorf("unhandled pool event %s", abiEventName)
	}

	params, err := dexconsumer.MarshalDecodedParams(data)
	if err != nil {
		return nil, fmt.Errorf("marshalling %s params: %w", abiEventName, err)
	}

	ev := &entity.UniswapV3PoolEvent{
		PoolID:         pool.ID,
		BlockNumber:    blockNumber,
		BlockVersion:   version,
		BlockTimestamp: ts,
		TxHash:         txHash,
		LogIndex:       int(logIndex),
		EventName:      name,
		Params:         params,
	}
	if err := ev.Validate(); err != nil {
		return nil, fmt.Errorf("validating %s: %w", abiEventName, err)
	}
	return ev, nil
}

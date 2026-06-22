package curveindexer

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// StableswapHandler decodes events from Curve stableswap pools (both pre-NG and NG).
// It implements PoolClassHandler for KindStableswapPreNG and KindStableswapNG.
type StableswapHandler struct {
	stableABI  *abi.ABI
	eventsByID map[common.Hash]*abi.Event
}

// NewStableswapHandler constructs a StableswapHandler with a pre-parsed ABI.
func NewStableswapHandler(stableABI *abi.ABI) *StableswapHandler {
	eventsByID := make(map[common.Hash]*abi.Event, len(stableABI.Events))
	for _, ev := range stableABI.Events {
		ev := ev
		eventsByID[ev.ID] = &ev
	}
	return &StableswapHandler{
		stableABI:  stableABI,
		eventsByID: eventsByID,
	}
}

// Handles returns true for stableswap pool kinds (both pre-NG and NG).
func (h *StableswapHandler) Handles(kind PoolKind) bool {
	return kind == KindStableswapPreNG || kind == KindStableswapNG
}

// DecodeEvents extracts typed records from a single transaction receipt.
//
// Capture-net design: every log on the pool address is also appended as a
// CapturedEvent so protocol_event is a complete mirror of the on-chain log
// surface. Typed events carry a JSON payload of their decoded fields; unknown
// topic0 events carry a JSON payload of {topics, data}. This means Captured
// is always a superset of Swaps and Liquidity.
func (h *StableswapHandler) DecodeEvents(
	receipt shared.TransactionReceipt,
	pool RegisteredPool,
	chainID, blockNumber int64,
	version int,
	ts time.Time,
) (DecodedEvents, error) {
	var result DecodedEvents

	for _, log := range receipt.Logs {
		addr := common.HexToAddress(log.Address)
		if addr != pool.Address {
			continue
		}

		if len(log.Topics) == 0 {
			continue
		}

		logIndex, err := parseHexUint(log.LogIndex)
		if err != nil {
			return DecodedEvents{}, fmt.Errorf("parsing log index %q: %w", log.LogIndex, err)
		}
		txHash := common.HexToHash(log.TransactionHash)

		topic0 := common.HexToHash(log.Topics[0])
		ev, known := h.eventsByID[topic0]

		if !known {
			payload, _ := json.Marshal(map[string]any{
				"topics": log.Topics,
				"data":   log.Data,
			})
			result.Captured = append(result.Captured, CapturedEvent{
				Pool:      pool,
				LogIndex:  logIndex,
				TxHash:    txHash,
				EventName: log.Topics[0],
				Payload:   payload,
			})
			continue
		}

		eventData, err := decodeLog(ev, log)
		if err != nil {
			return DecodedEvents{}, fmt.Errorf("decoding %s log (index %s): %w", ev.Name, log.LogIndex, err)
		}

		switch ev.Name {
		case "TokenExchange":
			swap, err := extractStableswapTokenExchange(eventData, pool, logIndex, txHash, log)
			if err != nil {
				return DecodedEvents{}, fmt.Errorf("extracting TokenExchange: %w", err)
			}
			result.Swaps = append(result.Swaps, swap)

		case "AddLiquidity":
			liq, err := extractStableswapAddLiquidity(eventData, pool, logIndex, txHash)
			if err != nil {
				return DecodedEvents{}, fmt.Errorf("extracting AddLiquidity: %w", err)
			}
			result.Liquidity = append(result.Liquidity, liq)

		case "RemoveLiquidity":
			liq, err := extractStableswapRemoveLiquidity(eventData, pool, logIndex, txHash)
			if err != nil {
				return DecodedEvents{}, fmt.Errorf("extracting RemoveLiquidity: %w", err)
			}
			result.Liquidity = append(result.Liquidity, liq)

		case "RemoveLiquidityOne":
			liq, err := extractStableswapRemoveLiquidityOne(eventData, pool, logIndex, txHash)
			if err != nil {
				return DecodedEvents{}, fmt.Errorf("extracting RemoveLiquidityOne: %w", err)
			}
			result.Liquidity = append(result.Liquidity, liq)

		case "RemoveLiquidityImbalance":
			liq, err := extractStableswapRemoveLiquidityImbalance(eventData, pool, logIndex, txHash)
			if err != nil {
				return DecodedEvents{}, fmt.Errorf("extracting RemoveLiquidityImbalance: %w", err)
			}
			result.Liquidity = append(result.Liquidity, liq)
		}

		// Capture net: all known pool-address logs are also stored in Captured
		// so protocol_event is a full mirror of on-chain activity.
		payload, _ := json.Marshal(eventData)
		result.Captured = append(result.Captured, CapturedEvent{
			Pool:      pool,
			LogIndex:  logIndex,
			TxHash:    txHash,
			EventName: ev.Name,
			Payload:   payload,
		})
	}

	return result, nil
}

// SnapshotState is a stub; full implementation is in Task 9.
func (h *StableswapHandler) SnapshotState(
	_ context.Context,
	_ outbound.Multicaller,
	pool RegisteredPool,
	blockNumber int64,
	version int,
	ts time.Time,
) (StateSnapshot, error) {
	return StateSnapshot{}, nil
}

// ---------------------------------------------------------------------------
// Typed event extractors
// ---------------------------------------------------------------------------

func extractStableswapTokenExchange(
	data map[string]any,
	pool RegisteredPool,
	logIndex uint,
	txHash common.Hash,
	log shared.Log,
) (SwapRecord, error) {
	// buyer is indexed; it was decoded from Topics[1] into data["buyer"].
	buyer, err := getAddrField(data, "buyer")
	if err != nil {
		return SwapRecord{}, err
	}
	soldID, err := getBigIntField(data, "sold_id")
	if err != nil {
		return SwapRecord{}, err
	}
	tokensSold, err := getBigIntField(data, "tokens_sold")
	if err != nil {
		return SwapRecord{}, err
	}
	boughtID, err := getBigIntField(data, "bought_id")
	if err != nil {
		return SwapRecord{}, err
	}
	tokensBought, err := getBigIntField(data, "tokens_bought")
	if err != nil {
		return SwapRecord{}, err
	}

	return SwapRecord{
		Pool:         pool,
		LogIndex:     logIndex,
		TxHash:       txHash,
		Buyer:        buyer,
		SoldID:       int(soldID.Int64()),
		BoughtID:     int(boughtID.Int64()),
		TokensSold:   tokensSold,
		TokensBought: tokensBought,
		Fee:          nil, // stableswap TokenExchange carries no fee field
	}, nil
}

func extractStableswapAddLiquidity(
	data map[string]any,
	pool RegisteredPool,
	logIndex uint,
	txHash common.Hash,
) (LiquidityRecord, error) {
	provider, err := getAddrField(data, "provider")
	if err != nil {
		return LiquidityRecord{}, err
	}
	amounts, err := getBigIntSliceField(data, "token_amounts")
	if err != nil {
		return LiquidityRecord{}, err
	}
	fees, err := getBigIntSliceField(data, "fees")
	if err != nil {
		return LiquidityRecord{}, err
	}
	invariant, err := getBigIntField(data, "invariant")
	if err != nil {
		return LiquidityRecord{}, err
	}
	supply, err := getBigIntField(data, "token_supply")
	if err != nil {
		return LiquidityRecord{}, err
	}
	return LiquidityRecord{
		Pool:         pool,
		LogIndex:     logIndex,
		TxHash:       txHash,
		Provider:     provider,
		Kind:         LiquidityAdd,
		TokenAmounts: amounts,
		Fees:         fees,
		Invariant:    invariant,
		TokenSupply:  supply,
	}, nil
}

func extractStableswapRemoveLiquidity(
	data map[string]any,
	pool RegisteredPool,
	logIndex uint,
	txHash common.Hash,
) (LiquidityRecord, error) {
	provider, err := getAddrField(data, "provider")
	if err != nil {
		return LiquidityRecord{}, err
	}
	amounts, err := getBigIntSliceField(data, "token_amounts")
	if err != nil {
		return LiquidityRecord{}, err
	}
	fees, err := getBigIntSliceField(data, "fees")
	if err != nil {
		return LiquidityRecord{}, err
	}
	supply, err := getBigIntField(data, "token_supply")
	if err != nil {
		return LiquidityRecord{}, err
	}
	return LiquidityRecord{
		Pool:         pool,
		LogIndex:     logIndex,
		TxHash:       txHash,
		Provider:     provider,
		Kind:         LiquidityRemove,
		TokenAmounts: amounts,
		Fees:         fees,
		TokenSupply:  supply,
	}, nil
}

func extractStableswapRemoveLiquidityOne(
	data map[string]any,
	pool RegisteredPool,
	logIndex uint,
	txHash common.Hash,
) (LiquidityRecord, error) {
	provider, err := getAddrField(data, "provider")
	if err != nil {
		return LiquidityRecord{}, err
	}
	tokenAmount, err := getBigIntField(data, "token_amount")
	if err != nil {
		return LiquidityRecord{}, err
	}
	coinAmount, err := getBigIntField(data, "coin_amount")
	if err != nil {
		return LiquidityRecord{}, err
	}
	supply, err := getBigIntField(data, "token_supply")
	if err != nil {
		return LiquidityRecord{}, err
	}
	return LiquidityRecord{
		Pool:         pool,
		LogIndex:     logIndex,
		TxHash:       txHash,
		Provider:     provider,
		Kind:         LiquidityRemoveOne,
		TokenAmounts: []*big.Int{tokenAmount, coinAmount},
		TokenSupply:  supply,
	}, nil
}

func extractStableswapRemoveLiquidityImbalance(
	data map[string]any,
	pool RegisteredPool,
	logIndex uint,
	txHash common.Hash,
) (LiquidityRecord, error) {
	provider, err := getAddrField(data, "provider")
	if err != nil {
		return LiquidityRecord{}, err
	}
	amounts, err := getBigIntSliceField(data, "token_amounts")
	if err != nil {
		return LiquidityRecord{}, err
	}
	fees, err := getBigIntSliceField(data, "fees")
	if err != nil {
		return LiquidityRecord{}, err
	}
	invariant, err := getBigIntField(data, "invariant")
	if err != nil {
		return LiquidityRecord{}, err
	}
	supply, err := getBigIntField(data, "token_supply")
	if err != nil {
		return LiquidityRecord{}, err
	}
	return LiquidityRecord{
		Pool:         pool,
		LogIndex:     logIndex,
		TxHash:       txHash,
		Provider:     provider,
		Kind:         LiquidityRemoveImbalance,
		TokenAmounts: amounts,
		Fees:         fees,
		Invariant:    invariant,
		TokenSupply:  supply,
	}, nil
}

// ---------------------------------------------------------------------------
// ABI decode helpers (mirrors morpho_indexer pattern)
// ---------------------------------------------------------------------------

// decodeLog extracts both indexed (from topics) and non-indexed (from data) fields
// into a flat map, following the morpho_indexer parseTopics/parseData pattern.
func decodeLog(ev *abi.Event, log shared.Log) (map[string]any, error) {
	out := make(map[string]any)

	var indexed abi.Arguments
	for _, arg := range ev.Inputs {
		if arg.Indexed {
			indexed = append(indexed, arg)
		}
	}
	if len(indexed) > 0 {
		hashes := make([]common.Hash, 0, len(log.Topics)-1)
		for i := 1; i < len(log.Topics); i++ {
			hashes = append(hashes, common.HexToHash(log.Topics[i]))
		}
		if err := abi.ParseTopicsIntoMap(out, indexed, hashes); err != nil {
			return nil, fmt.Errorf("parsing indexed params: %w", err)
		}
	}

	var nonIndexed abi.Arguments
	for _, arg := range ev.Inputs {
		if !arg.Indexed {
			nonIndexed = append(nonIndexed, arg)
		}
	}
	if len(nonIndexed) > 0 && len(log.Data) > 2 {
		raw := common.FromHex(log.Data)
		if err := nonIndexed.UnpackIntoMap(out, raw); err != nil {
			return nil, fmt.Errorf("parsing non-indexed params: %w", err)
		}
	}

	return out, nil
}

func getAddrField(data map[string]any, key string) (common.Address, error) {
	v, ok := data[key]
	if !ok {
		return common.Address{}, fmt.Errorf("missing field: %s", key)
	}
	addr, ok := v.(common.Address)
	if !ok {
		return common.Address{}, fmt.Errorf("field %s: unexpected type %T", key, v)
	}
	return addr, nil
}

func getBigIntField(data map[string]any, key string) (*big.Int, error) {
	v, ok := data[key]
	if !ok {
		return nil, fmt.Errorf("missing field: %s", key)
	}
	b, ok := v.(*big.Int)
	if !ok {
		return nil, fmt.Errorf("field %s: unexpected type %T", key, v)
	}
	return b, nil
}

func getBigIntSliceField(data map[string]any, key string) ([]*big.Int, error) {
	v, ok := data[key]
	if !ok {
		return nil, fmt.Errorf("missing field: %s", key)
	}
	slice, ok := v.([]*big.Int)
	if !ok {
		return nil, fmt.Errorf("field %s: unexpected type %T", key, v)
	}
	return slice, nil
}

// parseHexUint parses a 0x-prefixed hex string into a uint, as used in
// shared.Log.LogIndex.
func parseHexUint(s string) (uint, error) {
	s = strings.TrimPrefix(s, "0x")
	if s == "" || s == "0" {
		return 0, nil
	}
	n, err := strconv.ParseUint(s, 16, 64)
	if err != nil {
		return 0, err
	}
	return uint(n), nil
}

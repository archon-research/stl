package curveindexer

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
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
		if !common.IsHexAddress(log.Address) {
			return DecodedEvents{}, fmt.Errorf("invalid log address %q", log.Address)
		}
		addr := common.HexToAddress(log.Address)
		if addr != pool.Address {
			continue
		}

		logIndex, err := parseHexUint(log.LogIndex)
		if err != nil {
			return DecodedEvents{}, fmt.Errorf("parsing log index %q: %w", log.LogIndex, err)
		}
		txHash := common.HexToHash(log.TransactionHash)

		if len(log.Topics) == 0 {
			payload, _ := json.Marshal(map[string]any{"topics": log.Topics, "data": log.Data})
			result.Captured = append(result.Captured, CapturedEvent{
				Pool:      pool,
				LogIndex:  logIndex,
				TxHash:    txHash,
				EventName: "",
				Payload:   payload,
			})
			continue
		}

		topic0 := common.HexToHash(log.Topics[0])

		// Pre-NG pools emit fixed-array liquidity events whose topic0s differ from the
		// NG ABI. Dispatch them via word-slicing before falling back to the ABI lookup.
		if pool.Kind == KindStableswapPreNG {
			rec, matched, err := decodeClassicLiquidity(log, pool)
			if err != nil {
				return DecodedEvents{}, fmt.Errorf("decoding classic liquidity log (index %s): %w", log.LogIndex, err)
			}
			if matched {
				result.Liquidity = append(result.Liquidity, *rec)
				payload, _ := json.Marshal(map[string]any{"topics": log.Topics, "data": log.Data})
				result.Captured = append(result.Captured, CapturedEvent{
					Pool:      pool,
					LogIndex:  logIndex,
					TxHash:    txHash,
					EventName: log.Topics[0],
					Payload:   payload,
				})
				continue
			}
		}

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
			swap, err := extractStableswapTokenExchange(eventData, pool, logIndex, txHash)
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
		payload, err := json.Marshal(eventData)
		if err != nil {
			return DecodedEvents{}, fmt.Errorf("marshalling %s capture payload: %w", ev.Name, err)
		}
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

// SnapshotState reads stableswap pool state at the given block via multicall.
// Call order is deterministic; results are decoded in the same order.
// Required calls (AllowFailure=false) that revert propagate as errors (transient-retry contract).
// NG-only calls (price_oracle, last_price) are AllowFailure=true and only issued for KindStableswapNG.
func (h *StableswapHandler) SnapshotState(
	ctx context.Context,
	mc outbound.Multicaller,
	pool RegisteredPool,
	blockNumber int64,
	version int,
	ts time.Time,
) (StateSnapshot, error) {
	calls, err := h.buildSnapshotCalls(pool)
	if err != nil {
		return StateSnapshot{}, fmt.Errorf("building snapshot calls for pool %s: %w", pool.Address, err)
	}

	results, err := mc.Execute(ctx, calls, big.NewInt(blockNumber))
	if err != nil {
		return StateSnapshot{}, fmt.Errorf("executing snapshot multicall for pool %s: %w", pool.Address, err)
	}

	st, err := h.decodeSnapshotResults(pool, blockNumber, version, ts, calls, results)
	if err != nil {
		return StateSnapshot{}, fmt.Errorf("decoding snapshot results for pool %s: %w", pool.Address, err)
	}

	return StateSnapshot{
		Pool:         pool,
		BlockNumber:  blockNumber,
		BlockVersion: version,
		Timestamp:    ts,
		Stableswap:   st,
	}, nil
}

// buildSnapshotCalls constructs the ordered multicall call list for a stableswap pool.
// Call order:
//  1. balances(i) for i in 0..n-1
//  2. get_virtual_price()
//  3. totalSupply()
//  4. A()
//  5. fee()
//  6. get_dy(i,j,10^CoinDecimals[i]) for every ordered pair i!=j, i asc then j asc
//  7. (NG only) price_oracle(), last_price() with AllowFailure=true
func (h *StableswapHandler) buildSnapshotCalls(pool RegisteredPool) ([]outbound.Call, error) {
	var calls []outbound.Call

	// 1. balances(i) for each coin
	for i := 0; i < pool.NCoins; i++ {
		data, err := h.stableABI.Pack("balances", big.NewInt(int64(i)))
		if err != nil {
			return nil, fmt.Errorf("packing balances(%d): %w", i, err)
		}
		calls = append(calls, outbound.Call{Target: pool.Address, AllowFailure: false, CallData: data})
	}

	// 2. get_virtual_price()
	data, err := h.stableABI.Pack("get_virtual_price")
	if err != nil {
		return nil, fmt.Errorf("packing get_virtual_price: %w", err)
	}
	calls = append(calls, outbound.Call{Target: pool.Address, AllowFailure: false, CallData: data})

	// 3. totalSupply()
	data, err = h.stableABI.Pack("totalSupply")
	if err != nil {
		return nil, fmt.Errorf("packing totalSupply: %w", err)
	}
	calls = append(calls, outbound.Call{Target: pool.Address, AllowFailure: false, CallData: data})

	// 4. A()
	data, err = h.stableABI.Pack("A")
	if err != nil {
		return nil, fmt.Errorf("packing A: %w", err)
	}
	calls = append(calls, outbound.Call{Target: pool.Address, AllowFailure: false, CallData: data})

	// 5. fee()
	data, err = h.stableABI.Pack("fee")
	if err != nil {
		return nil, fmt.Errorf("packing fee: %w", err)
	}
	calls = append(calls, outbound.Call{Target: pool.Address, AllowFailure: false, CallData: data})

	// 6. get_dy(i, j, 10^decimals[i]) for every ordered pair i!=j
	for i := 0; i < pool.NCoins; i++ {
		for j := 0; j < pool.NCoins; j++ {
			if i == j {
				continue
			}
			if i >= len(pool.CoinDecimals) {
				return nil, fmt.Errorf("pool %s coin %d missing decimals (have %d, n_coins %d)", pool.Address, i, len(pool.CoinDecimals), pool.NCoins)
			}
			dx := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(pool.CoinDecimals[i])), nil)
			data, err = h.stableABI.Pack("get_dy", big.NewInt(int64(i)), big.NewInt(int64(j)), dx)
			if err != nil {
				return nil, fmt.Errorf("packing get_dy(%d,%d): %w", i, j, err)
			}
			calls = append(calls, outbound.Call{Target: pool.Address, AllowFailure: false, CallData: data})
		}
	}

	// 7. NG-only: price_oracle() and last_price() with AllowFailure=true
	if pool.Kind == KindStableswapNG {
		data, err = h.stableABI.Pack("price_oracle")
		if err != nil {
			return nil, fmt.Errorf("packing price_oracle: %w", err)
		}
		calls = append(calls, outbound.Call{Target: pool.Address, AllowFailure: true, CallData: data})

		data, err = h.stableABI.Pack("last_price")
		if err != nil {
			return nil, fmt.Errorf("packing last_price: %w", err)
		}
		calls = append(calls, outbound.Call{Target: pool.Address, AllowFailure: true, CallData: data})
	}

	return calls, nil
}

// decodeSnapshotResults decodes the multicall results in the same order as buildSnapshotCalls.
func (h *StableswapHandler) decodeSnapshotResults(
	pool RegisteredPool,
	blockNumber int64,
	version int,
	ts time.Time,
	calls []outbound.Call,
	results []outbound.Result,
) (*entity.CurveStableswapState, error) {
	if len(results) != len(calls) {
		return nil, fmt.Errorf("multicall returned %d results, expected %d", len(results), len(calls))
	}

	idx := 0

	// 1. balances
	balances := make([]*big.Int, pool.NCoins)
	for i := 0; i < pool.NCoins; i++ {
		v, err := shared.UnpackUint(h.stableABI, "balances", results[idx])
		if err != nil {
			return nil, fmt.Errorf("balances(%d): %w", i, err)
		}
		balances[i] = v
		idx++
	}

	// 2. get_virtual_price
	virtualPrice, err := shared.UnpackUint(h.stableABI, "get_virtual_price", results[idx])
	if err != nil {
		return nil, fmt.Errorf("get_virtual_price: %w", err)
	}
	idx++

	// 3. totalSupply
	totalSupply, err := shared.UnpackUint(h.stableABI, "totalSupply", results[idx])
	if err != nil {
		return nil, fmt.Errorf("totalSupply: %w", err)
	}
	idx++

	// 4. A
	a, err := shared.UnpackUint(h.stableABI, "A", results[idx])
	if err != nil {
		return nil, fmt.Errorf("decoding A: %w", err)
	}
	idx++

	// 5. fee
	fee, err := shared.UnpackUint(h.stableABI, "fee", results[idx])
	if err != nil {
		return nil, fmt.Errorf("fee: %w", err)
	}
	idx++

	// 6. get_dy for each ordered pair
	nPairs := pool.NCoins * (pool.NCoins - 1)
	spotDy := make([]*big.Int, 0, nPairs)
	for i := 0; i < pool.NCoins; i++ {
		for j := 0; j < pool.NCoins; j++ {
			if i == j {
				continue
			}
			v, err := shared.UnpackUint(h.stableABI, "get_dy", results[idx])
			if err != nil {
				return nil, fmt.Errorf("get_dy(%d,%d): %w", i, j, err)
			}
			spotDy = append(spotDy, v)
			idx++
		}
	}

	// 7. NG-only: price_oracle and last_price
	var priceOracle, lastPrice *big.Int
	if pool.Kind == KindStableswapNG {
		if results[idx].Success {
			po, err := shared.UnpackUint(h.stableABI, "price_oracle", results[idx])
			if err != nil {
				return nil, fmt.Errorf("price_oracle: %w", err)
			}
			priceOracle = po
		}
		idx++
		if results[idx].Success {
			lp, err := shared.UnpackUint(h.stableABI, "last_price", results[idx])
			if err != nil {
				return nil, fmt.Errorf("last_price: %w", err)
			}
			lastPrice = lp
		}
	}

	return entity.NewCurveStableswapState(entity.CurveStableswapStateParams{
		CurvePoolID:  pool.ID,
		BlockNumber:  blockNumber,
		BlockVersion: version,
		Timestamp:    ts,
		Balances:     balances,
		VirtualPrice: virtualPrice,
		TotalSupply:  totalSupply,
		A:            a,
		Fee:          fee,
		SpotDy:       spotDy,
		LastPrice:    lastPrice,
		PriceOracle:  priceOracle,
	})
}

// ---------------------------------------------------------------------------
// Typed event extractors
// ---------------------------------------------------------------------------

func extractStableswapTokenExchange(
	data map[string]any,
	pool RegisteredPool,
	logIndex uint,
	txHash common.Hash,
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
	// token_id is int128 in the NG ABI; go-ethereum decodes it as *big.Int.
	tokenID, err := getBigIntField(data, "token_id")
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
	coinIdx := int(tokenID.Int64())
	return LiquidityRecord{
		Pool:         pool,
		LogIndex:     logIndex,
		TxHash:       txHash,
		Provider:     provider,
		Kind:         LiquidityRemoveOne,
		TokenAmounts: []*big.Int{tokenAmount, coinAmount},
		CoinIndex:    &coinIdx,
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

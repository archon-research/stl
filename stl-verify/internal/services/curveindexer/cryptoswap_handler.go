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

// CryptoswapHandler decodes events from Curve cryptoswap pools (twocrypto and tricrypto NG).
// It implements PoolClassHandler for KindCryptoswap only.
type CryptoswapHandler struct {
	cryptoABI  *abi.ABI
	eventsByID map[common.Hash]*abi.Event
}

// NewCryptoswapHandler constructs a CryptoswapHandler with a pre-parsed ABI.
func NewCryptoswapHandler(cryptoABI *abi.ABI) *CryptoswapHandler {
	eventsByID := make(map[common.Hash]*abi.Event, len(cryptoABI.Events))
	for _, ev := range cryptoABI.Events {
		ev := ev
		eventsByID[ev.ID] = &ev
	}
	return &CryptoswapHandler{
		cryptoABI:  cryptoABI,
		eventsByID: eventsByID,
	}
}

// Handles returns true only for cryptoswap pool kind.
func (h *CryptoswapHandler) Handles(kind PoolKind) bool {
	return kind == KindCryptoswap
}

// DecodeEvents extracts typed records from a single transaction receipt.
//
// Capture-net design: every log on the pool address is also appended as a
// CapturedEvent so protocol_event is a complete mirror of the on-chain log
// surface. Typed events carry a JSON payload of their decoded fields; unknown
// topic0 events carry a JSON payload of {topics, data}. Captured is always a
// superset of Swaps and Liquidity.
func (h *CryptoswapHandler) DecodeEvents(
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
			swap, err := extractCryptoswapTokenExchange(eventData, pool, logIndex, txHash)
			if err != nil {
				return DecodedEvents{}, fmt.Errorf("extracting TokenExchange: %w", err)
			}
			result.Swaps = append(result.Swaps, swap)

		case "AddLiquidity":
			liq, err := extractCryptoswapAddLiquidity(eventData, pool, logIndex, txHash)
			if err != nil {
				return DecodedEvents{}, fmt.Errorf("extracting AddLiquidity: %w", err)
			}
			result.Liquidity = append(result.Liquidity, liq)

		case "RemoveLiquidity":
			liq, err := extractCryptoswapRemoveLiquidity(eventData, pool, logIndex, txHash)
			if err != nil {
				return DecodedEvents{}, fmt.Errorf("extracting RemoveLiquidity: %w", err)
			}
			result.Liquidity = append(result.Liquidity, liq)

		case "RemoveLiquidityOne":
			liq, err := extractCryptoswapRemoveLiquidityOne(eventData, pool, logIndex, txHash)
			if err != nil {
				return DecodedEvents{}, fmt.Errorf("extracting RemoveLiquidityOne: %w", err)
			}
			result.Liquidity = append(result.Liquidity, liq)
		}

		// Capture net: all known pool-address logs are also stored in Captured.
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

// SnapshotState reads cryptoswap pool state at the given block via multicall.
// Call order is deterministic; results are decoded in the same order.
// Required calls (AllowFailure=false) that revert propagate as errors.
// D() and xcp_profit() are AllowFailure=true: a revert maps to nil, not an error.
func (h *CryptoswapHandler) SnapshotState(
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
		Cryptoswap:   st,
	}, nil
}

// buildSnapshotCalls constructs the ordered multicall call list for a cryptoswap pool.
// Call order:
//  1. balances(i) for i in 0..n-1
//  2. get_virtual_price()
//  3. totalSupply()
//  4. A()
//  5. gamma()
//  6. fee()
//  7. get_dy(i,j,10^CoinDecimals[i]) for every ordered pair i!=j, i asc then j asc
//     Note: get_dy args are uint256 for cryptoswap (not int128).
//  8. price_scale(i) for i=0..n-2
//  9. price_oracle(i) for i=0..n-2
//  10. last_prices(i) for i=0..n-2
//  11. D() AllowFailure=true
//  12. xcp_profit() AllowFailure=true
func (h *CryptoswapHandler) buildSnapshotCalls(pool RegisteredPool) ([]outbound.Call, error) {
	var calls []outbound.Call

	// 1. balances(i) for each coin
	for i := 0; i < pool.NCoins; i++ {
		data, err := h.cryptoABI.Pack("balances", big.NewInt(int64(i)))
		if err != nil {
			return nil, fmt.Errorf("packing balances(%d): %w", i, err)
		}
		calls = append(calls, outbound.Call{Target: pool.Address, AllowFailure: false, CallData: data})
	}

	// 2. get_virtual_price()
	data, err := h.cryptoABI.Pack("get_virtual_price")
	if err != nil {
		return nil, fmt.Errorf("packing get_virtual_price: %w", err)
	}
	calls = append(calls, outbound.Call{Target: pool.Address, AllowFailure: false, CallData: data})

	// 3. totalSupply()
	data, err = h.cryptoABI.Pack("totalSupply")
	if err != nil {
		return nil, fmt.Errorf("packing totalSupply: %w", err)
	}
	calls = append(calls, outbound.Call{Target: pool.Address, AllowFailure: false, CallData: data})

	// 4. A()
	data, err = h.cryptoABI.Pack("A")
	if err != nil {
		return nil, fmt.Errorf("packing A: %w", err)
	}
	calls = append(calls, outbound.Call{Target: pool.Address, AllowFailure: false, CallData: data})

	// 5. gamma()
	data, err = h.cryptoABI.Pack("gamma")
	if err != nil {
		return nil, fmt.Errorf("packing gamma: %w", err)
	}
	calls = append(calls, outbound.Call{Target: pool.Address, AllowFailure: false, CallData: data})

	// 6. fee()
	data, err = h.cryptoABI.Pack("fee")
	if err != nil {
		return nil, fmt.Errorf("packing fee: %w", err)
	}
	calls = append(calls, outbound.Call{Target: pool.Address, AllowFailure: false, CallData: data})

	// 7. get_dy(i, j, 10^decimals[i]) for every ordered pair i!=j
	// Cryptoswap get_dy takes uint256 args (not int128).
	for i := 0; i < pool.NCoins; i++ {
		for j := 0; j < pool.NCoins; j++ {
			if i == j {
				continue
			}
			if i >= len(pool.CoinDecimals) {
				return nil, fmt.Errorf("pool %s coin %d missing decimals (have %d, n_coins %d)", pool.Address, i, len(pool.CoinDecimals), pool.NCoins)
			}
			dx := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(pool.CoinDecimals[i])), nil)
			data, err = h.cryptoABI.Pack("get_dy", big.NewInt(int64(i)), big.NewInt(int64(j)), dx)
			if err != nil {
				return nil, fmt.Errorf("packing get_dy(%d,%d): %w", i, j, err)
			}
			calls = append(calls, outbound.Call{Target: pool.Address, AllowFailure: false, CallData: data})
		}
	}

	// 8. price_scale(i) for i=0..n-2
	for i := 0; i < pool.NCoins-1; i++ {
		data, err = h.cryptoABI.Pack("price_scale", big.NewInt(int64(i)))
		if err != nil {
			return nil, fmt.Errorf("packing price_scale(%d): %w", i, err)
		}
		calls = append(calls, outbound.Call{Target: pool.Address, AllowFailure: false, CallData: data})
	}

	// 9. price_oracle(i) for i=0..n-2
	for i := 0; i < pool.NCoins-1; i++ {
		data, err = h.cryptoABI.Pack("price_oracle", big.NewInt(int64(i)))
		if err != nil {
			return nil, fmt.Errorf("packing price_oracle(%d): %w", i, err)
		}
		calls = append(calls, outbound.Call{Target: pool.Address, AllowFailure: false, CallData: data})
	}

	// 10. last_prices(i) for i=0..n-2
	for i := 0; i < pool.NCoins-1; i++ {
		data, err = h.cryptoABI.Pack("last_prices", big.NewInt(int64(i)))
		if err != nil {
			return nil, fmt.Errorf("packing last_prices(%d): %w", i, err)
		}
		calls = append(calls, outbound.Call{Target: pool.Address, AllowFailure: false, CallData: data})
	}

	// 11. D() - AllowFailure=true; revert maps to nil D in the snapshot.
	data, err = h.cryptoABI.Pack("D")
	if err != nil {
		return nil, fmt.Errorf("packing D: %w", err)
	}
	calls = append(calls, outbound.Call{Target: pool.Address, AllowFailure: true, CallData: data})

	// 12. xcp_profit() - AllowFailure=true; revert maps to nil XcpProfit in the snapshot.
	data, err = h.cryptoABI.Pack("xcp_profit")
	if err != nil {
		return nil, fmt.Errorf("packing xcp_profit: %w", err)
	}
	calls = append(calls, outbound.Call{Target: pool.Address, AllowFailure: true, CallData: data})

	return calls, nil
}

// decodeSnapshotResults decodes the multicall results in the same order as buildSnapshotCalls.
func (h *CryptoswapHandler) decodeSnapshotResults(
	pool RegisteredPool,
	blockNumber int64,
	version int,
	ts time.Time,
	calls []outbound.Call,
	results []outbound.Result,
) (*entity.CurveCryptoswapState, error) {
	if len(results) != len(calls) {
		return nil, fmt.Errorf("multicall returned %d results, expected %d", len(results), len(calls))
	}

	idx := 0

	// 1. balances
	balances := make([]*big.Int, pool.NCoins)
	for i := 0; i < pool.NCoins; i++ {
		v, err := shared.UnpackUint(h.cryptoABI, "balances", results[idx])
		if err != nil {
			return nil, fmt.Errorf("balances(%d): %w", i, err)
		}
		balances[i] = v
		idx++
	}

	// 2. get_virtual_price
	virtualPrice, err := shared.UnpackUint(h.cryptoABI, "get_virtual_price", results[idx])
	if err != nil {
		return nil, fmt.Errorf("get_virtual_price: %w", err)
	}
	idx++

	// 3. totalSupply
	totalSupply, err := shared.UnpackUint(h.cryptoABI, "totalSupply", results[idx])
	if err != nil {
		return nil, fmt.Errorf("totalSupply: %w", err)
	}
	idx++

	// 4. A
	a, err := shared.UnpackUint(h.cryptoABI, "A", results[idx])
	if err != nil {
		return nil, fmt.Errorf("decoding A: %w", err)
	}
	idx++

	// 5. gamma
	gamma, err := shared.UnpackUint(h.cryptoABI, "gamma", results[idx])
	if err != nil {
		return nil, fmt.Errorf("gamma: %w", err)
	}
	idx++

	// 6. fee
	fee, err := shared.UnpackUint(h.cryptoABI, "fee", results[idx])
	if err != nil {
		return nil, fmt.Errorf("fee: %w", err)
	}
	idx++

	// 7. get_dy for each ordered pair
	nPairs := pool.NCoins * (pool.NCoins - 1)
	spotDy := make([]*big.Int, 0, nPairs)
	for i := 0; i < pool.NCoins; i++ {
		for j := 0; j < pool.NCoins; j++ {
			if i == j {
				continue
			}
			v, err := shared.UnpackUint(h.cryptoABI, "get_dy", results[idx])
			if err != nil {
				return nil, fmt.Errorf("get_dy(%d,%d): %w", i, j, err)
			}
			spotDy = append(spotDy, v)
			idx++
		}
	}

	// 8. price_scale(i) for i=0..n-2
	nPriceEntries := pool.NCoins - 1
	priceScale := make([]*big.Int, nPriceEntries)
	for i := 0; i < nPriceEntries; i++ {
		v, err := shared.UnpackUint(h.cryptoABI, "price_scale", results[idx])
		if err != nil {
			return nil, fmt.Errorf("price_scale(%d): %w", i, err)
		}
		priceScale[i] = v
		idx++
	}

	// 9. price_oracle(i) for i=0..n-2
	priceOracle := make([]*big.Int, nPriceEntries)
	for i := 0; i < nPriceEntries; i++ {
		v, err := shared.UnpackUint(h.cryptoABI, "price_oracle", results[idx])
		if err != nil {
			return nil, fmt.Errorf("price_oracle(%d): %w", i, err)
		}
		priceOracle[i] = v
		idx++
	}

	// 10. last_prices(i) for i=0..n-2
	lastPrices := make([]*big.Int, nPriceEntries)
	for i := 0; i < nPriceEntries; i++ {
		v, err := shared.UnpackUint(h.cryptoABI, "last_prices", results[idx])
		if err != nil {
			return nil, fmt.Errorf("last_prices(%d): %w", i, err)
		}
		lastPrices[i] = v
		idx++
	}

	// 11. D() - AllowFailure=true; nil on revert.
	var d *big.Int
	if v, err := shared.UnpackUint(h.cryptoABI, "D", results[idx]); err == nil {
		d = v
	}
	idx++

	// 12. xcp_profit() - AllowFailure=true; nil on revert.
	var xcpProfit *big.Int
	if v, err := shared.UnpackUint(h.cryptoABI, "xcp_profit", results[idx]); err == nil {
		xcpProfit = v
	}
	idx++ //nolint:ineffassign // idx used by lockstep pattern; kept for symmetry.

	return entity.NewCurveCryptoswapState(entity.CurveCryptoswapStateParams{
		CurvePoolID:  pool.ID,
		BlockNumber:  blockNumber,
		BlockVersion: version,
		Timestamp:    ts,
		Balances:     balances,
		VirtualPrice: virtualPrice,
		TotalSupply:  totalSupply,
		A:            a,
		Gamma:        gamma,
		Fee:          fee,
		D:            d,
		XcpProfit:    xcpProfit,
		PriceScale:   priceScale,
		PriceOracle:  priceOracle,
		LastPrices:   lastPrices,
		SpotDy:       spotDy,
	})
}

// ---------------------------------------------------------------------------
// Typed event extractors
// ---------------------------------------------------------------------------

func extractCryptoswapTokenExchange(
	data map[string]any,
	pool RegisteredPool,
	logIndex uint,
	txHash common.Hash,
) (SwapRecord, error) {
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
	fee, err := getBigIntField(data, "fee")
	if err != nil {
		return SwapRecord{}, err
	}
	// packed_price_scale is not stored; it is present in the event but discarded.

	return SwapRecord{
		Pool:         pool,
		LogIndex:     logIndex,
		TxHash:       txHash,
		Buyer:        buyer,
		SoldID:       int(soldID.Int64()),
		BoughtID:     int(boughtID.Int64()),
		TokensSold:   tokensSold,
		TokensBought: tokensBought,
		Fee:          fee,
	}, nil
}

func extractCryptoswapAddLiquidity(
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
	// fee is a single uint256; store as 1-element slice for uniform Fees representation.
	feeVal, err := getBigIntField(data, "fee")
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
		Fees:         []*big.Int{feeVal},
		TokenSupply:  supply,
	}, nil
}

func extractCryptoswapRemoveLiquidity(
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
		TokenSupply:  supply,
	}, nil
}

// extractCryptoswapRemoveLiquidityOne handles the cryptoswap variant which DOES
// carry coin_index (unlike some stableswap versions). TokenAmounts is set to
// [token_amount, coin_amount] for storage parity.
func extractCryptoswapRemoveLiquidityOne(
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
	coinIndexBig, err := getBigIntField(data, "coin_index")
	if err != nil {
		return LiquidityRecord{}, err
	}
	coinAmount, err := getBigIntField(data, "coin_amount")
	if err != nil {
		return LiquidityRecord{}, err
	}
	// approx_fee is present in the event but not stored in LiquidityRecord.

	coinIdx := int(coinIndexBig.Int64())
	return LiquidityRecord{
		Pool:         pool,
		LogIndex:     logIndex,
		TxHash:       txHash,
		Provider:     provider,
		Kind:         LiquidityRemoveOne,
		TokenAmounts: []*big.Int{tokenAmount, coinAmount},
		CoinIndex:    &coinIdx,
	}, nil
}

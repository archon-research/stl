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
	stableABI   *abi.ABI
	eventsByID  map[common.Hash]*abi.Event
	classicSigs map[int]classicLiquiditySigs
}

// NewStableswapHandler constructs a StableswapHandler with a pre-parsed ABI.
func NewStableswapHandler(stableABI *abi.ABI) *StableswapHandler {
	eventsByID := make(map[common.Hash]*abi.Event, len(stableABI.Events))
	for _, ev := range stableABI.Events {
		eventsByID[ev.ID] = &ev
	}
	return &StableswapHandler{
		stableABI:   stableABI,
		eventsByID:  eventsByID,
		classicSigs: make(map[int]classicLiquiditySigs),
	}
}

func (h *StableswapHandler) classicSigsFor(n int) classicLiquiditySigs {
	if s, ok := h.classicSigs[n]; ok {
		return s
	}
	s := buildClassicSigs(n)
	h.classicSigs[n] = s
	return s
}

// Warm precomputes the classic liquidity-event signatures for an nCoins pool so
// the per-block decode path only reads the cache.
func (h *StableswapHandler) Warm(nCoins int) { h.classicSigsFor(nCoins) }

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

	sigs := h.classicSigsFor(pool.NCoins)

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
			result.Captured, err = appendRawCaptured(result.Captured, pool, logIndex, txHash, "", log)
			if err != nil {
				return DecodedEvents{}, err
			}
			continue
		}

		topic0 := common.HexToHash(log.Topics[0])

		// Pre-NG pools emit fixed-array liquidity events whose topic0s differ from the
		// NG ABI. Dispatch them via word-slicing before falling back to the ABI lookup.
		if pool.Kind == KindStableswapPreNG {
			rec, matched, err := decodeClassicLiquidity(log, pool, sigs)
			if err != nil {
				return DecodedEvents{}, fmt.Errorf("decoding classic liquidity log (index %s): %w", log.LogIndex, err)
			}
			if matched {
				result.Liquidity = append(result.Liquidity, *rec)
				result.Captured, err = appendRawCaptured(result.Captured, pool, logIndex, txHash, log.Topics[0], log)
				if err != nil {
					return DecodedEvents{}, err
				}
				continue
			}
		}

		ev, known := h.eventsByID[topic0]

		if !known {
			result.Captured, err = appendRawCaptured(result.Captured, pool, logIndex, txHash, log.Topics[0], log)
			if err != nil {
				return DecodedEvents{}, err
			}
			continue
		}

		eventData, err := decodeLog(ev, log)
		if err != nil {
			return DecodedEvents{}, fmt.Errorf("decoding %s log (index %s): %w", ev.Name, log.LogIndex, err)
		}

		if paramName, isParam := parameterEventName(ev.Name); isParam {
			rec, err := extractParameterEvent(eventData, ev.Name, paramName, pool, logIndex, txHash)
			if err != nil {
				return DecodedEvents{}, fmt.Errorf("extracting %s: %w", ev.Name, err)
			}
			result.ParameterEvents = append(result.ParameterEvents, rec)
		} else if isLpTokenEvent(ev.Name) {
			rec, err := extractLpTokenEvent(eventData, ev.Name, pool, logIndex, txHash)
			if err != nil {
				return DecodedEvents{}, fmt.Errorf("extracting %s: %w", ev.Name, err)
			}
			result.LpTokenEvents = append(result.LpTokenEvents, rec)
		}

		switch ev.Name {
		case "TokenExchange":
			swap, err := extractStableswapTokenExchange(eventData, pool, logIndex, txHash, false)
			if err != nil {
				return DecodedEvents{}, fmt.Errorf("extracting TokenExchange: %w", err)
			}
			result.Swaps = append(result.Swaps, swap)

		case "TokenExchangeUnderlying":
			swap, err := extractStableswapTokenExchange(eventData, pool, logIndex, txHash, true)
			if err != nil {
				return DecodedEvents{}, fmt.Errorf("extracting TokenExchangeUnderlying: %w", err)
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

	st, cfg, err := h.decodeSnapshotResults(pool, blockNumber, version, ts, calls, results)
	if err != nil {
		return StateSnapshot{}, fmt.Errorf("decoding snapshot results for pool %s: %w", pool.Address, err)
	}

	return StateSnapshot{
		Pool:             pool,
		BlockNumber:      blockNumber,
		BlockVersion:     version,
		Timestamp:        ts,
		Stableswap:       st,
		StableswapConfig: cfg,
	}, nil
}

// buildSnapshotCalls constructs the ordered multicall call list for a stableswap pool.
// Call order (existing calls 1-7 keep their positions; decode is in lockstep):
//  1. balances(i) for i in 0..n-1
//  2. get_virtual_price()
//  3. totalSupply()
//  4. A()
//  5. fee()
//  6. get_dy(i,j,10^CoinDecimals[i]) for every ordered pair i!=j, i asc then j asc
//  7. (NG only) price_oracle(), last_price() with AllowFailure=true
//
// Extended reads (all AllowFailure=true; a revert leaves the field nil and never
// fails the whole snapshot):
//  8. A_precise()
//  9. admin_balances(i) for i in 0..n-1
//  10. calc_token_amount([10^decimals[i] for each i], true)
//  11. calc_withdraw_one_coin(1e18, i) for i in 0..n-1
//  12. (NG only) stored_rates(), ema_price(), get_p()
//  13. config getters: initial_A, initial_A_time, future_A, future_A_time, admin_fee, future_fee
//  14. (pre-NG only) future_admin_fee()
//  15. (NG only) ma_exp_time(), oracle_method()
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
	// totalSupply lives on the LP token, which for pre-NG pools is a separate
	// contract from the pool; fall back to the pool address for NG pools that
	// are their own LP token.
	lpTarget := pool.Address
	if pool.LpTokenAddress != nil {
		lpTarget = *pool.LpTokenAddress
	}
	data, err = h.stableABI.Pack("totalSupply")
	if err != nil {
		return nil, fmt.Errorf("packing totalSupply: %w", err)
	}
	calls = append(calls, outbound.Call{Target: lpTarget, AllowFailure: false, CallData: data})

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

	// 8. A_precise()
	allow := func(data []byte) {
		calls = append(calls, outbound.Call{Target: pool.Address, AllowFailure: true, CallData: data})
	}
	data, err = h.stableABI.Pack("A_precise")
	if err != nil {
		return nil, fmt.Errorf("packing A_precise: %w", err)
	}
	allow(data)

	// 9. admin_balances(i) for each coin
	for i := 0; i < pool.NCoins; i++ {
		data, err = h.stableABI.Pack("admin_balances", big.NewInt(int64(i)))
		if err != nil {
			return nil, fmt.Errorf("packing admin_balances(%d): %w", i, err)
		}
		allow(data)
	}

	// 10. calc_token_amount(unit deposit of 10^decimals[i] per coin, is_deposit=true)
	deposits := make([]*big.Int, pool.NCoins)
	for i := 0; i < pool.NCoins; i++ {
		if i >= len(pool.CoinDecimals) {
			return nil, fmt.Errorf("pool %s coin %d missing decimals (have %d, n_coins %d)", pool.Address, i, len(pool.CoinDecimals), pool.NCoins)
		}
		deposits[i] = new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(pool.CoinDecimals[i])), nil)
	}
	data, err = packCalcTokenAmount(deposits, true)
	if err != nil {
		return nil, fmt.Errorf("packing calc_token_amount: %w", err)
	}
	allow(data)

	// 11. calc_withdraw_one_coin(1e18, i) for each coin
	oneLP := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	for i := 0; i < pool.NCoins; i++ {
		data, err = h.stableABI.Pack("calc_withdraw_one_coin", oneLP, big.NewInt(int64(i)))
		if err != nil {
			return nil, fmt.Errorf("packing calc_withdraw_one_coin(%d): %w", i, err)
		}
		allow(data)
	}

	// 12. NG-only: stored_rates(), ema_price(), get_p()
	if pool.Kind == KindStableswapNG {
		for _, fn := range []string{"stored_rates", "ema_price", "get_p"} {
			data, err = h.stableABI.Pack(fn)
			if err != nil {
				return nil, fmt.Errorf("packing %s: %w", fn, err)
			}
			allow(data)
		}
	}

	// 13. config getters (both classes)
	for _, fn := range []string{"initial_A", "initial_A_time", "future_A", "future_A_time", "admin_fee", "future_fee"} {
		data, err = h.stableABI.Pack(fn)
		if err != nil {
			return nil, fmt.Errorf("packing %s: %w", fn, err)
		}
		allow(data)
	}

	// 14. pre-NG only: future_admin_fee()
	if pool.Kind == KindStableswapPreNG {
		data, err = h.stableABI.Pack("future_admin_fee")
		if err != nil {
			return nil, fmt.Errorf("packing future_admin_fee: %w", err)
		}
		allow(data)
	}

	// 15. NG-only: ma_exp_time(), oracle_method()
	if pool.Kind == KindStableswapNG {
		for _, fn := range []string{"ma_exp_time", "oracle_method"} {
			data, err = h.stableABI.Pack(fn)
			if err != nil {
				return nil, fmt.Errorf("packing %s: %w", fn, err)
			}
			allow(data)
		}
	}

	return calls, nil
}

// decodeSnapshotResults decodes the multicall results in the same order as
// buildSnapshotCalls, returning the per-block state row and the close-to-static
// config (nil when a required config getter reverted).
func (h *StableswapHandler) decodeSnapshotResults(
	pool RegisteredPool,
	blockNumber int64,
	version int,
	ts time.Time,
	calls []outbound.Call,
	results []outbound.Result,
) (*entity.CurveStableswapState, *entity.CurveStableswapConfig, error) {
	if len(results) != len(calls) {
		return nil, nil, fmt.Errorf("multicall returned %d results, expected %d", len(results), len(calls))
	}

	idx := 0

	// 1. balances
	balances := make([]*big.Int, pool.NCoins)
	for i := 0; i < pool.NCoins; i++ {
		v, err := shared.UnpackUint(h.stableABI, "balances", results[idx])
		if err != nil {
			return nil, nil, fmt.Errorf("balances(%d): %w", i, err)
		}
		balances[i] = v
		idx++
	}

	// 2. get_virtual_price
	virtualPrice, err := shared.UnpackUint(h.stableABI, "get_virtual_price", results[idx])
	if err != nil {
		return nil, nil, fmt.Errorf("get_virtual_price: %w", err)
	}
	idx++

	// 3. totalSupply
	totalSupply, err := shared.UnpackUint(h.stableABI, "totalSupply", results[idx])
	if err != nil {
		return nil, nil, fmt.Errorf("totalSupply: %w", err)
	}
	idx++

	// 4. A
	a, err := shared.UnpackUint(h.stableABI, "A", results[idx])
	if err != nil {
		return nil, nil, fmt.Errorf("decoding A: %w", err)
	}
	idx++

	// 5. fee
	fee, err := shared.UnpackUint(h.stableABI, "fee", results[idx])
	if err != nil {
		return nil, nil, fmt.Errorf("fee: %w", err)
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
				return nil, nil, fmt.Errorf("get_dy(%d,%d): %w", i, j, err)
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
				return nil, nil, fmt.Errorf("price_oracle: %w", err)
			}
			priceOracle = po
		}
		idx++
		if results[idx].Success {
			lp, err := shared.UnpackUint(h.stableABI, "last_price", results[idx])
			if err != nil {
				return nil, nil, fmt.Errorf("last_price: %w", err)
			}
			lastPrice = lp
		}
		idx++
	}

	// optUint reads an AllowFailure=true scalar at the cursor: nil on revert,
	// error only on a decode failure of a successful result, advancing idx.
	optUint := func(method string) (*big.Int, error) {
		defer func() { idx++ }()
		if !results[idx].Success {
			return nil, nil
		}
		return shared.UnpackUint(h.stableABI, method, results[idx])
	}

	// 8. A_precise
	aPrecise, err := optUint("A_precise")
	if err != nil {
		return nil, nil, fmt.Errorf("A_precise: %w", err)
	}

	// 9. admin_balances(i)
	adminBalances := make([]*big.Int, pool.NCoins)
	allAdminOK := true
	for i := 0; i < pool.NCoins; i++ {
		v, err := optUint("admin_balances")
		if err != nil {
			return nil, nil, fmt.Errorf("admin_balances(%d): %w", i, err)
		}
		if v == nil {
			allAdminOK = false
		}
		adminBalances[i] = v
	}
	if !allAdminOK {
		adminBalances = nil
	}

	// 10. calc_token_amount (packed manually per N, so not unpacked via the ABI)
	var calcTokenAmount *big.Int
	if results[idx].Success {
		v, err := unpackSingleUint(results[idx])
		if err != nil {
			return nil, nil, fmt.Errorf("calc_token_amount: %w", err)
		}
		calcTokenAmount = v
	}
	idx++

	// 11. calc_withdraw_one_coin(1e18, i)
	calcWithdraw := make([]*big.Int, pool.NCoins)
	allWithdrawOK := true
	for i := 0; i < pool.NCoins; i++ {
		v, err := optUint("calc_withdraw_one_coin")
		if err != nil {
			return nil, nil, fmt.Errorf("calc_withdraw_one_coin(%d): %w", i, err)
		}
		if v == nil {
			allWithdrawOK = false
		}
		calcWithdraw[i] = v
	}
	if !allWithdrawOK {
		calcWithdraw = nil
	}

	// 12. NG-only: stored_rates, ema_price, get_p
	var storedRates []*big.Int
	var emaPrice, getP *big.Int
	if pool.Kind == KindStableswapNG {
		if results[idx].Success {
			sr, err := unpackUintArray(results[idx], pool.NCoins)
			if err != nil {
				return nil, nil, fmt.Errorf("stored_rates: %w", err)
			}
			storedRates = sr
		}
		idx++
		emaPrice, err = optUint("ema_price")
		if err != nil {
			return nil, nil, fmt.Errorf("ema_price: %w", err)
		}
		getP, err = optUint("get_p")
		if err != nil {
			return nil, nil, fmt.Errorf("get_p: %w", err)
		}
	}

	// 13. config getters
	initialA, err := optUint("initial_A")
	if err != nil {
		return nil, nil, fmt.Errorf("initial_A: %w", err)
	}
	initialATime, err := optUint("initial_A_time")
	if err != nil {
		return nil, nil, fmt.Errorf("initial_A_time: %w", err)
	}
	futureA, err := optUint("future_A")
	if err != nil {
		return nil, nil, fmt.Errorf("future_A: %w", err)
	}
	futureATime, err := optUint("future_A_time")
	if err != nil {
		return nil, nil, fmt.Errorf("future_A_time: %w", err)
	}
	adminFee, err := optUint("admin_fee")
	if err != nil {
		return nil, nil, fmt.Errorf("admin_fee: %w", err)
	}
	futureFee, err := optUint("future_fee")
	if err != nil {
		return nil, nil, fmt.Errorf("future_fee: %w", err)
	}

	// 14. pre-NG only: future_admin_fee
	var futureAdminFee *big.Int
	if pool.Kind == KindStableswapPreNG {
		futureAdminFee, err = optUint("future_admin_fee")
		if err != nil {
			return nil, nil, fmt.Errorf("future_admin_fee: %w", err)
		}
	}

	// 15. NG-only: ma_exp_time, oracle_method
	var maExpTime, oracleMethod *big.Int
	if pool.Kind == KindStableswapNG {
		maExpTime, err = optUint("ma_exp_time")
		if err != nil {
			return nil, nil, fmt.Errorf("ma_exp_time: %w", err)
		}
		oracleMethod, err = optUint("oracle_method")
		if err != nil {
			return nil, nil, fmt.Errorf("oracle_method: %w", err)
		}
	}

	state, err := entity.NewCurveStableswapState(entity.CurveStableswapStateParams{
		CurvePoolID:         pool.ID,
		BlockNumber:         blockNumber,
		BlockVersion:        version,
		Timestamp:           ts,
		Balances:            balances,
		VirtualPrice:        virtualPrice,
		TotalSupply:         totalSupply,
		A:                   a,
		Fee:                 fee,
		SpotDy:              spotDy,
		LastPrice:           lastPrice,
		PriceOracle:         priceOracle,
		APrecise:            aPrecise,
		AdminBalances:       adminBalances,
		StoredRates:         storedRates,
		EmaPrice:            emaPrice,
		GetP:                getP,
		CalcTokenAmount:     calcTokenAmount,
		CalcWithdrawOneCoin: calcWithdraw,
	})
	if err != nil {
		return nil, nil, err
	}

	cfg, err := buildStableswapConfig(pool, blockNumber, version, ts, stableswapConfigReads{
		initialA:       initialA,
		initialATime:   initialATime,
		futureA:        futureA,
		futureATime:    futureATime,
		adminFee:       adminFee,
		futureFee:      futureFee,
		futureAdminFee: futureAdminFee,
		maExpTime:      maExpTime,
		oracleMethod:   oracleMethod,
	})
	if err != nil {
		return nil, nil, err
	}

	return state, cfg, nil
}

// stableswapConfigReads holds the raw config getter results; nil means the read
// reverted (AllowFailure=true).
type stableswapConfigReads struct {
	initialA       *big.Int
	initialATime   *big.Int
	futureA        *big.Int
	futureATime    *big.Int
	adminFee       *big.Int
	futureFee      *big.Int
	futureAdminFee *big.Int // pre-NG only
	maExpTime      *big.Int // NG only
	oracleMethod   *big.Int // NG only
}

// buildStableswapConfig assembles a CurveStableswapConfig from the config getter
// reads, or returns (nil, nil) when any of the four required NOT-NULL value
// fields (initial_a, future_a, admin_fee, future_fee) reverted (we never persist
// a partial config row). The *_time fields default to 0 when their optional read
// reverted; this matches the BIGINT NOT NULL DEFAULT 0 columns.
func buildStableswapConfig(
	pool RegisteredPool,
	blockNumber int64,
	version int,
	ts time.Time,
	r stableswapConfigReads,
) (*entity.CurveStableswapConfig, error) {
	if r.initialA == nil || r.futureA == nil || r.adminFee == nil || r.futureFee == nil {
		return nil, nil
	}
	timeOrZero := func(b *big.Int) int64 {
		if b == nil || !b.IsInt64() {
			return 0
		}
		return b.Int64()
	}
	var maExpTime *int64
	if r.maExpTime != nil && r.maExpTime.IsInt64() {
		v := r.maExpTime.Int64()
		maExpTime = &v
	}
	return entity.NewCurveStableswapConfig(entity.CurveStableswapConfigParams{
		CurvePoolID:    pool.ID,
		BlockNumber:    blockNumber,
		BlockVersion:   version,
		Timestamp:      ts,
		InitialA:       r.initialA,
		InitialATime:   timeOrZero(r.initialATime),
		FutureA:        r.futureA,
		FutureATime:    timeOrZero(r.futureATime),
		AdminFee:       r.adminFee,
		FutureFee:      r.futureFee,
		FutureAdminFee: r.futureAdminFee,
		MaExpTime:      maExpTime,
		OracleMethod:   r.oracleMethod,
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
	isUnderlying bool,
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
		IsUnderlying: isUnderlying,
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

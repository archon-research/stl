package curveindexer

import (
	"context"
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
	cryptoSigs map[int]cryptoswapLiquiditySigs
}

// NewCryptoswapHandler constructs a CryptoswapHandler with a pre-parsed ABI.
func NewCryptoswapHandler(cryptoABI *abi.ABI) *CryptoswapHandler {
	eventsByID := make(map[common.Hash]*abi.Event, len(cryptoABI.Events))
	for _, ev := range cryptoABI.Events {
		eventsByID[ev.ID] = &ev
	}
	return &CryptoswapHandler{
		cryptoABI:  cryptoABI,
		eventsByID: eventsByID,
		cryptoSigs: make(map[int]cryptoswapLiquiditySigs),
	}
}

func (h *CryptoswapHandler) cryptoSigsFor(n int) cryptoswapLiquiditySigs {
	if s, ok := h.cryptoSigs[n]; ok {
		return s
	}
	s := buildCryptoswapSigs(n)
	h.cryptoSigs[n] = s
	return s
}

// Warm precomputes the cryptoswap liquidity-event signatures for an nCoins pool
// so the per-block decode path only reads the cache.
func (h *CryptoswapHandler) Warm(nCoins int) { h.cryptoSigsFor(nCoins) }

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

	sigs := h.cryptoSigsFor(pool.NCoins)

	for _, log := range receipt.Logs {
		if !common.IsHexAddress(log.Address) {
			return DecodedEvents{}, fmt.Errorf("invalid log address %q", log.Address)
		}
		addr := common.HexToAddress(log.Address)
		if !logBelongsToPool(addr, pool) {
			continue
		}

		logIndex, err := parseHexUint(log.LogIndex)
		if err != nil {
			return DecodedEvents{}, fmt.Errorf("parsing log index %q: %w", log.LogIndex, err)
		}
		txHash := common.HexToHash(log.TransactionHash)

		if len(log.Topics) == 0 {
			result.Captured, err = appendRawCaptured(result.Captured, addr, logIndex, txHash, "", log)
			if err != nil {
				return DecodedEvents{}, err
			}
			continue
		}

		topic0 := common.HexToHash(log.Topics[0])

		// Cryptoswap liquidity events use fixed-size arrays whose topic0s differ
		// from the dynamic-array ABI. Dispatch them via word-slicing before the
		// ABI lookup so they are typed rather than falling through to captured-only.
		rec, matched, err := decodeCryptoLiquidity(log, pool, sigs)
		if err != nil {
			return DecodedEvents{}, fmt.Errorf("decoding cryptoswap liquidity log (index %s): %w", log.LogIndex, err)
		}
		if matched {
			result.Liquidity = append(result.Liquidity, *rec)
			result.Captured, err = appendRawCaptured(result.Captured, addr, logIndex, txHash, log.Topics[0], log)
			if err != nil {
				return DecodedEvents{}, err
			}
			continue
		}

		ev, known := h.eventsByID[topic0]

		if !known {
			result.Captured, err = appendRawCaptured(result.Captured, addr, logIndex, txHash, log.Topics[0], log)
			if err != nil {
				return DecodedEvents{}, err
			}
			continue
		}

		eventData, err := decodeLog(ev, log)
		if err != nil {
			return DecodedEvents{}, fmt.Errorf("decoding %s log (index %s): %w", ev.Name, log.LogIndex, err)
		}

		// Parameter/admin events (RampAgamma/NewParameters/CommitNewParameters/
		// ClaimAdminFee) and LP-token Transfer/Approval reuse the shared decoders
		// (extractParameterEvent/abiParamEventNames, extractLpTokenEvent) so the
		// cryptoswap path carries the same governance/LP surface as stableswap.
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
			swap, err := extractCryptoswapTokenExchange(eventData, pool, logIndex, txHash)
			if err != nil {
				return DecodedEvents{}, fmt.Errorf("extracting TokenExchange: %w", err)
			}
			result.Swaps = append(result.Swaps, swap)
		}

		// Capture net: all known pool-address logs are also stored in Captured.
		result.Captured, err = appendDecodedCaptured(result.Captured, addr, logIndex, txHash, ev.Name, eventData)
		if err != nil {
			return DecodedEvents{}, err
		}
	}

	return result, nil
}

// SnapshotState reads cryptoswap pool state at the given block via multicall,
// pinned to blockHash (see outbound.Multicaller.ExecuteAtHash) so a reorg
// between publish and processing cannot silently answer from the wrong fork.
// Call order is deterministic; results are decoded in the same order.
// Every issued call that reverts propagates as an error (no-swallowed-errors):
// a reverted read is never collapsed into a nil/NULL field. admin_balances() is
// not issued for cryptoswap pools (see buildExtendedSnapshotCalls), so its column
// is NULL by structural design rather than because a call silently failed.
func (h *CryptoswapHandler) SnapshotState(
	ctx context.Context,
	mc outbound.Multicaller,
	pool RegisteredPool,
	blockNumber int64,
	version int,
	blockHash common.Hash,
	ts time.Time,
) (StateSnapshot, error) {
	calls, err := h.buildSnapshotCalls(pool)
	if err != nil {
		return StateSnapshot{}, fmt.Errorf("building snapshot calls for pool %s: %w", pool.Address, err)
	}

	results, err := mc.ExecuteAtHash(ctx, calls, blockHash)
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
		Cryptoswap:       st,
		CryptoswapConfig: cfg,
	}, nil
}

// buildSnapshotCalls constructs the ordered multicall call list for a cryptoswap pool.
// Call order (existing calls 1-12 keep their positions; decode is in lockstep):
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
//
// Extended reads (issued with AllowFailure=true so one revert does not abort the
// whole multicall, but a revert is still decoded as an error and stops the block).
// admin_balances() is NOT issued (see buildExtendedSnapshotCalls):
//  13. lp_price()
//  14. xcp_profit_a()
//  15. last_prices_timestamp()
//  16. get_dx(i,j,10^decimals[j]) for every ordered pair i!=j, i asc then j asc
//  17. calc_token_amount([10^decimals[i] for each i], true)
//  18. calc_withdraw_one_coin(1e18, i) for i in 0..n-1
//  19. config getters: initial_A_gamma, future_A_gamma, initial_A_gamma_time,
//     future_A_gamma_time, mid_fee, out_fee, fee_gamma, allowed_extra_profit,
//     adjustment_step, ma_time, ADMIN_FEE
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
	// totalSupply lives on the LP token, which for pre-NG pools is a separate
	// contract from the pool; fall back to the pool address for NG pools that
	// are their own LP token.
	lpTarget := pool.Address
	if pool.LpTokenAddress != nil {
		lpTarget = *pool.LpTokenAddress
	}
	data, err = h.cryptoABI.Pack("totalSupply")
	if err != nil {
		return nil, fmt.Errorf("packing totalSupply: %w", err)
	}
	calls = append(calls, outbound.Call{Target: lpTarget, AllowFailure: false, CallData: data})

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

	// 11. D() - AllowFailure=true so it does not abort the multicall, but a revert
	// is still decoded as an error (no-swallowed-errors).
	data, err = h.cryptoABI.Pack("D")
	if err != nil {
		return nil, fmt.Errorf("packing D: %w", err)
	}
	calls = append(calls, outbound.Call{Target: pool.Address, AllowFailure: true, CallData: data})

	// 12. xcp_profit() - AllowFailure=true; a revert is decoded as an error.
	data, err = h.cryptoABI.Pack("xcp_profit")
	if err != nil {
		return nil, fmt.Errorf("packing xcp_profit: %w", err)
	}
	calls = append(calls, outbound.Call{Target: pool.Address, AllowFailure: true, CallData: data})

	extended, err := h.buildExtendedSnapshotCalls(pool)
	if err != nil {
		return nil, err
	}
	calls = append(calls, extended...)

	return calls, nil
}

// buildExtendedSnapshotCalls builds the extended state + config reads. Every
// call is AllowFailure=true so one revert does not abort the whole multicall,
// but a revert is still decoded as an error.
//
// admin_balances() is deliberately NOT issued for cryptoswap pools: Tricrypto-NG
// has no admin_balances getter (it always reverts), and the only indexed
// cryptoswap pool (TriCryptoUSDC) is Tricrypto-NG. Issuing it would force a
// choice between a swallowed revert and a poison-stall on every block. We gate it
// out instead, leaving curve_cryptoswap_state.admin_balances NULL by design. A
// future twocrypto variant that exposes admin_balances would re-enable this read
// behind a variant check.
func (h *CryptoswapHandler) buildExtendedSnapshotCalls(pool RegisteredPool) ([]outbound.Call, error) {
	var calls []outbound.Call
	allow := func(data []byte) {
		calls = append(calls, outbound.Call{Target: pool.Address, AllowFailure: true, CallData: data})
	}

	// 13-15. lp_price(), xcp_profit_a(), last_prices_timestamp()
	for _, fn := range []string{"lp_price", "xcp_profit_a", "last_prices_timestamp"} {
		data, err := h.cryptoABI.Pack(fn)
		if err != nil {
			return nil, fmt.Errorf("packing %s: %w", fn, err)
		}
		allow(data)
	}

	// 16. get_dx(i, j, 10^decimals[j]) for every ordered pair i!=j: one unit of
	// the bought coin j, asking how much coin i it costs.
	for i := 0; i < pool.NCoins; i++ {
		for j := 0; j < pool.NCoins; j++ {
			if i == j {
				continue
			}
			if j >= len(pool.CoinDecimals) {
				return nil, fmt.Errorf("pool %s coin %d missing decimals (have %d, n_coins %d)", pool.Address, j, len(pool.CoinDecimals), pool.NCoins)
			}
			dy := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(pool.CoinDecimals[j])), nil)
			data, err := h.cryptoABI.Pack("get_dx", big.NewInt(int64(i)), big.NewInt(int64(j)), dy)
			if err != nil {
				return nil, fmt.Errorf("packing get_dx(%d,%d): %w", i, j, err)
			}
			allow(data)
		}
	}

	// 17. calc_token_amount(unit deposit of 10^decimals[i] per coin, is_deposit=true)
	deposits := make([]*big.Int, pool.NCoins)
	for i := 0; i < pool.NCoins; i++ {
		if i >= len(pool.CoinDecimals) {
			return nil, fmt.Errorf("pool %s coin %d missing decimals (have %d, n_coins %d)", pool.Address, i, len(pool.CoinDecimals), pool.NCoins)
		}
		deposits[i] = new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(pool.CoinDecimals[i])), nil)
	}
	data, err := packCalcTokenAmount(deposits, true)
	if err != nil {
		return nil, fmt.Errorf("packing calc_token_amount: %w", err)
	}
	allow(data)

	// 18. calc_withdraw_one_coin(1e18, i) for each coin
	oneLP := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	for i := 0; i < pool.NCoins; i++ {
		data, err := h.cryptoABI.Pack("calc_withdraw_one_coin", oneLP, big.NewInt(int64(i)))
		if err != nil {
			return nil, fmt.Errorf("packing calc_withdraw_one_coin(%d): %w", i, err)
		}
		allow(data)
	}

	// 19. config getters. Cryptoswap admin_fee is the ADMIN_FEE constant; the pool
	// has no admin_fee() getter.
	for _, fn := range cryptoswapConfigGetters {
		data, err := h.cryptoABI.Pack(fn)
		if err != nil {
			return nil, fmt.Errorf("packing %s: %w", fn, err)
		}
		allow(data)
	}

	return calls, nil
}

// cryptoswapConfigGetters is the ordered list of config view methods read at the
// tail of every cryptoswap snapshot. Order MUST stay in lockstep with the decode
// in decodeCryptoswapConfig.
var cryptoswapConfigGetters = []string{
	"initial_A_gamma", "future_A_gamma", "initial_A_gamma_time", "future_A_gamma_time",
	"mid_fee", "out_fee", "fee_gamma", "allowed_extra_profit", "adjustment_step",
	"ma_time", "ADMIN_FEE",
}

// decodeSnapshotResults decodes the multicall results in the same order as
// buildSnapshotCalls, returning the per-block state row and the close-to-static
// config (nil when a required config getter reverted).
func (h *CryptoswapHandler) decodeSnapshotResults(
	pool RegisteredPool,
	blockNumber int64,
	version int,
	ts time.Time,
	calls []outbound.Call,
	results []outbound.Result,
) (*entity.CurveCryptoswapState, *entity.CurveCryptoswapConfig, error) {
	if len(results) != len(calls) {
		return nil, nil, fmt.Errorf("multicall returned %d results, expected %d", len(results), len(calls))
	}

	idx := 0

	// 1. balances
	balances := make([]*big.Int, pool.NCoins)
	for i := 0; i < pool.NCoins; i++ {
		v, err := shared.UnpackUint(h.cryptoABI, "balances", results[idx])
		if err != nil {
			return nil, nil, fmt.Errorf("balances(%d): %w", i, err)
		}
		balances[i] = v
		idx++
	}

	// 2. get_virtual_price
	virtualPrice, err := shared.UnpackUint(h.cryptoABI, "get_virtual_price", results[idx])
	if err != nil {
		return nil, nil, fmt.Errorf("get_virtual_price: %w", err)
	}
	idx++

	// 3. totalSupply
	totalSupply, err := shared.UnpackUint(h.cryptoABI, "totalSupply", results[idx])
	if err != nil {
		return nil, nil, fmt.Errorf("totalSupply: %w", err)
	}
	idx++

	// 4. A
	amp, err := shared.UnpackUint(h.cryptoABI, "A", results[idx])
	if err != nil {
		return nil, nil, fmt.Errorf("decoding A: %w", err)
	}
	idx++

	// 5. gamma
	gamma, err := shared.UnpackUint(h.cryptoABI, "gamma", results[idx])
	if err != nil {
		return nil, nil, fmt.Errorf("gamma: %w", err)
	}
	idx++

	// 6. fee
	fee, err := shared.UnpackUint(h.cryptoABI, "fee", results[idx])
	if err != nil {
		return nil, nil, fmt.Errorf("fee: %w", err)
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
				return nil, nil, fmt.Errorf("get_dy(%d,%d): %w", i, j, err)
			}
			spotDy = append(spotDy, v)
			idx++
		}
	}

	// 8. price_scale(i) for i=0..n-2
	nPriceEntries := pool.NCoins - 1
	priceScale := make([]*big.Int, nPriceEntries)
	for i := range nPriceEntries {
		v, err := shared.UnpackUint(h.cryptoABI, "price_scale", results[idx])
		if err != nil {
			return nil, nil, fmt.Errorf("price_scale(%d): %w", i, err)
		}
		priceScale[i] = v
		idx++
	}

	// 9. price_oracle(i) for i=0..n-2
	priceOracle := make([]*big.Int, nPriceEntries)
	for i := range nPriceEntries {
		v, err := shared.UnpackUint(h.cryptoABI, "price_oracle", results[idx])
		if err != nil {
			return nil, nil, fmt.Errorf("price_oracle(%d): %w", i, err)
		}
		priceOracle[i] = v
		idx++
	}

	// 10. last_prices(i) for i=0..n-2
	lastPrices := make([]*big.Int, nPriceEntries)
	for i := range nPriceEntries {
		v, err := shared.UnpackUint(h.cryptoABI, "last_prices", results[idx])
		if err != nil {
			return nil, nil, fmt.Errorf("last_prices(%d): %w", i, err)
		}
		lastPrices[i] = v
		idx++
	}

	// optUint reads an AllowFailure=true scalar at the cursor, advancing idx. A
	// revert is an error (no-swallowed-errors): the call was issued, so a failure
	// stops the block rather than collapsing to a nil field.
	optUint := func(method string) (*big.Int, error) {
		defer func() { idx++ }()
		return optionalUintResult(h.cryptoABI, method, results[idx], pool.Address, blockNumber)
	}

	// 11. D()
	d, err := optUint("D")
	if err != nil {
		return nil, nil, fmt.Errorf("decoding D: %w", err)
	}

	// 12. xcp_profit()
	xcpProfit, err := optUint("xcp_profit")
	if err != nil {
		return nil, nil, fmt.Errorf("xcp_profit: %w", err)
	}

	// admin_balances() is not issued for cryptoswap pools (see
	// buildExtendedSnapshotCalls); the column stays NULL by structural design.
	var adminBalances []*big.Int

	// 13-15. lp_price, xcp_profit_a, last_prices_timestamp
	lpPrice, err := optUint("lp_price")
	if err != nil {
		return nil, nil, fmt.Errorf("lp_price: %w", err)
	}
	xcpProfitA, err := optUint("xcp_profit_a")
	if err != nil {
		return nil, nil, fmt.Errorf("xcp_profit_a: %w", err)
	}
	lastPricesTs, err := optUint("last_prices_timestamp")
	if err != nil {
		return nil, nil, fmt.Errorf("last_prices_timestamp: %w", err)
	}
	// A revert already errored out above (optUint), so lastPricesTs is non-nil
	// here; the guard is defensive. An issued call that returned a value outside
	// int64 is a real data-quality error, not a structural absence.
	var lastPricesTimestamp *int64
	if lastPricesTs != nil {
		if !lastPricesTs.IsInt64() {
			return nil, nil, fmt.Errorf("last_prices_timestamp for pool %s: value %s overflows int64", pool.Address, lastPricesTs.String())
		}
		v := lastPricesTs.Int64()
		lastPricesTimestamp = &v
	}

	// 16. get_dx(i,j) for each ordered pair
	getDx := make([]*big.Int, 0, nPairs)
	for i := 0; i < pool.NCoins; i++ {
		for j := 0; j < pool.NCoins; j++ {
			if i == j {
				continue
			}
			v, err := optUint("get_dx")
			if err != nil {
				return nil, nil, fmt.Errorf("get_dx(%d,%d): %w", i, j, err)
			}
			getDx = append(getDx, v)
		}
	}

	// 17. calc_token_amount (packed manually per N, so not unpacked via the ABI).
	calcTokenAmount, err := unpackSingleUint(results[idx])
	if err != nil {
		return nil, nil, fmt.Errorf("calc_token_amount: %w", err)
	}
	idx++

	// 18. calc_withdraw_one_coin(1e18, i)
	calcWithdraw := make([]*big.Int, pool.NCoins)
	for i := 0; i < pool.NCoins; i++ {
		v, err := optUint("calc_withdraw_one_coin")
		if err != nil {
			return nil, nil, fmt.Errorf("calc_withdraw_one_coin(%d): %w", i, err)
		}
		calcWithdraw[i] = v
	}

	// 19. config getters
	cfgReads, err := h.decodeCryptoswapConfigReads(optUint)
	if err != nil {
		return nil, nil, err
	}

	state, err := entity.NewCurveCryptoswapState(entity.CurveCryptoswapStateParams{
		CurvePoolID:  pool.ID,
		BlockNumber:  blockNumber,
		BlockVersion: version,
		Timestamp:    ts,
		Balances:     balances,
		VirtualPrice: virtualPrice,
		TotalSupply:  totalSupply,
		Amp:          amp,
		Gamma:        gamma,
		Fee:          fee,
		D:            d,
		XcpProfit:    xcpProfit,
		PriceScale:   priceScale,
		PriceOracle:  priceOracle,
		LastPrices:   lastPrices,
		SpotDy:       spotDy,

		AdminBalances:       adminBalances,
		LpPrice:             lpPrice,
		XcpProfitA:          xcpProfitA,
		LastPricesTimestamp: lastPricesTimestamp,
		GetDx:               getDx,
		CalcTokenAmount:     calcTokenAmount,
		CalcWithdrawOneCoin: calcWithdraw,
	})
	if err != nil {
		return nil, nil, err
	}

	cfg, err := buildCryptoswapConfig(pool, blockNumber, version, ts, cfgReads)
	if err != nil {
		return nil, nil, err
	}

	return state, cfg, nil
}

// cryptoswapConfigReads holds the raw config getter results in
// cryptoswapConfigGetters order. Every getter is issued for every cryptoswap
// pool, so a reverted getter is an error upstream (optUint); a nil here would be
// an upstream decode bug, not a real revert.
type cryptoswapConfigReads struct {
	initialAGamma      *big.Int
	futureAGamma       *big.Int
	initialAGammaTime  *big.Int
	futureAGammaTime   *big.Int
	midFee             *big.Int
	outFee             *big.Int
	feeGamma           *big.Int
	allowedExtraProfit *big.Int
	adjustmentStep     *big.Int
	maTime             *big.Int
	adminFee           *big.Int // from the ADMIN_FEE constant
}

// decodeCryptoswapConfigReads reads the config getters at the cursor in
// cryptoswapConfigGetters order via the supplied optUint accessor. The dst order
// MUST stay in lockstep with cryptoswapConfigGetters.
func (h *CryptoswapHandler) decodeCryptoswapConfigReads(optUint func(string) (*big.Int, error)) (cryptoswapConfigReads, error) {
	var r cryptoswapConfigReads
	dst := []**big.Int{
		&r.initialAGamma, &r.futureAGamma, &r.initialAGammaTime, &r.futureAGammaTime,
		&r.midFee, &r.outFee, &r.feeGamma, &r.allowedExtraProfit, &r.adjustmentStep,
		&r.maTime, &r.adminFee,
	}
	if len(dst) != len(cryptoswapConfigGetters) {
		return cryptoswapConfigReads{}, fmt.Errorf("config getter/dst length mismatch: %d getters, %d destinations", len(cryptoswapConfigGetters), len(dst))
	}
	for i, fn := range cryptoswapConfigGetters {
		v, err := optUint(fn)
		if err != nil {
			return cryptoswapConfigReads{}, fmt.Errorf("%s: %w", fn, err)
		}
		*dst[i] = v
	}
	return r, nil
}

// buildCryptoswapConfig assembles a CurveCryptoswapConfig from the config getter
// reads. Every required NOT-NULL value field is always issued, so a nil here
// means an upstream decode bug rather than a real revert (a revert errors in
// optUint); we fail hard rather than persist a partial config row. The *_time
// fields are always issued; a successful read outside int64 is an error.
func buildCryptoswapConfig(
	pool RegisteredPool,
	blockNumber int64,
	version int,
	ts time.Time,
	r cryptoswapConfigReads,
) (*entity.CurveCryptoswapConfig, error) {
	required := []*big.Int{
		r.initialAGamma, r.futureAGamma, r.midFee, r.outFee, r.feeGamma,
		r.allowedExtraProfit, r.adjustmentStep, r.maTime, r.adminFee,
	}
	for _, v := range required {
		if v == nil {
			return nil, fmt.Errorf("cryptoswap config for pool %s missing a required getter", pool.Address)
		}
	}
	// timeOrError converts a non-nil *big.Int to int64. Both time getters are
	// always issued for cryptoswap, so nil here is a decode bug. An out-of-range
	// value silently coercing to 0 would persist a wrong timestamp; we error
	// instead.
	timeOrError := func(name string, b *big.Int) (int64, error) {
		if b == nil {
			return 0, fmt.Errorf("cryptoswap config for pool %s: %s getter returned nil (decode bug)", pool.Address, name)
		}
		if !b.IsInt64() {
			return 0, fmt.Errorf("cryptoswap config for pool %s: %s value %s overflows int64", pool.Address, name, b.String())
		}
		return b.Int64(), nil
	}
	initialAGammaTime, err := timeOrError("initial_A_gamma_time", r.initialAGammaTime)
	if err != nil {
		return nil, err
	}
	futureAGammaTime, err := timeOrError("future_A_gamma_time", r.futureAGammaTime)
	if err != nil {
		return nil, err
	}
	return entity.NewCurveCryptoswapConfig(entity.CurveCryptoswapConfigParams{
		CurvePoolID:        pool.ID,
		BlockNumber:        blockNumber,
		BlockVersion:       version,
		Timestamp:          ts,
		InitialAGamma:      r.initialAGamma,
		FutureAGamma:       r.futureAGamma,
		InitialAGammaTime:  initialAGammaTime,
		FutureAGammaTime:   futureAGammaTime,
		MidFee:             r.midFee,
		OutFee:             r.outFee,
		FeeGamma:           r.feeGamma,
		AllowedExtraProfit: r.allowedExtraProfit,
		AdjustmentStep:     r.adjustmentStep,
		MaTime:             r.maTime,
		AdminFee:           r.adminFee,
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
	soldIdx, err := coinIndexOrError("sold_id", soldID, pool.NCoins)
	if err != nil {
		return SwapRecord{}, err
	}
	boughtIdx, err := coinIndexOrError("bought_id", boughtID, pool.NCoins)
	if err != nil {
		return SwapRecord{}, err
	}

	return SwapRecord{
		Pool:         pool,
		LogIndex:     logIndex,
		TxHash:       txHash,
		Buyer:        buyer,
		SoldID:       soldIdx,
		BoughtID:     boughtIdx,
		TokensSold:   tokensSold,
		TokensBought: tokensBought,
		Fee:          fee,
	}, nil
}

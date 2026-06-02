package curve_dex

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// blockchainService bundles the ABIs and the multicaller used by the worker.
type blockchainService struct {
	multicaller outbound.Multicaller
	logger      *slog.Logger

	v1Read        *abi.ABI
	ngRead        *abi.ABI
	ngOracleNoArg *abi.ABI
	lpRead        *abi.ABI
	gaugeRead     *abi.ABI
}

func newBlockchainService(mc outbound.Multicaller, logger *slog.Logger) (*blockchainService, error) {
	v1, err := abis.GetCurveStableswapV1ReadABI()
	if err != nil {
		return nil, fmt.Errorf("loading V1 read ABI: %w", err)
	}
	ng, err := abis.GetCurveStableswapNGReadABI()
	if err != nil {
		return nil, fmt.Errorf("loading NG read ABI: %w", err)
	}
	ngNoArg, err := abis.GetCurveStableswapNGNoArgOracleABI()
	if err != nil {
		return nil, fmt.Errorf("loading NG no-arg oracle ABI: %w", err)
	}
	lp, err := abis.GetCurveLPTokenReadABI()
	if err != nil {
		return nil, fmt.Errorf("loading LP token read ABI: %w", err)
	}
	gauge, err := abis.GetCurveGaugeReadABI()
	if err != nil {
		return nil, fmt.Errorf("loading gauge read ABI: %w", err)
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &blockchainService{
		multicaller:   mc,
		logger:        logger,
		v1Read:        v1,
		ngRead:        ng,
		ngOracleNoArg: ngNoArg,
		lpRead:        lp,
		gaugeRead:     gauge,
	}, nil
}

// readPoolState issues the event-triggered pool multicall:
//
//	balances(i) × N + virtual_price + A + fee
//	+ (NG only) last_price(i) + price_oracle(i) per non-zero i
//	+ totalSupply (from LP token if separate, else from pool address)
//	+ get_dy(i, j, 10^decimals[i]) for every (i, j) directional pair.
//
// One round-trip. Successful sub-calls fill the result; reverts surface as
// errors so callers can decide whether to retry.
func (b *blockchainService) readPoolState(ctx context.Context, pool *entity.CurvePool, blockNumber int64) (*poolMulticallResult, error) {
	readABI := b.v1Read
	isNG := pool.PoolKind == entity.CurvePoolKindNG
	if isNG {
		readABI = b.ngRead
	}

	n := int(pool.NCoins)
	if n < 2 {
		return nil, fmt.Errorf("pool %s has %d coins, need at least 2", pool.Address.Hex(), n)
	}

	var calls []outbound.Call

	// balances(i) for every coin index.
	for i := range n {
		data, err := readABI.Pack("balances", big.NewInt(int64(i)))
		if err != nil {
			return nil, fmt.Errorf("packing balances(%d): %w", i, err)
		}
		calls = append(calls, outbound.Call{Target: pool.Address, CallData: data})
	}

	// virtual_price, A, fee.
	for _, name := range []string{"get_virtual_price", "A", "fee"} {
		data, err := readABI.Pack(name)
		if err != nil {
			return nil, fmt.Errorf("packing %s: %w", name, err)
		}
		calls = append(calls, outbound.Call{Target: pool.Address, CallData: data})
	}

	// NG-only: last_price(i) and price_oracle(i) for indices 1..N-1.
	if isNG {
		for i := 1; i < n; i++ {
			lpData, err := readABI.Pack("last_price", big.NewInt(int64(i-1)))
			if err != nil {
				return nil, fmt.Errorf("packing last_price(%d): %w", i, err)
			}
			poData, err := readABI.Pack("price_oracle", big.NewInt(int64(i-1)))
			if err != nil {
				return nil, fmt.Errorf("packing price_oracle(%d): %w", i, err)
			}
			calls = append(calls,
				outbound.Call{Target: pool.Address, CallData: lpData, AllowFailure: true},
				outbound.Call{Target: pool.Address, CallData: poData, AllowFailure: true},
			)
		}
	}

	// NG 2-coin fallback: factory-v2-era pools (e.g. stETH-ng) expose
	// last_price()/price_oracle() WITHOUT the index argument — the indexed
	// selector deterministically reverts on them. Issue both variants in the
	// same round-trip; decode prefers indexed, falls back to no-arg. Only
	// meaningful for n==2 (the no-arg oracle is the coin1/coin0 price).
	hasNoArgFallback := isNG && n == 2
	if hasNoArgFallback {
		lpData, err := b.ngOracleNoArg.Pack("last_price")
		if err != nil {
			return nil, fmt.Errorf("packing last_price(): %w", err)
		}
		poData, err := b.ngOracleNoArg.Pack("price_oracle")
		if err != nil {
			return nil, fmt.Errorf("packing price_oracle(): %w", err)
		}
		calls = append(calls,
			outbound.Call{Target: pool.Address, CallData: lpData, AllowFailure: true},
			outbound.Call{Target: pool.Address, CallData: poData, AllowFailure: true},
		)
	}

	// totalSupply (LP token contract if separate, else the pool itself).
	lpTarget := pool.Address
	if pool.LPTokenAddress != nil {
		lpTarget = *pool.LPTokenAddress
	}
	tsData, err := b.lpRead.Pack("totalSupply")
	if err != nil {
		return nil, fmt.Errorf("packing totalSupply: %w", err)
	}
	calls = append(calls, outbound.Call{Target: lpTarget, CallData: tsData})

	// get_dy(i, j, 10^decimals[i]) for every directional pair.
	type dyKey struct{ i, j int16 }
	dyOrder := make([]dyKey, 0, n*(n-1))
	for i := range n {
		dx := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(pool.CoinDecimals[i])), nil)
		for j := range n {
			if i == j {
				continue
			}
			data, err := readABI.Pack("get_dy", big.NewInt(int64(i)), big.NewInt(int64(j)), dx)
			if err != nil {
				return nil, fmt.Errorf("packing get_dy(%d,%d): %w", i, j, err)
			}
			calls = append(calls, outbound.Call{Target: pool.Address, CallData: data, AllowFailure: true})
			dyOrder = append(dyOrder, dyKey{int16(i), int16(j)})
		}
	}

	results, err := b.multicaller.Execute(ctx, calls, big.NewInt(blockNumber))
	if err != nil {
		return nil, fmt.Errorf("multicall pool state at block %d: %w", blockNumber, err)
	}
	if len(results) != len(calls) {
		return nil, fmt.Errorf("pool multicall returned %d results, expected %d", len(results), len(calls))
	}

	out := &poolMulticallResult{Balances: make([]*big.Int, n)}
	idx := 0
	for i := range n {
		v, err := unpackUint(readABI, "balances", results[idx])
		if err != nil {
			return nil, fmt.Errorf("decoding balances(%d): %w", i, err)
		}
		out.Balances[i] = v
		idx++
	}
	vp, err := unpackUint(readABI, "get_virtual_price", results[idx])
	idx++
	if err != nil {
		return nil, fmt.Errorf("decoding get_virtual_price: %w", err)
	}
	out.VirtualPrice = vp
	a, err := unpackUint(readABI, "A", results[idx])
	idx++
	if err != nil {
		return nil, fmt.Errorf("decoding A: %w", err)
	}
	out.AFactor = a
	fee, err := unpackUint(readABI, "fee", results[idx])
	idx++
	if err != nil {
		return nil, fmt.Errorf("decoding fee: %w", err)
	}
	out.Fee = fee

	if isNG {
		// NG `last_price(i)` and `price_oracle(i)` are EMA cumulatives that
		// stay uninitialised until the pool has at least one swap, and can
		// revert transiently around oracle resets. Treat reverts as expected,
		// store nil, and let the consumer decide whether to use a fallback.
		lps := make([]*big.Int, n-1)
		pos := make([]*big.Int, n-1)
		for i := 0; i < n-1; i++ {
			lps[i] = b.tryUnpackUint(ctx, readABI, "last_price", results[idx], pool.Address, blockNumber)
			idx++
			pos[i] = b.tryUnpackUint(ctx, readABI, "price_oracle", results[idx], pool.Address, blockNumber)
			idx++
		}
		if hasNoArgFallback {
			// Factory-v2-era pools: indexed selector reverts, no-arg works.
			// Prefer the indexed value when present (true NG pools), else
			// take the no-arg one. Both reverting leaves nil (uninitialised
			// oracle), which the writer stores as a NULL element.
			lpNoArg := b.tryUnpackUint(ctx, b.ngOracleNoArg, "last_price", results[idx], pool.Address, blockNumber)
			idx++
			poNoArg := b.tryUnpackUint(ctx, b.ngOracleNoArg, "price_oracle", results[idx], pool.Address, blockNumber)
			idx++
			if lps[0] == nil {
				lps[0] = lpNoArg
			}
			if pos[0] == nil {
				pos[0] = poNoArg
			}
		}
		out.LastPrice = lps
		out.PriceOracle = pos
	}

	ts, err := unpackUint(b.lpRead, "totalSupply", results[idx])
	idx++
	if err != nil {
		return nil, fmt.Errorf("decoding totalSupply: %w", err)
	}
	out.TotalSupply = ts

	dys := make([]dyEntry, 0, len(dyOrder))
	for k, key := range dyOrder {
		r := results[idx+k]
		if !r.Success {
			// get_dy can revert on degenerate states (paused pools, empty
			// reserves, exotic Stableswap-NG variants). Write Dy=nil so the
			// nullable DB column lands as NULL — consumers can then
			// distinguish revert from a genuine zero with IS NOT NULL,
			// instead of having to flag "dy=0" as ambiguous. The snapshot
			// row still lands (Dx is still set; we control it).
			dys = append(dys, dyEntry{I: key.i, J: key.j,
				Dx: new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(pool.CoinDecimals[key.i])), nil),
				Dy: nil})
			continue
		}
		v, err := unpackUint(readABI, "get_dy", r)
		if err != nil {
			return nil, fmt.Errorf("decoding get_dy(%d,%d): %w", key.i, key.j, err)
		}
		dys = append(dys, dyEntry{I: key.i, J: key.j,
			Dx: new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(pool.CoinDecimals[key.i])), nil),
			Dy: v})
	}
	out.ExchangeDy = dys
	return out, nil
}

// readGaugeState issues the gauge view-method multicall: inflation_rate,
// working_supply, totalSupply, is_killed, reward_count. Reward token lookups
// are deferred to readGaugeRewards because reward_count is dynamic.
func (b *blockchainService) readGaugeState(ctx context.Context, gauge common.Address, blockNumber int64) (*gaugeMulticallResult, error) {
	names := []string{"inflation_rate", "working_supply", "totalSupply", "is_killed", "reward_count"}
	calls := make([]outbound.Call, len(names))
	for i, n := range names {
		data, err := b.gaugeRead.Pack(n)
		if err != nil {
			return nil, fmt.Errorf("packing %s: %w", n, err)
		}
		calls[i] = outbound.Call{Target: gauge, CallData: data, AllowFailure: true}
	}
	results, err := b.multicaller.Execute(ctx, calls, big.NewInt(blockNumber))
	if err != nil {
		return nil, fmt.Errorf("multicall gauge state at block %d: %w", blockNumber, err)
	}
	if len(results) != len(calls) {
		return nil, fmt.Errorf("gauge multicall returned %d results, expected %d", len(results), len(calls))
	}

	// inflation_rate / working_supply / totalSupply exist on every Curve
	// LiquidityGauge version, V1 (2020) onward. A revert on those means the
	// contract isn't a recognisable gauge (corrupted ABI, wrong address,
	// mid-upgrade proxy, etc.) — UNEXPECTED, so propagate so the SQS handler
	// can retry. AllowFailure: true is set per call only so the multicall
	// itself succeeds and we can attribute the failure to the specific method
	// below; the unpackUint helper turns r.Success=false into an error which
	// we re-wrap with method + block context.
	//
	// is_killed / reward_count were ADDED in LiquidityGaugeV2 — V1 gauges
	// (e.g. 3pool's 0xbFcF63294aD7105dEa65aA58F8AE5BE2D9d0952A) revert on
	// both, deterministically. Erroring here would poison the FIFO queue with
	// infinite redeliveries, so degrade to nil (= unknown / unsupported)
	// instead. Verified against mainnet in the 2026-06-02 E2E run.
	out := &gaugeMulticallResult{}
	inf, err := unpackUint(b.gaugeRead, "inflation_rate", results[0])
	if err != nil {
		return nil, fmt.Errorf("decoding inflation_rate for gauge %s at block %d: %w", gauge.Hex(), blockNumber, err)
	}
	out.InflationRate = inf
	ws, err := unpackUint(b.gaugeRead, "working_supply", results[1])
	if err != nil {
		return nil, fmt.Errorf("decoding working_supply for gauge %s at block %d: %w", gauge.Hex(), blockNumber, err)
	}
	out.WorkingSupply = ws
	ts, err := unpackUint(b.gaugeRead, "totalSupply", results[2])
	if err != nil {
		return nil, fmt.Errorf("decoding totalSupply for gauge %s at block %d: %w", gauge.Hex(), blockNumber, err)
	}
	out.TotalSupply = ts
	if results[3].Success {
		unpacked, err := b.gaugeRead.Unpack("is_killed", results[3].ReturnData)
		if err != nil || len(unpacked) != 1 {
			return nil, fmt.Errorf("decoding is_killed for gauge %s at block %d: %w", gauge.Hex(), blockNumber, err)
		}
		killed, ok := unpacked[0].(bool)
		if !ok {
			return nil, fmt.Errorf("is_killed returned %T, want bool", unpacked[0])
		}
		out.IsKilled = &killed
	} else {
		b.logger.Warn("is_killed unsupported on gauge (pre-V2); storing NULL",
			"gauge", gauge.Hex(), "block", blockNumber)
	}
	if results[4].Success {
		rc, err := unpackUint(b.gaugeRead, "reward_count", results[4])
		if err != nil {
			return nil, fmt.Errorf("decoding reward_count for gauge %s at block %d: %w", gauge.Hex(), blockNumber, err)
		}
		c := int32(rc.Int64())
		out.RewardCount = &c
	} else {
		b.logger.Warn("reward_count unsupported on gauge (pre-V2); storing NULL",
			"gauge", gauge.Hex(), "block", blockNumber)
	}
	return out, nil
}

// readGaugeRewards loads the per-reward-token data block (token, rate,
// period_finish) for `count` slots in one multicall round-trip.
func (b *blockchainService) readGaugeRewards(ctx context.Context, gauge common.Address, count int32, blockNumber int64) ([]common.Address, []*big.Int, []*big.Int, error) {
	if count <= 0 {
		return nil, nil, nil, nil
	}

	// First call set: reward_tokens(i) for each slot.
	tokCalls := make([]outbound.Call, count)
	for i := range count {
		data, err := b.gaugeRead.Pack("reward_tokens", big.NewInt(int64(i)))
		if err != nil {
			return nil, nil, nil, fmt.Errorf("packing reward_tokens(%d): %w", i, err)
		}
		tokCalls[i] = outbound.Call{Target: gauge, CallData: data, AllowFailure: true}
	}
	tokRes, err := b.multicaller.Execute(ctx, tokCalls, big.NewInt(blockNumber))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("multicall reward_tokens: %w", err)
	}
	// reward_tokens(i) is bounded by reward_count which we just read in the
	// previous multicall — every slot in [0, count) is expected to resolve.
	// A revert here is UNEXPECTED (suggests a non-standard gauge or mid-upgrade
	// proxy) so propagate; SQS will retry the message and we'll get a clean
	// snapshot on a stable block.
	tokens := make([]common.Address, count)
	for i, r := range tokRes {
		if !r.Success {
			return nil, nil, nil, fmt.Errorf("reward_tokens(%d) reverted on gauge %s at block %d", i, gauge.Hex(), blockNumber)
		}
		unpacked, err := b.gaugeRead.Unpack("reward_tokens", r.ReturnData)
		if err != nil || len(unpacked) == 0 {
			return nil, nil, nil, fmt.Errorf("decoding reward_tokens(%d) for gauge %s at block %d: %w", i, gauge.Hex(), blockNumber, err)
		}
		a, ok := unpacked[0].(common.Address)
		if !ok {
			return nil, nil, nil, fmt.Errorf("reward_tokens(%d) returned %T, want address", i, unpacked[0])
		}
		tokens[i] = a
	}

	// Second call set: reward_data(token) per resolved token.
	dataCalls := make([]outbound.Call, count)
	for i := range count {
		data, err := b.gaugeRead.Pack("reward_data", tokens[i])
		if err != nil {
			return nil, nil, nil, fmt.Errorf("packing reward_data(%d): %w", i, err)
		}
		dataCalls[i] = outbound.Call{Target: gauge, CallData: data, AllowFailure: true}
	}
	dataRes, err := b.multicaller.Execute(ctx, dataCalls, big.NewInt(blockNumber))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("multicall reward_data: %w", err)
	}
	// reward_data(token) is mandatory for every token reward_tokens returned
	// above; the gauge ABI guarantees it returns (token, distributor,
	// period_finish, rate, last_update, integral). A revert is UNEXPECTED —
	// propagate so the message retries.
	rates := make([]*big.Int, count)
	finishes := make([]*big.Int, count)
	for i, r := range dataRes {
		if !r.Success {
			return nil, nil, nil, fmt.Errorf("reward_data(%s) reverted on gauge %s at block %d", tokens[i].Hex(), gauge.Hex(), blockNumber)
		}
		unpacked, err := b.gaugeRead.Unpack("reward_data", r.ReturnData)
		if err != nil || len(unpacked) < 4 {
			return nil, nil, nil, fmt.Errorf("decoding reward_data(%s) for gauge %s at block %d: %w", tokens[i].Hex(), gauge.Hex(), blockNumber, err)
		}
		if pf, ok := unpacked[2].(*big.Int); ok {
			finishes[i] = pf
		}
		if rate, ok := unpacked[3].(*big.Int); ok {
			rates[i] = rate
		}
	}
	return tokens, rates, finishes, nil
}

// readLPBalances issues balanceOf(user) on the LP-token contract for every
// non-zero address in `users`, pinned to `blockNumber`. Returns a parallel
// slice — nil at slot i means the call reverted or didn't unpack (logged at
// warn level). lpToken should be the result of effectiveLPToken(pool).
func (b *blockchainService) readLPBalances(ctx context.Context, lpToken common.Address, users []common.Address, blockNumber int64) ([]*big.Int, error) {
	if len(users) == 0 {
		return nil, nil
	}
	calls := make([]outbound.Call, len(users))
	for i, u := range users {
		data, err := b.lpRead.Pack("balanceOf", u)
		if err != nil {
			return nil, fmt.Errorf("packing balanceOf(%s): %w", u.Hex(), err)
		}
		calls[i] = outbound.Call{Target: lpToken, CallData: data, AllowFailure: true}
	}
	results, err := b.multicaller.Execute(ctx, calls, big.NewInt(blockNumber))
	if err != nil {
		return nil, fmt.Errorf("multicall balanceOf: %w", err)
	}
	if len(results) != len(calls) {
		return nil, fmt.Errorf("balanceOf multicall returned %d results, expected %d", len(results), len(calls))
	}
	// ERC-20 balanceOf(address) on a healthy LP-token contract must succeed
	// for any 20-byte address (returns 0 for unknown users). A revert here
	// is UNEXPECTED — propagate so the SQS handler can retry.
	out := make([]*big.Int, len(users))
	for i, r := range results {
		v, err := unpackUint(b.lpRead, "balanceOf", r)
		if err != nil {
			return nil, fmt.Errorf("decoding balanceOf(%s) for LP token %s at block %d: %w", users[i].Hex(), lpToken.Hex(), blockNumber, err)
		}
		out[i] = v
	}
	return out, nil
}

// readERC20Metadata reads `symbol()` + `decimals()` on an ERC-20 token in a
// single multicall, pinned to `blockNumber`. Used when registering reward
// tokens encountered on a gauge that we haven't seen before — the worker
// must not register them with the hard-coded "" / 18 placeholders because
// any non-18-decimal token (USDC, WBTC, …) silently breaks every downstream
// "amount / 10^decimals" USD conversion. ERC-20 view methods on a real
// contract must succeed, so a revert here is UNEXPECTED and propagates.
func (b *blockchainService) readERC20Metadata(ctx context.Context, token common.Address, blockNumber int64) (string, uint8, error) {
	erc20, err := abis.GetERC20ABI()
	if err != nil {
		return "", 0, fmt.Errorf("loading ERC20 read ABI: %w", err)
	}
	symData, err := erc20.Pack("symbol")
	if err != nil {
		return "", 0, fmt.Errorf("packing symbol: %w", err)
	}
	decData, err := erc20.Pack("decimals")
	if err != nil {
		return "", 0, fmt.Errorf("packing decimals: %w", err)
	}
	results, err := b.multicaller.Execute(ctx, []outbound.Call{
		{Target: token, CallData: symData},
		{Target: token, CallData: decData},
	}, big.NewInt(blockNumber))
	if err != nil {
		return "", 0, fmt.Errorf("multicall ERC20 metadata: %w", err)
	}
	if len(results) != 2 {
		return "", 0, fmt.Errorf("ERC20 metadata multicall returned %d results, expected 2", len(results))
	}
	if !results[0].Success {
		return "", 0, fmt.Errorf("symbol() reverted on %s at block %d", token.Hex(), blockNumber)
	}
	symUnp, err := erc20.Unpack("symbol", results[0].ReturnData)
	if err != nil || len(symUnp) == 0 {
		return "", 0, fmt.Errorf("decoding symbol on %s: %w", token.Hex(), err)
	}
	symbol, ok := symUnp[0].(string)
	if !ok {
		return "", 0, fmt.Errorf("symbol returned %T, want string", symUnp[0])
	}
	if !results[1].Success {
		return "", 0, fmt.Errorf("decimals() reverted on %s at block %d", token.Hex(), blockNumber)
	}
	decUnp, err := erc20.Unpack("decimals", results[1].ReturnData)
	if err != nil || len(decUnp) == 0 {
		return "", 0, fmt.Errorf("decoding decimals on %s: %w", token.Hex(), err)
	}
	decimals, ok := decUnp[0].(uint8)
	if !ok {
		return "", 0, fmt.Errorf("decimals returned %T, want uint8", decUnp[0])
	}
	return symbol, decimals, nil
}

// readGaugeType issues `GaugeController.gauge_types(addr)` to determine which
// flavour of gauge this is. Mainnet convention: 0 = Liquidity (stableswap), the
// only type the worker indexes. Other values are crypto/lending/etc. and must
// be skipped without further probing. The view must succeed for any address
// (returns 0 for unknown gauges historically), so a revert is UNEXPECTED.
func (b *blockchainService) readGaugeType(ctx context.Context, controller, gauge common.Address, blockNumber int64) (*big.Int, error) {
	gcRead, err := abis.GetCurveGaugeControllerReadABI()
	if err != nil {
		return nil, fmt.Errorf("loading gauge controller read ABI: %w", err)
	}
	data, err := gcRead.Pack("gauge_types", gauge)
	if err != nil {
		return nil, fmt.Errorf("packing gauge_types(%s): %w", gauge.Hex(), err)
	}
	results, err := b.multicaller.Execute(ctx, []outbound.Call{{Target: controller, CallData: data}}, big.NewInt(blockNumber))
	if err != nil {
		return nil, fmt.Errorf("multicall gauge_types: %w", err)
	}
	if len(results) != 1 || !results[0].Success {
		return nil, fmt.Errorf("gauge_types(%s) reverted", gauge.Hex())
	}
	unpacked, err := gcRead.Unpack("gauge_types", results[0].ReturnData)
	if err != nil || len(unpacked) == 0 {
		return nil, fmt.Errorf("decoding gauge_types: %w", err)
	}
	v, ok := unpacked[0].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("gauge_types returned %T, want *big.Int", unpacked[0])
	}
	return v, nil
}

// readMetaRegistryGauge issues `MetaRegistry.get_gauge(pool)` on the canonical
// mainnet MetaRegistry at `0xF98B45FA17DE75FB1aD0e7aFD971b0ca00e379fC`. Returns
// the zero address if the pool has no registered gauge. The MetaRegistry view
// must succeed for any pool address (it returns zero for unknown pools rather
// than reverting), so a revert here is UNEXPECTED and propagates.
//
// blockNumber == nil means "latest" (multicaller convention). The startup
// bootstrap path passes nil because there's no event block in scope; pinning
// to genesis (big.NewInt(0)) would hit "no code at address" since the
// MetaRegistry was deployed in 2022.
func (b *blockchainService) readMetaRegistryGauge(ctx context.Context, metaRegistry, pool common.Address, blockNumber *big.Int) (common.Address, error) {
	mr, err := abis.GetCurveMetaRegistryReadABI()
	if err != nil {
		return common.Address{}, fmt.Errorf("loading MetaRegistry read ABI: %w", err)
	}
	data, err := mr.Pack("get_gauge", pool)
	if err != nil {
		return common.Address{}, fmt.Errorf("packing get_gauge(%s): %w", pool.Hex(), err)
	}
	results, err := b.multicaller.Execute(ctx, []outbound.Call{{Target: metaRegistry, CallData: data}}, blockNumber)
	if err != nil {
		return common.Address{}, fmt.Errorf("multicall MetaRegistry.get_gauge: %w", err)
	}
	if len(results) != 1 || !results[0].Success {
		return common.Address{}, fmt.Errorf("MetaRegistry.get_gauge(%s) reverted", pool.Hex())
	}
	unpacked, err := mr.Unpack("get_gauge", results[0].ReturnData)
	if err != nil || len(unpacked) == 0 {
		return common.Address{}, fmt.Errorf("decoding MetaRegistry.get_gauge: %w", err)
	}
	addr, ok := unpacked[0].(common.Address)
	if !ok {
		return common.Address{}, fmt.Errorf("MetaRegistry.get_gauge returned %T, want address", unpacked[0])
	}
	return addr, nil
}

// readGaugeLPToken reads gauge.lp_token() to resolve which pool a gauge belongs
// to. Used during gauge discovery from GaugeController NewGauge events.
func (b *blockchainService) readGaugeLPToken(ctx context.Context, gauge common.Address, blockNumber int64) (common.Address, error) {
	data, err := b.gaugeRead.Pack("lp_token")
	if err != nil {
		return common.Address{}, fmt.Errorf("packing lp_token: %w", err)
	}
	results, err := b.multicaller.Execute(ctx, []outbound.Call{{Target: gauge, CallData: data, AllowFailure: true}}, big.NewInt(blockNumber))
	if err != nil {
		return common.Address{}, fmt.Errorf("multicall lp_token: %w", err)
	}
	if len(results) != 1 || !results[0].Success {
		return common.Address{}, fmt.Errorf("gauge %s lp_token() reverted", gauge.Hex())
	}
	unpacked, err := b.gaugeRead.Unpack("lp_token", results[0].ReturnData)
	if err != nil || len(unpacked) == 0 {
		return common.Address{}, fmt.Errorf("decoding lp_token: %w", err)
	}
	a, ok := unpacked[0].(common.Address)
	if !ok {
		return common.Address{}, fmt.Errorf("lp_token returned %T, want address", unpacked[0])
	}
	return a, nil
}

// tryUnpackUint wraps unpackUint with a warn log on revert/unpack failure and
// returns nil instead of an error so the caller can keep building a partial
// result row. Use only for sub-calls whose absence is acceptable (oracles,
// gauge metrics); callers needing strict success should use unpackUint.
func (b *blockchainService) tryUnpackUint(ctx context.Context, a *abi.ABI, method string, r outbound.Result, target common.Address, blockNumber int64) *big.Int {
	v, err := unpackUint(a, method, r)
	if err != nil {
		b.logger.WarnContext(ctx, "multicall sub-call failed",
			"method", method, "target", target.Hex(), "block", blockNumber, "err", err)
		return nil
	}
	return v
}

// unpackUint decodes a single uint256-returning view method's result.
func unpackUint(a *abi.ABI, method string, r outbound.Result) (*big.Int, error) {
	if !r.Success {
		return nil, fmt.Errorf("%s reverted", method)
	}
	unpacked, err := a.Unpack(method, r.ReturnData)
	if err != nil {
		return nil, err
	}
	if len(unpacked) == 0 {
		return nil, fmt.Errorf("%s returned no values", method)
	}
	v, ok := unpacked[0].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("%s returned %T, want *big.Int", method, unpacked[0])
	}
	return v, nil
}

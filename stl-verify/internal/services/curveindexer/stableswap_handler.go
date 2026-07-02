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
		if !logBelongsToPool(addr, pool) {
			continue
		}

		logIndex, err := shared.ParseHexUint(log.LogIndex)
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

		// Pre-NG pools emit fixed-array liquidity events whose topic0s differ from the
		// NG ABI. Dispatch them via word-slicing before falling back to the ABI lookup.
		if pool.Kind == KindStableswapPreNG {
			rec, matched, err := decodeClassicLiquidity(log, pool, sigs)
			if err != nil {
				return DecodedEvents{}, fmt.Errorf("decoding classic liquidity log (index %s): %w", log.LogIndex, err)
			}
			if matched {
				result.Liquidity = append(result.Liquidity, *rec)
				result.Captured, err = appendRawCaptured(result.Captured, addr, logIndex, txHash, log.Topics[0], log)
				if err != nil {
					return DecodedEvents{}, err
				}
				continue
			}
		}

		ev, known := h.eventsByID[topic0]

		if !known {
			result.Captured, err = appendRawCaptured(result.Captured, addr, logIndex, txHash, log.Topics[0], log)
			if err != nil {
				return DecodedEvents{}, err
			}
			continue
		}

		eventData, err := shared.DecodeLog(*ev, log)
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
		result.Captured, err = appendDecodedCaptured(result.Captured, addr, logIndex, txHash, ev.Name, eventData)
		if err != nil {
			return DecodedEvents{}, err
		}
	}

	return result, nil
}

// SnapshotState reads stableswap pool state at the given block via multicall,
// pinned to blockHash (see outbound.Multicaller.ExecuteAtHash) so a reorg
// between publish and processing cannot silently answer from the wrong fork.
// Every issued call that reverts propagates as an error (transient-retry
// contract): a reverted read is never collapsed into a nil/NULL field. Reads
// that legitimately do not exist for a pool class are not issued at all
// (NG-only price_oracle/last_price/stored_rates/ema_price/get_p, pre-NG-only
// future_admin_fee), so a NULL column is always a structural fact, never a
// swallowed failure.
func (h *StableswapHandler) SnapshotState(
	ctx context.Context,
	mc outbound.Multicaller,
	pool RegisteredPool,
	blockNumber int64,
	version int,
	blockHash common.Hash,
	ts time.Time,
) (StateSnapshot, error) {
	acc := &stableswapSnapshotAcc{}
	reads := h.stableswapSnapshotReads(pool, blockNumber, acc)
	if err := shared.RunSnapshotReads(ctx, mc, pool, blockHash, reads); err != nil {
		return StateSnapshot{}, fmt.Errorf("snapshotting pool %s state: %w", pool.Address, err)
	}

	st, cfg, err := acc.build(pool, blockNumber, version, ts)
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

// stableswapSnapshotAcc accumulates the raw getter results for one pool
// snapshot as shared.SnapshotRead Decode callbacks fill it in, then builds the
// state and config rows in build(). Each field's zero value (nil) is only
// ever observed for a read that was structurally gated out for this pool
// (NG-only, pre-NG-only, or HasAPrecise-gated); a reverted issued read never
// reaches build() as nil because optUint below turns a revert into an error.
type stableswapSnapshotAcc struct {
	balances     []*big.Int
	virtualPrice *big.Int
	totalSupply  *big.Int
	amp          *big.Int
	fee          *big.Int
	spotDy       []*big.Int

	priceOracle *big.Int // NG only
	lastPrice   *big.Int // NG only

	aPrecise        *big.Int // HasAPrecise only
	adminBalances   []*big.Int
	calcTokenAmount *big.Int
	calcWithdraw    []*big.Int

	storedRates []*big.Int // NG only
	emaPrice    *big.Int   // NG only
	getP        *big.Int   // NG only

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

// build assembles the state and config rows from the accumulated reads. It is
// called once RunSnapshotReads has driven every read's Decode.
func (acc *stableswapSnapshotAcc) build(pool RegisteredPool, blockNumber int64, version int, ts time.Time) (*entity.CurveStableswapState, *entity.CurveStableswapConfig, error) {
	state, err := entity.NewCurveStableswapState(entity.CurveStableswapStateParams{
		CurvePoolID:         pool.ID,
		BlockNumber:         blockNumber,
		BlockVersion:        version,
		Timestamp:           ts,
		Balances:            acc.balances,
		VirtualPrice:        acc.virtualPrice,
		TotalSupply:         acc.totalSupply,
		Amp:                 acc.amp,
		Fee:                 acc.fee,
		SpotDy:              acc.spotDy,
		LastPrice:           acc.lastPrice,
		PriceOracle:         acc.priceOracle,
		APrecise:            acc.aPrecise,
		AdminBalances:       acc.adminBalances,
		StoredRates:         acc.storedRates,
		EmaPrice:            acc.emaPrice,
		GetP:                acc.getP,
		CalcTokenAmount:     acc.calcTokenAmount,
		CalcWithdrawOneCoin: acc.calcWithdraw,
	})
	if err != nil {
		return nil, nil, err
	}

	cfg, err := buildStableswapConfig(pool, blockNumber, version, ts, stableswapConfigReads{
		initialA:       acc.initialA,
		initialATime:   acc.initialATime,
		futureA:        acc.futureA,
		futureATime:    acc.futureATime,
		adminFee:       acc.adminFee,
		futureFee:      acc.futureFee,
		futureAdminFee: acc.futureAdminFee,
		maExpTime:      acc.maExpTime,
		oracleMethod:   acc.oracleMethod,
	})
	if err != nil {
		return nil, nil, err
	}

	return state, cfg, nil
}

// stableswapSnapshotReads describes the full stableswap snapshot as
// self-contained pack/decode units, in the exact order the calls are packed
// into the multicall. Conditional reads (NG-only, HasAPrecise-gated, pre-NG
// only) are appended only when their gate holds, so a gated-out read
// contributes neither a call nor a decode step rather than being skipped by a
// branch inside a shared decode loop.
func (h *StableswapHandler) stableswapSnapshotReads(pool RegisteredPool, blockNumber int64, acc *stableswapSnapshotAcc) []shared.SnapshotRead[RegisteredPool] {
	var reads []shared.SnapshotRead[RegisteredPool]

	// 1. balances(i) for each coin.
	for i := 0; i < pool.NCoins; i++ {
		i := i
		reads = append(reads, shared.SnapshotRead[RegisteredPool]{
			Name: fmt.Sprintf("balances(%d)", i),
			Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
				data, err := h.stableABI.Pack("balances", big.NewInt(int64(i)))
				if err != nil {
					return nil, fmt.Errorf("packing balances(%d): %w", i, err)
				}
				return []outbound.Call{{Target: pool.Address, AllowFailure: false, CallData: data}}, nil
			},
			Decode: func(pool RegisteredPool, results []outbound.Result) error {
				v, err := shared.UnpackUint(h.stableABI, "balances", results[0])
				if err != nil {
					return fmt.Errorf("balances(%d): %w", i, err)
				}
				if acc.balances == nil {
					acc.balances = make([]*big.Int, pool.NCoins)
				}
				acc.balances[i] = v
				return nil
			},
		})
	}

	// 2. get_virtual_price()
	reads = append(reads, shared.SnapshotRead[RegisteredPool]{
		Name: "get_virtual_price",
		Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
			data, err := h.stableABI.Pack("get_virtual_price")
			if err != nil {
				return nil, fmt.Errorf("packing get_virtual_price: %w", err)
			}
			return []outbound.Call{{Target: pool.Address, AllowFailure: false, CallData: data}}, nil
		},
		Decode: func(pool RegisteredPool, results []outbound.Result) error {
			v, err := shared.UnpackUint(h.stableABI, "get_virtual_price", results[0])
			if err != nil {
				return fmt.Errorf("get_virtual_price: %w", err)
			}
			acc.virtualPrice = v
			return nil
		},
	})

	// 3. totalSupply(). Lives on the LP token, which for pre-NG pools is a
	// separate contract from the pool; falls back to the pool address for NG
	// pools that are their own LP token.
	reads = append(reads, shared.SnapshotRead[RegisteredPool]{
		Name: "totalSupply",
		Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
			lpTarget := pool.Address
			if pool.LpTokenAddress != nil {
				lpTarget = *pool.LpTokenAddress
			}
			data, err := h.stableABI.Pack("totalSupply")
			if err != nil {
				return nil, fmt.Errorf("packing totalSupply: %w", err)
			}
			return []outbound.Call{{Target: lpTarget, AllowFailure: false, CallData: data}}, nil
		},
		Decode: func(pool RegisteredPool, results []outbound.Result) error {
			v, err := shared.UnpackUint(h.stableABI, "totalSupply", results[0])
			if err != nil {
				return fmt.Errorf("totalSupply: %w", err)
			}
			acc.totalSupply = v
			return nil
		},
	})

	// 4. A()
	reads = append(reads, shared.SnapshotRead[RegisteredPool]{
		Name: "A",
		Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
			data, err := h.stableABI.Pack("A")
			if err != nil {
				return nil, fmt.Errorf("packing A: %w", err)
			}
			return []outbound.Call{{Target: pool.Address, AllowFailure: false, CallData: data}}, nil
		},
		Decode: func(pool RegisteredPool, results []outbound.Result) error {
			v, err := shared.UnpackUint(h.stableABI, "A", results[0])
			if err != nil {
				return fmt.Errorf("decoding A: %w", err)
			}
			acc.amp = v
			return nil
		},
	})

	// 5. fee()
	reads = append(reads, shared.SnapshotRead[RegisteredPool]{
		Name: "fee",
		Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
			data, err := h.stableABI.Pack("fee")
			if err != nil {
				return nil, fmt.Errorf("packing fee: %w", err)
			}
			return []outbound.Call{{Target: pool.Address, AllowFailure: false, CallData: data}}, nil
		},
		Decode: func(pool RegisteredPool, results []outbound.Result) error {
			v, err := shared.UnpackUint(h.stableABI, "fee", results[0])
			if err != nil {
				return fmt.Errorf("fee: %w", err)
			}
			acc.fee = v
			return nil
		},
	})

	// 6. get_dy(i, j, 10^decimals[i]) for every ordered pair i!=j, packed and
	// decoded together as one N*(N-1)-call read so the pair ordering lives in
	// one place.
	reads = append(reads, shared.SnapshotRead[RegisteredPool]{
		Name: "get_dy",
		Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
			var calls []outbound.Call
			for i := 0; i < pool.NCoins; i++ {
				for j := 0; j < pool.NCoins; j++ {
					if i == j {
						continue
					}
					if i >= len(pool.CoinDecimals) {
						return nil, fmt.Errorf("pool %s coin %d missing decimals (have %d, n_coins %d)", pool.Address, i, len(pool.CoinDecimals), pool.NCoins)
					}
					dx := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(pool.CoinDecimals[i])), nil)
					data, err := h.stableABI.Pack("get_dy", big.NewInt(int64(i)), big.NewInt(int64(j)), dx)
					if err != nil {
						return nil, fmt.Errorf("packing get_dy(%d,%d): %w", i, j, err)
					}
					calls = append(calls, outbound.Call{Target: pool.Address, AllowFailure: false, CallData: data})
				}
			}
			return calls, nil
		},
		Decode: func(pool RegisteredPool, results []outbound.Result) error {
			nPairs := pool.NCoins * (pool.NCoins - 1)
			spotDy := make([]*big.Int, 0, nPairs)
			k := 0
			for i := 0; i < pool.NCoins; i++ {
				for j := 0; j < pool.NCoins; j++ {
					if i == j {
						continue
					}
					v, err := shared.UnpackUint(h.stableABI, "get_dy", results[k])
					if err != nil {
						return fmt.Errorf("get_dy(%d,%d): %w", i, j, err)
					}
					spotDy = append(spotDy, v)
					k++
				}
			}
			acc.spotDy = spotDy
			return nil
		},
	})

	// 7. NG-only: price_oracle() and last_price() with AllowFailure=true.
	//
	// These no-arg oracle getters (and stored_rates/ema_price/get_p below) are
	// gated by class, NOT by a per-pool capability like A_precise (curated in
	// curve_pool.has_a_precise). Every stETH-ng-shaped plain_ng pool exposes them,
	// but some plain_ng pools (e.g. GHO/crvUSD) expose only the indexed
	// price_oracle(uint256) form and revert on the no-arg selector, which would
	// poison-stall the block. Before seeding such a pool, add curated capability
	// columns for these getters as we did for A_precise (deferred to the
	// pool-expansion follow-up, VEC-330/331).
	if pool.Kind == KindStableswapNG {
		reads = append(reads, shared.SnapshotRead[RegisteredPool]{
			Name: "price_oracle",
			Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
				data, err := h.stableABI.Pack("price_oracle")
				if err != nil {
					return nil, fmt.Errorf("packing price_oracle: %w", err)
				}
				return []outbound.Call{{Target: pool.Address, AllowFailure: true, CallData: data}}, nil
			},
			Decode: func(pool RegisteredPool, results []outbound.Result) error {
				v, err := shared.OptionalUintResult(h.stableABI, "price_oracle", results[0], pool.Address, blockNumber)
				if err != nil {
					return fmt.Errorf("price_oracle: %w", err)
				}
				acc.priceOracle = v
				return nil
			},
		})
		reads = append(reads, shared.SnapshotRead[RegisteredPool]{
			Name: "last_price",
			Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
				data, err := h.stableABI.Pack("last_price")
				if err != nil {
					return nil, fmt.Errorf("packing last_price: %w", err)
				}
				return []outbound.Call{{Target: pool.Address, AllowFailure: true, CallData: data}}, nil
			},
			Decode: func(pool RegisteredPool, results []outbound.Result) error {
				v, err := shared.OptionalUintResult(h.stableABI, "last_price", results[0], pool.Address, blockNumber)
				if err != nil {
					return fmt.Errorf("last_price: %w", err)
				}
				acc.lastPrice = v
				return nil
			},
		})
	}

	// 8. A_precise(), gated by pool.HasAPrecise (see the field doc on RegisteredPool).
	if pool.HasAPrecise {
		reads = append(reads, shared.SnapshotRead[RegisteredPool]{
			Name: "A_precise",
			Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
				data, err := h.stableABI.Pack("A_precise")
				if err != nil {
					return nil, fmt.Errorf("packing A_precise: %w", err)
				}
				return []outbound.Call{{Target: pool.Address, AllowFailure: true, CallData: data}}, nil
			},
			Decode: func(pool RegisteredPool, results []outbound.Result) error {
				v, err := shared.OptionalUintResult(h.stableABI, "A_precise", results[0], pool.Address, blockNumber)
				if err != nil {
					return fmt.Errorf("reading A_precise: %w", err)
				}
				acc.aPrecise = v
				return nil
			},
		})
	}

	// 9. admin_balances(i) for each coin.
	reads = append(reads, shared.SnapshotRead[RegisteredPool]{
		Name: "admin_balances",
		Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
			var calls []outbound.Call
			for i := 0; i < pool.NCoins; i++ {
				data, err := h.stableABI.Pack("admin_balances", big.NewInt(int64(i)))
				if err != nil {
					return nil, fmt.Errorf("packing admin_balances(%d): %w", i, err)
				}
				calls = append(calls, outbound.Call{Target: pool.Address, AllowFailure: true, CallData: data})
			}
			return calls, nil
		},
		Decode: func(pool RegisteredPool, results []outbound.Result) error {
			adminBalances := make([]*big.Int, pool.NCoins)
			for i := 0; i < pool.NCoins; i++ {
				v, err := shared.OptionalUintResult(h.stableABI, "admin_balances", results[i], pool.Address, blockNumber)
				if err != nil {
					return fmt.Errorf("admin_balances(%d): %w", i, err)
				}
				adminBalances[i] = v
			}
			acc.adminBalances = adminBalances
			return nil
		},
	})

	// 10. calc_token_amount(unit deposit of 10^decimals[i] per coin, is_deposit=true)
	reads = append(reads, shared.SnapshotRead[RegisteredPool]{
		Name: "calc_token_amount",
		Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
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
			return []outbound.Call{{Target: pool.Address, AllowFailure: true, CallData: data}}, nil
		},
		Decode: func(pool RegisteredPool, results []outbound.Result) error {
			v, err := shared.UnpackSingleUint(results[0])
			if err != nil {
				return fmt.Errorf("calc_token_amount: %w", err)
			}
			acc.calcTokenAmount = v
			return nil
		},
	})

	// 11. calc_withdraw_one_coin(1e18, i) for each coin.
	reads = append(reads, shared.SnapshotRead[RegisteredPool]{
		Name: "calc_withdraw_one_coin",
		Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
			oneLP := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
			var calls []outbound.Call
			for i := 0; i < pool.NCoins; i++ {
				data, err := h.stableABI.Pack("calc_withdraw_one_coin", oneLP, big.NewInt(int64(i)))
				if err != nil {
					return nil, fmt.Errorf("packing calc_withdraw_one_coin(%d): %w", i, err)
				}
				calls = append(calls, outbound.Call{Target: pool.Address, AllowFailure: true, CallData: data})
			}
			return calls, nil
		},
		Decode: func(pool RegisteredPool, results []outbound.Result) error {
			calcWithdraw := make([]*big.Int, pool.NCoins)
			for i := 0; i < pool.NCoins; i++ {
				v, err := shared.OptionalUintResult(h.stableABI, "calc_withdraw_one_coin", results[i], pool.Address, blockNumber)
				if err != nil {
					return fmt.Errorf("calc_withdraw_one_coin(%d): %w", i, err)
				}
				calcWithdraw[i] = v
			}
			acc.calcWithdraw = calcWithdraw
			return nil
		},
	})

	// 12. NG-only: stored_rates(), ema_price(), get_p()
	if pool.Kind == KindStableswapNG {
		reads = append(reads, shared.SnapshotRead[RegisteredPool]{
			Name: "stored_rates",
			Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
				data, err := h.stableABI.Pack("stored_rates")
				if err != nil {
					return nil, fmt.Errorf("packing stored_rates: %w", err)
				}
				return []outbound.Call{{Target: pool.Address, AllowFailure: true, CallData: data}}, nil
			},
			Decode: func(pool RegisteredPool, results []outbound.Result) error {
				v, err := shared.UnpackUintArray(results[0], pool.NCoins)
				if err != nil {
					return fmt.Errorf("stored_rates: %w", err)
				}
				acc.storedRates = v
				return nil
			},
		})
		reads = append(reads, shared.SnapshotRead[RegisteredPool]{
			Name: "ema_price",
			Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
				data, err := h.stableABI.Pack("ema_price")
				if err != nil {
					return nil, fmt.Errorf("packing ema_price: %w", err)
				}
				return []outbound.Call{{Target: pool.Address, AllowFailure: true, CallData: data}}, nil
			},
			Decode: func(pool RegisteredPool, results []outbound.Result) error {
				v, err := shared.OptionalUintResult(h.stableABI, "ema_price", results[0], pool.Address, blockNumber)
				if err != nil {
					return fmt.Errorf("ema_price: %w", err)
				}
				acc.emaPrice = v
				return nil
			},
		})
		reads = append(reads, shared.SnapshotRead[RegisteredPool]{
			Name: "get_p",
			Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
				data, err := h.stableABI.Pack("get_p")
				if err != nil {
					return nil, fmt.Errorf("packing get_p: %w", err)
				}
				return []outbound.Call{{Target: pool.Address, AllowFailure: true, CallData: data}}, nil
			},
			Decode: func(pool RegisteredPool, results []outbound.Result) error {
				v, err := shared.OptionalUintResult(h.stableABI, "get_p", results[0], pool.Address, blockNumber)
				if err != nil {
					return fmt.Errorf("get_p: %w", err)
				}
				acc.getP = v
				return nil
			},
		})
	}

	// 13. config getters (both classes): initial_A, initial_A_time, future_A,
	// future_A_time, admin_fee, future_fee.
	configGetters := []struct {
		name string
		dst  **big.Int
	}{
		{"initial_A", &acc.initialA},
		{"initial_A_time", &acc.initialATime},
		{"future_A", &acc.futureA},
		{"future_A_time", &acc.futureATime},
		{"admin_fee", &acc.adminFee},
		{"future_fee", &acc.futureFee},
	}
	for _, g := range configGetters {
		reads = append(reads, shared.SnapshotRead[RegisteredPool]{
			Name: g.name,
			Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
				data, err := h.stableABI.Pack(g.name)
				if err != nil {
					return nil, fmt.Errorf("packing %s: %w", g.name, err)
				}
				return []outbound.Call{{Target: pool.Address, AllowFailure: true, CallData: data}}, nil
			},
			Decode: func(pool RegisteredPool, results []outbound.Result) error {
				v, err := shared.OptionalUintResult(h.stableABI, g.name, results[0], pool.Address, blockNumber)
				if err != nil {
					return fmt.Errorf("%s: %w", g.name, err)
				}
				*g.dst = v
				return nil
			},
		})
	}

	// 14. pre-NG only: future_admin_fee()
	if pool.Kind == KindStableswapPreNG {
		reads = append(reads, shared.SnapshotRead[RegisteredPool]{
			Name: "future_admin_fee",
			Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
				data, err := h.stableABI.Pack("future_admin_fee")
				if err != nil {
					return nil, fmt.Errorf("packing future_admin_fee: %w", err)
				}
				return []outbound.Call{{Target: pool.Address, AllowFailure: true, CallData: data}}, nil
			},
			Decode: func(pool RegisteredPool, results []outbound.Result) error {
				v, err := shared.OptionalUintResult(h.stableABI, "future_admin_fee", results[0], pool.Address, blockNumber)
				if err != nil {
					return fmt.Errorf("future_admin_fee: %w", err)
				}
				acc.futureAdminFee = v
				return nil
			},
		})
	}

	// 15. NG-only: ma_exp_time(), oracle_method()
	if pool.Kind == KindStableswapNG {
		reads = append(reads, shared.SnapshotRead[RegisteredPool]{
			Name: "ma_exp_time",
			Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
				data, err := h.stableABI.Pack("ma_exp_time")
				if err != nil {
					return nil, fmt.Errorf("packing ma_exp_time: %w", err)
				}
				return []outbound.Call{{Target: pool.Address, AllowFailure: true, CallData: data}}, nil
			},
			Decode: func(pool RegisteredPool, results []outbound.Result) error {
				v, err := shared.OptionalUintResult(h.stableABI, "ma_exp_time", results[0], pool.Address, blockNumber)
				if err != nil {
					return fmt.Errorf("ma_exp_time: %w", err)
				}
				acc.maExpTime = v
				return nil
			},
		})
		reads = append(reads, shared.SnapshotRead[RegisteredPool]{
			Name: "oracle_method",
			Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
				data, err := h.stableABI.Pack("oracle_method")
				if err != nil {
					return nil, fmt.Errorf("packing oracle_method: %w", err)
				}
				return []outbound.Call{{Target: pool.Address, AllowFailure: true, CallData: data}}, nil
			},
			Decode: func(pool RegisteredPool, results []outbound.Result) error {
				v, err := shared.OptionalUintResult(h.stableABI, "oracle_method", results[0], pool.Address, blockNumber)
				if err != nil {
					return fmt.Errorf("oracle_method: %w", err)
				}
				acc.oracleMethod = v
				return nil
			},
		})
	}

	return reads
}

// stableswapConfigReads holds the raw config getter results. A field is nil only
// when its getter was structurally not issued (futureAdminFee for NG, the NG-only
// fields for pre-NG); an issued getter that reverted is an error upstream, never a
// nil here.
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
// reads. The four required NOT-NULL value fields (initial_a, future_a, admin_fee,
// future_fee) are always issued for both classes, so a nil here means an
// upstream decode bug rather than a real revert (a revert errors in optUint); we
// fail hard rather than persist a partial config row. The *_time fields are always
// issued; a successful read outside int64 is an error, not a coercion to 0.
func buildStableswapConfig(
	pool RegisteredPool,
	blockNumber int64,
	version int,
	ts time.Time,
	r stableswapConfigReads,
) (*entity.CurveStableswapConfig, error) {
	if r.initialA == nil || r.futureA == nil || r.adminFee == nil || r.futureFee == nil {
		return nil, fmt.Errorf("stableswap config for pool %s missing a required getter (initial_a/future_a/admin_fee/future_fee)", pool.Address)
	}
	// timeOrError converts a non-nil *big.Int to int64. A nil value is only
	// possible for a field that was structurally not issued (none of the time
	// fields here are in that category), so we treat it as a decode bug and
	// hard-error. An out-of-int64-range value is also an error: silently
	// coercing to 0 would persist a wrong timestamp without any signal.
	timeOrError := func(name string, b *big.Int) (int64, error) {
		if b == nil {
			return 0, fmt.Errorf("stableswap config for pool %s: %s getter returned nil (decode bug)", pool.Address, name)
		}
		if !b.IsInt64() {
			return 0, fmt.Errorf("stableswap config for pool %s: %s value %s overflows int64", pool.Address, name, b.String())
		}
		return b.Int64(), nil
	}
	initialATime, err := timeOrError("initial_A_time", r.initialATime)
	if err != nil {
		return nil, err
	}
	futureATime, err := timeOrError("future_A_time", r.futureATime)
	if err != nil {
		return nil, err
	}
	// maExpTime is nil when not issued (pre-NG pools: the call is structurally
	// gated out), so nil is the correct absent value. An issued call that
	// returned a value outside int64 is still an error.
	var maExpTime *int64
	if r.maExpTime != nil {
		if !r.maExpTime.IsInt64() {
			return nil, fmt.Errorf("stableswap config for pool %s: ma_exp_time value %s overflows int64", pool.Address, r.maExpTime.String())
		}
		v := r.maExpTime.Int64()
		maExpTime = &v
	}
	return entity.NewCurveStableswapConfig(entity.CurveStableswapConfigParams{
		CurvePoolID:    pool.ID,
		BlockNumber:    blockNumber,
		BlockVersion:   version,
		Timestamp:      ts,
		InitialA:       r.initialA,
		InitialATime:   initialATime,
		FutureA:        r.futureA,
		FutureATime:    futureATime,
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
	buyer, err := shared.GetAddrField(data, "buyer")
	if err != nil {
		return SwapRecord{}, err
	}
	soldID, err := shared.GetBigIntField(data, "sold_id")
	if err != nil {
		return SwapRecord{}, err
	}
	tokensSold, err := shared.GetBigIntField(data, "tokens_sold")
	if err != nil {
		return SwapRecord{}, err
	}
	boughtID, err := shared.GetBigIntField(data, "bought_id")
	if err != nil {
		return SwapRecord{}, err
	}
	tokensBought, err := shared.GetBigIntField(data, "tokens_bought")
	if err != nil {
		return SwapRecord{}, err
	}
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
	provider, err := shared.GetAddrField(data, "provider")
	if err != nil {
		return LiquidityRecord{}, err
	}
	amounts, err := shared.GetBigIntSliceField(data, "token_amounts")
	if err != nil {
		return LiquidityRecord{}, err
	}
	fees, err := shared.GetBigIntSliceField(data, "fees")
	if err != nil {
		return LiquidityRecord{}, err
	}
	invariant, err := shared.GetBigIntField(data, "invariant")
	if err != nil {
		return LiquidityRecord{}, err
	}
	supply, err := shared.GetBigIntField(data, "token_supply")
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
	provider, err := shared.GetAddrField(data, "provider")
	if err != nil {
		return LiquidityRecord{}, err
	}
	amounts, err := shared.GetBigIntSliceField(data, "token_amounts")
	if err != nil {
		return LiquidityRecord{}, err
	}
	fees, err := shared.GetBigIntSliceField(data, "fees")
	if err != nil {
		return LiquidityRecord{}, err
	}
	supply, err := shared.GetBigIntField(data, "token_supply")
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
	provider, err := shared.GetAddrField(data, "provider")
	if err != nil {
		return LiquidityRecord{}, err
	}
	// token_id is int128 in the NG ABI; go-ethereum decodes it as *big.Int.
	tokenID, err := shared.GetBigIntField(data, "token_id")
	if err != nil {
		return LiquidityRecord{}, err
	}
	tokenAmount, err := shared.GetBigIntField(data, "token_amount")
	if err != nil {
		return LiquidityRecord{}, err
	}
	coinAmount, err := shared.GetBigIntField(data, "coin_amount")
	if err != nil {
		return LiquidityRecord{}, err
	}
	supply, err := shared.GetBigIntField(data, "token_supply")
	if err != nil {
		return LiquidityRecord{}, err
	}
	// token_id is int128; reject anything outside [0, NCoins) rather than store a
	// garbage coin_index.
	coinIdx, err := coinIndexOrError("stableswap RemoveLiquidityOne coin_index", tokenID, pool.NCoins)
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
	provider, err := shared.GetAddrField(data, "provider")
	if err != nil {
		return LiquidityRecord{}, err
	}
	amounts, err := shared.GetBigIntSliceField(data, "token_amounts")
	if err != nil {
		return LiquidityRecord{}, err
	}
	fees, err := shared.GetBigIntSliceField(data, "fees")
	if err != nil {
		return LiquidityRecord{}, err
	}
	invariant, err := shared.GetBigIntField(data, "invariant")
	if err != nil {
		return LiquidityRecord{}, err
	}
	supply, err := shared.GetBigIntField(data, "token_supply")
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

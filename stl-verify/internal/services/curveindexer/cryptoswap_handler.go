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
	"github.com/archon-research/stl/stl-verify/internal/services/dexconsumer"
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
// dexconsumer.CapturedLog so protocol_event is a complete mirror of the on-chain
// log surface. Typed events carry a JSON payload of their decoded fields; unknown
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

		logIndex, err := shared.ParseHexUint(log.LogIndex)
		if err != nil {
			return DecodedEvents{}, fmt.Errorf("parsing log index %q: %w", log.LogIndex, err)
		}
		txHash := common.HexToHash(log.TransactionHash)

		if len(log.Topics) == 0 {
			result.Captured, err = appendRawCaptured(result.Captured, addr, logIndex, txHash, dexconsumer.AnonymousLogEventName, log)
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

		eventData, err := shared.DecodeLog(*ev, log)
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
		captured, err := dexconsumer.NewDecodedCapturedLog(addr, logIndex, txHash, ev.Name, eventData)
		if err != nil {
			return DecodedEvents{}, err
		}
		result.Captured = append(result.Captured, captured)
	}

	return result, nil
}

// SnapshotState reads cryptoswap pool state at the given block via multicall,
// pinned to blockHash (see outbound.Multicaller.ExecuteAtHash) so a reorg
// between publish and processing cannot silently answer from the wrong fork.
// Every issued call that reverts propagates as an error (no-swallowed-errors):
// a reverted read is never collapsed into a nil/NULL field. admin_balances() is
// not issued for cryptoswap pools (see cryptoswapExtendedSnapshotReads), so its
// column is NULL by structural design rather than because a call silently failed.
func (h *CryptoswapHandler) SnapshotState(
	ctx context.Context,
	mc outbound.Multicaller,
	pool RegisteredPool,
	blockNumber int64,
	version int,
	blockHash common.Hash,
	ts time.Time,
) (*entity.CurveCryptoswapState, *entity.CurveCryptoswapConfig, error) {
	acc := &cryptoswapSnapshotAcc{}
	reads := h.cryptoswapSnapshotReads(pool, blockNumber, acc)
	if err := shared.RunSnapshotReads(ctx, mc, pool, blockHash, reads); err != nil {
		return nil, nil, fmt.Errorf("snapshotting pool %s state: %w", pool.Address, err)
	}

	st, cfg, err := acc.build(pool, blockNumber, version, ts)
	if err != nil {
		return nil, nil, fmt.Errorf("decoding snapshot results for pool %s: %w", pool.Address, err)
	}

	return st, cfg, nil
}

// cryptoswapConfigGetters is the ordered list of config view methods read at the
// tail of every cryptoswap snapshot, in cryptoswapConfigReads field order.
var cryptoswapConfigGetters = []string{
	"initial_A_gamma", "future_A_gamma", "initial_A_gamma_time", "future_A_gamma_time",
	"mid_fee", "out_fee", "fee_gamma", "allowed_extra_profit", "adjustment_step",
	"ma_time", "ADMIN_FEE",
}

// cryptoswapSnapshotAcc accumulates the raw getter results for one pool
// snapshot as shared.SnapshotRead Decode callbacks fill it in, then builds the
// state and config rows in build(). AdminBalances has no corresponding read
// (see cryptoswapSnapshotReads): it stays nil by structural design, not
// because a call reverted.
type cryptoswapSnapshotAcc struct {
	balances     []*big.Int
	virtualPrice *big.Int
	totalSupply  *big.Int
	amp          *big.Int
	gamma        *big.Int
	fee          *big.Int
	spotDy       []*big.Int
	priceScale   []*big.Int
	priceOracle  []*big.Int
	lastPrices   []*big.Int
	d            *big.Int
	xcpProfit    *big.Int

	lpPrice             *big.Int
	xcpProfitA          *big.Int
	lastPricesTimestamp *int64
	getDx               []*big.Int
	calcTokenAmount     *big.Int
	calcWithdraw        []*big.Int

	cfg cryptoswapConfigReads
}

// build assembles the state and config rows from the accumulated reads. It is
// called once RunSnapshotReads has driven every read's Decode.
func (acc *cryptoswapSnapshotAcc) build(pool RegisteredPool, blockNumber int64, version int, ts time.Time) (*entity.CurveCryptoswapState, *entity.CurveCryptoswapConfig, error) {
	state, err := entity.NewCurveCryptoswapState(entity.CurveCryptoswapStateParams{
		CurvePoolID:  pool.ID,
		BlockNumber:  blockNumber,
		BlockVersion: version,
		Timestamp:    ts,
		Balances:     acc.balances,
		VirtualPrice: acc.virtualPrice,
		TotalSupply:  acc.totalSupply,
		Amp:          acc.amp,
		Gamma:        acc.gamma,
		Fee:          acc.fee,
		D:            acc.d,
		XcpProfit:    acc.xcpProfit,
		PriceScale:   acc.priceScale,
		PriceOracle:  acc.priceOracle,
		LastPrices:   acc.lastPrices,
		SpotDy:       acc.spotDy,

		AdminBalances:       nil,
		LpPrice:             acc.lpPrice,
		XcpProfitA:          acc.xcpProfitA,
		LastPricesTimestamp: acc.lastPricesTimestamp,
		GetDx:               acc.getDx,
		CalcTokenAmount:     acc.calcTokenAmount,
		CalcWithdrawOneCoin: acc.calcWithdraw,
	})
	if err != nil {
		return nil, nil, err
	}

	cfg, err := buildCryptoswapConfig(pool, blockNumber, version, ts, acc.cfg)
	if err != nil {
		return nil, nil, err
	}

	return state, cfg, nil
}

// cryptoswapConfigReads holds the raw config getter results in
// cryptoswapConfigGetters order. Every getter is issued for every cryptoswap
// pool, so a reverted getter is an error upstream (OptionalUintResult); a nil
// here would be an upstream decode bug, not a real revert.
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

// cryptoswapSnapshotReads describes the full cryptoswap snapshot as
// self-contained pack/decode units, in the exact order the calls are packed
// into the multicall. admin_balances() is never appended here (see the doc on
// cryptoswapSnapshotAcc): Tricrypto-NG has no admin_balances getter (it always
// reverts), and the only indexed cryptoswap pool (TriCryptoUSDC) is
// Tricrypto-NG. Issuing it would force a choice between a swallowed revert and
// a poison-stall on every block, so it is gated out structurally rather than
// conditionally; a future twocrypto variant that exposes admin_balances would
// need its own capability check here.
func (h *CryptoswapHandler) cryptoswapSnapshotReads(pool RegisteredPool, blockNumber int64, acc *cryptoswapSnapshotAcc) []shared.SnapshotRead[RegisteredPool] {
	var reads []shared.SnapshotRead[RegisteredPool]
	reads = append(reads, h.cryptoSwapBalanceReads(pool, acc)...)
	reads = append(reads, h.cryptoSwapVirtualPriceReads(acc)...)
	reads = append(reads, h.cryptoSwapTotalSupplyReads(acc)...)
	reads = append(reads, h.cryptoSwapAmpReads(acc)...)
	reads = append(reads, h.cryptoSwapGammaReads(acc)...)
	reads = append(reads, h.cryptoSwapFeeReads(acc)...)
	reads = append(reads, h.cryptoSwapGetDyReads(acc)...)
	reads = append(reads, h.cryptoSwapPriceScaleReads(acc)...)
	reads = append(reads, h.cryptoSwapPriceOracleReads(acc)...)
	reads = append(reads, h.cryptoSwapLastPricesReads(acc)...)
	reads = append(reads, h.cryptoSwapDReads(blockNumber, acc)...)
	reads = append(reads, h.cryptoSwapXcpProfitReads(blockNumber, acc)...)
	reads = append(reads, h.cryptoSwapLpPriceReads(blockNumber, acc)...)
	reads = append(reads, h.cryptoSwapXcpProfitAReads(blockNumber, acc)...)
	reads = append(reads, h.cryptoSwapLastPricesTimestampReads(blockNumber, acc)...)
	reads = append(reads, h.cryptoSwapGetDxReads(blockNumber, acc)...)
	reads = append(reads, h.cryptoSwapCalcTokenAmountReads(acc)...)
	reads = append(reads, h.cryptoSwapCalcWithdrawReads(blockNumber, acc)...)
	reads = append(reads, h.cryptoSwapConfigGetterReads(blockNumber, acc)...)
	return reads
}

// balances(i) for each coin.
func (h *CryptoswapHandler) cryptoSwapBalanceReads(pool RegisteredPool, acc *cryptoswapSnapshotAcc) []shared.SnapshotRead[RegisteredPool] {
	var reads []shared.SnapshotRead[RegisteredPool]
	for i := 0; i < pool.NCoins; i++ {
		i := i
		reads = append(reads, shared.SnapshotRead[RegisteredPool]{
			Name: fmt.Sprintf("balances(%d)", i),
			Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
				data, err := h.cryptoABI.Pack("balances", big.NewInt(int64(i)))
				if err != nil {
					return nil, fmt.Errorf("packing balances(%d): %w", i, err)
				}
				return []outbound.Call{{Target: pool.Address, AllowFailure: false, CallData: data}}, nil
			},
			Decode: func(pool RegisteredPool, results []outbound.Result) error {
				v, err := shared.UnpackUint(h.cryptoABI, "balances", results[0])
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
	return reads
}

// get_virtual_price()
func (h *CryptoswapHandler) cryptoSwapVirtualPriceReads(acc *cryptoswapSnapshotAcc) []shared.SnapshotRead[RegisteredPool] {
	var reads []shared.SnapshotRead[RegisteredPool]
	reads = append(reads, shared.SnapshotRead[RegisteredPool]{
		Name: "get_virtual_price",
		Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
			data, err := h.cryptoABI.Pack("get_virtual_price")
			if err != nil {
				return nil, fmt.Errorf("packing get_virtual_price: %w", err)
			}
			return []outbound.Call{{Target: pool.Address, AllowFailure: false, CallData: data}}, nil
		},
		Decode: func(pool RegisteredPool, results []outbound.Result) error {
			v, err := shared.UnpackUint(h.cryptoABI, "get_virtual_price", results[0])
			if err != nil {
				return fmt.Errorf("get_virtual_price: %w", err)
			}
			acc.virtualPrice = v
			return nil
		},
	})
	return reads
}

// totalSupply(). Lives on the LP token, which for pre-NG pools is a
// separate contract from the pool; falls back to the pool address for NG
// pools that are their own LP token.
func (h *CryptoswapHandler) cryptoSwapTotalSupplyReads(acc *cryptoswapSnapshotAcc) []shared.SnapshotRead[RegisteredPool] {
	var reads []shared.SnapshotRead[RegisteredPool]
	reads = append(reads, shared.SnapshotRead[RegisteredPool]{
		Name: "totalSupply",
		Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
			lpTarget := pool.Address
			if pool.LpTokenAddress != nil {
				lpTarget = *pool.LpTokenAddress
			}
			data, err := h.cryptoABI.Pack("totalSupply")
			if err != nil {
				return nil, fmt.Errorf("packing totalSupply: %w", err)
			}
			return []outbound.Call{{Target: lpTarget, AllowFailure: false, CallData: data}}, nil
		},
		Decode: func(pool RegisteredPool, results []outbound.Result) error {
			v, err := shared.UnpackUint(h.cryptoABI, "totalSupply", results[0])
			if err != nil {
				return fmt.Errorf("totalSupply: %w", err)
			}
			acc.totalSupply = v
			return nil
		},
	})
	return reads
}

// A()
func (h *CryptoswapHandler) cryptoSwapAmpReads(acc *cryptoswapSnapshotAcc) []shared.SnapshotRead[RegisteredPool] {
	var reads []shared.SnapshotRead[RegisteredPool]
	reads = append(reads, shared.SnapshotRead[RegisteredPool]{
		Name: "A",
		Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
			data, err := h.cryptoABI.Pack("A")
			if err != nil {
				return nil, fmt.Errorf("packing A: %w", err)
			}
			return []outbound.Call{{Target: pool.Address, AllowFailure: false, CallData: data}}, nil
		},
		Decode: func(pool RegisteredPool, results []outbound.Result) error {
			v, err := shared.UnpackUint(h.cryptoABI, "A", results[0])
			if err != nil {
				return fmt.Errorf("decoding A: %w", err)
			}
			acc.amp = v
			return nil
		},
	})
	return reads
}

// gamma()
func (h *CryptoswapHandler) cryptoSwapGammaReads(acc *cryptoswapSnapshotAcc) []shared.SnapshotRead[RegisteredPool] {
	var reads []shared.SnapshotRead[RegisteredPool]
	reads = append(reads, shared.SnapshotRead[RegisteredPool]{
		Name: "gamma",
		Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
			data, err := h.cryptoABI.Pack("gamma")
			if err != nil {
				return nil, fmt.Errorf("packing gamma: %w", err)
			}
			return []outbound.Call{{Target: pool.Address, AllowFailure: false, CallData: data}}, nil
		},
		Decode: func(pool RegisteredPool, results []outbound.Result) error {
			v, err := shared.UnpackUint(h.cryptoABI, "gamma", results[0])
			if err != nil {
				return fmt.Errorf("gamma: %w", err)
			}
			acc.gamma = v
			return nil
		},
	})
	return reads
}

// fee()
func (h *CryptoswapHandler) cryptoSwapFeeReads(acc *cryptoswapSnapshotAcc) []shared.SnapshotRead[RegisteredPool] {
	var reads []shared.SnapshotRead[RegisteredPool]
	reads = append(reads, shared.SnapshotRead[RegisteredPool]{
		Name: "fee",
		Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
			data, err := h.cryptoABI.Pack("fee")
			if err != nil {
				return nil, fmt.Errorf("packing fee: %w", err)
			}
			return []outbound.Call{{Target: pool.Address, AllowFailure: false, CallData: data}}, nil
		},
		Decode: func(pool RegisteredPool, results []outbound.Result) error {
			v, err := shared.UnpackUint(h.cryptoABI, "fee", results[0])
			if err != nil {
				return fmt.Errorf("fee: %w", err)
			}
			acc.fee = v
			return nil
		},
	})
	return reads
}

// get_dy(i, j, 10^decimals[i]) for every ordered pair i!=j. Cryptoswap
// get_dy takes uint256 args (not int128).
func (h *CryptoswapHandler) cryptoSwapGetDyReads(acc *cryptoswapSnapshotAcc) []shared.SnapshotRead[RegisteredPool] {
	var reads []shared.SnapshotRead[RegisteredPool]
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
					data, err := h.cryptoABI.Pack("get_dy", big.NewInt(int64(i)), big.NewInt(int64(j)), dx)
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
					v, err := shared.UnpackUint(h.cryptoABI, "get_dy", results[k])
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
	return reads
}

// price_scale(i) for i=0..n-2
func (h *CryptoswapHandler) cryptoSwapPriceScaleReads(acc *cryptoswapSnapshotAcc) []shared.SnapshotRead[RegisteredPool] {
	var reads []shared.SnapshotRead[RegisteredPool]
	reads = append(reads, shared.SnapshotRead[RegisteredPool]{
		Name: "price_scale",
		Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
			var calls []outbound.Call
			for i := 0; i < pool.NCoins-1; i++ {
				data, err := h.cryptoABI.Pack("price_scale", big.NewInt(int64(i)))
				if err != nil {
					return nil, fmt.Errorf("packing price_scale(%d): %w", i, err)
				}
				calls = append(calls, outbound.Call{Target: pool.Address, AllowFailure: false, CallData: data})
			}
			return calls, nil
		},
		Decode: func(pool RegisteredPool, results []outbound.Result) error {
			priceScale := make([]*big.Int, pool.NCoins-1)
			for i := range pool.NCoins - 1 {
				v, err := shared.UnpackUint(h.cryptoABI, "price_scale", results[i])
				if err != nil {
					return fmt.Errorf("price_scale(%d): %w", i, err)
				}
				priceScale[i] = v
			}
			acc.priceScale = priceScale
			return nil
		},
	})
	return reads
}

// price_oracle(i) for i=0..n-2
func (h *CryptoswapHandler) cryptoSwapPriceOracleReads(acc *cryptoswapSnapshotAcc) []shared.SnapshotRead[RegisteredPool] {
	var reads []shared.SnapshotRead[RegisteredPool]
	reads = append(reads, shared.SnapshotRead[RegisteredPool]{
		Name: "price_oracle",
		Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
			var calls []outbound.Call
			for i := 0; i < pool.NCoins-1; i++ {
				data, err := h.cryptoABI.Pack("price_oracle", big.NewInt(int64(i)))
				if err != nil {
					return nil, fmt.Errorf("packing price_oracle(%d): %w", i, err)
				}
				calls = append(calls, outbound.Call{Target: pool.Address, AllowFailure: false, CallData: data})
			}
			return calls, nil
		},
		Decode: func(pool RegisteredPool, results []outbound.Result) error {
			priceOracle := make([]*big.Int, pool.NCoins-1)
			for i := range pool.NCoins - 1 {
				v, err := shared.UnpackUint(h.cryptoABI, "price_oracle", results[i])
				if err != nil {
					return fmt.Errorf("price_oracle(%d): %w", i, err)
				}
				priceOracle[i] = v
			}
			acc.priceOracle = priceOracle
			return nil
		},
	})
	return reads
}

// last_prices(i) for i=0..n-2
func (h *CryptoswapHandler) cryptoSwapLastPricesReads(acc *cryptoswapSnapshotAcc) []shared.SnapshotRead[RegisteredPool] {
	var reads []shared.SnapshotRead[RegisteredPool]
	reads = append(reads, shared.SnapshotRead[RegisteredPool]{
		Name: "last_prices",
		Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
			var calls []outbound.Call
			for i := 0; i < pool.NCoins-1; i++ {
				data, err := h.cryptoABI.Pack("last_prices", big.NewInt(int64(i)))
				if err != nil {
					return nil, fmt.Errorf("packing last_prices(%d): %w", i, err)
				}
				calls = append(calls, outbound.Call{Target: pool.Address, AllowFailure: false, CallData: data})
			}
			return calls, nil
		},
		Decode: func(pool RegisteredPool, results []outbound.Result) error {
			lastPrices := make([]*big.Int, pool.NCoins-1)
			for i := range pool.NCoins - 1 {
				v, err := shared.UnpackUint(h.cryptoABI, "last_prices", results[i])
				if err != nil {
					return fmt.Errorf("last_prices(%d): %w", i, err)
				}
				lastPrices[i] = v
			}
			acc.lastPrices = lastPrices
			return nil
		},
	})
	return reads
}

// D() - AllowFailure=true so it does not abort the multicall, but a
// revert is still decoded as an error (no-swallowed-errors).
func (h *CryptoswapHandler) cryptoSwapDReads(blockNumber int64, acc *cryptoswapSnapshotAcc) []shared.SnapshotRead[RegisteredPool] {
	var reads []shared.SnapshotRead[RegisteredPool]
	reads = append(reads, shared.SnapshotRead[RegisteredPool]{
		Name: "D",
		Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
			data, err := h.cryptoABI.Pack("D")
			if err != nil {
				return nil, fmt.Errorf("packing D: %w", err)
			}
			return []outbound.Call{{Target: pool.Address, AllowFailure: true, CallData: data}}, nil
		},
		Decode: func(pool RegisteredPool, results []outbound.Result) error {
			v, err := shared.OptionalUintResult(h.cryptoABI, "D", results[0], pool.Address, blockNumber)
			if err != nil {
				return fmt.Errorf("decoding D: %w", err)
			}
			acc.d = v
			return nil
		},
	})
	return reads
}

// xcp_profit() - AllowFailure=true; a revert is decoded as an error.
func (h *CryptoswapHandler) cryptoSwapXcpProfitReads(blockNumber int64, acc *cryptoswapSnapshotAcc) []shared.SnapshotRead[RegisteredPool] {
	var reads []shared.SnapshotRead[RegisteredPool]
	reads = append(reads, shared.SnapshotRead[RegisteredPool]{
		Name: "xcp_profit",
		Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
			data, err := h.cryptoABI.Pack("xcp_profit")
			if err != nil {
				return nil, fmt.Errorf("packing xcp_profit: %w", err)
			}
			return []outbound.Call{{Target: pool.Address, AllowFailure: true, CallData: data}}, nil
		},
		Decode: func(pool RegisteredPool, results []outbound.Result) error {
			v, err := shared.OptionalUintResult(h.cryptoABI, "xcp_profit", results[0], pool.Address, blockNumber)
			if err != nil {
				return fmt.Errorf("xcp_profit: %w", err)
			}
			acc.xcpProfit = v
			return nil
		},
	})
	return reads
}

// lp_price()
func (h *CryptoswapHandler) cryptoSwapLpPriceReads(blockNumber int64, acc *cryptoswapSnapshotAcc) []shared.SnapshotRead[RegisteredPool] {
	var reads []shared.SnapshotRead[RegisteredPool]
	reads = append(reads, shared.SnapshotRead[RegisteredPool]{
		Name: "lp_price",
		Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
			data, err := h.cryptoABI.Pack("lp_price")
			if err != nil {
				return nil, fmt.Errorf("packing lp_price: %w", err)
			}
			return []outbound.Call{{Target: pool.Address, AllowFailure: true, CallData: data}}, nil
		},
		Decode: func(pool RegisteredPool, results []outbound.Result) error {
			v, err := shared.OptionalUintResult(h.cryptoABI, "lp_price", results[0], pool.Address, blockNumber)
			if err != nil {
				return fmt.Errorf("lp_price: %w", err)
			}
			acc.lpPrice = v
			return nil
		},
	})
	return reads
}

// xcp_profit_a()
func (h *CryptoswapHandler) cryptoSwapXcpProfitAReads(blockNumber int64, acc *cryptoswapSnapshotAcc) []shared.SnapshotRead[RegisteredPool] {
	var reads []shared.SnapshotRead[RegisteredPool]
	reads = append(reads, shared.SnapshotRead[RegisteredPool]{
		Name: "xcp_profit_a",
		Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
			data, err := h.cryptoABI.Pack("xcp_profit_a")
			if err != nil {
				return nil, fmt.Errorf("packing xcp_profit_a: %w", err)
			}
			return []outbound.Call{{Target: pool.Address, AllowFailure: true, CallData: data}}, nil
		},
		Decode: func(pool RegisteredPool, results []outbound.Result) error {
			v, err := shared.OptionalUintResult(h.cryptoABI, "xcp_profit_a", results[0], pool.Address, blockNumber)
			if err != nil {
				return fmt.Errorf("xcp_profit_a: %w", err)
			}
			acc.xcpProfitA = v
			return nil
		},
	})
	return reads
}

// last_prices_timestamp()
func (h *CryptoswapHandler) cryptoSwapLastPricesTimestampReads(blockNumber int64, acc *cryptoswapSnapshotAcc) []shared.SnapshotRead[RegisteredPool] {
	var reads []shared.SnapshotRead[RegisteredPool]
	reads = append(reads, shared.SnapshotRead[RegisteredPool]{
		Name: "last_prices_timestamp",
		Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
			data, err := h.cryptoABI.Pack("last_prices_timestamp")
			if err != nil {
				return nil, fmt.Errorf("packing last_prices_timestamp: %w", err)
			}
			return []outbound.Call{{Target: pool.Address, AllowFailure: true, CallData: data}}, nil
		},
		Decode: func(pool RegisteredPool, results []outbound.Result) error {
			v, err := shared.OptionalUintResult(h.cryptoABI, "last_prices_timestamp", results[0], pool.Address, blockNumber)
			if err != nil {
				return fmt.Errorf("last_prices_timestamp: %w", err)
			}
			// A revert already errored out above, so v is non-nil here; the
			// guard is defensive. An issued call that returned a value outside
			// int64 is a real data-quality error, not a structural absence.
			if v != nil {
				if !v.IsInt64() {
					return fmt.Errorf("last_prices_timestamp for pool %s: value %s overflows int64", pool.Address, v.String())
				}
				ts := v.Int64()
				acc.lastPricesTimestamp = &ts
			}
			return nil
		},
	})
	return reads
}

// get_dx(i, j, 10^decimals[j]) for every ordered pair i!=j: one unit of
// the bought coin j, asking how much coin i it costs.
func (h *CryptoswapHandler) cryptoSwapGetDxReads(blockNumber int64, acc *cryptoswapSnapshotAcc) []shared.SnapshotRead[RegisteredPool] {
	var reads []shared.SnapshotRead[RegisteredPool]
	reads = append(reads, shared.SnapshotRead[RegisteredPool]{
		Name: "get_dx",
		Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
			var calls []outbound.Call
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
					calls = append(calls, outbound.Call{Target: pool.Address, AllowFailure: true, CallData: data})
				}
			}
			return calls, nil
		},
		Decode: func(pool RegisteredPool, results []outbound.Result) error {
			nPairs := pool.NCoins * (pool.NCoins - 1)
			getDx := make([]*big.Int, 0, nPairs)
			k := 0
			for i := 0; i < pool.NCoins; i++ {
				for j := 0; j < pool.NCoins; j++ {
					if i == j {
						continue
					}
					v, err := shared.OptionalUintResult(h.cryptoABI, "get_dx", results[k], pool.Address, blockNumber)
					if err != nil {
						return fmt.Errorf("get_dx(%d,%d): %w", i, j, err)
					}
					getDx = append(getDx, v)
					k++
				}
			}
			acc.getDx = getDx
			return nil
		},
	})
	return reads
}

// calc_token_amount(unit deposit of 10^decimals[i] per coin, is_deposit=true)
func (h *CryptoswapHandler) cryptoSwapCalcTokenAmountReads(acc *cryptoswapSnapshotAcc) []shared.SnapshotRead[RegisteredPool] {
	var reads []shared.SnapshotRead[RegisteredPool]
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
	return reads
}

// calc_withdraw_one_coin(1e18, i) for each coin.
func (h *CryptoswapHandler) cryptoSwapCalcWithdrawReads(blockNumber int64, acc *cryptoswapSnapshotAcc) []shared.SnapshotRead[RegisteredPool] {
	var reads []shared.SnapshotRead[RegisteredPool]
	reads = append(reads, shared.SnapshotRead[RegisteredPool]{
		Name: "calc_withdraw_one_coin",
		Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
			oneLP := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
			var calls []outbound.Call
			for i := 0; i < pool.NCoins; i++ {
				data, err := h.cryptoABI.Pack("calc_withdraw_one_coin", oneLP, big.NewInt(int64(i)))
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
				v, err := shared.OptionalUintResult(h.cryptoABI, "calc_withdraw_one_coin", results[i], pool.Address, blockNumber)
				if err != nil {
					return fmt.Errorf("calc_withdraw_one_coin(%d): %w", i, err)
				}
				calcWithdraw[i] = v
			}
			acc.calcWithdraw = calcWithdraw
			return nil
		},
	})
	return reads
}

// config getters. Cryptoswap admin_fee is the ADMIN_FEE constant; the
// pool has no admin_fee() getter. All 11 getters are packed and decoded
// together so cryptoswapConfigGetters stays the single source of order.
func (h *CryptoswapHandler) cryptoSwapConfigGetterReads(blockNumber int64, acc *cryptoswapSnapshotAcc) []shared.SnapshotRead[RegisteredPool] {
	var reads []shared.SnapshotRead[RegisteredPool]
	reads = append(reads, shared.SnapshotRead[RegisteredPool]{
		Name: "cryptoswap config getters",
		Pack: func(pool RegisteredPool) ([]outbound.Call, error) {
			var calls []outbound.Call
			for _, fn := range cryptoswapConfigGetters {
				data, err := h.cryptoABI.Pack(fn)
				if err != nil {
					return nil, fmt.Errorf("packing %s: %w", fn, err)
				}
				calls = append(calls, outbound.Call{Target: pool.Address, AllowFailure: true, CallData: data})
			}
			return calls, nil
		},
		Decode: func(pool RegisteredPool, results []outbound.Result) error {
			dst := []**big.Int{
				&acc.cfg.initialAGamma, &acc.cfg.futureAGamma, &acc.cfg.initialAGammaTime, &acc.cfg.futureAGammaTime,
				&acc.cfg.midFee, &acc.cfg.outFee, &acc.cfg.feeGamma, &acc.cfg.allowedExtraProfit, &acc.cfg.adjustmentStep,
				&acc.cfg.maTime, &acc.cfg.adminFee,
			}
			if len(dst) != len(cryptoswapConfigGetters) {
				return fmt.Errorf("config getter/dst length mismatch: %d getters, %d destinations", len(cryptoswapConfigGetters), len(dst))
			}
			for i, fn := range cryptoswapConfigGetters {
				v, err := shared.OptionalUintResult(h.cryptoABI, fn, results[i], pool.Address, blockNumber)
				if err != nil {
					return fmt.Errorf("%s: %w", fn, err)
				}
				*dst[i] = v
			}
			return nil
		},
	})
	return reads
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
	fee, err := shared.GetBigIntField(data, "fee")
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

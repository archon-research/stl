package balancer_dex

import (
	"context"
	"fmt"
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

	vaultRead *abi.ABI
	poolRead  *abi.ABI
	erc20Read *abi.ABI
}

func newBlockchainService(mc outbound.Multicaller) (*blockchainService, error) {
	vaultRead, err := abis.GetBalancerV2VaultReadABI()
	if err != nil {
		return nil, fmt.Errorf("loading Balancer V2 vault read ABI: %w", err)
	}
	poolRead, err := abis.GetBalancerV2ComposableStableReadABI()
	if err != nil {
		return nil, fmt.Errorf("loading Balancer V2 pool read ABI: %w", err)
	}
	erc20Read, err := abis.GetERC20ABI()
	if err != nil {
		return nil, fmt.Errorf("loading ERC20 read ABI: %w", err)
	}
	return &blockchainService{
		multicaller: mc,
		vaultRead:   vaultRead,
		poolRead:    poolRead,
		erc20Read:   erc20Read,
	}, nil
}

// readERC20MetadataBatch reads `symbol()` + `decimals()` for every token in
// `tokens` in a SINGLE multicall (2N sub-calls). Used by populatePoolTokens so
// the first-run hydration is one round-trip instead of N. Returns parallel
// slices in token order. Any revert is UNEXPECTED and propagates.
func (b *blockchainService) readERC20MetadataBatch(ctx context.Context, tokens []common.Address, blockNumber int64) ([]string, []uint8, error) {
	if len(tokens) == 0 {
		return nil, nil, nil
	}
	calls := make([]outbound.Call, 0, 2*len(tokens))
	for _, tok := range tokens {
		symData, err := b.erc20Read.Pack("symbol")
		if err != nil {
			return nil, nil, fmt.Errorf("packing symbol for %s: %w", tok.Hex(), err)
		}
		decData, err := b.erc20Read.Pack("decimals")
		if err != nil {
			return nil, nil, fmt.Errorf("packing decimals for %s: %w", tok.Hex(), err)
		}
		calls = append(calls,
			outbound.Call{Target: tok, CallData: symData},
			outbound.Call{Target: tok, CallData: decData},
		)
	}
	results, err := b.multicaller.Execute(ctx, calls, big.NewInt(blockNumber))
	if err != nil {
		return nil, nil, fmt.Errorf("multicall ERC20 metadata batch: %w", err)
	}
	if len(results) != 2*len(tokens) {
		return nil, nil, fmt.Errorf("ERC20 metadata batch returned %d results, expected %d", len(results), 2*len(tokens))
	}
	syms := make([]string, len(tokens))
	decs := make([]uint8, len(tokens))
	for i, tok := range tokens {
		symRes, decRes := results[2*i], results[2*i+1]
		if !symRes.Success {
			return nil, nil, fmt.Errorf("symbol() reverted on %s at block %d", tok.Hex(), blockNumber)
		}
		symUnp, err := b.erc20Read.Unpack("symbol", symRes.ReturnData)
		if err != nil || len(symUnp) == 0 {
			return nil, nil, fmt.Errorf("decoding symbol on %s: %w", tok.Hex(), err)
		}
		sym, ok := symUnp[0].(string)
		if !ok {
			return nil, nil, fmt.Errorf("symbol returned %T, want string", symUnp[0])
		}
		if !decRes.Success {
			return nil, nil, fmt.Errorf("decimals() reverted on %s at block %d", tok.Hex(), blockNumber)
		}
		decUnp, err := b.erc20Read.Unpack("decimals", decRes.ReturnData)
		if err != nil || len(decUnp) == 0 {
			return nil, nil, fmt.Errorf("decoding decimals on %s: %w", tok.Hex(), err)
		}
		dec, ok := decUnp[0].(uint8)
		if !ok {
			return nil, nil, fmt.Errorf("decimals returned %T, want uint8", decUnp[0])
		}
		syms[i] = sym
		decs[i] = dec
	}
	return syms, decs, nil
}

// readBPTBalances issues balanceOf(user) on the pool's BPT contract (the pool
// address itself on ComposableStable) for every non-zero address in `users`,
// pinned to `blockNumber`. balanceOf on a healthy ERC-20 must succeed for any
// 20-byte address — a revert here is UNEXPECTED, so propagate and let SQS
// retry.
func (b *blockchainService) readBPTBalances(ctx context.Context, bpt common.Address, users []common.Address, blockNumber int64) ([]*big.Int, error) {
	if len(users) == 0 {
		return nil, nil
	}
	calls := make([]outbound.Call, len(users))
	for i, u := range users {
		data, err := b.erc20Read.Pack("balanceOf", u)
		if err != nil {
			return nil, fmt.Errorf("packing balanceOf(%s): %w", u.Hex(), err)
		}
		calls[i] = outbound.Call{Target: bpt, CallData: data}
	}
	results, err := b.multicaller.Execute(ctx, calls, big.NewInt(blockNumber))
	if err != nil {
		return nil, fmt.Errorf("multicall balanceOf on BPT %s: %w", bpt.Hex(), err)
	}
	if len(results) != len(calls) {
		return nil, fmt.Errorf("balanceOf multicall returned %d results, expected %d", len(results), len(calls))
	}
	out := make([]*big.Int, len(users))
	for i, r := range results {
		if !r.Success {
			return nil, fmt.Errorf("balanceOf(%s) reverted on BPT %s at block %d", users[i].Hex(), bpt.Hex(), blockNumber)
		}
		unpacked, err := b.erc20Read.Unpack("balanceOf", r.ReturnData)
		if err != nil || len(unpacked) == 0 {
			return nil, fmt.Errorf("decoding balanceOf(%s) on BPT %s at block %d: %w", users[i].Hex(), bpt.Hex(), blockNumber, err)
		}
		v, ok := unpacked[0].(*big.Int)
		if !ok {
			return nil, fmt.Errorf("balanceOf returned %T, want *big.Int", unpacked[0])
		}
		out[i] = v
	}
	return out, nil
}

// readVaultTokens issues a single Vault.getPoolTokens(poolId) call. Used at
// first-run population of balancer_pool_token so we can identify the phantom
// BPT slot (slot.address == pool.address) and persist the slot ordering.
func (b *blockchainService) readVaultTokens(ctx context.Context, vault common.Address, poolID common.Hash, blockNumber int64) (*vaultTokensResult, error) {
	data, err := b.vaultRead.Pack("getPoolTokens", poolID)
	if err != nil {
		return nil, fmt.Errorf("packing getPoolTokens: %w", err)
	}
	results, err := b.multicaller.Execute(ctx, []outbound.Call{{Target: vault, CallData: data}}, big.NewInt(blockNumber))
	if err != nil {
		return nil, fmt.Errorf("multicall getPoolTokens at block %d: %w", blockNumber, err)
	}
	if len(results) != 1 || !results[0].Success {
		return nil, fmt.Errorf("getPoolTokens reverted for pool %s", poolID.Hex())
	}
	return decodeVaultTokens(b.vaultRead, results[0].ReturnData)
}

// readPoolState issues the event-triggered multicall: getPoolTokens(poolId) on
// the Vault plus a panel of pool-contract view methods covering amp, BPT rate,
// supply, swap fee, pause state, scaling factors, and per-token rates.
//
// For weighted / legacy stable pools, some methods (getActualSupply,
// getAmplificationParameter on weighted, getRate on legacy stable) revert; we
// mark those calls AllowFailure and leave the result fields nil.
func (b *blockchainService) readPoolState(ctx context.Context, pool *entity.BalancerPool, tokens []*entity.BalancerPoolToken, tokenAddresses []common.Address, blockNumber int64) (*poolMulticallResult, error) {
	isComposable := pool.PoolKind == entity.BalancerPoolKindComposableStable

	var calls []outbound.Call

	// 0: Vault.getPoolTokens(poolId).
	vaultData, err := b.vaultRead.Pack("getPoolTokens", pool.PoolID)
	if err != nil {
		return nil, fmt.Errorf("packing getPoolTokens: %w", err)
	}
	calls = append(calls, outbound.Call{Target: pool.VaultAddress, CallData: vaultData})

	// 1: getAmplificationParameter (allow failure — only stable variants).
	ampData, err := b.poolRead.Pack("getAmplificationParameter")
	if err != nil {
		return nil, fmt.Errorf("packing getAmplificationParameter: %w", err)
	}
	calls = append(calls, outbound.Call{Target: pool.Address, CallData: ampData, AllowFailure: true})

	// 2: getRate (BPT rate; allow failure for non-composable variants).
	rateData, err := b.poolRead.Pack("getRate")
	if err != nil {
		return nil, fmt.Errorf("packing getRate: %w", err)
	}
	calls = append(calls, outbound.Call{Target: pool.Address, CallData: rateData, AllowFailure: true})

	// 3: getActualSupply (composable_stable only — allow failure on legacy).
	actualData, err := b.poolRead.Pack("getActualSupply")
	if err != nil {
		return nil, fmt.Errorf("packing getActualSupply: %w", err)
	}
	calls = append(calls, outbound.Call{Target: pool.Address, CallData: actualData, AllowFailure: true})

	// 4: totalSupply.
	tsData, err := b.poolRead.Pack("totalSupply")
	if err != nil {
		return nil, fmt.Errorf("packing totalSupply: %w", err)
	}
	calls = append(calls, outbound.Call{Target: pool.Address, CallData: tsData, AllowFailure: true})

	// 5: getScalingFactors.
	sfData, err := b.poolRead.Pack("getScalingFactors")
	if err != nil {
		return nil, fmt.Errorf("packing getScalingFactors: %w", err)
	}
	calls = append(calls, outbound.Call{Target: pool.Address, CallData: sfData, AllowFailure: true})

	// 6: getSwapFeePercentage.
	feeData, err := b.poolRead.Pack("getSwapFeePercentage")
	if err != nil {
		return nil, fmt.Errorf("packing getSwapFeePercentage: %w", err)
	}
	calls = append(calls, outbound.Call{Target: pool.Address, CallData: feeData, AllowFailure: true})

	// 7: getPausedState.
	pausedData, err := b.poolRead.Pack("getPausedState")
	if err != nil {
		return nil, fmt.Errorf("packing getPausedState: %w", err)
	}
	calls = append(calls, outbound.Call{Target: pool.Address, CallData: pausedData, AllowFailure: true})

	// 8..N: getTokenRate(token) per non-phantom token slot. Order matches the
	// tokens slice; phantom slot is skipped here (rate = 1e18 by convention)
	// and filled with nil at decode-time.
	rateOrder := make([]int, 0, len(tokens))
	for i, t := range tokens {
		if t.IsPhantom {
			continue
		}
		// Pack getTokenRate against the slot's known token address; this
		// matches the on-chain convention used by composable_stable pools.
		// Skip if we don't have a tokenAddresses entry for that index.
		if i >= len(tokenAddresses) {
			continue
		}
		trData, err := b.poolRead.Pack("getTokenRate", tokenAddresses[i])
		if err != nil {
			return nil, fmt.Errorf("packing getTokenRate(%d): %w", i, err)
		}
		calls = append(calls, outbound.Call{Target: pool.Address, CallData: trData, AllowFailure: true})
		rateOrder = append(rateOrder, i)
	}

	results, err := b.multicaller.Execute(ctx, calls, big.NewInt(blockNumber))
	if err != nil {
		return nil, fmt.Errorf("multicall pool state at block %d: %w", blockNumber, err)
	}
	if len(results) != len(calls) {
		return nil, fmt.Errorf("pool multicall returned %d results, expected %d", len(results), len(calls))
	}

	out := &poolMulticallResult{}

	// 0: Vault.getPoolTokens.
	vt, err := decodeVaultTokens(b.vaultRead, results[0].ReturnData)
	if err != nil {
		return nil, fmt.Errorf("decoding getPoolTokens: %w", err)
	}
	out.Vault = vt

	// 1: getAmplificationParameter.
	if results[1].Success {
		amp, updating, err := decodeAmplificationParameter(b.poolRead, results[1].ReturnData)
		if err == nil {
			out.AmpFactor = amp
			out.AmpIsUpdating = &updating
		}
	}

	// 2: getRate.
	if results[2].Success {
		if v, err := unpackUint(b.poolRead, "getRate", results[2]); err == nil {
			out.BptRate = v
		}
	}

	// 3: getActualSupply (composable_stable only). The Success guard tolerates
	// a revert (paused / pre-init), but a Success=true with an undecodable
	// payload indicates an ABI/contract drift — fail hard per CLAUDE.md so
	// the snapshot is never silently written with this column as NULL.
	if isComposable && results[3].Success {
		v, err := unpackUint(b.poolRead, "getActualSupply", results[3])
		if err != nil {
			return nil, fmt.Errorf("decoding getActualSupply for pool %s: %w", pool.Address.Hex(), err)
		}
		out.ActualSupply = v
	}

	// 4: totalSupply. Mandatory on every Balancer V2 pool type. These plain
	// view methods cannot legitimately revert on a healthy pool, so a
	// Success=false (revert) is as much an error as an undecodable payload:
	// either way, failing here and letting SQS retry beats persisting a row
	// with a NULL in a mandatory column (a stale snapshot downstream consumers
	// can't distinguish from real data).
	if !results[4].Success {
		return nil, fmt.Errorf("totalSupply reverted for pool %s at block %d", pool.Address.Hex(), blockNumber)
	}
	totalSupply, err := unpackUint(b.poolRead, "totalSupply", results[4])
	if err != nil {
		return nil, fmt.Errorf("decoding totalSupply for pool %s: %w", pool.Address.Hex(), err)
	}
	out.TotalSupply = totalSupply

	// 5: getScalingFactors. Mandatory: every Balancer V2 pool exposes a
	// scaling-factor vector that downstream price math depends on.
	if !results[5].Success {
		return nil, fmt.Errorf("getScalingFactors reverted for pool %s at block %d", pool.Address.Hex(), blockNumber)
	}
	scalingFactors, err := unpackUintSlice(b.poolRead, "getScalingFactors", results[5])
	if err != nil {
		return nil, fmt.Errorf("decoding getScalingFactors for pool %s: %w", pool.Address.Hex(), err)
	}
	out.ScalingFactors = scalingFactors

	// 6: getSwapFeePercentage. Mandatory: required for every fee/yield
	// calculation downstream; a silent NULL would skew accounting.
	if !results[6].Success {
		return nil, fmt.Errorf("getSwapFeePercentage reverted for pool %s at block %d", pool.Address.Hex(), blockNumber)
	}
	swapFee, err := unpackUint(b.poolRead, "getSwapFeePercentage", results[6])
	if err != nil {
		return nil, fmt.Errorf("decoding getSwapFeePercentage for pool %s: %w", pool.Address.Hex(), err)
	}
	out.SwapFee = swapFee

	// 7: getPausedState.
	if results[7].Success {
		if paused, err := decodePausedState(b.poolRead, results[7].ReturnData); err == nil {
			out.Paused = &paused
		}
	}

	// 8..N: per-slot getTokenRate. Token rates are written into the rate array
	// at the slot index in `tokens` order; phantom slots stay nil.
	if len(tokens) > 0 {
		tokenRates := make([]*big.Int, len(tokens))
		idx := 8
		for _, slotIdx := range rateOrder {
			r := results[idx]
			idx++
			if !r.Success {
				continue
			}
			if v, err := unpackUint(b.poolRead, "getTokenRate", r); err == nil {
				tokenRates[slotIdx] = v
			}
		}
		out.TokenRates = tokenRates
	}

	return out, nil
}

// -----------------------------------------------------------------------------
// Decoders
// -----------------------------------------------------------------------------

func decodeVaultTokens(vaultRead *abi.ABI, data []byte) (*vaultTokensResult, error) {
	unpacked, err := vaultRead.Unpack("getPoolTokens", data)
	if err != nil {
		return nil, fmt.Errorf("unpacking getPoolTokens: %w", err)
	}
	if len(unpacked) != 3 {
		return nil, fmt.Errorf("getPoolTokens returned %d values, want 3", len(unpacked))
	}
	tokens, ok := unpacked[0].([]common.Address)
	if !ok {
		return nil, fmt.Errorf("getPoolTokens tokens type %T", unpacked[0])
	}
	balances, ok := unpacked[1].([]*big.Int)
	if !ok {
		return nil, fmt.Errorf("getPoolTokens balances type %T", unpacked[1])
	}
	lastChange, ok := unpacked[2].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("getPoolTokens lastChangeBlock type %T", unpacked[2])
	}
	// Copy slices to insulate from the ABI library's internal buffers.
	out := &vaultTokensResult{
		Tokens:          make([]common.Address, len(tokens)),
		Balances:        make([]*big.Int, len(balances)),
		LastChangeBlock: new(big.Int).Set(lastChange),
	}
	copy(out.Tokens, tokens)
	for i, b := range balances {
		out.Balances[i] = new(big.Int).Set(b)
	}
	return out, nil
}

func decodeAmplificationParameter(poolRead *abi.ABI, data []byte) (*big.Int, bool, error) {
	unpacked, err := poolRead.Unpack("getAmplificationParameter", data)
	if err != nil {
		return nil, false, err
	}
	if len(unpacked) != 3 {
		return nil, false, fmt.Errorf("getAmplificationParameter returned %d values", len(unpacked))
	}
	value, ok := unpacked[0].(*big.Int)
	if !ok {
		return nil, false, fmt.Errorf("amp value type %T", unpacked[0])
	}
	isUpdating, ok := unpacked[1].(bool)
	if !ok {
		return nil, false, fmt.Errorf("isUpdating type %T", unpacked[1])
	}
	// precision (unpacked[2]) ignored — composable_stable pools always use a
	// fixed 1000-scaled amp; we write only the raw value plus the isUpdating
	// flag into the typed columns.
	return new(big.Int).Set(value), isUpdating, nil
}

func decodePausedState(poolRead *abi.ABI, data []byte) (bool, error) {
	unpacked, err := poolRead.Unpack("getPausedState", data)
	if err != nil {
		return false, err
	}
	if len(unpacked) == 0 {
		return false, fmt.Errorf("getPausedState returned no values")
	}
	paused, ok := unpacked[0].(bool)
	if !ok {
		return false, fmt.Errorf("paused type %T", unpacked[0])
	}
	return paused, nil
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
	return new(big.Int).Set(v), nil
}

// unpackUintSlice decodes a single uint256[]-returning view method's result.
func unpackUintSlice(a *abi.ABI, method string, r outbound.Result) ([]*big.Int, error) {
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
	s, ok := unpacked[0].([]*big.Int)
	if !ok {
		return nil, fmt.Errorf("%s returned %T, want []*big.Int", method, unpacked[0])
	}
	out := make([]*big.Int, len(s))
	for i, b := range s {
		out[i] = new(big.Int).Set(b)
	}
	return out, nil
}

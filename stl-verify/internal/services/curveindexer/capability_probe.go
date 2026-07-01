package curveindexer

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// ProbePoolCapabilities determines, once at startup, which stableswap pools
// expose A_precise() and returns the pools with HasAPrecise resolved. The result
// MUST be the slice handed to NewCurveService: a snapshot issues A_precise only
// when the pool's flag is set, so constructing a service from un-probed pools
// reintroduces the poison-stall this guards against. Returning the resolved slice
// (rather than mutating in place) makes that ordering contract a data dependency
// the caller cannot skip.
//
// A_precise availability is a per-pool fact, not a per-class one: the oldest
// pre-NG pools (e.g. 3pool) revert on A_precise() while others (stETH classic)
// expose it. Without this probe the snapshot would issue A_precise() on every
// stableswap pool and poison-stall on those that revert it every block.
//
// It issues one Multicall3 batch with an AllowFailure=true A_precise() call per
// stableswap pool (both pre-NG and NG), then sets HasAPrecise from the per-call
// Success flag. Cryptoswap pools never call A_precise, so they are skipped and
// left HasAPrecise=false.
//
// A transport/RPC error from the batch itself is returned (startup fails and
// retries): it means the capabilities are unknown, not that A_precise is absent.
// Only a per-call !Success is interpreted as "the pool lacks A_precise".
//
// blockNumber is the block to probe at (capabilities are static, so any recent
// block answers the same); the Multicaller contract requires it to be non-nil.
func ProbePoolCapabilities(ctx context.Context, mc outbound.Multicaller, stableABI *abi.ABI, pools []RegisteredPool, blockNumber *big.Int) ([]RegisteredPool, error) {
	if mc == nil {
		return nil, fmt.Errorf("multicaller is required")
	}
	if stableABI == nil {
		return nil, fmt.Errorf("stableswap ABI is required")
	}
	if blockNumber == nil {
		return nil, fmt.Errorf("block number is required")
	}

	aPreciseData, err := stableABI.Pack("A_precise")
	if err != nil {
		return nil, fmt.Errorf("packing A_precise: %w", err)
	}

	resolved := make([]RegisteredPool, len(pools))
	copy(resolved, pools)

	calls := make([]outbound.Call, 0, len(resolved))
	for i := range resolved {
		switch resolved[i].Kind {
		case KindStableswapPreNG, KindStableswapNG:
			calls = append(calls, outbound.Call{Target: resolved[i].Address, AllowFailure: true, CallData: aPreciseData})
		}
	}

	if len(calls) == 0 {
		return resolved, nil
	}

	results, err := mc.Execute(ctx, calls, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("executing A_precise capability probe: %w", err)
	}
	if len(results) != len(calls) {
		return nil, fmt.Errorf("unexpected A_precise probe result count: got %d, want %d", len(results), len(calls))
	}

	// Re-walk the pools with a result cursor, applying the same stableswap filter
	// used to build calls so results map back to the pool that produced them.
	k := 0
	for i := range resolved {
		switch resolved[i].Kind {
		case KindStableswapPreNG, KindStableswapNG:
			resolved[i].HasAPrecise = results[k].Success
			k++
		}
	}
	return resolved, nil
}

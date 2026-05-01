// Package rpcerr classifies JSON-RPC errors returned by Ethereum nodes and
// provides the "all calls must have succeeded" policy helper.
//
// Exists because multiple workers — DirectCaller, prime_debt, morpho_indexer,
// aavelike, and others in the future — need to distinguish an EVM revert
// (the contract gave a definitive "no data" answer, which pairs legitimately
// with AllowFailure: true) from an RPC transport error (429, 5xx, network
// timeout — the contract never got to run, retry is mandatory).
//
// Post-VEC-188, Success: false in a multicall.Result uniformly means
// "contract reverted" — transport errors now error out of Execute before
// results are produced. Callers that cannot tolerate a revert (e.g. ERC20
// metadata fetches, where zero values would persist corrupt rows) use
// RequireAllSucceeded to surface any Success: false as a top-level error.
//
// See VEC-188 for the invariant this package enforces.
package rpcerr

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/rpc"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// IsEVMRevert reports whether err represents an intentional EVM revert
// returned by the target contract.
//
// Returns true only for signals we recognize as reverts:
//   - JSON-RPC error code 3 (geth's standard for "execution reverted").
//   - Any JSON-RPC error whose lower-cased message contains the substring
//     "execution reverted" — covers geth's lower-case phrasing as well as
//     Erigon / Nethermind variants that capitalise differently
//     ("Execution reverted", "Execution Reverted: …").
//
// Returns false for every other shape, including non-rpc.Error values.
// The classification is deliberately conservative: mislabelling a
// transport error as a revert causes silent data loss; the reverse only
// causes an extra retry. Case-insensitive matching is the right default
// because a persistent contract revert misclassified as transport would
// loop until DLQ, whereas a one-off transport mis-tagged as revert just
// records a single false zero for that block.
func IsEVMRevert(err error) bool {
	if err == nil {
		return false
	}
	var rpcErr rpc.Error
	if !errors.As(err, &rpcErr) {
		return false
	}
	if rpcErr.ErrorCode() == 3 {
		return true
	}
	return strings.Contains(strings.ToLower(rpcErr.Error()), "execution reverted")
}

// RequireAllSucceeded returns a non-nil error if any result in results has
// Success: false, or if results is empty.
//
// Use this in callers where a per-sub-call revert would produce corrupt
// downstream data (e.g. ERC20 metadata fetches that would persist
// zero-valued rows). op is a short human-readable label included in the
// error message — typically the name of the logical operation being
// performed, such as "getTokenMetadata" or "BatchGetTokenMetadata".
//
// The parameter is deliberately not named "context" to avoid shadowing the
// context.Context type that most callers also reference.
func RequireAllSucceeded(results []outbound.Result, op string) error {
	if len(results) == 0 {
		return fmt.Errorf("%s: expected at least one result, got zero", op)
	}
	var failed []int
	for i, r := range results {
		if !r.Success {
			failed = append(failed, i)
		}
	}
	if len(failed) == 0 {
		return nil
	}
	idxs := make([]string, len(failed))
	for i, n := range failed {
		idxs[i] = strconv.Itoa(n)
	}
	return fmt.Errorf("%s: %d of %d sub-calls reverted (indices %s)",
		op, len(failed), len(results), strings.Join(idxs, ","))
}

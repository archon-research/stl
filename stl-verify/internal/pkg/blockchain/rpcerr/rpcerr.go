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
	"net/http"
	"slices"
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

// IsGasExhausted reports whether err is the node's answer that an eth_call ran
// past its gas cap. The verdict is read from the JSON-RPC error's message and,
// for nodes that put the detail in the error's data instead (Nethermind's -32015
// "VM execution error." carries "Out of gas" there), from that data. No error
// code is specific to it: -32000 and -32015 carry reverts too, so the text is
// the discriminator.
//
// This is a verdict on the whole batch: what one sub-call of an aggregate3 would
// answer is established only by issuing it alone.
func IsGasExhausted(err error) bool {
	if err == nil {
		return false
	}
	var rpcErr rpc.Error
	if !errors.As(err, &rpcErr) {
		return false
	}
	text := strings.ToLower(rpcErr.Error())
	var dataErr rpc.DataError
	if errors.As(err, &dataErr) && dataErr.ErrorData() != nil {
		text += " " + strings.ToLower(fmt.Sprint(dataErr.ErrorData()))
	}
	return strings.Contains(text, "out of gas") || strings.Contains(text, "gas required exceeds")
}

// IsRequestTooLarge reports whether the provider refused the request for its
// size (HTTP 413) before any call ran.
func IsRequestTooLarge(err error) bool {
	var httpErr rpc.HTTPError
	return errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusRequestEntityTooLarge
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

// ErrBlockUnavailableAtHash marks a hash-pinned read the node refused because it
// holds no block at that hash. It says nothing about why: a node that has
// dropped an orphaned fork and one that is simply behind or pruned answer
// identically, so a caller acting on it must establish the block's fate against
// the canonical chain before treating the failure as permanent.
var ErrBlockUnavailableAtHash = errors.New("block unavailable at hash")

// blockUnavailablePhrases are the answers observed for a hash a node cannot
// resolve: drpc's -32001 "block not found: hash …", geth's "header for hash not
// found", reth's "header not found", and the older "unknown block". No error
// code is specific to the condition, so the text is the discriminator, matched
// lower-cased. Alchemy's exact wording is unverified, which is why this set is a
// floor rather than a closed enumeration — a phrasing missing from it only leaves
// a message retrying, the behaviour that predates the classifier.
var blockUnavailablePhrases = []string{
	"block not found",
	"header for hash not found",
	"header not found",
	"unknown block",
}

// IsBlockUnavailableAtHash reports whether err is the node's answer that it
// cannot serve the block the caller pinned to.
func IsBlockUnavailableAtHash(err error) bool {
	if err == nil {
		return false
	}
	var rpcErr rpc.Error
	if !errors.As(err, &rpcErr) {
		return false
	}
	text := strings.ToLower(rpcErr.Error())
	return slices.ContainsFunc(blockUnavailablePhrases, func(phrase string) bool {
		return strings.Contains(text, phrase)
	})
}

// TagBlockUnavailableAtHash annotates err with ErrBlockUnavailableAtHash when it
// is that answer, so a caller far from the RPC layer matches the condition with
// errors.Is rather than re-deriving the provider phrasings. Every other error,
// nil included, is returned unchanged.
func TagBlockUnavailableAtHash(err error) error {
	if !IsBlockUnavailableAtHash(err) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrBlockUnavailableAtHash, err)
}

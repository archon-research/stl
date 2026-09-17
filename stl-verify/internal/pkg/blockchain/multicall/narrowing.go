package multicall

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"slices"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/rpcerr"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Narrowing decorates a Multicaller so a batch the node cannot answer as a
// whole is split in half and re-issued until every call's own answer is known.
//
// Two answers qualify. Gas exhaustion: a sub-call that traps (jumps into
// invalid bytecode) burns the 63/64 of the budget aggregate3 hands it, so a
// batch of them compounds to nothing and the node reports "out of gas" for the
// whole eth_call, while each alone leaves the outer frame its 1/64 and comes
// back as Success:false. An oversized request (HTTP 413) is the provider's
// per-request cap. A single call that still exhausts gas is the node's fault
// (a failover cap, say), not the contract's, and propagates like every other
// error, at every width.
type Narrowing struct {
	inner     outbound.Multicaller
	logger    *slog.Logger
	telemetry *Telemetry // optional; nil disables the counter
}

var _ outbound.Multicaller = (*Narrowing)(nil)

// NarrowingOption configures a Narrowing at construction.
type NarrowingOption func(*Narrowing)

// WithNarrowingLogger sets the logger the WARN for each narrowed batch goes to.
func WithNarrowingLogger(logger *slog.Logger) NarrowingOption {
	return func(n *Narrowing) { n.logger = logger }
}

// WithNarrowingTelemetry attaches the multicall.batches.narrowed counter.
func WithNarrowingTelemetry(t *Telemetry) NarrowingOption {
	return func(n *Narrowing) { n.telemetry = t }
}

// NewNarrowingClient builds the Multicall3 client the morpho binaries share:
// telemetry, the client, and Narrowing around it, so a trapping contract cannot
// fail a whole batch anywhere they read the chain. chainName labels the metrics.
func NewNarrowingClient(ethClient *ethclient.Client, multicall3 common.Address, chainName string, logger *slog.Logger) (outbound.Multicaller, error) {
	tel, err := NewTelemetry(chainName)
	if err != nil {
		return nil, fmt.Errorf("creating multicall telemetry: %w", err)
	}
	client, err := NewClient(ethClient, multicall3, WithTelemetry(tel))
	if err != nil {
		return nil, fmt.Errorf("creating multicall client: %w", err)
	}
	return NewNarrowing(client, WithNarrowingLogger(logger), WithNarrowingTelemetry(tel)), nil
}

// NewNarrowing wraps inner. Without options it logs to slog.Default() and
// counts nothing.
func NewNarrowing(inner outbound.Multicaller, opts ...NarrowingOption) *Narrowing {
	n := &Narrowing{inner: inner, logger: slog.Default()}
	for _, opt := range opts {
		opt(n)
	}
	return n
}

// Execute forwards to the inner multicaller, narrowing the batch on a
// narrowable error. Results keep the order of calls.
func (n *Narrowing) Execute(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
	return n.execute(ctx, calls, blockNumberString(blockNumber), func(sub []outbound.Call) ([]outbound.Result, error) {
		return n.inner.Execute(ctx, sub, blockNumber)
	})
}

// ExecuteAtHash is Execute on the hash-pinned path; every re-issued batch stays
// pinned to blockHash.
func (n *Narrowing) ExecuteAtHash(ctx context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
	return n.execute(ctx, calls, blockHash.Hex(), func(sub []outbound.Call) ([]outbound.Result, error) {
		return n.inner.ExecuteAtHash(ctx, sub, blockHash)
	})
}

// Address forwards to the inner multicaller.
func (n *Narrowing) Address() common.Address { return n.inner.Address() }

func (n *Narrowing) execute(ctx context.Context, calls []outbound.Call, blockDesc string, issue func([]outbound.Call) ([]outbound.Result, error)) ([]outbound.Result, error) {
	run := &narrowingRun{issue: issue}
	results, err := run.narrow(calls)
	if run.reason != "" {
		n.report(ctx, calls, blockDesc, run, err)
	}
	return results, err
}

// narrowReason is why the node refused a batch whole; the reason label of the
// multicall.batches.narrowed counter.
type narrowReason string

const (
	narrowReasonGasExhausted    narrowReason = "gas_exhausted"
	narrowReasonRequestTooLarge narrowReason = "request_too_large"
)

// narrowable classifies err as one of the two refusals a smaller batch can
// answer. Any other error would answer a throttled or broken provider with
// more requests, so it propagates as it is.
func narrowable(err error) (narrowReason, bool) {
	switch {
	case rpcerr.IsGasExhausted(err):
		return narrowReasonGasExhausted, true
	case rpcerr.IsRequestTooLarge(err):
		return narrowReasonRequestTooLarge, true
	}
	return "", false
}

// narrowingRun carries one top-level batch through its splits so the outcome
// is reported once, not once per level.
type narrowingRun struct {
	issue  func([]outbound.Call) ([]outbound.Result, error)
	reason narrowReason // class of the first refusal; empty when nothing narrowed
	issued int          // batches sent to the node, the original included

	// culprits are the distinct targets of the narrowest batches the node still
	// refused: the smallest neighbourhood the trapping calls can hide in, where
	// the head of a fifty-candidate batch would name the wrong addresses.
	culpritWidth int
	culprits     []common.Address
}

func (r *narrowingRun) narrow(calls []outbound.Call) ([]outbound.Result, error) {
	r.issued++
	results, err := r.issue(calls)
	if err == nil || len(calls) < 2 {
		return results, err
	}
	reason, ok := narrowable(err)
	if !ok {
		return nil, err
	}
	r.refused(calls, reason)
	mid := len(calls) / 2
	left, err := r.narrow(calls[:mid])
	if err != nil {
		return nil, err
	}
	right, err := r.narrow(calls[mid:])
	if err != nil {
		return nil, err
	}
	return slices.Concat(left, right), nil
}

func (r *narrowingRun) refused(calls []outbound.Call, reason narrowReason) {
	if r.reason == "" {
		r.reason = reason
	}
	switch {
	case r.culpritWidth == 0 || len(calls) < r.culpritWidth:
		r.culpritWidth = len(calls)
		r.culprits = appendDistinctTargets(nil, calls)
	case len(calls) == r.culpritWidth:
		r.culprits = appendDistinctTargets(r.culprits, calls)
	}
}

func appendDistinctTargets(targets []common.Address, calls []outbound.Call) []common.Address {
	for _, c := range calls {
		if !slices.Contains(targets, c.Target) {
			targets = append(targets, c.Target)
		}
	}
	return targets
}

// maxReportedTargets bounds the addresses named in the WARN; a backfill batch
// holds fifty candidates.
const maxReportedTargets = 8

func (n *Narrowing) report(ctx context.Context, calls []outbound.Call, blockDesc string, run *narrowingRun, err error) {
	outcome := "answered"
	if err != nil {
		outcome = "aborted"
	}
	attrs := []any{
		"reason", run.reason,
		"outcome", outcome,
		"block", blockDesc,
		"calls", len(calls),
		"batchesIssued", run.issued,
		"culprits", hexTargets(run.culprits),
	}
	if err != nil {
		attrs = append(attrs, "error", err)
	}
	n.logger.Warn("multicall batch narrowed — the node could not answer it whole", attrs...)
	if n.telemetry != nil {
		n.telemetry.recordNarrowed(ctx, run.reason)
	}
}

func hexTargets(targets []common.Address) []string {
	out := make([]string, 0, min(len(targets), maxReportedTargets+1))
	for i, t := range targets {
		if i == maxReportedTargets {
			out = append(out, "…")
			break
		}
		out = append(out, t.Hex())
	}
	return out
}

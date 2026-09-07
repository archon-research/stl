package shared

import (
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// SnapshotRead is one self-describing state read: it packs 1..N multicall
// calls and decodes exactly those results back. Pack and decode for a read
// sit together, so adding or reordering a read is one local edit and there is
// no hand-maintained positional cursor shared across separate pack/decode
// functions.
//
// A read that packs zero calls is allowed: Decode then receives an empty
// slice rather than RunSnapshotReads special-casing it. That is the simpler,
// more permissive rule (a conditional or no-op read needs no extra branch in
// the executor).
type SnapshotRead[P any] struct {
	Name   string
	Pack   func(pool P) ([]outbound.Call, error)
	Decode func(pool P, results []outbound.Result) error
}

// RunSnapshotReads packs every read in order (recording how many calls each
// contributed), issues ONE ExecuteAtHash for the concatenated calls pinned to
// blockHash, then hands each read's Decode exactly its own slice of results.
// The executor tracks per-read offsets internally, so no caller maintains an
// index cursor across reads.
//
// Errors from Pack or Decode are wrapped with the read's Name so a failure is
// immediately attributable to one read. A result-count mismatch from the
// multicaller is a contract violation (not a per-read problem) and fails hard
// rather than mis-routing results to the wrong read.
func RunSnapshotReads[P any](ctx context.Context, mc outbound.Multicaller, pool P, blockHash common.Hash, reads []SnapshotRead[P]) error {
	var calls []outbound.Call
	counts := make([]int, len(reads))

	for i, read := range reads {
		readCalls, err := read.Pack(pool)
		if err != nil {
			return fmt.Errorf("packing %s reads: %w", read.Name, err)
		}
		counts[i] = len(readCalls)
		calls = append(calls, readCalls...)
	}

	results, err := mc.ExecuteAtHash(ctx, calls, blockHash)
	if err != nil {
		return fmt.Errorf("executing snapshot read multicall: %w", err)
	}
	if len(results) != len(calls) {
		return fmt.Errorf("snapshot read result count mismatch: got %d results, want %d (packed calls)", len(results), len(calls))
	}

	offset := 0
	for i, read := range reads {
		count := counts[i]
		if err := read.Decode(pool, results[offset:offset+count]); err != nil {
			return fmt.Errorf("decoding %s reads: %w", read.Name, err)
		}
		offset += count
	}

	return nil
}

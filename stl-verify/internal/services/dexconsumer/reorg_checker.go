package dexconsumer

import (
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// blockStateReorgChecker answers "was this exact block reorged out?" from the
// watcher's own record in block_states, which is the authoritative source: the
// watcher is the component that decided the block was canonical and published
// it, and it is the same component that detects the reorg, marks the orphaned
// hash, and republishes the canonical block at that height as a new version.
//
// Deliberately NOT a chain-RPC lookup. Asking an RPC node "what is canonical at
// height N?" re-derives, from a source that can disagree with us, a fact we
// already own: a node that is lagging or transiently on a minority fork can
// answer with a different hash and we would discard a block the watcher
// considers canonical and never republishes — a permanent, silent data loss.
// The watcher's orphan marker cannot lie about our own pipeline's view, and
// `is_orphaned = true` is precisely the condition under which the canonical
// replacement was republished, which is what makes discarding this block safe.
type blockStateReorgChecker struct {
	blockStates outbound.BlockStateRepository
}

// NewBlockStateReorgChecker builds the ReorgChecker used by the DEX workers.
// blockStates must be scoped to the worker's chain (the repository takes the
// chain ID at construction).
func NewBlockStateReorgChecker(blockStates outbound.BlockStateRepository) ReorgChecker {
	return &blockStateReorgChecker{blockStates: blockStates}
}

// ReorgedOut reports true only when the watcher recorded THIS block hash as
// orphaned. Every other outcome reports false (or an error), so the caller
// retries and never discards a block we cannot prove was reorged out:
//   - hash unknown to the watcher: it never recorded this block, so we cannot
//     conclude it was replaced.
//   - recorded but not orphaned: still canonical as far as our pipeline knows.
//   - lookup failed: unknown, never assume.
func (c *blockStateReorgChecker) ReorgedOut(ctx context.Context, blockNumber int64, blockHash common.Hash) (bool, error) {
	state, err := c.blockStates.GetBlockByHash(ctx, blockHash.Hex())
	if err != nil {
		return false, fmt.Errorf("reading block state for hash %s: %w", blockHash.Hex(), err)
	}
	if state == nil {
		return false, nil
	}
	if state.Number != blockNumber {
		// The hash is globally unique in block_states, so a height mismatch means
		// the event and our record disagree about the same block. Refuse to judge.
		return false, fmt.Errorf("block state for hash %s has number %d, event says %d",
			blockHash.Hex(), state.Number, blockNumber)
	}
	return state.IsOrphaned, nil
}

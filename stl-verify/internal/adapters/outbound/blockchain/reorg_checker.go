package blockchain

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
)

// headerFetcher is the subset of the eth client this adapter needs. Narrow so
// the finality + comparison logic below is unit-testable without a live node.
type headerFetcher interface {
	HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error)
}

// ReorgChecker answers "was this published block reorged out of the canonical
// chain?" against chain RPC, and is the dexconsumer.ReorgChecker implementation
// the DEX workers use.
//
// It only ever answers true for a block at or below the FINALIZED head. That
// bound is the whole point: above it, our RPC node's view of "canonical at N"
// can legitimately differ from the watcher's (tip churn, or a lagging /
// minority-fork node behind a load balancer). A false "reorged" verdict there
// would ack a block the watcher considers canonical and never republishes,
// permanently dropping it. Finalized blocks are identical on every honest node,
// so a mismatch below that line is a real, irreversible reorg.
// It satisfies dexconsumer.ReorgChecker structurally; conformance is enforced by
// the compiler where it is wired (dexbootstrap assigns it to Deps.ReorgChecker).
// The interface is deliberately NOT imported here: adapters depend on ports and
// domain, never on service packages.
type ReorgChecker struct {
	eth headerFetcher
}

// NewReorgChecker builds a ReorgChecker over an eth client.
func NewReorgChecker(eth headerFetcher) *ReorgChecker {
	return &ReorgChecker{eth: eth}
}

// ReorgedOut reports true only when blockNumber is at or below the finalized
// head AND the finalized chain holds a different block hash at that height.
// A not-yet-finalized block returns (false, nil): not skippable, retry instead —
// by the time it finalizes the answer is definitive either way. Any failure to
// determine the answer returns an error so the caller retries and never skips.
func (c *ReorgChecker) ReorgedOut(ctx context.Context, blockNumber int64, blockHash common.Hash) (bool, error) {
	finalized, err := c.header(ctx, big.NewInt(int64(rpc.FinalizedBlockNumber)))
	if err != nil {
		return false, fmt.Errorf("fetching finalized head: %w", err)
	}
	if blockNumber > finalized.Number.Int64() {
		// Still within the reorg window: our node's "canonical" answer here is not
		// authoritative, so refuse to judge. The caller retries; once the height
		// finalizes, either the block reads fine or it is provably reorged out.
		return false, nil
	}

	canonical, err := c.header(ctx, big.NewInt(blockNumber))
	if err != nil {
		return false, fmt.Errorf("fetching canonical header at block %d: %w", blockNumber, err)
	}
	return canonical.Hash() != blockHash, nil
}

// header fetches a header and rejects a nil header without an error, so an
// unexpected RPC shape surfaces as an error rather than a nil-deref panic on the
// (hot) error path this adapter runs on.
func (c *ReorgChecker) header(ctx context.Context, number *big.Int) (*types.Header, error) {
	h, err := c.eth.HeaderByNumber(ctx, number)
	if err != nil {
		return nil, err
	}
	if h == nil || h.Number == nil {
		return nil, fmt.Errorf("nil header returned for block arg %v", number)
	}
	return h, nil
}

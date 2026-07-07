package outbound

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

type Multicaller interface {
	Execute(ctx context.Context, calls []Call, blockNumber *big.Int) ([]Result, error)
	// ExecuteAtHash pins the read to a specific block hash rather than a number.
	// Required for state reads keyed by (blockNumber, version): after a reorg an
	// archive node answers eth_call-by-number with the new canonical state, which
	// can silently disagree with the reorged (older-version) data being processed.
	// Pinning by hash makes the read unambiguous: the node answers from that exact
	// block even if it has since been reorged out, and errors only when it no
	// longer has the block at all, rather than answering from a different fork.
	ExecuteAtHash(ctx context.Context, calls []Call, blockHash common.Hash) ([]Result, error)
	Address() common.Address
}

type Call struct {
	Target       common.Address
	AllowFailure bool
	CallData     []byte
}

type Result struct {
	Success    bool
	ReturnData []byte
}

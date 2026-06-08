package outbound

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// Multicaller issues a batch of contract reads pinned to a specific block.
// blockNumber == nil means "latest" — use for one-shot startup calls where
// reproducibility against a specific block is not required (e.g., reading
// immutable view methods like contract addresses). Pass a concrete block for
// every event-handler path so reorg-versioned writes are self-consistent.
type Multicaller interface {
	Execute(ctx context.Context, calls []Call, blockNumber *big.Int) ([]Result, error)
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

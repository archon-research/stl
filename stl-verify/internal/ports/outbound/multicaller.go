package outbound

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

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

package eth

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

type BlockQuerier struct {
	client *ethclient.Client
}

func NewBlockQuerier(client *ethclient.Client) *BlockQuerier {
	return &BlockQuerier{client: client}
}

func (q *BlockQuerier) BlockNumber(ctx context.Context) (uint64, error) {
	n, err := q.client.BlockNumber(ctx)
	if err != nil {
		return 0, fmt.Errorf("eth block number: %w", err)
	}
	return n, nil
}

func (q *BlockQuerier) FinalizedBlockNumber(ctx context.Context) (uint64, error) {
	header, err := q.client.HeaderByNumber(
		ctx, big.NewInt(int64(rpc.FinalizedBlockNumber)),
	)
	if err != nil {
		return 0, fmt.Errorf("eth finalized block header: %w", err)
	}
	return header.Number.Uint64(), nil
}

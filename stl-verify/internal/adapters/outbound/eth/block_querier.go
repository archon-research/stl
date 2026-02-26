package eth

import (
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/ethclient"
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

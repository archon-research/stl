package outbound

import "context"

type BlockQuerier interface {
	BlockNumber(ctx context.Context) (uint64, error)
	FinalizedBlockNumber(ctx context.Context) (uint64, error)
}

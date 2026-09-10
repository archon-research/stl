// Package blocktime memoizes block header timestamps for a single offline run
// (backfill, bootstrap) so a block bearing several events is fetched from the
// node only once.
//
// Keyed and fetched by block HASH, not number: the replay paths that use this
// pin every state read to the log's block hash, so a log's timestamp must come
// from that exact block. A number-pinned HeaderByNumber could return a different
// block across a reorg, stamping snapshots with the wrong time.
package blocktime

import (
	"context"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
)

// HeaderFetcher is the subset of *ethclient.Client the cache needs, narrowed so
// the cache can be tested with a fake.
type HeaderFetcher interface {
	HeaderByHash(ctx context.Context, hash common.Hash) (*ethtypes.Header, error)
}

// Cache is a single-run, non-concurrent block hash → timestamp memo.
type Cache struct {
	fetcher HeaderFetcher
	cache   map[common.Hash]time.Time
}

func New(fetcher HeaderFetcher) *Cache {
	return &Cache{fetcher: fetcher, cache: make(map[common.Hash]time.Time)}
}

// TimestampAt returns the UTC timestamp of the block identified by blockHash,
// fetching the header on the first request for that hash.
func (c *Cache) TimestampAt(ctx context.Context, blockHash common.Hash) (time.Time, error) {
	if ts, ok := c.cache[blockHash]; ok {
		return ts, nil
	}
	header, err := c.fetcher.HeaderByHash(ctx, blockHash)
	if err != nil {
		return time.Time{}, fmt.Errorf("fetching header for block %s: %w", blockHash.Hex(), err)
	}
	ts := time.Unix(int64(header.Time), 0).UTC()
	c.cache[blockHash] = ts
	return ts, nil
}

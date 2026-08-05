package blocktime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
)

type fakeHeaderFetcher struct {
	calls      map[common.Hash]int
	timeByHash map[common.Hash]uint64
	err        error
}

func (f *fakeHeaderFetcher) HeaderByHash(_ context.Context, hash common.Hash) (*ethtypes.Header, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.calls[hash]++
	return &ethtypes.Header{Time: f.timeByHash[hash]}, nil
}

// TestCache_FetchesEachHashOnce fetches each distinct block hash once and reuses
// the result.
func TestCache_FetchesEachHashOnce(t *testing.T) {
	hashA := common.HexToHash("0xaa")
	hashB := common.HexToHash("0xbb")
	f := &fakeHeaderFetcher{calls: map[common.Hash]int{}, timeByHash: map[common.Hash]uint64{hashA: 1_700_000_000, hashB: 1_700_000_500}}
	c := New(f)

	ts1, err := c.TimestampAt(context.Background(), hashA)
	if err != nil {
		t.Fatalf("TimestampAt(hashA): %v", err)
	}
	ts2, err := c.TimestampAt(context.Background(), hashA)
	if err != nil {
		t.Fatalf("TimestampAt(hashA) again: %v", err)
	}
	if !ts1.Equal(ts2) || !ts1.Equal(time.Unix(1_700_000_000, 0).UTC()) {
		t.Errorf("timestamps = %v / %v, want %v", ts1, ts2, time.Unix(1_700_000_000, 0).UTC())
	}
	if f.calls[hashA] != 1 {
		t.Errorf("hashA fetched %d times, want 1 (cached)", f.calls[hashA])
	}

	if _, err := c.TimestampAt(context.Background(), hashB); err != nil {
		t.Fatalf("TimestampAt(hashB): %v", err)
	}
	if f.calls[hashB] != 1 {
		t.Errorf("hashB fetched %d times, want 1", f.calls[hashB])
	}
}

// TestCache_FetchErrorPropagates: a header fetch failure must surface (transient
// RPC failure → stop, retry), never yield a zero timestamp.
func TestCache_FetchErrorPropagates(t *testing.T) {
	f := &fakeHeaderFetcher{calls: map[common.Hash]int{}, err: errors.New("rpc down")}
	c := New(f)
	if _, err := c.TimestampAt(context.Background(), common.HexToHash("0xaa")); err == nil {
		t.Fatal("expected the fetch error to propagate")
	}
}

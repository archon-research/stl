package orderbook_watcher

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

type mockSubscriber struct {
	ch     chan entity.OrderbookSnapshot
	closed bool
}

func newMockSubscriber() *mockSubscriber {
	return &mockSubscriber{ch: make(chan entity.OrderbookSnapshot, 10)}
}
func (m *mockSubscriber) Subscribe(_ context.Context) (<-chan entity.OrderbookSnapshot, error) {
	return m.ch, nil
}
func (m *mockSubscriber) Unsubscribe() error { m.closed = true; close(m.ch); return nil }
func (m *mockSubscriber) HealthCheck(_ context.Context) error { return nil }

type mockRepo struct {
	mu        sync.Mutex
	snapshots []*entity.OrderbookSnapshot
}

func (m *mockRepo) SaveSnapshots(_ context.Context, snaps []*entity.OrderbookSnapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snapshots = append(m.snapshots, snaps...)
	return nil
}
func (m *mockRepo) GetLatestSnapshot(_ context.Context, _, _ string) (*entity.OrderbookSnapshot, error) {
	return nil, nil
}
func (m *mockRepo) GetLatestSnapshotsForSymbol(_ context.Context, _ string) ([]*entity.OrderbookSnapshot, error) {
	return nil, nil
}
func (m *mockRepo) GetSnapshotsInRange(_ context.Context, _ string, _, _ time.Time) ([]*entity.OrderbookSnapshot, error) {
	return nil, nil
}

type mockCache struct {
	mu    sync.Mutex
	items map[string]*entity.OrderbookSnapshot
}

func newMockCache() *mockCache {
	return &mockCache{items: make(map[string]*entity.OrderbookSnapshot)}
}
func (m *mockCache) SetLatest(_ context.Context, snap *entity.OrderbookSnapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items[snap.Exchange+":"+snap.Symbol] = snap
	return nil
}

func TestService_CachesEveryUpdate(t *testing.T) {
	sub := newMockSubscriber()
	repo := &mockRepo{}
	cache := newMockCache()

	svc, err := NewService(Config{FlushInterval: 1 * time.Hour}, sub, repo, cache, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := svc.Start(ctx); err != nil {
		t.Fatal(err)
	}

	sub.ch <- entity.OrderbookSnapshot{
		Exchange: "binance", Symbol: "BTC", CapturedAt: time.Now(),
		Bids: []entity.OrderbookLevel{{Price: 95000, Size: 1, Liquidity: 95000}},
		Asks: []entity.OrderbookLevel{{Price: 95001, Size: 1, Liquidity: 95001}},
	}
	time.Sleep(100 * time.Millisecond)

	cache.mu.Lock()
	_, ok := cache.items["binance:BTC"]
	cache.mu.Unlock()
	if !ok {
		t.Error("expected snapshot in cache")
	}

	cancel()
	svc.Stop()
}

func TestService_FlushesToDBOnInterval(t *testing.T) {
	sub := newMockSubscriber()
	repo := &mockRepo{}
	cache := newMockCache()

	svc, err := NewService(Config{FlushInterval: 200 * time.Millisecond}, sub, repo, cache, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := svc.Start(ctx); err != nil {
		t.Fatal(err)
	}

	sub.ch <- entity.OrderbookSnapshot{Exchange: "binance", Symbol: "BTC", CapturedAt: time.Now(), Bids: []entity.OrderbookLevel{}, Asks: []entity.OrderbookLevel{}}
	sub.ch <- entity.OrderbookSnapshot{Exchange: "bybit", Symbol: "BTC", CapturedAt: time.Now(), Bids: []entity.OrderbookLevel{}, Asks: []entity.OrderbookLevel{}}
	time.Sleep(400 * time.Millisecond)

	repo.mu.Lock()
	count := len(repo.snapshots)
	repo.mu.Unlock()
	if count == 0 {
		t.Error("expected snapshots flushed to repo")
	}

	cancel()
	svc.Stop()
}

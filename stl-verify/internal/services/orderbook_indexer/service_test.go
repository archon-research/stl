package orderbook_indexer

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/cex"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// --- fakes --------------------------------------------------------------

type fakeConsumer struct {
	mu      sync.Mutex
	queue   []outbound.SQSMessage
	deleted []string
	closed  bool
	recvErr error
}

func (c *fakeConsumer) ReceiveMessages(ctx context.Context, max int) ([]outbound.SQSMessage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.recvErr != nil {
		return nil, c.recvErr
	}
	if len(c.queue) == 0 {
		// Don't return instantly forever — sleep briefly so the consume
		// loop doesn't hot-spin in tests.
		c.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-time.After(10 * time.Millisecond):
		}
		c.mu.Lock()
	}
	n := len(c.queue)
	if n > max {
		n = max
	}
	out := append([]outbound.SQSMessage(nil), c.queue[:n]...)
	c.queue = c.queue[n:]
	return out, nil
}

func (c *fakeConsumer) DeleteMessage(ctx context.Context, receiptHandle string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deleted = append(c.deleted, receiptHandle)
	return nil
}

func (c *fakeConsumer) Close() error {
	c.closed = true
	return nil
}

func (c *fakeConsumer) enqueue(msg outbound.SQSMessage) {
	c.mu.Lock()
	c.queue = append(c.queue, msg)
	c.mu.Unlock()
}

func (c *fakeConsumer) deletedSnapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.deleted...)
}

type fakeRepo struct {
	mu      sync.Mutex
	saved   [][]*entity.OrderbookSnapshot
	saveErr error
}

func (r *fakeRepo) SaveSnapshots(ctx context.Context, snaps []*entity.OrderbookSnapshot) error {
	if r.saveErr != nil {
		return r.saveErr
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.saved = append(r.saved, snaps)
	return nil
}

func (r *fakeRepo) GetLatestSnapshot(ctx context.Context, exchange, symbol string) (*entity.OrderbookSnapshot, error) {
	return nil, nil
}

func (r *fakeRepo) GetLatestSnapshotsForSymbol(ctx context.Context, symbol string) ([]*entity.OrderbookSnapshot, error) {
	return nil, nil
}

func (r *fakeRepo) GetSnapshotsInRange(ctx context.Context, symbol string, from, to time.Time) ([]*entity.OrderbookSnapshot, error) {
	return nil, nil
}

func (r *fakeRepo) totalSavedSnapshots() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	total := 0
	for _, batch := range r.saved {
		total += len(batch)
	}
	return total
}

type fakeCache struct {
	mu     sync.Mutex
	writes []*entity.OrderbookSnapshot
	setErr error
}

func (c *fakeCache) SetLatest(ctx context.Context, snap *entity.OrderbookSnapshot) error {
	if c.setErr != nil {
		return c.setErr
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writes = append(c.writes, snap)
	return nil
}

func (c *fakeCache) writeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.writes)
}

// fakeParser is a parser that always returns one snapshot for the
// configured exchange when given any non-empty payload.
type fakeParser struct {
	exchange string
	count    atomic.Int32
	parseErr error
}

func (p *fakeParser) Exchange() string { return p.exchange }
func (p *fakeParser) ParseMessage(msg []byte) ([]entity.OrderbookSnapshot, error) {
	if p.parseErr != nil {
		return nil, p.parseErr
	}
	if len(msg) == 0 {
		return nil, nil
	}
	p.count.Add(1)
	return []entity.OrderbookSnapshot{{
		Exchange:   p.exchange,
		Symbol:     "BTC",
		CapturedAt: time.Now(),
	}}, nil
}

// --- helpers ------------------------------------------------------------

func envelopeMessage(t *testing.T, source string, payload []byte, receiptHandle string) outbound.SQSMessage {
	t.Helper()
	body, err := json.Marshal(entity.RawCEXMessage{
		Source:     source,
		CapturedAt: time.Now(),
		Payload:    payload,
	})
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return outbound.SQSMessage{
		MessageID:     receiptHandle,
		ReceiptHandle: receiptHandle,
		Body:          string(body),
	}
}

// --- tests --------------------------------------------------------------

func TestNewService_Validation(t *testing.T) {
	parsers := map[string]cex.Parser{"binance": &fakeParser{exchange: "binance"}}
	cases := []struct {
		name     string
		consumer outbound.SQSConsumer
		parsers  map[string]cex.Parser
		repo     outbound.OrderbookRepository
		wantErr  bool
	}{
		{"nil consumer", nil, parsers, &fakeRepo{}, true},
		{"empty parsers", &fakeConsumer{}, map[string]cex.Parser{}, &fakeRepo{}, true},
		{"nil repo", &fakeConsumer{}, parsers, nil, true},
		{"ok", &fakeConsumer{}, parsers, &fakeRepo{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewService(Config{}, tc.consumer, tc.parsers, tc.repo, &fakeCache{})
			if (err != nil) != tc.wantErr {
				t.Errorf("got err=%v, wantErr=%v", err, tc.wantErr)
			}
		})
	}
}

func TestService_HappyPath_ParsesCachesAndFlushes(t *testing.T) {
	consumer := &fakeConsumer{}
	repo := &fakeRepo{}
	cache := &fakeCache{}
	parser := &fakeParser{exchange: "binance"}

	svc, err := NewService(
		Config{FlushInterval: 50 * time.Millisecond},
		consumer,
		map[string]cex.Parser{"binance": parser},
		repo,
		cache,
	)
	if err != nil {
		t.Fatal(err)
	}

	consumer.enqueue(envelopeMessage(t, "binance", []byte(`{"depth":[]}`), "r1"))
	consumer.enqueue(envelopeMessage(t, "binance", []byte(`{"depth":[]}`), "r2"))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()

	// Wait until both messages are processed (cache writes happen
	// immediately, before the buffer flush).
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && cache.writeCount() < 2 {
		time.Sleep(10 * time.Millisecond)
	}
	if cache.writeCount() < 2 {
		t.Fatalf("expected 2 cache writes, got %d", cache.writeCount())
	}

	// Both messages should be deleted from SQS after processing.
	if len(consumer.deletedSnapshot()) < 2 {
		t.Errorf("expected at least 2 deleted messages, got %d", len(consumer.deletedSnapshot()))
	}

	// Wait for flush ticker to fire.
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) && repo.totalSavedSnapshots() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if repo.totalSavedSnapshots() == 0 {
		t.Errorf("expected flushed snapshots, got 0")
	}

	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run returned error: %v", err)
	}
}

func TestService_UnknownSourceMessageIsDeleted(t *testing.T) {
	consumer := &fakeConsumer{}
	consumer.enqueue(envelopeMessage(t, "unknown-cex", []byte("x"), "r-unknown"))

	svc, err := NewService(
		Config{FlushInterval: 50 * time.Millisecond},
		consumer,
		map[string]cex.Parser{"binance": &fakeParser{exchange: "binance"}},
		&fakeRepo{},
		&fakeCache{},
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	go svc.Run(ctx)

	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) && len(consumer.deletedSnapshot()) == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	got := consumer.deletedSnapshot()
	if len(got) != 1 || got[0] != "r-unknown" {
		t.Errorf("expected single delete of r-unknown, got %v", got)
	}
}

func TestService_MalformedMessageIsDeleted(t *testing.T) {
	consumer := &fakeConsumer{}
	consumer.enqueue(outbound.SQSMessage{
		MessageID:     "bad",
		ReceiptHandle: "r-bad",
		Body:          `not json at all`,
	})

	svc, err := NewService(
		Config{FlushInterval: 50 * time.Millisecond},
		consumer,
		map[string]cex.Parser{"binance": &fakeParser{exchange: "binance"}},
		&fakeRepo{},
		&fakeCache{},
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	go svc.Run(ctx)

	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) && len(consumer.deletedSnapshot()) == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if got := consumer.deletedSnapshot(); len(got) != 1 || got[0] != "r-bad" {
		t.Errorf("expected delete of r-bad, got %v", got)
	}
}

func TestService_ParseErrorKeepsMessage(t *testing.T) {
	consumer := &fakeConsumer{}
	consumer.enqueue(envelopeMessage(t, "binance", []byte("x"), "r-keep"))

	parser := &fakeParser{exchange: "binance", parseErr: errors.New("bad payload")}
	svc, err := NewService(
		Config{FlushInterval: 50 * time.Millisecond},
		consumer,
		map[string]cex.Parser{"binance": parser},
		&fakeRepo{},
		&fakeCache{},
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	go svc.Run(ctx)

	time.Sleep(200 * time.Millisecond)
	if len(consumer.deletedSnapshot()) != 0 {
		t.Errorf("expected no deletes on parse failure (SQS redelivery), got %v", consumer.deletedSnapshot())
	}
}

func TestService_RunFlushesOnShutdown(t *testing.T) {
	consumer := &fakeConsumer{}
	repo := &fakeRepo{}
	parser := &fakeParser{exchange: "binance"}

	// Long flush interval — only the shutdown flush should fire.
	svc, err := NewService(
		Config{FlushInterval: time.Hour},
		consumer,
		map[string]cex.Parser{"binance": parser},
		repo,
		&fakeCache{},
	)
	if err != nil {
		t.Fatal(err)
	}

	consumer.enqueue(envelopeMessage(t, "binance", []byte("x"), "r1"))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()

	// Give the consumer a moment to process.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && parser.count.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run returned error: %v", err)
	}
	if repo.totalSavedSnapshots() == 0 {
		t.Error("expected shutdown flush to save buffered snapshots")
	}
}

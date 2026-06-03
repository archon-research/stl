// Package poller provides the shared streaming lifecycle for REST-polling
// exchange order-book adapters. Each exchange adapter embeds a *Poller and
// supplies only its exchange-specific FetchFunc and config defaults; the
// concurrency-sensitive machinery (one-shot streaming guard, ticker loop,
// dual-cancellation sends, channel ownership) lives here once.
package poller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// FetchFunc retrieves and normalizes a single order-book snapshot for one
// symbol. Implementations are exchange-specific (REST call plus response
// parsing) and are responsible for returning a fully populated, validated
// snapshot or an error. The Poller forwards whatever a FetchFunc returns to
// the snapshot channel as-is; it does not re-validate the result.
type FetchFunc func(ctx context.Context, symbol string) (entity.OrderBookSnapshot, error)

type Poller struct {
	name      string
	fetch     FetchFunc
	pollEvery time.Duration
	bufSize   int

	symbols []string
	once    sync.Once
	// streaming guards against a second Stream call reusing the channels:
	// the stream goroutine owns snapCh/errCh and closes them on exit, so a
	// second goroutine writing/closing the same channels would panic and race.
	streaming atomic.Bool
	closeCh   chan struct{}
}

// New builds a Poller. pollEvery and bufSize are assumed already validated and
// defaulted by the embedding adapter's Config. A nil fetch or non-positive
// pollEvery is a programming error (the resulting Poller could never stream),
// so New panics rather than returning a silently broken value.
func New(name string, fetch FetchFunc, pollEvery time.Duration, bufSize int) *Poller {
	if fetch == nil {
		panic("poller: fetch must not be nil")
	}
	if pollEvery <= 0 {
		panic("poller: pollEvery must be positive")
	}
	return &Poller{
		name:      name,
		fetch:     fetch,
		pollEvery: pollEvery,
		bufSize:   bufSize,
		closeCh:   make(chan struct{}),
	}
}

func (p *Poller) Name() string {
	return p.name
}

func (p *Poller) Connect(_ context.Context, symbols []string) error {
	if len(symbols) == 0 {
		return errors.New("symbols must not be empty")
	}
	for _, s := range symbols {
		if strings.TrimSpace(s) == "" {
			return errors.New("symbol must not be blank")
		}
		if strings.TrimSpace(s) != s {
			return fmt.Errorf("symbol %q must not have surrounding whitespace", s)
		}
	}
	cp := make([]string, len(symbols))
	copy(cp, symbols)
	p.symbols = cp
	return nil
}

func (p *Poller) Stream(ctx context.Context) (<-chan entity.OrderBookSnapshot, <-chan error) {
	if len(p.symbols) == 0 {
		return failedStream(errors.New("Connect must be called before Stream"))
	}
	if !p.streaming.CompareAndSwap(false, true) {
		return failedStream(errors.New("Stream already called; create a new Adapter to start a new stream"))
	}

	snapCh := make(chan entity.OrderBookSnapshot, p.bufSize)
	errCh := make(chan error, 1)

	symbols := make([]string, len(p.symbols))
	copy(symbols, p.symbols)

	go func() {
		defer close(snapCh)
		defer close(errCh)

		ticker := time.NewTicker(p.pollEvery)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-p.closeCh:
				return
			case <-ticker.C:
				for _, sym := range symbols {
					snap, err := p.fetch(ctx, sym)
					if err != nil {
						errCh <- fmt.Errorf("fetching %s: %w", sym, err)
						return
					}
					// Inner select: stay cancellable while blocked trying to
					// deliver into a full snapCh (slow/absent consumer).
					// Without these cases the goroutine would leak.
					select {
					case snapCh <- snap:
					case <-ctx.Done():
						return
					case <-p.closeCh:
						return
					}
				}
			}
		}
	}()

	return snapCh, errCh
}

func (p *Poller) Close() error {
	p.once.Do(func() { close(p.closeCh) })
	return nil
}

// failedStream returns already-closed channels carrying a single terminal
// error. Used for misuse paths (Stream before Connect, or a second Stream
// call) so the caller observes the error and a closed snapshot channel without
// any goroutine being started.
func failedStream(err error) (<-chan entity.OrderBookSnapshot, <-chan error) {
	snapCh := make(chan entity.OrderBookSnapshot)
	errCh := make(chan error, 1)
	errCh <- err
	close(snapCh)
	close(errCh)
	return snapCh, errCh
}

package allocation_tracker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"sync"
)

// SourceRegistry routes token entries to the appropriate PositionSource.
type SourceRegistry struct {
	sources []PositionSource
	logger  *slog.Logger

	mu            sync.Mutex
	seenUntracked map[string]struct{}
}

func NewSourceRegistry(logger *slog.Logger) *SourceRegistry {
	return &SourceRegistry{
		logger:        logger.With("component", "source-registry"),
		seenUntracked: make(map[string]struct{}),
	}
}

func (r *SourceRegistry) Register(source PositionSource) {
	r.sources = append(r.sources, source)
	r.logger.Info("registered position source", "name", source.Name())
}

func (r *SourceRegistry) Route(entry *TokenEntry) PositionSource {
	for _, source := range r.sources {
		if source.Supports(entry.TokenType, entry.Protocol) {
			return source
		}
	}
	return nil
}

// warnUnsupportedOnce warns that an entry has no matching source at all, so its
// positions are silently not tracked.
func (r *SourceRegistry) warnUnsupportedOnce(entry *TokenEntry) {
	r.warnUntrackedOnce(entry, "unsupported entry skipped; position not tracked")
}

// warnStubbedOnce warns that an entry matched a not-yet-implemented stub source.
// The stub returns no balances, so these positions are silently not tracked even
// though they route to a "supported" source — surface it like a true gap.
func (r *SourceRegistry) warnStubbedOnce(entry *TokenEntry) {
	r.warnUntrackedOnce(entry, "entry matched a not-yet-implemented stub source; position not tracked")
}

// warnUntrackedOnce logs an untracked entry at Warn, but only once per distinct
// (token type, protocol), so a steady-state gap stays visible without flooding
// the logs on every sweep.
func (r *SourceRegistry) warnUntrackedOnce(entry *TokenEntry, msg string) {
	key := entry.TokenType + "|" + entry.Protocol

	r.mu.Lock()
	_, seen := r.seenUntracked[key]
	if !seen {
		r.seenUntracked[key] = struct{}{}
	}
	r.mu.Unlock()

	if seen {
		return
	}
	r.logger.Warn(msg,
		"type", entry.TokenType,
		"protocol", entry.Protocol,
		"exampleContract", entry.ContractAddress.Hex())
}

// FetchAll groups entries by source, fetches in batch, unions both the balance
// and supply maps across sources, and returns the aggregated FetchResult.
func (r *SourceRegistry) FetchAll(ctx context.Context, entries []*TokenEntry, blockNumber int64) (*FetchResult, error) {
	grouped := make(map[PositionSource][]*TokenEntry)
	for _, entry := range entries {
		source := r.Route(entry)
		if source == nil {
			r.warnUnsupportedOnce(entry)
			continue
		}
		if _, ok := source.(placeholderSource); ok {
			// Matched a stub that records nothing — flag it so these positions
			// aren't silently dropped, then still let the (no-op) stub run.
			r.warnStubbedOnce(entry)
		}
		grouped[source] = append(grouped[source], entry)
	}

	aggregate := NewFetchResult()
	var errs []error

	for source, sourceEntries := range grouped {
		res, err := source.FetchBalances(ctx, sourceEntries, blockNumber)
		if err != nil {
			r.logger.Error("source fetch failed",
				"source", source.Name(),
				"entries", len(sourceEntries),
				"error", err)
			errs = append(errs, fmt.Errorf("%s: %w", source.Name(), err))
			continue
		}
		if res == nil {
			// A source must return a non-nil result or an error; a bare (nil, nil)
			// would silently drop its whole group, so surface it as a fetch failure.
			errs = append(errs, fmt.Errorf("%s: returned nil result without error", source.Name()))
			continue
		}
		maps.Copy(aggregate.Balances, res.Balances)
		maps.Copy(aggregate.Supplies, res.Supplies)
	}

	if len(errs) > 0 {
		return aggregate, fmt.Errorf("partial fetch failures: %w", errors.Join(errs...))
	}
	return aggregate, nil
}

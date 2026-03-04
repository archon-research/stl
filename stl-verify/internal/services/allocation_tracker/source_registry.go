package allocation_tracker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
)

// SourceRegistry routes token entries to the appropriate PositionSource.
type SourceRegistry struct {
	sources []PositionSource
	logger  *slog.Logger
}

func NewSourceRegistry(logger *slog.Logger) *SourceRegistry {
	return &SourceRegistry{
		logger: logger.With("component", "source-registry"),
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

// FetchAll groups entries by source, fetches in batch, returns all results.
func (r *SourceRegistry) FetchAll(ctx context.Context, entries []*TokenEntry, blockNumber int64) (map[EntryKey]*PositionBalance, error) {
	grouped := make(map[PositionSource][]*TokenEntry)
	for _, entry := range entries {
		source := r.Route(entry)
		if source == nil {
			r.logger.Debug("unsupported entry skipped",
				"contract", entry.ContractAddress.Hex(),
				"type", entry.TokenType,
				"protocol", entry.Protocol)
			continue
		}
		grouped[source] = append(grouped[source], entry)
	}

	results := make(map[EntryKey]*PositionBalance)
	var errs []error

	for source, sourceEntries := range grouped {
		balances, err := source.FetchBalances(ctx, sourceEntries, blockNumber)
		if err != nil {
			r.logger.Error("source fetch failed",
				"source", source.Name(),
				"entries", len(sourceEntries),
				"error", err)
			errs = append(errs, fmt.Errorf("%s: %w", source.Name(), err))
			continue
		}
		maps.Copy(results, balances)
	}

	if len(errs) > 0 {
		return results, fmt.Errorf("partial fetch failures: %w", errors.Join(errs...))
	}
	return results, nil
}

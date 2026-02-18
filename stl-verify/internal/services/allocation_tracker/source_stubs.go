package allocation_tracker

import (
	"context"
	"log/slog"
)

// SkipSource is a no-op for token types handled by other workers.
type SkipSource struct {
	name      string
	tokenType string
	protocols map[string]bool
	logger    *slog.Logger
}

func NewSkipSource(name, tokenType string, protocols []string, logger *slog.Logger) *SkipSource {
	m := make(map[string]bool, len(protocols))
	for _, p := range protocols {
		m[p] = true
	}
	return &SkipSource{name: name, tokenType: tokenType, protocols: m, logger: logger}
}

func (s *SkipSource) Name() string { return s.name }

func (s *SkipSource) Supports(tokenType, protocol string) bool {
	if tokenType != s.tokenType {
		return false
	}
	if len(s.protocols) == 0 {
		return true
	}
	return s.protocols[protocol]
}

func (s *SkipSource) FetchBalances(ctx context.Context, entries []*TokenEntry, blockNumber int64) (map[EntryKey]*PositionBalance, error) {
	s.logger.Debug("skipping — handled by existing worker", "source", s.name, "count", len(entries))
	return make(map[EntryKey]*PositionBalance), nil
}

// StubSource is a placeholder for types not yet implemented.
type StubSource struct {
	name      string
	tokenType string
	logger    *slog.Logger
}

func NewStubSource(name, tokenType string, logger *slog.Logger) *StubSource {
	return &StubSource{name: name, tokenType: tokenType, logger: logger}
}

func (s *StubSource) Name() string { return s.name }

func (s *StubSource) Supports(tokenType, protocol string) bool {
	return tokenType == s.tokenType
}

func (s *StubSource) FetchBalances(ctx context.Context, entries []*TokenEntry, blockNumber int64) (map[EntryKey]*PositionBalance, error) {
	s.logger.Debug("stub — not yet implemented", "source", s.name, "count", len(entries))
	return make(map[EntryKey]*PositionBalance), nil
}

// DefaultSkipSources returns sources for types handled by the existing SparkLend worker.
func DefaultSkipSources(logger *slog.Logger) []PositionSource {
	return []PositionSource{
		NewSkipSource("sparklend-skip", "atoken", []string{"sparklend"}, logger),
		NewSkipSource("aave-skip", "atoken", []string{"aave"}, logger),
		NewSkipSource("anchorage-skip", "anchorage", nil, logger),
	}
}

// DefaultStubSources returns placeholders for types not yet implemented.
func DefaultStubSources(logger *slog.Logger) []PositionSource {
	return []PositionSource{
		NewStubSource("psm3", "psm3", logger),
		NewStubSource("centrifuge", "centrifuge", logger),
		NewStubSource("centrifuge-feeder", "centrifuge_feeder", logger),
		NewStubSource("uni-v3-pool", "uni_v3_pool", logger),
		NewStubSource("uni-v3-lp", "uni_v3_lp", logger),
		NewStubSource("galaxy-clo", "galaxy_clo", logger),
	}
}

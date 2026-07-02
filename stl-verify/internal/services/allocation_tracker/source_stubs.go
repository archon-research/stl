package allocation_tracker

import (
	"context"
	"log/slog"

	"github.com/ethereum/go-ethereum/common"
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

func (s *SkipSource) FetchBalances(ctx context.Context, entries []*TokenEntry, blockNumber int64, blockHash common.Hash) (*FetchResult, error) {
	s.logger.Debug("skipping — handled by existing worker", "source", s.name, "count", len(entries))
	return NewFetchResult(), nil
}

// placeholderSource marks a PositionSource that matches entries but records no
// balances yet (StubSource). The registry uses it to surface silently-untracked
// positions, distinct from a SkipSource whose work is intentionally done elsewhere.
type placeholderSource interface {
	isPlaceholder()
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

// isPlaceholder marks StubSource as a no-op placeholder (see placeholderSource).
func (s *StubSource) isPlaceholder() {}

func (s *StubSource) Name() string { return s.name }

func (s *StubSource) Supports(tokenType, protocol string) bool {
	return tokenType == s.tokenType
}

func (s *StubSource) FetchBalances(ctx context.Context, entries []*TokenEntry, blockNumber int64, blockHash common.Hash) (*FetchResult, error) {
	s.logger.Debug("stub — not yet implemented", "source", s.name, "count", len(entries))
	return NewFetchResult(), nil
}

// defaultSkipSources returns sources for types handled by the existing SparkLend worker.
func defaultSkipSources(logger *slog.Logger) []PositionSource {
	return []PositionSource{
		NewSkipSource("anchorage-skip", "anchorage", nil, logger),
	}
}

// defaultStubSources returns placeholders for types not yet implemented.
func defaultStubSources(logger *slog.Logger) []PositionSource {
	// "centrifuge" is intentionally absent: Centrifuge tranche tokens are plain
	// ERC20s and are handled by BalanceOfSource. centrifuge_feeder is a different
	// mechanism and remains a stub until implemented.
	return []PositionSource{
		NewStubSource("psm3", "psm3", logger),
		NewStubSource("centrifuge-feeder", "centrifuge_feeder", logger),
		NewStubSource("galaxy-clo", "galaxy_clo", logger),
	}
}

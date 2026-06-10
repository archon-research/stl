package allocation_tracker

import (
	"fmt"
	"log/slog"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// BuildSourceRegistry assembles the production SourceRegistry — skip sources, the
// balanceOf / erc4626 / curve / uni-v3 fetchers, and the not-yet-implemented stubs —
// in the registration order the worker relies on (earlier sources win in Route).
//
// The multicaller is only used at fetch time (FetchBalances); routing (Route /
// Supports) needs no live client, so tests can pass a nil multicaller to assert which
// source every contract entry routes to. Keeping assembly here rather than inline in
// main means the routing guardrail test (TestEveryContractEntryRoutes) and the worker
// share a single definition and cannot drift.
func BuildSourceRegistry(mc outbound.Multicaller, logger *slog.Logger) (*SourceRegistry, error) {
	registry := NewSourceRegistry(logger)

	for _, s := range DefaultSkipSources(logger) {
		registry.Register(s)
	}

	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		return nil, fmt.Errorf("erc20 abi: %w", err)
	}
	atokenReadABI, err := abis.GetATokenReadABI()
	if err != nil {
		return nil, fmt.Errorf("atoken read abi: %w", err)
	}
	registry.Register(NewBalanceOfSource(mc, erc20ABI, atokenReadABI, logger))

	erc4626, err := NewERC4626Source(mc, logger)
	if err != nil {
		return nil, fmt.Errorf("erc4626 source: %w", err)
	}
	registry.Register(erc4626)

	curveABI, err := abis.GetCurvePoolABI()
	if err != nil {
		return nil, fmt.Errorf("curve abi: %w", err)
	}
	registry.Register(NewCurveSource(mc, curveABI, logger))

	uniV3, err := NewUniV3Source(mc, logger)
	if err != nil {
		return nil, fmt.Errorf("uni-v3 source: %w", err)
	}
	registry.Register(uniV3)

	for _, s := range DefaultStubSources(logger) {
		registry.Register(s)
	}

	return registry, nil
}

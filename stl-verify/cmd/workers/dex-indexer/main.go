package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"

	"github.com/archon-research/stl/stl-verify/cmd/workers/internal/dexbootstrap"
	"github.com/archon-research/stl/stl-verify/internal/common/sqsutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/lifecycle"
	"github.com/archon-research/stl/stl-verify/internal/services/dexconsumer"
)

var (
	GitCommit string
	GitBranch string
	BuildTime string
)

func init() {
	buildinfo.PopulateFromVCS(&GitCommit, &BuildTime)
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	err := run(ctx, os.Args[1:], lifecycle.ForceExitAfter(lifecycle.ShutdownTailBudget))
	cancel()
	if err != nil {
		slog.Error("fatal error", "error", err)
		os.Exit(1)
	}
}

// newRegistry builds the DEX -> Factory map explicitly (no init()
// registration / package-level singletons), so the set of supported DEXes is
// visible at the single call site in run.
func newRegistry() map[string]Factory {
	factories := []Factory{curveFactory{}, uniswapV3Factory{}}
	registry := make(map[string]Factory, len(factories))
	for _, f := range factories {
		registry[f.Kind()] = f
	}
	return registry
}

// This worker has no lifecycle.Service to stop: RunLoop bounds its own shutdown,
// leaving deps.Close() as an unbounded tail — pgxpool.Close waits on an abandoned
// handler's connection past the pod grace period, in silence. onTeardownTimeout
// bounds that tail; tests pass nil, which ForceExitAfter would take down with them.
func run(ctx context.Context, args []string, onTeardownTimeout func()) error {
	cfg, err := dexbootstrap.ParseConfig("dex-indexer", args)
	if err != nil {
		return err
	}

	registry := newRegistry()
	f, ok := registry[cfg.Dex]
	if !ok {
		keys := make([]string, 0, len(registry))
		for k := range registry {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		return fmt.Errorf("unknown DEX %q, valid values: %v", cfg.Dex, keys)
	}

	deps, err := dexbootstrap.Bootstrap(ctx, cfg, dexbootstrap.BootstrapOptions{
		ServiceName:  f.ServiceName(),
		MetricPrefix: f.MetricPrefix(),
		BuildTime:    BuildTime,
		GitBranch:    GitBranch,
	})
	if err != nil {
		return err
	}
	defer deps.Close()
	// Registered after Close so it runs first, arming the guard that bounds it.
	defer armTeardownGuard(onTeardownTimeout)
	if err := deps.CommonDeps().Validate(); err != nil {
		return fmt.Errorf("validating deps: %w", err)
	}

	// The visibility-timeout guard is fatal, so it runs before BuildHandler loads
	// the pool registry: a misconfigured pod would otherwise re-run that load on
	// every CrashLoopBackOff cycle before refusing.
	loop := sqsutil.Config{
		Consumer:     deps.SQSConsumer,
		MaxMessages:  cfg.MaxMessages,
		PollInterval: 1 * time.Second,
		Logger:       deps.Logger,
		ChainID:      cfg.ChainID,
	}
	if err := loop.Validate(); err != nil {
		return err
	}

	handler, err := f.BuildHandler(ctx, deps, cfg)
	if err != nil {
		return fmt.Errorf("building %s handler: %w", f.Kind(), err)
	}

	bp := dexconsumer.NewBlockProcessor(deps.CacheReader, deps.DexTelemetry, handler)
	sqsutil.RunLoop(ctx, loop, bp.ProcessBlockEvent)
	return nil
}

func armTeardownGuard(onTeardownTimeout func()) {
	if onTeardownTimeout != nil {
		onTeardownTimeout()
	}
}

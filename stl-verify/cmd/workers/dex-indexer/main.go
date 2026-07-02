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
	defer cancel()

	if err := run(ctx, os.Args[1:]); err != nil {
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

func run(ctx context.Context, args []string) error {
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
	})
	if err != nil {
		return err
	}
	defer deps.Close()
	if err := deps.CommonDeps().Validate(); err != nil {
		return fmt.Errorf("validating deps: %w", err)
	}

	handler, err := f.BuildHandler(ctx, deps, cfg)
	if err != nil {
		return fmt.Errorf("building %s handler: %w", f.Kind(), err)
	}

	bp := dexconsumer.NewBlockProcessor(deps.CacheReader, deps.DexTelemetry, handler)
	sqsutil.RunLoop(ctx, sqsutil.Config{
		Consumer:     deps.SQSConsumer,
		MaxMessages:  cfg.MaxMessages,
		PollInterval: 1 * time.Second,
		Logger:       deps.Logger,
		ChainID:      cfg.ChainID,
	}, bp.ProcessBlockEvent)
	return nil
}

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/archon-research/stl/stl-verify/cmd/workers/internal/dexbootstrap"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/lifecycle"
	"github.com/archon-research/stl/stl-verify/internal/services/curve_dex"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

var (
	GitCommit string
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

func run(ctx context.Context, args []string) error {
	cfg, err := dexbootstrap.ParseConfig("curve-dex-worker", args)
	if err != nil {
		return fmt.Errorf("parsing curve-dex-worker config: %w", err)
	}

	deps, err := dexbootstrap.Bootstrap(ctx, cfg, dexbootstrap.BootstrapOptions{
		ServiceName:  "curve-dex-worker",
		MetricPrefix: "curve",
		BuildTime:    BuildTime,
	})
	if err != nil {
		return fmt.Errorf("bootstrapping curve-dex-worker: %w", err)
	}
	defer deps.Close()

	curveRepo, err := postgres.NewCurvePoolRepository(deps.PostgresPool, deps.Logger, deps.BuildRegistry.BuildID())
	if err != nil {
		return fmt.Errorf("creating curve pool repository: %w", err)
	}

	service, err := curve_dex.NewService(
		curve_dex.Config{
			SQSConsumerConfig: shared.SQSConsumerConfig{
				MaxMessages: cfg.MaxMessages,
				Logger:      deps.Logger,
				ChainID:     cfg.ChainID,
			},
			Telemetry: deps.DexTelemetry,
		},
		deps.SQSConsumer,
		deps.CacheReader,
		deps.Multicaller,
		deps.TxManager,
		curveRepo,
		deps.TokenRepo,
		deps.ProtocolRepo,
		deps.EventRepo,
	)
	if err != nil {
		return fmt.Errorf("creating service: %w", err)
	}

	deps.Logger.Info("curve dex worker started, waiting for messages...")
	return lifecycle.Run(ctx, deps.Logger, service)
}

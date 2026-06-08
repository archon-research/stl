package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/cmd/workers/internal/dexbootstrap"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/pkg/lifecycle"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
	"github.com/archon-research/stl/stl-verify/internal/services/uniswap_v3_dex"
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

// resolveNFPMAddress reads NFPM_ADDRESS or falls back to the canonical
// mainnet address baked into uniswap_v3_dex.DefaultNFPMAddress. Pulled out
// from run() because it's the only worker-specific env read.
func resolveNFPMAddress() (common.Address, error) {
	nfpmStr := env.Get("NFPM_ADDRESS", "")
	if nfpmStr == "" {
		return uniswap_v3_dex.DefaultNFPMAddress, nil
	}
	if !common.IsHexAddress(nfpmStr) {
		return common.Address{}, fmt.Errorf("NFPM_ADDRESS %q is not a valid hex address", nfpmStr)
	}
	return common.HexToAddress(nfpmStr), nil
}

func run(ctx context.Context, args []string) error {
	cfg, err := dexbootstrap.ParseConfig("uniswap-v3-dex-worker", args)
	if err != nil {
		return fmt.Errorf("parsing uniswap-v3-dex-worker config: %w", err)
	}

	nfpmAddress, err := resolveNFPMAddress()
	if err != nil {
		return fmt.Errorf("resolving NFPM address: %w", err)
	}

	deps, err := dexbootstrap.Bootstrap(ctx, cfg, dexbootstrap.BootstrapOptions{
		ServiceName:  "uniswap-v3-dex-worker",
		MetricPrefix: "uniswap_v3",
		BuildTime:    BuildTime,
	})
	if err != nil {
		return fmt.Errorf("bootstrapping uniswap-v3-dex-worker: %w", err)
	}
	defer deps.Close()

	uniswapRepo, err := postgres.NewUniswapV3PoolRepository(deps.PostgresPool, deps.Logger, deps.BuildRegistry.BuildID())
	if err != nil {
		return fmt.Errorf("creating uniswap v3 pool repository: %w", err)
	}

	service, err := uniswap_v3_dex.NewService(
		uniswap_v3_dex.Config{
			SQSConsumerConfig: shared.SQSConsumerConfig{
				MaxMessages: cfg.MaxMessages,
				Logger:      deps.Logger,
				ChainID:     cfg.ChainID,
			},
			NFPMAddress: nfpmAddress,
			Telemetry:   deps.DexTelemetry,
		},
		deps.SQSConsumer,
		deps.CacheReader,
		deps.Multicaller,
		deps.TxManager,
		uniswapRepo,
		deps.TokenRepo,
		deps.ProtocolRepo,
		deps.EventRepo,
	)
	if err != nil {
		return fmt.Errorf("creating service: %w", err)
	}

	deps.Logger.Info("uniswap v3 dex worker started, waiting for messages...",
		"nfpm", nfpmAddress.Hex())
	return lifecycle.Run(ctx, deps.Logger, service)
}

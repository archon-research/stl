// Package main runs the CEX L2 order book indexer: a long-running daemon that
// streams aggregated order books from one exchange and persists periodic top-N
// snapshots to TimescaleDB. One pod per exchange, selected by the EXCHANGE env
// var (coinbase|okx|kraken).
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.opentelemetry.io/otel"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/orderbook"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/pkg/lifecycle"
	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/cex_orderbook_indexer"
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

	if err := run(ctx, newProvider); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

// providerFactory builds the order book provider for an exchange. main passes
// newProvider; tests inject a fake so run() can be exercised without dialing a
// real exchange.
type providerFactory func(exchange string, cfg orderbook.Config) (outbound.OrderbookProvider, error)

// cliConfig is the resolved environment configuration for one exchange worker.
type cliConfig struct {
	exchange string
	symbols  []string
	dbURL    string
	depth    int
	interval time.Duration
}

// parseConfig reads the worker's environment. EXCHANGE, SYMBOLS and DATABASE_URL
// are required; ORDERBOOK_DEPTH and ORDERBOOK_INTERVAL fall back to the indexer's
// defaults when unset.
func parseConfig() (cliConfig, error) {
	exchange := strings.ToLower(strings.TrimSpace(env.Get("EXCHANGE", "")))
	if exchange == "" {
		return cliConfig{}, fmt.Errorf("EXCHANGE environment variable is required")
	}

	symbols := parseSymbols(env.Get("SYMBOLS", ""))
	if len(symbols) == 0 {
		return cliConfig{}, fmt.Errorf("SYMBOLS environment variable is required (comma-separated)")
	}

	dbURL := env.Get("DATABASE_URL", "")
	if dbURL == "" {
		return cliConfig{}, fmt.Errorf("DATABASE_URL environment variable is required")
	}

	depth, err := env.GetInt("ORDERBOOK_DEPTH", 100)
	if err != nil {
		return cliConfig{}, err
	}
	if depth <= 0 {
		return cliConfig{}, fmt.Errorf("ORDERBOOK_DEPTH must be > 0, got %d", depth)
	}

	interval, err := env.GetDuration("ORDERBOOK_INTERVAL", 5*time.Second)
	if err != nil {
		return cliConfig{}, err
	}
	if interval <= 0 {
		return cliConfig{}, fmt.Errorf("ORDERBOOK_INTERVAL must be > 0, got %s", interval)
	}

	return cliConfig{exchange: exchange, symbols: symbols, dbURL: dbURL, depth: depth, interval: interval}, nil
}

// parseSymbols splits a comma-separated symbol list, trimming whitespace and
// dropping empty entries.
func parseSymbols(csv string) []string {
	var out []string
	for s := range strings.SplitSeq(csv, ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// newProvider builds the order book provider for the named exchange. cfg already
// carries the meter provider so the provider's dead-stream metrics are wired. An
// unknown exchange is a fatal misconfiguration, not a silent no-op.
func newProvider(exchange string, cfg orderbook.Config) (outbound.OrderbookProvider, error) {
	switch exchange {
	case "coinbase":
		return orderbook.NewCoinbaseProvider(cfg)
	case "okx":
		return orderbook.NewOKXProvider(cfg)
	case "kraken":
		return orderbook.NewKrakenProvider(cfg)
	default:
		return nil, fmt.Errorf("unknown exchange %q (want coinbase|okx|kraken)", exchange)
	}
}

func run(ctx context.Context, makeProvider providerFactory) error {
	cfg, err := parseConfig()
	if err != nil {
		return err
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: env.ParseLogLevel(slog.LevelInfo),
	}))
	slog.SetDefault(logger)
	logger.Info("starting cex-orderbook-indexer",
		"exchange", cfg.exchange, "symbols", cfg.symbols, "depth", cfg.depth, "interval", cfg.interval)

	// InitOTEL installs the global meter provider; the order book provider's
	// dead-stream gauge (orderbook.last_update.age) records against it. service.name
	// is the exchange so each pod reports a distinct time series.
	shutdownOTEL, err := telemetry.InitOTEL(ctx, telemetry.OTELConfig{
		ServiceName:    "cex-orderbook-indexer-" + cfg.exchange,
		ServiceVersion: GitCommit,
		BuildTime:      BuildTime,
		Logger:         logger,
	})
	if err != nil {
		return fmt.Errorf("initializing telemetry: %w", err)
	}
	// Bound shutdown so a wedged exporter cannot hang process exit; the shutdown
	// func reports no error of its own (it logs internally).
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		shutdownOTEL(shutdownCtx)
	}()

	obCfg := orderbook.DefaultConfig()
	obCfg.Logger = logger
	obCfg.MeterProvider = otel.GetMeterProvider()
	provider, err := makeProvider(cfg.exchange, obCfg)
	if err != nil {
		return err
	}

	pool, err := postgres.OpenPool(ctx, postgres.WorkerDBConfig(cfg.dbURL))
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer pool.Close()
	logger.Info("PostgreSQL connected")

	repo, err := postgres.NewOrderbookSnapshotRepository(pool, logger)
	if err != nil {
		return fmt.Errorf("creating repository: %w", err)
	}

	service, err := cex_orderbook_indexer.NewService(cex_orderbook_indexer.Config{
		Symbols:  cfg.symbols,
		Depth:    cfg.depth,
		Interval: cfg.interval,
		Logger:   logger,
	}, provider, repo)
	if err != nil {
		return fmt.Errorf("creating service: %w", err)
	}

	return lifecycle.Run(ctx, logger, service)
}

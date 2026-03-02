// Command mock-blockchain-server starts an in-memory mock Ethereum JSON-RPC server for stress testing.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/lifecycle"
	"github.com/archon-research/stl/stl-verify/internal/testutil/mockchain"
)

func main() {
	addr := flag.String("addr", ":8546", "listen address (e.g. :8546)")
	interval := flag.Duration("interval", 12*time.Second, "block emission interval (e.g. 1s, 500ms)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, *addr, *interval); err != nil {
		slog.Error("mock-blockchain-server failed", "error", err)
		os.Exit(1)
	}
}

// serverAdapter adapts mockchain.Server to the lifecycle.Service interface.
type serverAdapter struct {
	srv    *mockchain.Server
	store  *mockchain.DataStore
	addr   string
	logger *slog.Logger
}

func (a *serverAdapter) Start(_ context.Context) error {
	if err := a.srv.Start(a.addr); err != nil {
		return fmt.Errorf("starting server: %w", err)
	}

	a.logger.Info("mock blockchain server ready",
		"addr", a.srv.Addr(),
		"ws", "ws://"+a.srv.Addr().String(),
		"http", "http://"+a.srv.Addr().String(),
		"blocks", a.store.Len(),
	)

	return nil
}

func (a *serverAdapter) Stop() error {
	return a.srv.Stop()
}

func run(ctx context.Context, addr string, interval time.Duration) error {
	if interval <= 0 {
		return fmt.Errorf("interval must be positive, got %v", interval)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	store := mockchain.NewFixtureDataStore()
	srv := mockchain.NewServer(store)
	srv.SetInterval(interval)

	return lifecycle.Run(ctx, logger, &serverAdapter{
		srv:    srv,
		store:  store,
		addr:   addr,
		logger: logger,
	})
}

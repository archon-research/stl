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

	"github.com/archon-research/stl/stl-verify/internal/pkg/lifecycle"
	"github.com/archon-research/stl/stl-verify/internal/testutil/mockchain"
)

func main() {
	addr := flag.String("addr", ":8546", "listen address (e.g. :8546)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, *addr, nil); err != nil {
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
	addrCh chan<- string
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

	if a.addrCh != nil {
		select {
		case a.addrCh <- a.srv.Addr().String():
		default:
		}
	}

	return nil
}

func (a *serverAdapter) Stop() error {
	a.srv.Stop()
	return nil
}

func run(ctx context.Context, addr string, addrCh chan<- string) error {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	store := mockchain.NewTestDataStore()
	srv := mockchain.NewServer(store)

	return lifecycle.Run(ctx, logger, &serverAdapter{
		srv:    srv,
		store:  store,
		addr:   addr,
		logger: logger,
		addrCh: addrCh,
	})
}

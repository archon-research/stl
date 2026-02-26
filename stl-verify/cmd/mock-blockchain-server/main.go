// Command mock-blockchain-server starts an in-memory mock Ethereum JSON-RPC server for local development and stress testing.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

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

// run starts the mock blockchain server on addr and blocks until ctx is cancelled.
// If addrCh is non-nil, the bound address string is sent to it once the server is ready.
func run(ctx context.Context, addr string, addrCh chan<- string) error {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	store := mockchain.NewTestDataStore()
	srv := mockchain.NewServer(store)

	if err := srv.Start(addr); err != nil {
		return fmt.Errorf("starting server: %w", err)
	}
	defer srv.Stop()

	logger.Info("mock blockchain server ready",
		"addr", srv.Addr(),
		"ws", "ws://"+srv.Addr().String(),
		"http", "http://"+srv.Addr().String(),
		"blocks", store.Len(),
	)

	if addrCh != nil {
		select {
		case addrCh <- srv.Addr().String():
		default:
		}
	}

	<-ctx.Done()
	logger.Info("shutting down")
	return nil
}

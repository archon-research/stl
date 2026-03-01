// Command mock-blockchain-server starts an in-memory mock Ethereum JSON-RPC server for stress testing.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/archon-research/stl/stl-verify/internal/pkg/lifecycle"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil/mockchain"
)

func main() {
	addr := flag.String("addr", ":8546", "listen address (e.g. :8546)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, *addr); err != nil {
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

// newDataStore returns a DataStore pre-populated with 3 synthetic blocks for stress testing.
func newDataStore() *mockchain.DataStore {
	store := mockchain.NewDataStore()
	for i := 0; i < 3; i++ {
		hash := fmt.Sprintf("0x%064x", i+1)
		parentHash := "0x" + strings.Repeat("0", 64)
		if i > 0 {
			parentHash = fmt.Sprintf("0x%064x", i)
		}
		header := outbound.BlockHeader{
			Number:     fmt.Sprintf("0x%x", i+1),
			Hash:       hash,
			ParentHash: parentHash,
			Timestamp:  "0x67c00000",
		}
		headerJSON, err := json.Marshal(header)
		if err != nil {
			panic(fmt.Sprintf("seeding data store: %v", err))
		}
		store.AddHeader(header)
		store.Add(i, "block", headerJSON)
		store.Add(i, "receipts", json.RawMessage(`[]`))
		store.Add(i, "traces", json.RawMessage(`[]`))
		store.Add(i, "blobs", json.RawMessage(`[]`))
	}
	return store
}

func run(ctx context.Context, addr string) error {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	store := newDataStore()
	srv := mockchain.NewServer(store)

	return lifecycle.Run(ctx, logger, &serverAdapter{
		srv:    srv,
		store:  store,
		addr:   addr,
		logger: logger,
	})
}

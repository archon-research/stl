// Command mock-coingecko-server serves the mockgecko CoinGecko stand-in over
// HTTP, so the offchain price pipeline runs in the local kind cluster with no
// real key — the counterpart of mock-blockchain-server for the price vendor.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/testutil/mockgecko"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	entitlementDays := flag.Int("entitlement-days", 0,
		"serve only the last N days, answering older ranges with empty arrays like a limited plan (0 = unlimited)")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	err := run(ctx, logger, *addr, *entitlementDays)
	stop()
	if err != nil {
		logger.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger, addr string, entitlementDays int) error {
	server := mockgecko.NewServer(nil)
	if entitlementDays > 0 {
		server.SetEntitlementStart(time.Now().UTC().AddDate(0, 0, -entitlementDays))
	}

	httpSrv := &http.Server{Addr: addr, Handler: server}
	errCh := make(chan error, 1)
	go func() {
		logger.Info("mock CoinGecko server listening", "addr", addr, "entitlementDays", entitlementDays)
		errCh <- httpSrv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		return fmt.Errorf("serving: %w", err)
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("shutting down: %w", err)
	}
	return nil
}

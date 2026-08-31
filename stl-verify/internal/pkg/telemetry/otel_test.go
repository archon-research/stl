package telemetry

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
)

// A component that builds metric instruments before InitOTEL records every
// measurement into a placeholder whose Add has no delegate, and those
// measurements are never replayed. The one that matters most is the failure
// that ends the process during startup: it is recorded, dropped, and there is
// no later flush to rescue it, so a database refusing every connection can
// crash-loop a service while its error counters stay flat.
//
// Nothing at runtime can distinguish that from a healthy service, so InitOTEL
// refuses to start rather than let the binary export a hole.
func TestInitOTEL_RefusesToStartAfterAComponentBuiltInstruments(t *testing.T) {
	resetStartupSeeds(t)
	OnMeterProviderReady("postgres.OpenPool", func() {})

	shutdown, err := InitOTEL(context.Background(), OTELConfig{
		ServiceName: "startup-ordering-test",
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err == nil {
		shutdown(context.Background())
		t.Fatal("InitOTEL succeeded after a component registered a startup seed, want an error naming the component")
	}
	if !strings.Contains(err.Error(), "postgres.OpenPool") {
		t.Errorf("InitOTEL error = %q, want it to name postgres.OpenPool", err)
	}
}

// Without an OTLP endpoint no provider is ever installed, so a correctly-ordered
// component's seed stays pending for the life of the process. A second InitOTEL
// — what a test binary driving several run() calls does — must not read that
// leftover as the component having come first.
func TestInitOTEL_AcceptsASeedRegisteredAfterAnEarlierInit(t *testing.T) {
	resetStartupSeeds(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	config := OTELConfig{ServiceName: "startup-ordering-test", Logger: logger}

	shutdown, err := InitOTEL(context.Background(), config)
	if err != nil {
		t.Fatalf("first InitOTEL: %v", err)
	}
	shutdown(context.Background())

	OnMeterProviderReady("postgres.OpenPool", func() {})

	shutdown, err = InitOTEL(context.Background(), config)
	if err != nil {
		t.Fatalf("InitOTEL rejected a seed registered after an earlier init: %v", err)
	}
	shutdown(context.Background())
}

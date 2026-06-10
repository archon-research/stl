package multicall

import (
	"context"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// TestExecuteRecordsBatchSizeThroughExecute drives the instrumentation path via
// the real Execute method rather than calling recordBatch directly, so the
// Execute -> recordBatch wiring is exercised.
//
// The httptest server returns HTTP 500, which causes CallContract to fail after
// recordBatch has already run. We assert both that Execute returned an error and
// that the histogram captured count=1, sum=1.
func TestExecuteRecordsBatchSizeThroughExecute(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	tel, err := NewTelemetryWithProvider(mp, "mainnet")
	if err != nil {
		t.Fatalf("NewTelemetryWithProvider: %v", err)
	}

	// RPC endpoint that always fails so Execute reaches recordBatch then errors
	// at CallContract instead of needing a live node.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	rpcClient, err := rpc.DialHTTP(srv.URL)
	if err != nil {
		t.Fatalf("rpc.DialHTTP: %v", err)
	}
	ethClient := ethclient.NewClient(rpcClient)

	mc, err := NewClient(ethClient, common.HexToAddress("0xcA11bde05977b3631167028862bE2a173976CA11"), WithTelemetry(tel))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	calls := []outbound.Call{{Target: common.Address{}, CallData: []byte{0x01, 0x02, 0x03, 0x04}}}
	_, execErr := mc.Execute(context.Background(), calls, big.NewInt(100))
	if execErr == nil {
		t.Fatal("expected Execute to fail against a 500 endpoint")
	}

	dp := histDataPoint(t, reader, "multicall.batch.size")
	if dp.Count != 1 || dp.Sum != 1 {
		t.Errorf("count=%d sum=%d, want 1 and 1 (recorded before the failing RPC)", dp.Count, dp.Sum)
	}
}

func TestRecordBatchNilTelemetryIsNoOp(t *testing.T) {
	c := &Client{} // no telemetry
	c.recordBatch(context.Background(), 5) // must not panic
}

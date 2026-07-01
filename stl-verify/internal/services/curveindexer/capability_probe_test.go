package curveindexer

import (
	"context"
	"fmt"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// probeMulticaller is a Multicaller double for ProbePoolCapabilities tests. It
// records the calls it received and returns preset results or a transport error.
type probeMulticaller struct {
	captured []outbound.Call
	results  []outbound.Result
	err      error
}

func (m *probeMulticaller) Execute(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
	m.captured = append(m.captured, calls...)
	if m.err != nil {
		return nil, m.err
	}
	return m.results, nil
}

func (m *probeMulticaller) Address() common.Address { return common.Address{} }

func probeTestPools() []RegisteredPool {
	return []RegisteredPool{
		{ID: 1, Address: common.HexToAddress("0x01"), Kind: KindStableswapPreNG, NCoins: 2},
		{ID: 2, Address: common.HexToAddress("0x02"), Kind: KindStableswapPreNG, NCoins: 3},
		{ID: 3, Address: common.HexToAddress("0x03"), Kind: KindCryptoswap, NCoins: 3},
	}
}

// TestProbePoolCapabilities_SetsPerPoolFlag verifies that a per-call Success sets
// HasAPrecise=true and a per-call !Success sets false, per stableswap pool, while
// cryptoswap pools are skipped (no call, HasAPrecise stays false).
func TestProbePoolCapabilities_SetsPerPoolFlag(t *testing.T) {
	a, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	pools := probeTestPools()
	mc := &probeMulticaller{results: []outbound.Result{
		{Success: true},  // pool 1 has A_precise
		{Success: false}, // pool 2 lacks A_precise
	}}

	resolved, err := ProbePoolCapabilities(context.Background(), mc, a, pools, big.NewInt(100))
	if err != nil {
		t.Fatalf("ProbePoolCapabilities: %v", err)
	}

	if len(mc.captured) != 2 {
		t.Fatalf("issued %d calls, want 2 (one per stableswap pool, cryptoswap skipped)", len(mc.captured))
	}
	if !resolved[0].HasAPrecise {
		t.Error("pool 1 (Success) must have HasAPrecise=true")
	}
	if resolved[1].HasAPrecise {
		t.Error("pool 2 (!Success) must have HasAPrecise=false")
	}
	if resolved[2].HasAPrecise {
		t.Error("cryptoswap pool must be left HasAPrecise=false (not probed)")
	}
	// The input slice is not mutated: the resolved flags live only on the return value.
	if pools[0].HasAPrecise {
		t.Error("input pools must be left untouched (resolution returned, not mutated in place)")
	}
}

// TestProbePoolCapabilities_TransportErrorReturns verifies that a transport/RPC
// error from the batch is returned (startup fails and retries) rather than being
// interpreted as "A_precise absent".
func TestProbePoolCapabilities_TransportErrorReturns(t *testing.T) {
	a, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	pools := probeTestPools()
	mc := &probeMulticaller{err: fmt.Errorf("rpc down")}

	resolved, err := ProbePoolCapabilities(context.Background(), mc, a, pools, big.NewInt(100))
	if err == nil {
		t.Fatal("transport error must be returned, got nil")
	}
	if resolved != nil {
		t.Error("transport error must return a nil slice, not partially-resolved pools")
	}
}

// TestProbePoolCapabilities_NoStableswapPoolsSkipsBatch verifies that with only
// cryptoswap pools no multicall is issued and the probe succeeds.
func TestProbePoolCapabilities_NoStableswapPoolsSkipsBatch(t *testing.T) {
	a, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	pools := []RegisteredPool{
		{ID: 1, Address: common.HexToAddress("0x01"), Kind: KindCryptoswap, NCoins: 3},
	}
	mc := &probeMulticaller{}

	resolved, err := ProbePoolCapabilities(context.Background(), mc, a, pools, big.NewInt(100))
	if err != nil {
		t.Fatalf("ProbePoolCapabilities: %v", err)
	}
	if len(mc.captured) != 0 {
		t.Errorf("issued %d calls, want 0 (no stableswap pools)", len(mc.captured))
	}
	if len(resolved) != len(pools) {
		t.Errorf("resolved len = %d, want %d (all pools returned even when none probed)", len(resolved), len(pools))
	}
}

// TestProbePoolCapabilities_NilBlockNumberErrors verifies the probe rejects a nil
// block number (the Multicaller contract requires a concrete block).
func TestProbePoolCapabilities_NilBlockNumberErrors(t *testing.T) {
	a, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	pools := probeTestPools()
	mc := &probeMulticaller{}

	if _, err := ProbePoolCapabilities(context.Background(), mc, a, pools, nil); err == nil {
		t.Fatal("nil block number must error, got nil")
	}
	if len(mc.captured) != 0 {
		t.Errorf("issued %d calls, want 0 (must fail before executing)", len(mc.captured))
	}
}

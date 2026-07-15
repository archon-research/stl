package blockchain

import (
	"context"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var testVatAddress = common.HexToAddress("0x35D1b3F3D7966A1DFe207aa4514C12a259A0492B")

func newTestVatCaller(t *testing.T, mc outbound.Multicaller) *VatCaller {
	t.Helper()
	caller, err := NewVatCaller(mc, testVatAddress)
	if err != nil {
		t.Fatalf("create vat caller: %v", err)
	}
	return caller
}

// packIlksOutput packs a vat.ilks() return [Art, rate, spot, line, dust] with
// the given rate; the other slots are irrelevant to ReadDebts.
func packIlksOutput(t *testing.T, c *VatCaller, rate *big.Int) []byte {
	t.Helper()
	data, err := c.vatABI.Methods["ilks"].Outputs.Pack(big.NewInt(0), rate, big.NewInt(0), big.NewInt(0), big.NewInt(0))
	if err != nil {
		t.Fatalf("pack ilks output: %v", err)
	}
	return data
}

// packUrnsOutput packs a vat.urns() return [ink, art] with the given art; ink
// is irrelevant to ReadDebts.
func packUrnsOutput(t *testing.T, c *VatCaller, art *big.Int) []byte {
	t.Helper()
	data, err := c.vatABI.Methods["urns"].Outputs.Pack(big.NewInt(0), art)
	if err != nil {
		t.Fatalf("pack urns output: %v", err)
	}
	return data
}

func TestVatReadDebts_HappyPath_PinsBlockHash(t *testing.T) {
	blockHash := common.HexToHash("0xabc123abc123abc123abc123abc123abc123abc123abc123abc123abc123ab")

	queries := []entity.DebtQuery{
		{Ilk: [32]byte{'E', 'T', 'H', '-', 'A'}, VaultAddress: common.HexToAddress("0x1111111111111111111111111111111111111111")},
		{Ilk: [32]byte{'W', 'B', 'T', 'C', '-', 'A'}, VaultAddress: common.HexToAddress("0x2222222222222222222222222222222222222222")},
	}
	wantRates := []*big.Int{big.NewInt(1_000_000), big.NewInt(2_000_000)}
	wantArts := []*big.Int{big.NewInt(111), big.NewInt(222)}

	mc := testutil.NewMockMulticaller()
	caller := newTestVatCaller(t, mc)

	// Debt reads must pin to the block hash, not the number: after a reorg an
	// archive node answers eth_call-by-number with the new fork's debt state,
	// which can silently disagree with the reorged block being processed (VEC-471).
	mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		t.Fatal("ReadDebts must call ExecuteAtHash, not Execute")
		return nil, nil
	}
	mc.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, gotHash common.Hash) ([]outbound.Result, error) {
		if gotHash != blockHash {
			t.Errorf("blockHash = %s, want %s", gotHash, blockHash)
		}
		if len(calls) != len(queries)*2 {
			t.Fatalf("expected %d calls, got %d", len(queries)*2, len(calls))
		}
		results := make([]outbound.Result, len(calls))
		for i := range queries {
			if calls[i*2].Target != testVatAddress || calls[i*2+1].Target != testVatAddress {
				t.Errorf("query %d: calls not targeted at vat %s", i, testVatAddress.Hex())
			}
			results[i*2] = outbound.Result{Success: true, ReturnData: packIlksOutput(t, caller, wantRates[i])}
			results[i*2+1] = outbound.Result{Success: true, ReturnData: packUrnsOutput(t, caller, wantArts[i])}
		}
		return results, nil
	}

	got, err := caller.ReadDebts(context.Background(), queries, blockHash)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != len(queries) {
		t.Fatalf("expected %d results, got %d", len(queries), len(got))
	}
	for i, r := range got {
		if r.Err != nil {
			t.Fatalf("query %d: unexpected per-vault error: %v", i, r.Err)
		}
		if r.Reverted {
			t.Errorf("query %d: unexpected reverted=true", i)
		}
		if r.VaultAddress != queries[i].VaultAddress {
			t.Errorf("query %d: vault = %s, want %s", i, r.VaultAddress.Hex(), queries[i].VaultAddress.Hex())
		}
		if r.Rate.Cmp(wantRates[i]) != 0 {
			t.Errorf("query %d: rate = %s, want %s", i, r.Rate, wantRates[i])
		}
		if r.Art.Cmp(wantArts[i]) != 0 {
			t.Errorf("query %d: art = %s, want %s", i, r.Art, wantArts[i])
		}
	}
}

// A short multicall result set must be reported per-vault via DebtResult.Err,
// not swallowed and not escalated to the whole-batch outer error.
func TestVatReadDebts_MissingResultsSetsErr(t *testing.T) {
	queries := []entity.DebtQuery{
		{Ilk: [32]byte{'E', 'T', 'H', '-', 'A'}, VaultAddress: common.HexToAddress("0x1111111111111111111111111111111111111111")},
	}

	mc := testutil.NewMockMulticaller()
	caller := newTestVatCaller(t, mc)

	// Only one result for a query that needs two (ilks + urns).
	mc.ExecuteAtHashFn = func(_ context.Context, _ []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		return []outbound.Result{{Success: true, ReturnData: packIlksOutput(t, caller, big.NewInt(1))}}, nil
	}

	got, err := caller.ReadDebts(context.Background(), queries, common.HexToHash("0xdead"))
	if err != nil {
		t.Fatalf("unexpected outer error: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 result, got %d", len(got))
	}
	if got[0].Err == nil || !strings.Contains(got[0].Err.Error(), "missing multicall results") {
		t.Fatalf("expected missing-results error, got: %v", got[0].Err)
	}
}

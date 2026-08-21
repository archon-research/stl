package morpho_indexer

import (
	"context"
	"errors"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

func probeResult(success bool) outbound.Result {
	if success {
		return outbound.Result{Success: true, ReturnData: make([]byte, 32)}
	}
	return outbound.Result{Success: false, ReturnData: nil}
}

// TestAdapterProber_ProbeAdapterType covers the classification contract: exactly
// one of morpho() / morphoVaultV1() succeeds on a real adapter; neither or both
// ⇒ Unknown (recorded, not dropped).
func TestAdapterProber_ProbeAdapterType(t *testing.T) {
	prober, err := NewAdapterProber()
	if err != nil {
		t.Fatalf("NewAdapterProber: %v", err)
	}
	adapter := common.HexToAddress("0x7481968709b8f155652D42ebf468b22945907dC2")

	tests := []struct {
		name              string
		morphoOK, vaultOK bool
		want              entity.MorphoAdapterType
	}{
		{"morpho() only → MarketV1", true, false, entity.MorphoAdapterTypeMarketV1},
		{"morphoVaultV1() only → VaultV1", false, true, entity.MorphoAdapterTypeVaultV1},
		{"neither → Unknown", false, false, entity.MorphoAdapterTypeUnknown},
		{"both → Unknown", true, true, entity.MorphoAdapterTypeUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := testutil.NewMockMulticaller()
			mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
				if len(calls) != 2 || !calls[0].AllowFailure || !calls[1].AllowFailure {
					t.Fatalf("expected 2 AllowFailure calls, got %+v", calls)
				}
				return []outbound.Result{probeResult(tt.morphoOK), probeResult(tt.vaultOK)}, nil
			}
			got, err := prober.ProbeAdapterType(context.Background(), mc, adapter, big.NewInt(100))
			if err != nil {
				t.Fatalf("ProbeAdapterType: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %d, want %d", got, tt.want)
			}
		})
	}
}

func TestAdapterProber_NumProbeCalls(t *testing.T) {
	prober, err := NewAdapterProber()
	if err != nil {
		t.Fatalf("NewAdapterProber: %v", err)
	}
	if got := prober.NumProbeCalls(); got != 2 {
		t.Errorf("NumProbeCalls() = %d, want 2", got)
	}
}

// TestAdapterProber_SuccessButEmptyDataIsUnknown covers the len(ReturnData) > 0
// guard: a successful sub-call that returned no data must not count as a match.
func TestAdapterProber_SuccessButEmptyDataIsUnknown(t *testing.T) {
	prober, err := NewAdapterProber()
	if err != nil {
		t.Fatalf("NewAdapterProber: %v", err)
	}
	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{
			{Success: true, ReturnData: nil}, // morpho() "succeeded" but returned nothing
			{Success: false, ReturnData: nil},
		}, nil
	}
	got, err := prober.ProbeAdapterType(context.Background(), mc, common.HexToAddress("0x1"), big.NewInt(1))
	if err != nil {
		t.Fatalf("ProbeAdapterType: %v", err)
	}
	if got != entity.MorphoAdapterTypeUnknown {
		t.Errorf("got %d, want Unknown(99)", got)
	}
}

func TestAdapterProber_ProbeAdapterType_TransportErrorBubbles(t *testing.T) {
	prober, err := NewAdapterProber()
	if err != nil {
		t.Fatalf("NewAdapterProber: %v", err)
	}
	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return nil, errors.New("rpc down")
	}
	if _, err := prober.ProbeAdapterType(context.Background(), mc, common.HexToAddress("0x1"), big.NewInt(1)); err == nil {
		t.Fatal("expected transport error to bubble up, not classify as Unknown")
	}
}

func TestAdapterProber_ProbeAdapterType_ShortResults(t *testing.T) {
	prober, err := NewAdapterProber()
	if err != nil {
		t.Fatalf("NewAdapterProber: %v", err)
	}
	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{probeResult(true)}, nil
	}
	if _, err := prober.ProbeAdapterType(context.Background(), mc, common.HexToAddress("0x1"), big.NewInt(1)); err == nil {
		t.Fatal("expected error for short result slice")
	}
}

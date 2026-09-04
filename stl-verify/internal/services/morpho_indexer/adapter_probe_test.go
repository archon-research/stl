package morpho_indexer

import (
	"bytes"
	"context"
	"errors"
	"math/big"
	"slices"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

func probeResult(success bool) outbound.Result {
	if success {
		return outbound.Result{Success: true, ReturnData: make([]byte, 32)}
	}
	return outbound.Result{Success: false, ReturnData: nil}
}

// markerProbeResults builds a full probe response in which exactly the markers at
// the given indexes of adapterMarkers answered.
func markerProbeResults(answered ...int) []outbound.Result {
	results := make([]outbound.Result, adapterProbeCallsPerAdapter)
	for i := range results {
		results[i] = probeResult(slices.Contains(answered, i))
	}
	return results
}

// TestAdapterProber_ProbeAdapterType covers the classification contract: exactly
// one marker getter answers on a real adapter; none or several ⇒ Unknown
// (recorded, not dropped).
func TestAdapterProber_ProbeAdapterType(t *testing.T) {
	prober, err := NewAdapterProber()
	if err != nil {
		t.Fatalf("NewAdapterProber: %v", err)
	}
	adapter := common.HexToAddress("0x7481968709b8f155652D42ebf468b22945907dC2")

	tests := []struct {
		name     string
		answered []int
		want     entity.MorphoAdapterType
	}{
		{"morpho() only → MarketV1", []int{0}, entity.MorphoAdapterTypeMarketV1},
		{"morphoVaultV1() only → VaultV1", []int{1}, entity.MorphoAdapterTypeVaultV1},
		{"erc4626Vault() only → ERC4626Merkl", []int{2}, entity.MorphoAdapterTypeERC4626Merkl},
		{"box() only → Box", []int{3}, entity.MorphoAdapterTypeBox},
		{"comet() only → CompoundV3", []int{4}, entity.MorphoAdapterTypeCompoundV3},
		{"no marker answers → Unknown", nil, entity.MorphoAdapterTypeUnknown},
		{"morpho() and box() both answer → Unknown", []int{0, 3}, entity.MorphoAdapterTypeUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := testutil.NewMockMulticaller()
			mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
				if len(calls) != adapterProbeCallsPerAdapter {
					t.Fatalf("expected %d probe calls, got %d", adapterProbeCallsPerAdapter, len(calls))
				}
				for i, call := range calls {
					if !call.AllowFailure {
						t.Fatalf("probe call %d must allow failure: a revert is the classification signal", i)
					}
				}
				return markerProbeResults(tt.answered...), nil
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

// TestAdapterProber_ProbeCallsFollowTheMarkerTable pins the probe order, which
// classifyAdapter reads results back by position.
func TestAdapterProber_ProbeCallsFollowTheMarkerTable(t *testing.T) {
	prober, err := NewAdapterProber()
	if err != nil {
		t.Fatalf("NewAdapterProber: %v", err)
	}
	adapterABI, err := abis.GetVaultV2AdapterReadABI()
	if err != nil {
		t.Fatalf("GetVaultV2AdapterReadABI: %v", err)
	}
	adapter := common.HexToAddress("0x7481968709b8f155652D42ebf468b22945907dC2")

	calls := prober.ProbeCalls(adapter)
	if len(calls) != len(adapterMarkers) {
		t.Fatalf("ProbeCalls returned %d calls, want %d", len(calls), len(adapterMarkers))
	}
	for i, marker := range adapterMarkers {
		if calls[i].Target != adapter {
			t.Errorf("call %d targets %s, want %s", i, calls[i].Target, adapter)
		}
		want := adapterABI.Methods[marker.selector].ID
		if !bytes.Equal(calls[i].CallData[:4], want) {
			t.Errorf("call %d selector = %x, want %s (%x)", i, calls[i].CallData[:4], marker.selector, want)
		}
	}
}

func TestAdapterProber_NumProbeCalls(t *testing.T) {
	prober, err := NewAdapterProber()
	if err != nil {
		t.Fatalf("NewAdapterProber: %v", err)
	}
	if got := prober.NumProbeCalls(); got != 5 {
		t.Errorf("NumProbeCalls() = %d, want 5", got)
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
		results := markerProbeResults()
		results[0] = outbound.Result{Success: true, ReturnData: nil} // morpho() "succeeded" but returned nothing
		return results, nil
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

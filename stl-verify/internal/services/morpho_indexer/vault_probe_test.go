package morpho_indexer

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var (
	probeAsset    = common.HexToAddress("0xaaaa000000000000000000000000000000000000")
	trappingBlock = big.NewInt(25827558)
)

func newTestVaultProber(t *testing.T) (*VaultProber, *testutil.MockMulticaller) {
	t.Helper()
	p, err := NewVaultProber()
	if err != nil {
		t.Fatalf("NewVaultProber: %v", err)
	}
	return p, testutil.NewMockMulticaller()
}

func TestProbeVault_ConfirmsVaultFromIsolatedSelectorsAfterBatchExhaustsGas(t *testing.T) {
	tests := []struct {
		name        string
		perSelector [vaultProbeCallsPerAddress]isolatedAnswer
		wantVersion entity.MorphoVaultVersion
	}{
		{
			name:        "MetaMorpho selectors answering alone",
			perSelector: [vaultProbeCallsPerAddress]isolatedAnswer{answers(MorphoBlueAddress), answers(probeAsset), reverts(), reverts()},
			wantVersion: entity.MorphoVaultV1,
		},
		{
			name:        "VaultV2 selectors answering alone",
			perSelector: [vaultProbeCallsPerAddress]isolatedAnswer{reverts(), answers(probeAsset), answers(common.HexToAddress("0xc1")), answers(common.HexToAddress("0x1a"))},
			wantVersion: entity.MorphoVaultV2,
		},
		{
			name:        "a selector exhausting gas alone is a failed call, not a verdict",
			perSelector: [vaultProbeCallsPerAddress]isolatedAnswer{answers(MorphoBlueAddress), answers(probeAsset), exhausts(), exhausts()},
			wantVersion: entity.MorphoVaultV1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, mc := newTestVaultProber(t)
			mc.ExecuteFn = trappingResponder(p, trappingCandidate, tt.perSelector)

			probe, err := p.ProbeVault(context.Background(), mc, trappingCandidate, trappingBlock)
			if err != nil {
				t.Fatalf("ProbeVault: %v", err)
			}
			if probe.Version != tt.wantVersion {
				t.Errorf("Version = %d, want %d", probe.Version, tt.wantVersion)
			}
			if probe.AssetAddr != probeAsset {
				t.Errorf("AssetAddr = %s, want %s", probe.AssetAddr.Hex(), probeAsset.Hex())
			}
			if !probe.ProbedSelectorwise {
				t.Error("ProbedSelectorwise must be set so the caller can log the selector-wise confirmation")
			}
			if want := 1 + vaultProbeCallsPerAddress; mc.CallCount != want {
				t.Errorf("Execute calls = %d, want %d (one batched probe, then one per selector)", mc.CallCount, want)
			}
		})
	}
}

func TestProbeVault_RejectsCandidateAnsweringNoIsolatedSelector(t *testing.T) {
	tests := []struct {
		name        string
		perSelector [vaultProbeCallsPerAddress]isolatedAnswer
	}{
		{
			name:        "every selector reverts alone",
			perSelector: [vaultProbeCallsPerAddress]isolatedAnswer{reverts(), reverts(), reverts(), reverts()},
		},
		{
			name:        "every selector exhausts gas alone",
			perSelector: [vaultProbeCallsPerAddress]isolatedAnswer{exhausts(), exhausts(), exhausts(), exhausts()},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, mc := newTestVaultProber(t)
			mc.ExecuteFn = trappingResponder(p, trappingCandidate, tt.perSelector)

			_, err := p.ProbeVault(context.Background(), mc, trappingCandidate, trappingBlock)
			var nv *ErrNotVault
			if !errors.As(err, &nv) {
				t.Fatalf("want *ErrNotVault so discovery caches the address instead of retrying the block, got %T: %v", err, err)
			}
			if !nv.ProbedSelectorwise {
				t.Error("ProbedSelectorwise must be set so discovery discards the candidate loudly")
			}
			if nv.VaultShaped {
				t.Error("a candidate answering no discriminating selector is not vault-shaped")
			}
		})
	}
}

func TestProbeVault_RejectionAfterSelectorwiseProbeKeepsVaultShaped(t *testing.T) {
	p, mc := newTestVaultProber(t)
	mc.ExecuteFn = trappingResponder(p, trappingCandidate,
		[vaultProbeCallsPerAddress]isolatedAnswer{answers(MorphoBlueAddress), reverts(), reverts(), reverts()})

	_, err := p.ProbeVault(context.Background(), mc, trappingCandidate, trappingBlock)
	var nv *ErrNotVault
	if !errors.As(err, &nv) {
		t.Fatalf("want *ErrNotVault, got %T: %v", err, err)
	}
	if !nv.ProbedSelectorwise || !nv.VaultShaped {
		t.Errorf("ProbedSelectorwise = %v, VaultShaped = %v; want both: MORPHO() answered alone on a trapping contract", nv.ProbedSelectorwise, nv.VaultShaped)
	}
}

func TestProbeVault_TransientErrorDuringSelectorwiseProbeIsNotAVerdict(t *testing.T) {
	p, mc := newTestVaultProber(t)
	mc.ExecuteFn = trappingResponder(p, trappingCandidate,
		[vaultProbeCallsPerAddress]isolatedAnswer{answers(MorphoBlueAddress), throttled(), reverts(), reverts()})

	_, err := p.ProbeVault(context.Background(), mc, trappingCandidate, trappingBlock)
	if !errors.Is(err, throttledRPCError{}) {
		t.Fatalf("want the throttling error to propagate for retry, got %v", err)
	}
	var nv *ErrNotVault
	if errors.As(err, &nv) {
		t.Error("a throttled isolated call must not become a not-a-vault verdict — that would black-hole a real vault")
	}
}

func TestProbeVault_NonGasErrorIsNotRetriedSelectorwise(t *testing.T) {
	p, mc := newTestVaultProber(t)
	mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return nil, errors.New("connection timeout")
	}

	_, err := p.ProbeVault(context.Background(), mc, trappingCandidate, trappingBlock)
	if err == nil {
		t.Fatal("want the transport error to propagate")
	}
	if mc.CallCount != 1 {
		t.Errorf("Execute calls = %d, want 1: only gas exhaustion earns a selector-wise re-probe", mc.CallCount)
	}
}

func TestProbeVault_BatchedVerdictIsNotFlaggedProbedSelectorwise(t *testing.T) {
	p, mc := newTestVaultProber(t)
	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) != vaultProbeCallsPerAddress {
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
		return []outbound.Result{answers(MorphoBlueAddress).result, answers(probeAsset).result, {}, {}}, nil
	}

	probe, err := p.ProbeVault(context.Background(), mc, trappingCandidate, trappingBlock)
	if err != nil {
		t.Fatalf("ProbeVault: %v", err)
	}
	if probe.ProbedSelectorwise {
		t.Error("a verdict from the batched probe must not be flagged ProbedSelectorwise")
	}
}

func TestProbeVault_ShortBatchedResultIsAnError(t *testing.T) {
	p, mc := newTestVaultProber(t)
	mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{{}, {}}, nil
	}

	_, err := p.ProbeVault(context.Background(), mc, trappingCandidate, trappingBlock)
	var nv *ErrNotVault
	if err == nil || errors.As(err, &nv) {
		t.Fatalf("want a plain error for a short result set, got %v", err)
	}
}

func TestProbeSelectorwise_CountsSelectorsExhaustingAlone(t *testing.T) {
	p, mc := newTestVaultProber(t)
	mc.ExecuteFn = trappingResponder(p, trappingCandidate,
		[vaultProbeCallsPerAddress]isolatedAnswer{answers(MorphoBlueAddress), exhausts(), reverts(), exhausts()})

	results, exhausted, err := p.ProbeSelectorwise(context.Background(), mc, trappingCandidate, trappingBlock)
	if err != nil {
		t.Fatalf("ProbeSelectorwise: %v", err)
	}
	if exhausted != 2 {
		t.Errorf("exhausted = %d, want 2", exhausted)
	}
	if len(results) != vaultProbeCallsPerAddress {
		t.Errorf("len(results) = %d, want %d", len(results), vaultProbeCallsPerAddress)
	}
}

func TestProbeSelectorwise_IsolatedCallReturningWrongCountIsAnError(t *testing.T) {
	p, mc := newTestVaultProber(t)
	mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{{}, {}}, nil
	}

	_, _, err := p.ProbeSelectorwise(context.Background(), mc, trappingCandidate, trappingBlock)
	var nv *ErrNotVault
	if err == nil || errors.As(err, &nv) {
		t.Fatalf("want a plain error when one eth_call yields two results, got %v", err)
	}
}

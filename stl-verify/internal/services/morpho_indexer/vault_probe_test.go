package morpho_indexer

import (
	"context"
	"errors"
	"math/big"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/multicall"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var trappingBlock = big.NewInt(25827558)

// newNarrowedProber pairs a prober with the multicaller production gives it: the
// mock behind a narrowing decorator.
func newNarrowedProber(t *testing.T) (*VaultProber, *testutil.MockMulticaller, outbound.Multicaller) {
	t.Helper()
	p, err := NewVaultProber()
	if err != nil {
		t.Fatalf("NewVaultProber: %v", err)
	}
	mc := testutil.NewMockMulticaller()
	return p, mc, multicall.NewNarrowing(mc, multicall.WithNarrowingLogger(quietNarrowingLogger))
}

func TestProbeVault_RejectsATrappingCandidateThroughTheNarrowingMulticaller(t *testing.T) {
	p, mc, narrowed := newNarrowedProber(t)
	mc.ExecuteFn = trappingResponder(t, trappingCandidate,
		[vaultProbeCallsPerAddress]isolatedAnswer{reverts(), reverts(), reverts(), reverts()}, nil)

	_, err := p.ProbeVault(context.Background(), narrowed, trappingCandidate, trappingBlock)
	var nv *ErrNotVault
	if !errors.As(err, &nv) {
		t.Fatalf("want *ErrNotVault so discovery caches the address instead of retrying the block, got %T: %v", err, err)
	}
	if nv.VaultShaped {
		t.Error("a candidate answering no discriminating selector is not vault-shaped")
	}
	if mc.CallCount < 2 {
		t.Errorf("multicalls = %d, want the batched probe to have been narrowed", mc.CallCount)
	}
}

func TestProbeVault_ConfirmsAVaultAnsweringOneSelectorAtATime(t *testing.T) {
	p, mc, narrowed := newNarrowedProber(t)
	mc.ExecuteFn = trappingResponder(t, trappingCandidate,
		[vaultProbeCallsPerAddress]isolatedAnswer{answers(MorphoBlueAddress), answers(testLoanToken), reverts(), reverts()}, nil)

	probe, err := p.ProbeVault(context.Background(), narrowed, trappingCandidate, trappingBlock)
	if err != nil {
		t.Fatalf("ProbeVault: %v", err)
	}
	if probe.Version != entity.MorphoVaultV1 || probe.AssetAddr != testLoanToken {
		t.Errorf("probe = %+v, want a V1 vault on %s", probe, testLoanToken.Hex())
	}
}

func TestProbeVault_IsolatedGasExhaustionIsNotAVerdict(t *testing.T) {
	p, mc, narrowed := newNarrowedProber(t)
	mc.ExecuteFn = trappingResponder(t, trappingCandidate,
		[vaultProbeCallsPerAddress]isolatedAnswer{exhausts(), exhausts(), exhausts(), exhausts()}, nil)

	_, err := p.ProbeVault(context.Background(), narrowed, trappingCandidate, trappingBlock)
	if !errors.Is(err, testutil.GasExhaustedRPCError()) {
		t.Fatalf("want the node's gas error to propagate, got %v", err)
	}
	var nv *ErrNotVault
	if errors.As(err, &nv) {
		t.Error("a lone call cannot exhaust the outer frame, so an isolated exhaustion is the node's fault, never a verdict")
	}
}

func TestProbeVault_PropagatesATransportError(t *testing.T) {
	p, mc, narrowed := newNarrowedProber(t)
	mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return nil, errors.New("connection timeout")
	}

	_, err := p.ProbeVault(context.Background(), narrowed, trappingCandidate, trappingBlock)
	var nv *ErrNotVault
	if err == nil || errors.As(err, &nv) {
		t.Fatalf("want a plain error, got %v", err)
	}
	if mc.CallCount != 1 {
		t.Errorf("multicalls = %d, want 1: only gas exhaustion narrows", mc.CallCount)
	}
}

func TestProbeVault_ShortBatchedResultIsAnError(t *testing.T) {
	p, mc, narrowed := newNarrowedProber(t)
	mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{{}, {}}, nil
	}

	_, err := p.ProbeVault(context.Background(), narrowed, trappingCandidate, trappingBlock)
	var nv *ErrNotVault
	if err == nil || errors.As(err, &nv) {
		t.Fatalf("want a plain error for a short result set, got %v", err)
	}
}

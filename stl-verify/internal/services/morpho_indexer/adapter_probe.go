package morpho_indexer

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// adapterProbeCallsPerAdapter is the number of probe sub-calls per adapter
// (morpho, morphoVaultV1).
const adapterProbeCallsPerAdapter = 2

// AdapterProber classifies a VaultV2 liquidity adapter by probing the two
// type-discriminating selectors. On a real adapter exactly one succeeds:
// morpho() ⇒ MorphoMarketV1AdapterV2 (wraps a Morpho Blue market),
// morphoVaultV1() ⇒ MorphoVaultV1Adapter (wraps a nested MetaMorpho V1 vault).
// Neither (or, defensively, both) ⇒ MorphoAdapterTypeUnknown — recorded rather
// than dropped, the same forward-compat philosophy as VaultProber's VaultShaped
// sentinel, so a not-yet-modelled adapter kind surfaces instead of vanishing.
type AdapterProber struct {
	adapterABI            *abi.ABI
	morphoCallData        []byte
	morphoVaultV1CallData []byte
}

// NewAdapterProber creates an AdapterProber with pre-packed probe call data.
func NewAdapterProber() (*AdapterProber, error) {
	adapterABI, err := abis.GetVaultV2AdapterReadABI()
	if err != nil {
		return nil, fmt.Errorf("loading VaultV2 adapter ABI: %w", err)
	}
	morphoData, err := adapterABI.Pack("morpho")
	if err != nil {
		return nil, fmt.Errorf("packing morpho call: %w", err)
	}
	morphoVaultV1Data, err := adapterABI.Pack("morphoVaultV1")
	if err != nil {
		return nil, fmt.Errorf("packing morphoVaultV1 call: %w", err)
	}
	return &AdapterProber{
		adapterABI:            adapterABI,
		morphoCallData:        morphoData,
		morphoVaultV1CallData: morphoVaultV1Data,
	}, nil
}

// NumProbeCalls returns the number of multicall sub-calls one ProbeCalls batch
// contributes, so batch callers can slice a flat result array per adapter.
func (p *AdapterProber) NumProbeCalls() int { return adapterProbeCallsPerAdapter }

// ProbeCalls returns the classification multicall calls for a single adapter,
// in this order:
//
//	0: morpho()        — MorphoMarketV1AdapterV2 marker
//	1: morphoVaultV1() — MorphoVaultV1Adapter marker
//
// Both use AllowFailure: true because each adapter type reverts on the other's
// selector — a revert is the expected classification signal, not a failure.
func (p *AdapterProber) ProbeCalls(adapter common.Address) []outbound.Call {
	return []outbound.Call{
		{Target: adapter, AllowFailure: true, CallData: p.morphoCallData},
		{Target: adapter, AllowFailure: true, CallData: p.morphoVaultV1CallData},
	}
}

// ProbeAdapterType probes adapter at blockNum and returns its classified type.
//
// Adapter identity is immutable, so number-pinning (plain Execute) is
// acceptable — same rationale as getMarketParams / vault-metadata reads (see
// VEC-471). A both-fail (or both-succeed) outcome is a valid
// MorphoAdapterTypeUnknown with a nil error; the caller emits the WARN and
// still persists the adapter. Only a genuine multicall transport error
// propagates as a non-nil error, so a momentarily-unreachable adapter is
// retried rather than mis-recorded as Unknown.
func (p *AdapterProber) ProbeAdapterType(ctx context.Context, mc outbound.Multicaller, adapter common.Address, blockNum *big.Int) (entity.MorphoAdapterType, error) {
	results, err := mc.Execute(ctx, p.ProbeCalls(adapter), blockNum)
	if err != nil {
		return 0, fmt.Errorf("multicall adapter probe: %w", err)
	}
	if len(results) < adapterProbeCallsPerAdapter {
		return 0, fmt.Errorf("expected %d adapter probe results, got %d", adapterProbeCallsPerAdapter, len(results))
	}
	return classifyAdapter(results[0], results[1]), nil
}

// classifyAdapter maps the morpho() / morphoVaultV1() probe results to an
// adapter type. Exactly one selector succeeds on a real adapter; neither or
// both ⇒ Unknown.
func classifyAdapter(morphoResult, morphoVaultV1Result outbound.Result) entity.MorphoAdapterType {
	morphoOK := morphoResult.Success && len(morphoResult.ReturnData) > 0
	vaultV1OK := morphoVaultV1Result.Success && len(morphoVaultV1Result.ReturnData) > 0
	switch {
	case morphoOK && !vaultV1OK:
		return entity.MorphoAdapterTypeMarketV1
	case vaultV1OK && !morphoOK:
		return entity.MorphoAdapterTypeVaultV1
	default:
		return entity.MorphoAdapterTypeUnknown
	}
}

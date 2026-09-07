package morpho_indexer

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// adapterMarker pairs one type-discriminating getter with the adapter family it
// proves. Each family answers exactly its own marker and reverts on every other.
type adapterMarker struct {
	selector    string
	adapterType entity.MorphoAdapterType
}

// adapterMarkers is the ordered probe table: ProbeCalls emits one AllowFailure
// call per row in this order and classifyAdapter reads the results back by
// position, so modelling a new family is one row.
var adapterMarkers = [...]adapterMarker{
	{"morpho", entity.MorphoAdapterTypeMarketV1},
	{"morphoVaultV1", entity.MorphoAdapterTypeVaultV1},
	{"erc4626Vault", entity.MorphoAdapterTypeERC4626Merkl},
	{"box", entity.MorphoAdapterTypeBox},
	{"comet", entity.MorphoAdapterTypeCompoundV3},
}

// adapterProbeCallsPerAdapter is the number of probe sub-calls per adapter, one
// per marker getter.
const adapterProbeCallsPerAdapter = len(adapterMarkers)

// AdapterProber classifies a VaultV2 liquidity adapter by probing the
// type-discriminating marker getter of every modelled family (adapterMarkers).
// On a real adapter exactly one answers; none — or, defensively, several —
// ⇒ MorphoAdapterTypeUnknown, recorded rather than dropped, the same
// forward-compat philosophy as VaultProber's VaultShaped sentinel, so a
// not-yet-modelled adapter kind surfaces instead of vanishing.
type AdapterProber struct {
	markerCallData [adapterProbeCallsPerAdapter][]byte
}

// NewAdapterProber creates an AdapterProber with pre-packed probe call data.
func NewAdapterProber() (*AdapterProber, error) {
	adapterABI, err := abis.GetVaultV2AdapterReadABI()
	if err != nil {
		return nil, fmt.Errorf("loading VaultV2 adapter ABI: %w", err)
	}
	p := &AdapterProber{}
	for i, marker := range adapterMarkers {
		callData, err := adapterABI.Pack(marker.selector)
		if err != nil {
			return nil, fmt.Errorf("packing %s call: %w", marker.selector, err)
		}
		p.markerCallData[i] = callData
	}
	return p, nil
}

// NumProbeCalls returns the number of multicall sub-calls one ProbeCalls batch
// contributes, so batch callers can slice a flat result array per adapter.
func (p *AdapterProber) NumProbeCalls() int { return adapterProbeCallsPerAdapter }

// ProbeCalls returns the classification multicall calls for a single adapter,
// one per adapterMarkers row and in that table's order — the order
// classifyAdapter reads the results back in.
//
// Every call uses AllowFailure: true because an adapter reverts on every marker
// but its own — a revert is the expected classification signal, not a failure.
func (p *AdapterProber) ProbeCalls(adapter common.Address) []outbound.Call {
	calls := make([]outbound.Call, 0, adapterProbeCallsPerAdapter)
	for _, callData := range p.markerCallData {
		calls = append(calls, outbound.Call{Target: adapter, AllowFailure: true, CallData: callData})
	}
	return calls
}

// ProbeAdapterType probes adapter at blockNum and returns its classified type.
//
// Adapter identity is immutable, so number-pinning (plain Execute) is
// acceptable — same rationale as getMarketParams / vault-metadata reads (see
// VEC-471). An all-revert (or multi-answer) outcome is a valid
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
	return classifyAdapter(results), nil
}

// classifyAdapter maps marker probe results, positionally aligned with
// adapterMarkers, to an adapter type. Exactly one marker answers on a real
// adapter; none or several ⇒ Unknown.
func classifyAdapter(results []outbound.Result) entity.MorphoAdapterType {
	answered := entity.MorphoAdapterTypeUnknown
	matches := 0
	for i, marker := range adapterMarkers {
		if results[i].Success && len(results[i].ReturnData) > 0 {
			answered = marker.adapterType
			matches++
		}
	}
	if matches != 1 {
		return entity.MorphoAdapterTypeUnknown
	}
	return answered
}

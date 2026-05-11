package morpho_indexer

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// ErrNotVault indicates that an address is definitively not a Morpho-family
// vault (neither MetaMorpho V1/V1.1 nor Morpho VaultV2). Transient failures
// (network, DB) must NOT be wrapped with this type so that discovery is retried
// on the next event from that address.
//
// VaultShaped is set when at least one of the version-discriminating selectors
// (MORPHO, curator, liquidityAdapter) returned data even though the overall
// shape didn't match a known vault flavour — e.g. a MetaMorpho vault pointing
// at the wrong Morpho Blue, or a partial VaultV2 that only exposes one of the
// V2 markers. Discovery callers should log these at WARN+ so a future Morpho
// V3 doesn't sit invisible the way V2 did pre-VEC-198.
type ErrNotVault struct {
	Err         error
	VaultShaped bool
}

func (e *ErrNotVault) Error() string { return e.Err.Error() }
func (e *ErrNotVault) Unwrap() error { return e.Err }

// VaultProbeResult holds the result of probing an address for vault identity.
//
// MorphoAddr is the address returned by MORPHO() for MetaMorpho vaults; it is
// the zero address for VaultV2 (which has no MORPHO() function). Version
// reflects what the probe could determine from on-chain selectors alone:
//
//   - V1: MORPHO() succeeds. The detail-fetch phase may upgrade this to V1.1
//     if skimRecipient() also succeeds.
//   - V2: curator() and liquidityAdapter() succeed; MORPHO() reverts. Final.
type VaultProbeResult struct {
	MorphoAddr common.Address
	AssetAddr  common.Address
	Version    entity.MorphoVaultVersion
}

// VaultDetails holds vault metadata fetched from on-chain reads.
type VaultDetails struct {
	Name     string
	Symbol   string
	Decimals uint8
	Version  entity.MorphoVaultVersion
}

// VaultProber probes candidate addresses to determine if they are Morpho-family
// vaults (MetaMorpho V1/V1.1 or Morpho VaultV2) and fetches their metadata.
// Call data is pre-packed for efficient batching.
//
// Divergence vs docs/morpho_spec.md (recorded deliberately):
//
//   - Spec line 114–118 prescribes version detection by AccrueInterest event
//     signature shape (V1.1 = 2-field, V2 = 4-field). We use a probe instead:
//     `MORPHO()` succeeds → MetaMorpho (V1, optionally V1.1 if `skimRecipient()`
//     succeeds in the details phase); `MORPHO()` reverts but `curator()` and
//     `liquidityAdapter()` succeed → VaultV2; else not a vault.
//
//     Why: signature-shape detection only works after a vault has emitted at
//     least one AccrueInterest event. The indexer registers vaults at first
//     interaction (Deposit / Withdraw / Transfer) so `vault_version` must be
//     final at insert time. Combined with the project's append-only
//     convention on `morpho_vault` rows (see ON CONFLICT pattern in
//     stl-verify/internal/adapters/outbound/postgres/morpho_repository.go),
//     lazy-update from a later AccrueInterest is awkward. The probe is the
//     chosen mechanism; the result matches spec semantics, just classified
//     up-front.
//
//   - Spec line 752 lists `adapters() returns (address[])` as the V2 read
//     surface for discrimination. Chain-side verification on 2026-05-06
//     confirmed `adapters()` reverts on the deployed sparkUSDTbc; the working
//     V2 selectors are `curator()` and `liquidityAdapter()` (both real V2
//     functions, just absent from the spec text). The spec was corrected in
//     the same commit as this comment to match chain.
type VaultProber struct {
	metaMorphoABI *abi.ABI
	vaultV2ABI    *abi.ABI

	// Pre-packed call data for probe phase.
	// MetaMorpho-only: MORPHO. VaultV2-only: curator, liquidityAdapter.
	// Shared: asset.
	morphoCallData           []byte
	assetCallData            []byte
	curatorCallData          []byte
	liquidityAdapterCallData []byte

	// Pre-packed call data for details phase (name, symbol, decimals,
	// skimRecipient). skimRecipient only succeeds on MetaMorpho V1.1; it
	// reverts on V1 and on VaultV2.
	nameCallData     []byte
	symbolCallData   []byte
	decimalsCallData []byte
	skimCallData     []byte
}

// Number of probe calls per address (MORPHO, asset, curator, liquidityAdapter).
const vaultProbeCallsPerAddress = 4

// Number of details calls per address (name, symbol, decimals, skimRecipient).
const vaultDetailsCallsPerAddress = 4

// NumProbeCalls returns the number of multicall sub-calls a single
// ProbeCalls(addr) batch contributes. Single source of truth for callers
// that pack multi-address batches and need to slice the flat result array
// back into per-address windows.
func (p *VaultProber) NumProbeCalls() int { return vaultProbeCallsPerAddress }

// NumDetailsCalls returns the number of multicall sub-calls a single
// DetailsCalls(addr) batch contributes. Same rationale as NumProbeCalls.
func (p *VaultProber) NumDetailsCalls() int { return vaultDetailsCallsPerAddress }

// NewVaultProber creates a VaultProber with pre-packed ABI call data.
func NewVaultProber() (*VaultProber, error) {
	metaMorphoABI, err := abis.GetMetaMorphoReadABI()
	if err != nil {
		return nil, fmt.Errorf("loading MetaMorpho ABI: %w", err)
	}
	vaultV2ABI, err := abis.GetVaultV2ReadABI()
	if err != nil {
		return nil, fmt.Errorf("loading VaultV2 ABI: %w", err)
	}

	morphoData, err := metaMorphoABI.Pack("MORPHO")
	if err != nil {
		return nil, fmt.Errorf("packing MORPHO call: %w", err)
	}
	assetData, err := metaMorphoABI.Pack("asset")
	if err != nil {
		return nil, fmt.Errorf("packing asset call: %w", err)
	}
	curatorData, err := vaultV2ABI.Pack("curator")
	if err != nil {
		return nil, fmt.Errorf("packing curator call: %w", err)
	}
	liquidityAdapterData, err := vaultV2ABI.Pack("liquidityAdapter")
	if err != nil {
		return nil, fmt.Errorf("packing liquidityAdapter call: %w", err)
	}
	nameData, err := metaMorphoABI.Pack("name")
	if err != nil {
		return nil, fmt.Errorf("packing name call: %w", err)
	}
	symbolData, err := metaMorphoABI.Pack("symbol")
	if err != nil {
		return nil, fmt.Errorf("packing symbol call: %w", err)
	}
	decimalsData, err := metaMorphoABI.Pack("decimals")
	if err != nil {
		return nil, fmt.Errorf("packing decimals call: %w", err)
	}
	skimData, err := metaMorphoABI.Pack("skimRecipient")
	if err != nil {
		return nil, fmt.Errorf("packing skimRecipient call: %w", err)
	}

	return &VaultProber{
		metaMorphoABI:            metaMorphoABI,
		vaultV2ABI:               vaultV2ABI,
		morphoCallData:           morphoData,
		assetCallData:            assetData,
		curatorCallData:          curatorData,
		liquidityAdapterCallData: liquidityAdapterData,
		nameCallData:             nameData,
		symbolCallData:           symbolData,
		decimalsCallData:         decimalsData,
		skimCallData:             skimData,
	}, nil
}

// ProbeVault calls MORPHO(), asset(), curator(), and liquidityAdapter() on a
// single address to determine if it is a Morpho-family vault. Returns
// ErrNotVault if neither the MetaMorpho nor the VaultV2 path can be confirmed.
func (p *VaultProber) ProbeVault(ctx context.Context, mc outbound.Multicaller, addr common.Address, blockNum *big.Int) (*VaultProbeResult, error) {
	results, err := mc.Execute(ctx, p.ProbeCalls(addr), blockNum)
	if err != nil {
		return nil, fmt.Errorf("multicall vault probe: %w", err)
	}
	if len(results) < vaultProbeCallsPerAddress {
		return nil, fmt.Errorf("expected %d probe results, got %d", vaultProbeCallsPerAddress, len(results))
	}
	return p.ParseProbeResults(results[0], results[1], results[2], results[3], addr)
}

// ProbeCalls returns the probe multicall calls for a single address.
//
// Returns vaultProbeCallsPerAddress (4) calls in this order:
//
//	0: MORPHO()           — MetaMorpho V1/V1.1 marker
//	1: asset()            — shared (ERC4626)
//	2: curator()          — VaultV2 marker
//	3: liquidityAdapter() — VaultV2 marker
//
// All calls use AllowFailure: true because each variant is expected to revert
// on the other's selectors.
func (p *VaultProber) ProbeCalls(addr common.Address) []outbound.Call {
	return []outbound.Call{
		{Target: addr, AllowFailure: true, CallData: p.morphoCallData},
		{Target: addr, AllowFailure: true, CallData: p.assetCallData},
		{Target: addr, AllowFailure: true, CallData: p.curatorCallData},
		{Target: addr, AllowFailure: true, CallData: p.liquidityAdapterCallData},
	}
}

// ParseProbeResults parses the four probe multicall results for a single
// address and decides which vault variant (if any) it is.
//
// Decision tree:
//
//  1. asset() must succeed and return a non-zero address. Any other outcome
//     is ErrNotVault.
//  2. If MORPHO() succeeded, this is a MetaMorpho vault. Tentative version is
//     V1; the details phase will upgrade to V1.1 if skimRecipient() succeeds.
//  3. If MORPHO() reverted, fall back to the VaultV2 path: curator() and
//     liquidityAdapter() must both succeed.
//  4. Otherwise, ErrNotVault. The error is flagged VaultShaped if any
//     version-discriminating selector (MORPHO, curator, liquidityAdapter)
//     returned data — operators can then surface the address for manual
//     review (e.g. to spot a future Morpho V3 before it sits invisible for
//     months).
func (p *VaultProber) ParseProbeResults(morphoResult, assetResult, curatorResult, liquidityAdapterResult outbound.Result, addr common.Address) (*VaultProbeResult, error) {
	vaultShaped := anyProbeSelectorSucceeded(morphoResult, curatorResult, liquidityAdapterResult)

	asset, err := p.unpackAsset(assetResult, addr)
	if err != nil {
		// Inherit vault-shaped flag — asset() can fail on a contract that
		// nonetheless exposes a future version's markers we don't recognise.
		propagateVaultShaped(err, vaultShaped)
		return nil, err
	}
	if asset == (common.Address{}) {
		// asset() succeeded but returned 0x0. The address is shape-confirmed
		// only if some version-discriminating selector also responded.
		return nil, &ErrNotVault{
			Err:         fmt.Errorf("asset() returned zero address for %s", addr.Hex()),
			VaultShaped: vaultShaped,
		}
	}

	if morphoResult.Success && len(morphoResult.ReturnData) > 0 {
		morphoAddr, err := p.unpackMorphoAddress(morphoResult, addr)
		if err != nil {
			// MORPHO succeeded but unpack failed → unambiguously vault-shaped
			// (a real future version with a different MORPHO return type).
			propagateVaultShaped(err, true)
			return nil, err
		}
		return &VaultProbeResult{
			MorphoAddr: morphoAddr,
			AssetAddr:  asset,
			Version:    entity.MorphoVaultV1, // possibly upgraded to V1.1 in details phase
		}, nil
	}

	if err := p.verifyVaultV2Markers(curatorResult, liquidityAdapterResult, addr); err != nil {
		propagateVaultShaped(err, vaultShaped)
		return nil, err
	}
	return &VaultProbeResult{
		MorphoAddr: common.Address{},
		AssetAddr:  asset,
		Version:    entity.MorphoVaultV2,
	}, nil
}

// propagateVaultShaped sets the VaultShaped flag on err if err is *ErrNotVault.
// No-op for other error types so transient (transport / DB) errors are not
// silently reclassified.
func propagateVaultShaped(err error, vaultShaped bool) {
	var nv *ErrNotVault
	if errors.As(err, &nv) {
		nv.VaultShaped = vaultShaped
	}
}

// anyProbeSelectorSucceeded returns true if any of the version-discriminating
// probe selectors (MORPHO, curator, liquidityAdapter) returned data. asset()
// is excluded because it's an ERC4626 surface shared with every vault — its
// success alone doesn't make an address vault-shaped enough to warrant alert.
func anyProbeSelectorSucceeded(morphoResult, curatorResult, liquidityAdapterResult outbound.Result) bool {
	return (morphoResult.Success && len(morphoResult.ReturnData) > 0) ||
		(curatorResult.Success && len(curatorResult.ReturnData) > 0) ||
		(liquidityAdapterResult.Success && len(liquidityAdapterResult.ReturnData) > 0)
}

// unpackAsset extracts the asset() return value, returning ErrNotVault if the
// call failed or returned no useful data.
func (p *VaultProber) unpackAsset(assetResult outbound.Result, addr common.Address) (common.Address, error) {
	if !assetResult.Success || len(assetResult.ReturnData) == 0 {
		return common.Address{}, &ErrNotVault{Err: fmt.Errorf("asset() call failed for %s", addr.Hex())}
	}
	assetUnpacked, err := p.metaMorphoABI.Unpack("asset", assetResult.ReturnData)
	if err != nil {
		return common.Address{}, &ErrNotVault{Err: fmt.Errorf("unpacking asset() for %s: %w", addr.Hex(), err)}
	}
	if len(assetUnpacked) == 0 {
		return common.Address{}, &ErrNotVault{Err: fmt.Errorf("asset() returned no values for %s", addr.Hex())}
	}
	asset, ok := assetUnpacked[0].(common.Address)
	if !ok {
		return common.Address{}, &ErrNotVault{Err: fmt.Errorf("asset() returned unexpected type %T for %s", assetUnpacked[0], addr.Hex())}
	}
	return asset, nil
}

// unpackMorphoAddress extracts the MORPHO() return value. Caller must have
// already confirmed morphoResult.Success.
func (p *VaultProber) unpackMorphoAddress(morphoResult outbound.Result, addr common.Address) (common.Address, error) {
	morphoUnpacked, err := p.metaMorphoABI.Unpack("MORPHO", morphoResult.ReturnData)
	if err != nil {
		return common.Address{}, &ErrNotVault{Err: fmt.Errorf("unpacking MORPHO(): %w", err)}
	}
	if len(morphoUnpacked) == 0 {
		return common.Address{}, &ErrNotVault{Err: fmt.Errorf("MORPHO() returned no values for %s", addr.Hex())}
	}
	morphoAddr, ok := morphoUnpacked[0].(common.Address)
	if !ok {
		return common.Address{}, &ErrNotVault{Err: fmt.Errorf("MORPHO() returned unexpected type for %s", addr.Hex())}
	}
	return morphoAddr, nil
}

// verifyVaultV2Markers checks that both curator() and liquidityAdapter()
// returned addresses successfully. The returned values themselves are not
// retained — they're only checked as identification markers.
func (p *VaultProber) verifyVaultV2Markers(curatorResult, liquidityAdapterResult outbound.Result, addr common.Address) error {
	if !curatorResult.Success || len(curatorResult.ReturnData) == 0 {
		return &ErrNotVault{Err: fmt.Errorf("MORPHO() and curator() both failed — not a Morpho-family vault: %s", addr.Hex())}
	}
	if _, err := p.vaultV2ABI.Unpack("curator", curatorResult.ReturnData); err != nil {
		return &ErrNotVault{Err: fmt.Errorf("unpacking curator() for %s: %w", addr.Hex(), err)}
	}
	if !liquidityAdapterResult.Success || len(liquidityAdapterResult.ReturnData) == 0 {
		return &ErrNotVault{Err: fmt.Errorf("MORPHO() and liquidityAdapter() both failed — not a Morpho-family vault: %s", addr.Hex())}
	}
	if _, err := p.vaultV2ABI.Unpack("liquidityAdapter", liquidityAdapterResult.ReturnData); err != nil {
		return &ErrNotVault{Err: fmt.Errorf("unpacking liquidityAdapter() for %s: %w", addr.Hex(), err)}
	}
	return nil
}

// FetchVaultDetails calls name, symbol, decimals, and skimRecipient on a single
// confirmed vault address and returns its metadata.
//
// tentativeVersion is the version inferred during the probe phase. For V1
// vaults (MetaMorpho), the version may be upgraded to V1.1 here if
// skimRecipient() succeeds. For V2 vaults, the version is preserved as-is.
func (p *VaultProber) FetchVaultDetails(ctx context.Context, mc outbound.Multicaller, addr common.Address, tentativeVersion entity.MorphoVaultVersion, blockNum *big.Int) (*VaultDetails, error) {
	results, err := mc.Execute(ctx, p.DetailsCalls(addr), blockNum)
	if err != nil {
		return nil, fmt.Errorf("multicall vault details: %w", err)
	}
	if len(results) < vaultDetailsCallsPerAddress {
		return nil, fmt.Errorf("expected %d detail results, got %d", vaultDetailsCallsPerAddress, len(results))
	}
	return p.ParseDetailsResults(results[0], results[1], results[2], results[3], addr, tentativeVersion)
}

// DetailsCalls returns name, symbol, decimals, skimRecipient multicall calls
// for a single address.
//
// Returns vaultDetailsCallsPerAddress (4) calls.
func (p *VaultProber) DetailsCalls(addr common.Address) []outbound.Call {
	return []outbound.Call{
		{Target: addr, AllowFailure: true, CallData: p.nameCallData},
		{Target: addr, AllowFailure: true, CallData: p.symbolCallData},
		{Target: addr, AllowFailure: true, CallData: p.decimalsCallData},
		{Target: addr, AllowFailure: true, CallData: p.skimCallData},
	}
}

// ParseDetailsResults parses name, symbol, decimals, skimRecipient multicall
// results for a single vault address.
//
// tentativeVersion comes from the probe phase. The skimRecipient check only
// upgrades V1 → V1.1; V2 passes through unchanged because skimRecipient
// reverts on VaultV2 anyway.
func (p *VaultProber) ParseDetailsResults(nameResult, symbolResult, decimalsResult, skimResult outbound.Result, addr common.Address, tentativeVersion entity.MorphoVaultVersion) (*VaultDetails, error) {
	md := &VaultDetails{Version: tentativeVersion}

	if md.Version == entity.MorphoVaultV1 && skimResult.Success && len(skimResult.ReturnData) > 0 {
		if _, err := p.metaMorphoABI.Unpack("skimRecipient", skimResult.ReturnData); err == nil {
			md.Version = entity.MorphoVaultV1_1
		}
	}

	name, err := unpackStringField(p.metaMorphoABI, "name", nameResult, addr)
	if err != nil {
		return nil, err
	}
	md.Name = name

	symbol, err := unpackStringField(p.metaMorphoABI, "symbol", symbolResult, addr)
	if err != nil {
		return nil, err
	}
	md.Symbol = symbol

	decimals, err := unpackDecimalsField(p.metaMorphoABI, decimalsResult, addr)
	if err != nil {
		return nil, err
	}
	md.Decimals = decimals

	return md, nil
}

func unpackStringField(parser *abi.ABI, name string, result outbound.Result, addr common.Address) (string, error) {
	if !result.Success || len(result.ReturnData) == 0 {
		return "", fmt.Errorf("%s call failed for vault %s", name, addr.Hex())
	}
	unpacked, err := parser.Unpack(name, result.ReturnData)
	if err != nil {
		return "", fmt.Errorf("unpacking %s for vault %s: %w", name, addr.Hex(), err)
	}
	if len(unpacked) == 0 {
		return "", fmt.Errorf("empty %s result for vault %s", name, addr.Hex())
	}
	value, ok := unpacked[0].(string)
	if !ok {
		return "", fmt.Errorf("%s type assertion failed for vault %s: got %T", name, addr.Hex(), unpacked[0])
	}
	return value, nil
}

func unpackDecimalsField(parser *abi.ABI, result outbound.Result, addr common.Address) (uint8, error) {
	if !result.Success || len(result.ReturnData) == 0 {
		return 0, fmt.Errorf("decimals call failed for vault %s", addr.Hex())
	}
	unpacked, err := parser.Unpack("decimals", result.ReturnData)
	if err != nil {
		return 0, fmt.Errorf("unpacking decimals for vault %s: %w", addr.Hex(), err)
	}
	if len(unpacked) == 0 {
		return 0, fmt.Errorf("empty decimals result for vault %s", addr.Hex())
	}
	decimals, ok := unpacked[0].(uint8)
	if !ok {
		return 0, fmt.Errorf("decimals type assertion failed for vault %s: got %T", addr.Hex(), unpacked[0])
	}
	return decimals, nil
}

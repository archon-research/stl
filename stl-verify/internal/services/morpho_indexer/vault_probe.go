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

// ErrNotVault indicates that an address is definitively not a MetaMorpho vault.
// Transient failures (network, DB) must NOT be wrapped with this type so that
// discovery is retried on the next event from that address.
type ErrNotVault struct{ Err error }

func (e *ErrNotVault) Error() string { return e.Err.Error() }
func (e *ErrNotVault) Unwrap() error { return e.Err }

// VaultProbeResult holds the result of probing an address for vault identity.
type VaultProbeResult struct {
	MorphoAddr common.Address
	AssetAddr  common.Address
}

// VaultDetails holds vault metadata fetched from on-chain reads.
type VaultDetails struct {
	Name     string
	Symbol   string
	Decimals uint8
	Version  entity.MorphoVaultVersion
}

// VaultProber probes candidate addresses to determine if they are MetaMorpho vaults
// and fetches their metadata. Call data is pre-packed for efficient batching.
type VaultProber struct {
	metaMorphoABI *abi.ABI

	// Pre-packed call data for probe phase (MORPHO, asset).
	morphoCallData []byte
	assetCallData  []byte

	// Pre-packed call data for details phase (name, symbol, decimals, skimRecipient).
	nameCallData     []byte
	symbolCallData   []byte
	decimalsCallData []byte
	skimCallData     []byte
}

// NewVaultProber creates a VaultProber with pre-packed ABI call data.
func NewVaultProber() (*VaultProber, error) {
	metaMorphoABI, err := abis.GetMetaMorphoReadABI()
	if err != nil {
		return nil, fmt.Errorf("loading MetaMorpho ABI: %w", err)
	}

	morphoData, err := metaMorphoABI.Pack("MORPHO")
	if err != nil {
		return nil, fmt.Errorf("packing MORPHO call: %w", err)
	}
	assetData, err := metaMorphoABI.Pack("asset")
	if err != nil {
		return nil, fmt.Errorf("packing asset call: %w", err)
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
		metaMorphoABI:    metaMorphoABI,
		morphoCallData:   morphoData,
		assetCallData:    assetData,
		nameCallData:     nameData,
		symbolCallData:   symbolData,
		decimalsCallData: decimalsData,
		skimCallData:     skimData,
	}, nil
}

// ProbeVault calls MORPHO() and asset() on a single address to determine if it
// is a MetaMorpho vault. Returns ErrNotVault if the address is not a vault.
func (p *VaultProber) ProbeVault(ctx context.Context, mc outbound.Multicaller, addr common.Address, blockNum *big.Int) (*VaultProbeResult, error) {
	results, err := mc.Execute(ctx, p.ProbeCalls(addr), blockNum)
	if err != nil {
		return nil, fmt.Errorf("multicall vault probe: %w", err)
	}
	if len(results) < 2 {
		return nil, fmt.Errorf("expected 2 probe results, got %d", len(results))
	}
	return p.ParseProbeResults(results[0], results[1], addr)
}

// ProbeCalls returns the MORPHO() and asset() multicall calls for a single address.
// Returns 2 calls.
func (p *VaultProber) ProbeCalls(addr common.Address) []outbound.Call {
	return []outbound.Call{
		{Target: addr, AllowFailure: true, CallData: p.morphoCallData},
		{Target: addr, AllowFailure: true, CallData: p.assetCallData},
	}
}

// ParseProbeResults parses the MORPHO() and asset() multicall results for a single address.
// Returns ErrNotVault if the address is not a MetaMorpho vault.
func (p *VaultProber) ParseProbeResults(morphoResult, assetResult outbound.Result, addr common.Address) (*VaultProbeResult, error) {
	if !morphoResult.Success || len(morphoResult.ReturnData) == 0 {
		return nil, &ErrNotVault{Err: fmt.Errorf("MORPHO() call failed — not a MetaMorpho vault: %s", addr.Hex())}
	}
	morphoUnpacked, err := p.metaMorphoABI.Unpack("MORPHO", morphoResult.ReturnData)
	if err != nil {
		return nil, &ErrNotVault{Err: fmt.Errorf("unpacking MORPHO(): %w", err)}
	}
	if len(morphoUnpacked) == 0 {
		return nil, &ErrNotVault{Err: fmt.Errorf("MORPHO() returned no values for %s", addr.Hex())}
	}
	morphoAddr, ok := morphoUnpacked[0].(common.Address)
	if !ok {
		return nil, &ErrNotVault{Err: fmt.Errorf("MORPHO() returned unexpected type for %s", addr.Hex())}
	}

	if !assetResult.Success || len(assetResult.ReturnData) == 0 {
		return nil, &ErrNotVault{Err: fmt.Errorf("asset() call failed for %s", addr.Hex())}
	}
	assetUnpacked, err := p.metaMorphoABI.Unpack("asset", assetResult.ReturnData)
	if err != nil {
		return nil, &ErrNotVault{Err: fmt.Errorf("unpacking asset() for %s: %w", addr.Hex(), err)}
	}
	if len(assetUnpacked) == 0 {
		return nil, &ErrNotVault{Err: fmt.Errorf("asset() returned no values for %s", addr.Hex())}
	}
	asset, ok := assetUnpacked[0].(common.Address)
	if !ok {
		return nil, &ErrNotVault{Err: fmt.Errorf("asset() returned unexpected type %T for %s", assetUnpacked[0], addr.Hex())}
	}

	return &VaultProbeResult{MorphoAddr: morphoAddr, AssetAddr: asset}, nil
}

// FetchVaultDetails calls name, symbol, decimals, and skimRecipient on a single
// confirmed vault address and returns its metadata.
func (p *VaultProber) FetchVaultDetails(ctx context.Context, mc outbound.Multicaller, addr common.Address, blockNum *big.Int) (*VaultDetails, error) {
	results, err := mc.Execute(ctx, p.DetailsCalls(addr), blockNum)
	if err != nil {
		return nil, fmt.Errorf("multicall vault details: %w", err)
	}
	if len(results) < 4 {
		return nil, fmt.Errorf("expected 4 detail results, got %d", len(results))
	}
	return p.ParseDetailsResults(results[0], results[1], results[2], results[3], addr)
}

// DetailsCalls returns name, symbol, decimals, skimRecipient multicall calls for a single address.
// Returns 4 calls.
func (p *VaultProber) DetailsCalls(addr common.Address) []outbound.Call {
	return []outbound.Call{
		{Target: addr, AllowFailure: true, CallData: p.nameCallData},
		{Target: addr, AllowFailure: true, CallData: p.symbolCallData},
		{Target: addr, AllowFailure: true, CallData: p.decimalsCallData},
		{Target: addr, AllowFailure: true, CallData: p.skimCallData},
	}
}

// ParseDetailsResults parses name, symbol, decimals, skimRecipient multicall results
// for a single vault address.
func (p *VaultProber) ParseDetailsResults(nameResult, symbolResult, decimalsResult, skimResult outbound.Result, addr common.Address) (*VaultDetails, error) {
	md := &VaultDetails{Version: entity.MorphoVaultV1}

	// skimRecipient() only exists on V2 — success with valid return data means V2.
	if skimResult.Success && len(skimResult.ReturnData) > 0 {
		if _, err := p.metaMorphoABI.Unpack("skimRecipient", skimResult.ReturnData); err == nil {
			md.Version = entity.MorphoVaultV2
		}
	}

	if !nameResult.Success || len(nameResult.ReturnData) == 0 {
		return nil, fmt.Errorf("name call failed for vault %s", addr.Hex())
	}
	nameUnpacked, err := p.metaMorphoABI.Unpack("name", nameResult.ReturnData)
	if err != nil {
		return nil, fmt.Errorf("unpacking name for vault %s: %w", addr.Hex(), err)
	}
	if len(nameUnpacked) == 0 {
		return nil, fmt.Errorf("empty name result for vault %s", addr.Hex())
	}
	name, ok := nameUnpacked[0].(string)
	if !ok {
		return nil, fmt.Errorf("name type assertion failed for vault %s: got %T", addr.Hex(), nameUnpacked[0])
	}
	md.Name = name

	if !symbolResult.Success || len(symbolResult.ReturnData) == 0 {
		return nil, fmt.Errorf("symbol call failed for vault %s", addr.Hex())
	}
	symbolUnpacked, err := p.metaMorphoABI.Unpack("symbol", symbolResult.ReturnData)
	if err != nil {
		return nil, fmt.Errorf("unpacking symbol for vault %s: %w", addr.Hex(), err)
	}
	if len(symbolUnpacked) == 0 {
		return nil, fmt.Errorf("empty symbol result for vault %s", addr.Hex())
	}
	symbol, ok := symbolUnpacked[0].(string)
	if !ok {
		return nil, fmt.Errorf("symbol type assertion failed for vault %s: got %T", addr.Hex(), symbolUnpacked[0])
	}
	md.Symbol = symbol

	if !decimalsResult.Success || len(decimalsResult.ReturnData) == 0 {
		return nil, fmt.Errorf("decimals call failed for vault %s", addr.Hex())
	}
	decimalsUnpacked, err := p.metaMorphoABI.Unpack("decimals", decimalsResult.ReturnData)
	if err != nil {
		return nil, fmt.Errorf("unpacking decimals for vault %s: %w", addr.Hex(), err)
	}
	if len(decimalsUnpacked) == 0 {
		return nil, fmt.Errorf("empty decimals result for vault %s", addr.Hex())
	}
	decimals, ok := decimalsUnpacked[0].(uint8)
	if !ok {
		return nil, fmt.Errorf("decimals type assertion failed for vault %s: got %T", addr.Hex(), decimalsUnpacked[0])
	}
	md.Decimals = decimals

	return md, nil
}

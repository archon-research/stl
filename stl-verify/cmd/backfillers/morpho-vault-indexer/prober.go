package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/morpho_indexer"
)

// confirmedVault holds metadata for a confirmed Morpho-family vault
// (MetaMorpho V1/V1.1 or Morpho VaultV2).
type confirmedVault struct {
	Address     common.Address
	Name        string
	Symbol      string
	Asset       common.Address
	AssetSymbol string
	Decimals    uint8
	Version     entity.MorphoVaultVersion
	FirstBlock  int64
}

// vaultProber probes candidate addresses on-chain to determine if they are
// Morpho-family vaults.
type vaultProber struct {
	multicaller  outbound.Multicaller
	sharedProber *morpho_indexer.VaultProber
	erc20ABI     *abi.ABI
	logger       *slog.Logger
}

// callsPerProbe is how many sub-calls each candidate address consumes during
// the probe phase. Mirrors the multicall layout in
// morpho_indexer.VaultProber.ProbeCalls.
const callsPerProbe = 4

// callsPerMetadata is how many sub-calls each confirmed vault consumes during
// the metadata phase: 4 details (name, symbol, decimals, skimRecipient) + 1
// asset symbol from the underlying ERC20.
const callsPerMetadata = 5

// probeAllCandidates checks each candidate address by calling the shared probe
// (MORPHO/asset/curator/liquidityAdapter) via multicall. Returns confirmed
// vaults with their metadata.
func (p *vaultProber) probeAllCandidates(
	ctx context.Context,
	candidates map[common.Address]int64,
	probeBlock int64,
	batchSize int,
) ([]confirmedVault, error) {
	addrs := make([]common.Address, 0, len(candidates))
	for addr := range candidates {
		addrs = append(addrs, addr)
	}

	blockNum := new(big.Int).SetInt64(probeBlock)
	var confirmed []confirmedVault

	for i := 0; i < len(addrs); i += batchSize {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		end := min(i+batchSize, len(addrs))
		batch := addrs[i:end]

		vaults, err := p.probeBatchWithRetry(ctx, batch, candidates, blockNum)
		if err != nil {
			return nil, fmt.Errorf("probing batch %d-%d: %w", i, end, err)
		}
		confirmed = append(confirmed, vaults...)

		p.logger.Info("probe progress",
			"probed", end,
			"total", len(addrs),
			"confirmedSoFar", len(confirmed))
	}

	return confirmed, nil
}

// probeBatchWithRetry tries probeBatch, and on failure retries with progressively
// smaller sub-batches down to single-address probes. This handles "out of gas"
// errors from the multicall when a batch contains contracts that consume excessive gas.
func (p *vaultProber) probeBatchWithRetry(
	ctx context.Context,
	batch []common.Address,
	firstBlocks map[common.Address]int64,
	blockNum *big.Int,
) ([]confirmedVault, error) {
	vaults, err := p.probeBatch(ctx, batch, firstBlocks, blockNum)
	if err == nil {
		return vaults, nil
	}

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// Batch failed — retry with halved batch size, down to individual probes.
	p.logger.Warn("batch probe failed, retrying with smaller batches",
		"batchSize", len(batch),
		"error", err)

	if len(batch) == 1 {
		// Single address failed — skip it.
		p.logger.Warn("skipping candidate that fails probe",
			"address", batch[0].Hex(),
			"error", err)
		return nil, nil
	}

	mid := len(batch) / 2
	left, err := p.probeBatchWithRetry(ctx, batch[:mid], firstBlocks, blockNum)
	if err != nil {
		return nil, err
	}
	right, err := p.probeBatchWithRetry(ctx, batch[mid:], firstBlocks, blockNum)
	if err != nil {
		return nil, err
	}
	return append(left, right...), nil
}

// confirmedProbe pairs a probe-confirmed vault address with the version the
// probe could determine (V1 or V2; V1 may be upgraded to V1.1 in the metadata
// phase) and the asset address it returned.
type confirmedProbe struct {
	address common.Address
	asset   common.Address
	version entity.MorphoVaultVersion
}

// probeBatch probes a batch of candidate addresses. For each address, it calls
// the shared 4-call probe in a single multicall. Confirmed vaults get their
// metadata fetched in a follow-up multicall.
func (p *vaultProber) probeBatch(
	ctx context.Context,
	batch []common.Address,
	firstBlocks map[common.Address]int64,
	blockNum *big.Int,
) ([]confirmedVault, error) {
	calls := make([]outbound.Call, 0, len(batch)*callsPerProbe)
	for _, addr := range batch {
		calls = append(calls, p.sharedProber.ProbeCalls(addr)...)
	}

	results, err := p.multicaller.Execute(ctx, calls, blockNum)
	if err != nil {
		return nil, fmt.Errorf("multicall probe: %w", err)
	}

	probeConfirmed, err := p.collectProbeConfirmed(batch, results)
	if err != nil {
		return nil, err
	}
	if len(probeConfirmed) == 0 {
		return nil, nil
	}

	return p.fetchVaultMetadata(ctx, probeConfirmed, firstBlocks, blockNum)
}

// collectProbeConfirmed parses the per-candidate probe results and returns the
// addresses that look like Morpho-family vaults.
//
// MetaMorpho candidates whose MORPHO() points somewhere other than the
// canonical Morpho Blue singleton are dropped — that's a foreign deployment we
// don't index. VaultV2 candidates have no such constraint.
//
// *morpho_indexer.ErrNotVault is the only legitimate skip disposition: it means
// the address is definitively not a Morpho-family vault. Any other error is
// structural (ABI mismatch, unrecognised return shape) and is propagated so
// the caller can decide how to retry — silently dropping such errors causes
// the candidate to disappear from the backfill run with no recoverable trail.
func (p *vaultProber) collectProbeConfirmed(batch []common.Address, results []outbound.Result) ([]confirmedProbe, error) {
	confirmed := make([]confirmedProbe, 0, len(batch))
	for i, addr := range batch {
		base := i * callsPerProbe
		probeResult, err := p.sharedProber.ParseProbeResults(
			results[base], results[base+1], results[base+2], results[base+3], addr)
		if err != nil {
			var nv *morpho_indexer.ErrNotVault
			if errors.As(err, &nv) {
				continue
			}
			return nil, fmt.Errorf("parsing probe results for %s: %w", addr.Hex(), err)
		}
		if probeResult.Version != entity.MorphoVaultV2 && probeResult.MorphoAddr != morpho_indexer.MorphoBlueAddress {
			continue
		}
		// Zero-address asset is rejected upstream in ParseProbeResults.
		confirmed = append(confirmed, confirmedProbe{
			address: addr,
			asset:   probeResult.AssetAddr,
			version: probeResult.Version,
		})
	}
	return confirmed, nil
}

// fetchVaultMetadata fetches name, symbol, decimals, version, and asset symbol
// for confirmed vaults. The version may be upgraded from V1 → V1.1 here if
// skimRecipient succeeds.
func (p *vaultProber) fetchVaultMetadata(
	ctx context.Context,
	probeConfirmed []confirmedProbe,
	firstBlocks map[common.Address]int64,
	blockNum *big.Int,
) ([]confirmedVault, error) {
	assetSymbolData, err := p.erc20ABI.Pack("symbol")
	if err != nil {
		return nil, fmt.Errorf("packing asset symbol: %w", err)
	}

	calls := make([]outbound.Call, 0, len(probeConfirmed)*callsPerMetadata)
	for _, pc := range probeConfirmed {
		calls = append(calls, p.sharedProber.DetailsCalls(pc.address)...)
		calls = append(calls, outbound.Call{Target: pc.asset, AllowFailure: true, CallData: assetSymbolData})
	}

	results, err := p.multicaller.Execute(ctx, calls, blockNum)
	if err != nil {
		return nil, fmt.Errorf("multicall metadata: %w", err)
	}

	var vaults []confirmedVault
	for i, pc := range probeConfirmed {
		base := i * callsPerMetadata

		details, err := p.sharedProber.ParseDetailsResults(
			results[base], results[base+1], results[base+2], results[base+3], pc.address, pc.version)
		if err != nil {
			return nil, fmt.Errorf("parsing metadata for confirmed vault %s: %w", pc.address.Hex(), err)
		}

		v := confirmedVault{
			Address:    pc.address,
			Name:       details.Name,
			Symbol:     details.Symbol,
			Decimals:   details.Decimals,
			Version:    details.Version,
			Asset:      pc.asset,
			FirstBlock: firstBlocks[pc.address],
		}

		assetSymbolResult := results[base+(callsPerMetadata-1)]
		if assetSymbolResult.Success && len(assetSymbolResult.ReturnData) > 0 {
			if unpacked, err := p.erc20ABI.Unpack("symbol", assetSymbolResult.ReturnData); err == nil && len(unpacked) > 0 {
				if sym, ok := unpacked[0].(string); ok {
					v.AssetSymbol = sym
				}
			}
		}

		if v.AssetSymbol == "" {
			p.logger.Warn("skipping vault with empty asset symbol",
				"address", pc.address.Hex(),
				"name", v.Name,
				"asset", v.Asset.Hex())
			continue
		}

		p.logger.Info("confirmed vault",
			"address", pc.address.Hex(),
			"name", v.Name,
			"symbol", v.Symbol,
			"asset", v.Asset.Hex(),
			"assetSymbol", v.AssetSymbol,
			"version", v.Version,
			"firstBlock", v.FirstBlock)

		vaults = append(vaults, v)
	}

	return vaults, nil
}

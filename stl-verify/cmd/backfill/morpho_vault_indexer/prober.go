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

// confirmedVault holds metadata for a confirmed MetaMorpho vault.
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

// vaultProber probes candidate addresses on-chain to determine if they are MetaMorpho vaults.
type vaultProber struct {
	multicaller  outbound.Multicaller
	sharedProber *morpho_indexer.VaultProber
	erc20ABI     *abi.ABI
	logger       *slog.Logger
}

// probeAllCandidates checks each candidate address by calling MORPHO() and asset()
// via multicall. Returns confirmed vaults with their metadata.
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

		vaults, err := p.probeBatch(ctx, batch, candidates, blockNum)
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

// probeBatch probes a batch of candidate addresses. For each address, it calls
// MORPHO() and asset() in a single multicall. Confirmed vaults get their metadata
// fetched in a follow-up multicall.
func (p *vaultProber) probeBatch(
	ctx context.Context,
	batch []common.Address,
	firstBlocks map[common.Address]int64,
	blockNum *big.Int,
) ([]confirmedVault, error) {
	// Build multicall: 2 calls per candidate (MORPHO, asset)
	calls := make([]outbound.Call, 0, len(batch)*2)
	for _, addr := range batch {
		calls = append(calls, p.sharedProber.ProbeCalls(addr)...)
	}

	results, err := p.multicaller.Execute(ctx, calls, blockNum)
	if err != nil {
		return nil, fmt.Errorf("multicall probe: %w", err)
	}

	// Identify confirmed vaults from results
	var vaultAddrs []common.Address
	vaultAssets := make(map[common.Address]common.Address)

	for i, addr := range batch {
		probeResult, err := p.sharedProber.ParseProbeResults(results[i*2], results[i*2+1], addr)
		if err != nil {
			var nv *morpho_indexer.ErrNotVault
			if errors.As(err, &nv) {
				continue
			}
			return nil, fmt.Errorf("parsing probe for %s: %w", addr.Hex(), err)
		}
		if probeResult.MorphoAddr != morpho_indexer.MorphoBlueAddress {
			continue
		}
		if probeResult.AssetAddr == (common.Address{}) {
			continue
		}

		vaultAddrs = append(vaultAddrs, addr)
		vaultAssets[addr] = probeResult.AssetAddr
	}

	if len(vaultAddrs) == 0 {
		return nil, nil
	}

	// Fetch metadata for confirmed vaults
	return p.fetchVaultMetadata(ctx, vaultAddrs, vaultAssets, firstBlocks, blockNum)
}

// fetchVaultMetadata fetches name, symbol, decimals, version, and asset symbol for confirmed vaults.
func (p *vaultProber) fetchVaultMetadata(
	ctx context.Context,
	vaultAddrs []common.Address,
	vaultAssets map[common.Address]common.Address,
	firstBlocks map[common.Address]int64,
	blockNum *big.Int,
) ([]confirmedVault, error) {
	assetSymbolData, err := p.erc20ABI.Pack("symbol")
	if err != nil {
		return nil, fmt.Errorf("packing asset symbol: %w", err)
	}

	// 5 calls per vault: 4 details (name, symbol, decimals, skimRecipient) + 1 asset symbol
	calls := make([]outbound.Call, 0, len(vaultAddrs)*5)
	for _, addr := range vaultAddrs {
		calls = append(calls, p.sharedProber.DetailsCalls(addr)...)
		calls = append(calls, outbound.Call{Target: vaultAssets[addr], AllowFailure: true, CallData: assetSymbolData})
	}

	results, err := p.multicaller.Execute(ctx, calls, blockNum)
	if err != nil {
		return nil, fmt.Errorf("multicall metadata: %w", err)
	}

	var vaults []confirmedVault
	for i, addr := range vaultAddrs {
		base := i * 5

		// Parse vault details (name, symbol, decimals, skimRecipient) via shared prober
		details, err := p.sharedProber.ParseDetailsResults(
			results[base], results[base+1], results[base+2], results[base+3], addr)
		if err != nil {
			p.logger.Warn("skipping vault with failed metadata",
				"address", addr.Hex(),
				"error", err)
			continue
		}

		v := confirmedVault{
			Address:    addr,
			Name:       details.Name,
			Symbol:     details.Symbol,
			Decimals:   details.Decimals,
			Version:    details.Version,
			Asset:      vaultAssets[addr],
			FirstBlock: firstBlocks[addr],
		}

		// Parse asset symbol (ERC20 call on asset token)
		assetSymbolResult := results[base+4]
		if assetSymbolResult.Success && len(assetSymbolResult.ReturnData) > 0 {
			if unpacked, err := p.erc20ABI.Unpack("symbol", assetSymbolResult.ReturnData); err == nil && len(unpacked) > 0 {
				if sym, ok := unpacked[0].(string); ok {
					v.AssetSymbol = sym
				}
			}
		}

		if v.AssetSymbol == "" {
			p.logger.Warn("skipping vault with empty asset symbol",
				"address", addr.Hex(),
				"name", v.Name,
				"asset", v.Asset.Hex())
			continue
		}

		p.logger.Info("confirmed vault",
			"address", addr.Hex(),
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

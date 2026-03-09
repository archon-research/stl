package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/morpho_indexer"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
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
	multicaller   outbound.Multicaller
	metaMorphoABI *abi.ABI
	erc20ABI      *abi.ABI
	logger        *slog.Logger
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
	morphoData, err := p.metaMorphoABI.Pack("MORPHO")
	if err != nil {
		return nil, fmt.Errorf("packing MORPHO call: %w", err)
	}
	assetData, err := p.metaMorphoABI.Pack("asset")
	if err != nil {
		return nil, fmt.Errorf("packing asset call: %w", err)
	}

	// Build multicall: 2 calls per candidate (MORPHO, asset)
	calls := make([]outbound.Call, 0, len(batch)*2)
	for _, addr := range batch {
		calls = append(calls,
			outbound.Call{Target: addr, AllowFailure: true, CallData: morphoData},
			outbound.Call{Target: addr, AllowFailure: true, CallData: assetData},
		)
	}

	results, err := p.multicaller.Execute(ctx, calls, blockNum)
	if err != nil {
		return nil, fmt.Errorf("multicall probe: %w", err)
	}

	// Identify confirmed vaults from results
	var vaultAddrs []common.Address
	vaultAssets := make(map[common.Address]common.Address)

	for i, addr := range batch {
		morphoResult := results[i*2]
		assetResult := results[i*2+1]

		if !morphoResult.Success || len(morphoResult.ReturnData) == 0 {
			continue
		}
		morphoUnpacked, err := p.metaMorphoABI.Unpack("MORPHO", morphoResult.ReturnData)
		if err != nil || len(morphoUnpacked) == 0 {
			continue
		}
		morphoAddr, ok := morphoUnpacked[0].(common.Address)
		if !ok || morphoAddr != morpho_indexer.MorphoBlueAddress {
			continue
		}

		if !assetResult.Success || len(assetResult.ReturnData) == 0 {
			continue
		}
		assetUnpacked, err := p.metaMorphoABI.Unpack("asset", assetResult.ReturnData)
		if err != nil || len(assetUnpacked) == 0 {
			continue
		}
		asset, ok := assetUnpacked[0].(common.Address)
		if !ok || asset == (common.Address{}) {
			continue
		}

		vaultAddrs = append(vaultAddrs, addr)
		vaultAssets[addr] = asset
	}

	if len(vaultAddrs) == 0 {
		return nil, nil
	}

	// Fetch metadata for confirmed vaults
	return p.fetchVaultMetadata(ctx, vaultAddrs, vaultAssets, firstBlocks, blockNum)
}

// fetchVaultMetadata fetches name, symbol, decimals, and version for confirmed vaults.
func (p *vaultProber) fetchVaultMetadata(
	ctx context.Context,
	vaultAddrs []common.Address,
	vaultAssets map[common.Address]common.Address,
	firstBlocks map[common.Address]int64,
	blockNum *big.Int,
) ([]confirmedVault, error) {
	nameData, err := p.metaMorphoABI.Pack("name")
	if err != nil {
		return nil, fmt.Errorf("packing name: %w", err)
	}
	symbolData, err := p.metaMorphoABI.Pack("symbol")
	if err != nil {
		return nil, fmt.Errorf("packing symbol: %w", err)
	}
	skimData, err := p.metaMorphoABI.Pack("skimRecipient")
	if err != nil {
		return nil, fmt.Errorf("packing skimRecipient: %w", err)
	}
	decimalsData, err := p.erc20ABI.Pack("decimals")
	if err != nil {
		return nil, fmt.Errorf("packing decimals: %w", err)
	}
	assetSymbolData, err := p.erc20ABI.Pack("symbol")
	if err != nil {
		return nil, fmt.Errorf("packing asset symbol: %w", err)
	}

	// 5 calls per vault: name, symbol, skimRecipient (version detect), asset decimals, asset symbol
	calls := make([]outbound.Call, 0, len(vaultAddrs)*5)
	for _, addr := range vaultAddrs {
		calls = append(calls,
			outbound.Call{Target: addr, AllowFailure: true, CallData: nameData},
			outbound.Call{Target: addr, AllowFailure: true, CallData: symbolData},
			outbound.Call{Target: addr, AllowFailure: true, CallData: skimData},
			outbound.Call{Target: vaultAssets[addr], AllowFailure: true, CallData: decimalsData},
			outbound.Call{Target: vaultAssets[addr], AllowFailure: true, CallData: assetSymbolData},
		)
	}

	results, err := p.multicaller.Execute(ctx, calls, blockNum)
	if err != nil {
		return nil, fmt.Errorf("multicall metadata: %w", err)
	}

	var vaults []confirmedVault
	for i, addr := range vaultAddrs {
		nameResult := results[i*5]
		symbolResult := results[i*5+1]
		skimResult := results[i*5+2]
		decimalsResult := results[i*5+3]
		assetSymbolResult := results[i*5+4]

		v := confirmedVault{
			Address:    addr,
			Asset:      vaultAssets[addr],
			FirstBlock: firstBlocks[addr],
			Version:    entity.MorphoVaultV1,
		}

		// Version detection: skimRecipient() only exists on V2
		if skimResult.Success && len(skimResult.ReturnData) > 0 {
			if _, err := p.metaMorphoABI.Unpack("skimRecipient", skimResult.ReturnData); err == nil {
				v.Version = entity.MorphoVaultV2
			}
		}

		if nameResult.Success && len(nameResult.ReturnData) > 0 {
			if unpacked, err := p.metaMorphoABI.Unpack("name", nameResult.ReturnData); err == nil && len(unpacked) > 0 {
				if name, ok := unpacked[0].(string); ok {
					v.Name = name
				}
			}
		}

		if symbolResult.Success && len(symbolResult.ReturnData) > 0 {
			if unpacked, err := p.metaMorphoABI.Unpack("symbol", symbolResult.ReturnData); err == nil && len(unpacked) > 0 {
				if sym, ok := unpacked[0].(string); ok {
					v.Symbol = sym
				}
			}
		}

		if decimalsResult.Success && len(decimalsResult.ReturnData) > 0 {
			if unpacked, err := p.erc20ABI.Unpack("decimals", decimalsResult.ReturnData); err == nil && len(unpacked) > 0 {
				if dec, ok := unpacked[0].(uint8); ok {
					v.Decimals = dec
				}
			}
		}

		if assetSymbolResult.Success && len(assetSymbolResult.ReturnData) > 0 {
			if unpacked, err := p.erc20ABI.Unpack("symbol", assetSymbolResult.ReturnData); err == nil && len(unpacked) > 0 {
				if sym, ok := unpacked[0].(string); ok {
					v.AssetSymbol = sym
				}
			}
		}

		if v.Name == "" || v.Symbol == "" {
			p.logger.Warn("skipping vault with missing metadata",
				"address", addr.Hex(),
				"name", v.Name,
				"symbol", v.Symbol)
			continue
		}

		p.logger.Info("confirmed vault",
			"address", addr.Hex(),
			"name", v.Name,
			"symbol", v.Symbol,
			"asset", v.Asset.Hex(),
			"version", v.Version,
			"firstBlock", v.FirstBlock)

		vaults = append(vaults, v)
	}

	return vaults, nil
}

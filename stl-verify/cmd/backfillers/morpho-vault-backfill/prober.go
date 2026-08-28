package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"slices"
	"sync"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/erc20meta"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/rpcerr"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/morpho_indexer"
)

// errProbeGasExhausted marks a PROBE call the node answered out of gas (see
// rpcerr.IsGasExhausted). Only the probe phase tags it: an already-confirmed
// vault must never be discarded over its metadata read.
var errProbeGasExhausted = errors.New("probe call exhausted the eth_call gas budget")

// confirmedVault holds metadata for a confirmed Morpho-family vault
// (MetaMorpho V1/V1.1 or Morpho VaultV2).
type confirmedVault struct {
	Address     common.Address
	Name        string
	Symbol      string
	Asset       common.Address
	AssetSymbol string
	// AssetDecimals is the underlying asset ERC20's decimals(). It can differ
	// from the vault's share decimals (especially for VaultV2), so we fetch it
	// from the asset contract rather than reusing the vault's value.
	AssetDecimals uint8
	Version       entity.MorphoVaultVersion
	FirstBlock    int64
}

// vaultProber probes candidate addresses on-chain to determine if they are
// Morpho-family vaults.
type vaultProber struct {
	multicaller  outbound.Multicaller
	sharedProber *morpho_indexer.VaultProber
	erc20ABI     *abi.ABI
	logger       *slog.Logger
	telemetry    *morpho_indexer.Telemetry // optional, nil-safe
	unprobeable  unprobeableSet
}

// unprobeableSet remembers the candidates isolated as unprobeable, so the
// sub-ranges that re-encounter them skip the bisection instead of paying for it
// again. Process lifetime only: a restart re-derives every verdict from chain.
type unprobeableSet struct {
	mu   sync.Mutex
	seen map[common.Address]struct{}
}

func (s *unprobeableSet) add(addr common.Address) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seen == nil {
		s.seen = make(map[common.Address]struct{})
	}
	s.seen[addr] = struct{}{}
}

func (s *unprobeableSet) has(addr common.Address) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.seen[addr]
	return ok
}

// probeAllCandidates probes every candidate an earlier sub-range has not already
// classified unprobeable, batching the shared probe
// (MORPHO/asset/curator/liquidityAdapter) through multicall. Returns confirmed
// vaults with their metadata.
func (p *vaultProber) probeAllCandidates(
	ctx context.Context,
	candidates map[common.Address]int64,
	probeBlock int64,
	batchSize int,
) ([]confirmedVault, error) {
	addrs, classified := p.probeOrder(candidates)
	for _, addr := range classified {
		p.discardUnprobeable(ctx, addr, errProbeGasExhausted)
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

// probeOrder splits the candidate set into the sorted slice the batches are cut
// from and the addresses an earlier sub-range already classified unprobeable,
// which the caller accounts for.
//
// Sorted because map order reshuffles batch composition between attempts, which
// leaves a batch failure — and the bisection off it — unable to reproduce.
func (p *vaultProber) probeOrder(candidates map[common.Address]int64) (probeable, classified []common.Address) {
	addrs := make([]common.Address, 0, len(candidates))
	for addr := range candidates {
		addrs = append(addrs, addr)
	}
	slices.SortFunc(addrs, common.Address.Cmp)

	probeable = make([]common.Address, 0, len(addrs))
	for _, addr := range addrs {
		if p.unprobeable.has(addr) {
			classified = append(classified, addr)
			continue
		}
		probeable = append(probeable, addr)
	}
	return probeable, classified
}

// discardUnprobeable is the deliberate discard of a candidate observed to answer
// none of its probe selectors, each one isolated: not a Morpho-family vault, by
// structural determination rather than a failure a retry could clear.
//
// Never silent — one WARN naming the address every time it is discarded, plus
// the counter behind it.
func (p *vaultProber) discardUnprobeable(ctx context.Context, addr common.Address, cause error) {
	p.unprobeable.add(addr)
	p.logger.Warn("discarding unprobeable candidate",
		"address", addr.Hex(),
		"reason", string(morpho_indexer.UnprobeableGasExhausted),
		"cause", cause)
	p.telemetry.RecordUnprobeableCandidate(ctx, morpho_indexer.UnprobeableGasExhausted)
}

// probeBatchWithRetry tries probeBatch, and on failure retries with progressively
// smaller sub-batches down to single-address probes. Narrowing is what isolates
// the one address behind an "out of gas" multicall failure.
//
// At the floor the batch IS the candidate, and the two dispositions part there. A
// gas-exhausted probe hands off to probeCandidateSelectorwise, which is what
// turns the batch's verdict into the candidate's own; every other error
// propagates and fails the run. Those others are transport failures (429 /
// timeout / 5xx, already retried to exhaustion by the rpchttp client) or a
// structural error out of the multicall — ErrNotVault is consumed as a per-result
// Success:false inside collectProbeConfirmed and never surfaces as an error.
// Swallowing them would permanently black-hole a real vault while the backfill
// run exits 0; failing loudly is safe because a backfiller re-run is idempotent.
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

	if len(batch) == 1 {
		if errors.Is(err, errProbeGasExhausted) {
			return p.probeCandidateSelectorwise(ctx, batch[0], firstBlocks, blockNum, err)
		}
		return nil, fmt.Errorf("probing candidate %s: %w", batch[0].Hex(), err)
	}

	// Batch failed — retry with halved batch size, down to individual probes.
	p.logger.Warn("batch probe failed, retrying with smaller batches",
		"batchSize", len(batch),
		"error", err)

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

// probeCandidateSelectorwise re-probes one candidate a selector at a time, after
// its four-call probe exhausted the gas budget as a whole.
//
// A batched exhaustion cannot tell "answers nothing" from "answers three of four
// selectors and traps on the fourth", and discarding the second would hide a real
// vault. Isolated, a gas-exhausting selector is indistinguishable from a revert —
// which is what the probe's AllowFailure already models — so ParseProbeResults
// decides as normal, and only an address whose EVERY selector exhausts is
// discarded. That is the sole state in which "answers no discriminator" is
// observed rather than inferred.
//
// The fan-out costs one round trip per selector, paid only after a single-address
// batch has already failed, so the happy path is untouched.
func (p *vaultProber) probeCandidateSelectorwise(
	ctx context.Context,
	addr common.Address,
	firstBlocks map[common.Address]int64,
	blockNum *big.Int,
	cause error,
) ([]confirmedVault, error) {
	calls := p.sharedProber.ProbeCalls(addr)
	results := make([]outbound.Result, 0, len(calls))
	exhausted := 0
	for _, call := range calls {
		result, err := p.executeIsolatedProbeCall(ctx, call, blockNum)
		switch {
		case err == nil:
			results = append(results, result)
		case errors.Is(err, errProbeGasExhausted):
			results = append(results, outbound.Result{})
			exhausted++
		default:
			return nil, fmt.Errorf("probing candidate %s: %w", addr.Hex(), err)
		}
	}

	if exhausted == len(calls) {
		p.discardUnprobeable(ctx, addr, cause)
		return nil, nil
	}

	p.logger.Warn("candidate probed one selector at a time after gas exhaustion",
		"address", addr.Hex(),
		"exhaustedSelectors", exhausted,
		"totalSelectors", len(calls))
	return p.confirmProbedBatch(ctx, []common.Address{addr}, results, firstBlocks, blockNum)
}

// executeIsolatedProbeCall runs one probe selector as its own multicall, so the
// gas it burns is charged to nothing else.
func (p *vaultProber) executeIsolatedProbeCall(ctx context.Context, call outbound.Call, blockNum *big.Int) (outbound.Result, error) {
	results, err := p.multicaller.Execute(ctx, []outbound.Call{call}, blockNum)
	if err != nil {
		if rpcerr.IsGasExhausted(err) {
			return outbound.Result{}, fmt.Errorf("isolated probe call: %w: %w", err, errProbeGasExhausted)
		}
		return outbound.Result{}, fmt.Errorf("isolated probe call: %w", err)
	}
	if len(results) != 1 {
		return outbound.Result{}, fmt.Errorf("expected 1 result for an isolated probe call, got %d", len(results))
	}
	return results[0], nil
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
// the shared probe in a single multicall. Confirmed vaults get their metadata
// fetched in a follow-up multicall.
func (p *vaultProber) probeBatch(
	ctx context.Context,
	batch []common.Address,
	firstBlocks map[common.Address]int64,
	blockNum *big.Int,
) ([]confirmedVault, error) {
	callsPerProbe := p.sharedProber.NumProbeCalls()
	calls := make([]outbound.Call, 0, len(batch)*callsPerProbe)
	for _, addr := range batch {
		calls = append(calls, p.sharedProber.ProbeCalls(addr)...)
	}

	results, err := p.multicaller.Execute(ctx, calls, blockNum)
	if err != nil {
		if rpcerr.IsGasExhausted(err) {
			return nil, fmt.Errorf("multicall probe: %w: %w", err, errProbeGasExhausted)
		}
		return nil, fmt.Errorf("multicall probe: %w", err)
	}

	return p.confirmProbedBatch(ctx, batch, results, firstBlocks, blockNum)
}

// confirmProbedBatch turns one batch's probe results into confirmed vaults with
// their metadata, whatever shape of multicall produced those results.
func (p *vaultProber) confirmProbedBatch(
	ctx context.Context,
	batch []common.Address,
	results []outbound.Result,
	firstBlocks map[common.Address]int64,
	blockNum *big.Int,
) ([]confirmedVault, error) {
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
	callsPerProbe := p.sharedProber.NumProbeCalls()
	expected := len(batch) * callsPerProbe
	if len(results) != expected {
		return nil, fmt.Errorf("expected %d probe results for batch of %d, got %d", expected, len(batch), len(results))
	}
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

// numAssetExtensionCalls is the count of extra multicall sub-calls we append
// after each vault's details window (asset symbol() then asset decimals()).
// Keep in sync with assetSymbolOffset / assetDecimalsOffset.
const numAssetExtensionCalls = 2

// assetSymbolOffset and assetDecimalsOffset index the two asset-metadata
// trailing calls within a per-vault window of size callsPerMetadata. They are
// negative relative to the NEXT vault's base, i.e.
// results[base + (callsPerMetadata + offset)].
const (
	assetSymbolOffset   = -2
	assetDecimalsOffset = -1
)

// fetchVaultMetadata fetches name, symbol, decimals, version, and asset
// symbol+decimals for confirmed vaults. The version may be upgraded from
// V1 → V1.1 here if skimRecipient succeeds.
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
	assetDecimalsData, err := p.erc20ABI.Pack("decimals")
	if err != nil {
		return nil, fmt.Errorf("packing asset decimals: %w", err)
	}

	// Per-vault window: the prober's details calls plus two trailing asset
	// calls we tack on (symbol then decimals). The append site below and the
	// per-result reads downstream (assetSymbolOffset, assetDecimalsOffset)
	// must move together — change one and the window arithmetic breaks.
	callsPerMetadata := p.sharedProber.NumDetailsCalls() + numAssetExtensionCalls
	calls := make([]outbound.Call, 0, len(probeConfirmed)*callsPerMetadata)
	for _, pc := range probeConfirmed {
		calls = append(calls, p.sharedProber.DetailsCalls(pc.address)...)
		calls = append(calls, outbound.Call{Target: pc.asset, AllowFailure: true, CallData: assetSymbolData})
		calls = append(calls, outbound.Call{Target: pc.asset, AllowFailure: true, CallData: assetDecimalsData})
	}

	results, err := p.multicaller.Execute(ctx, calls, blockNum)
	if err != nil {
		return nil, fmt.Errorf("multicall metadata: %w", err)
	}

	expected := len(probeConfirmed) * callsPerMetadata
	if len(results) != expected {
		return nil, fmt.Errorf("expected %d metadata results for %d confirmed vaults, got %d", expected, len(probeConfirmed), len(results))
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
			Version:    details.Version,
			Asset:      pc.asset,
			FirstBlock: firstBlocks[pc.address],
		}

		// Asset symbol is best-effort and display-only. Decode across the modern
		// (string) and legacy (bytes32, e.g. MKR) ABIs via
		// erc20meta.DecodeStringOrBytes32, and persist the vault even when it
		// stays empty (reverted / undecodable symbol()). This mirrors the live
		// indexer's getTokenMetadata (internal/services/morpho_indexer/
		// blockchain_service.go): an empty symbol is left for the per-block
		// reconciliation sweep to fill in later. Dropping the vault over a
		// missing symbol would silently thin the backfill for no
		// data-integrity reason — unlike decimals below, symbol drives no
		// downstream math.
		assetSymbolResult := results[base+(callsPerMetadata+assetSymbolOffset)]
		if assetSymbolResult.Success && len(assetSymbolResult.ReturnData) > 0 {
			if sym, err := erc20meta.DecodeStringOrBytes32(p.erc20ABI, "symbol", assetSymbolResult.ReturnData); err == nil {
				v.AssetSymbol = sym
			}
		}

		// Asset decimals failure → skip the vault, unlike the best-effort asset
		// symbol above. Rationale: token_repository.GetOrCreateToken UPSERTs on
		// (chain_id, address) and on conflict preserves the existing row's
		// decimals (only created_at_block is reconciled via LEAST). So if we
		// persist AssetDecimals=0 here, the live indexer's later correct
		// value would be silently rejected. Skipping leaves the row absent;
		// the next backfill cycle or the live indexer will pick it up with
		// the correct decimals.
		assetDecimalsResult := results[base+(callsPerMetadata+assetDecimalsOffset)]
		dec, err := unpackAssetDecimals(p.erc20ABI, assetDecimalsResult)
		if err != nil {
			p.logger.Warn("skipping vault: asset decimals fetch failed",
				"address", pc.address.Hex(),
				"name", v.Name,
				"asset", v.Asset.Hex(),
				"error", err)
			continue
		}
		v.AssetDecimals = dec

		p.logger.Info("confirmed vault",
			"address", pc.address.Hex(),
			"name", v.Name,
			"symbol", v.Symbol,
			"asset", v.Asset.Hex(),
			"assetSymbol", v.AssetSymbol,
			"assetDecimals", v.AssetDecimals,
			"version", v.Version,
			"firstBlock", v.FirstBlock)

		vaults = append(vaults, v)
	}

	return vaults, nil
}

// unpackAssetDecimals decodes an ERC20 decimals() multicall return and folds
// the four failure modes (sub-call reverted, empty return data, unpack error,
// type assertion failure) into a single error. The caller's only sensible
// disposition is "skip this vault" — see fetchVaultMetadata's call site for
// the rationale (token table UPSERT preserves the existing decimals on
// conflict, so a wrong value here blocks the live indexer's correction).
//
// ERC20 decimals() is specified as uint8; we keep the strict assertion. A
// type mismatch surfaces as a skip-with-Warn, which is the same observable
// outcome as the live indexer's blockchain_service.getTokenMetadata where a
// non-uint8 return wraps into a hard error.
func unpackAssetDecimals(erc20ABI *abi.ABI, result outbound.Result) (uint8, error) {
	if !result.Success {
		return 0, errors.New("decimals call reverted")
	}
	if len(result.ReturnData) == 0 {
		return 0, errors.New("decimals call returned no data")
	}
	unpacked, err := erc20ABI.Unpack("decimals", result.ReturnData)
	if err != nil {
		return 0, fmt.Errorf("unpacking decimals: %w", err)
	}
	if len(unpacked) == 0 {
		return 0, errors.New("decimals unpack returned no values")
	}
	dec, ok := unpacked[0].(uint8)
	if !ok {
		return 0, fmt.Errorf("decimals type assertion: got %T", unpacked[0])
	}
	return dec, nil
}

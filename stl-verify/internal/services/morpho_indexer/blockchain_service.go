package morpho_indexer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"slices"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"go.opentelemetry.io/otel/attribute"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/erc20meta"
	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// MorphoBlueAddress is the immutable Morpho Blue singleton contract.
var MorphoBlueAddress = common.HexToAddress("0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb")

// MarketState holds on-chain market state from the market() function.
type MarketState struct {
	TotalSupplyAssets *big.Int
	TotalSupplyShares *big.Int
	TotalBorrowAssets *big.Int
	TotalBorrowShares *big.Int
	LastUpdate        *big.Int
	Fee               *big.Int
}

// PositionState holds on-chain position state from the position() function.
type PositionState struct {
	SupplyShares *big.Int
	BorrowShares *big.Int
	Collateral   *big.Int
}

// MarketParamsState holds on-chain market params from idToMarketParams().
type MarketParamsState struct {
	LoanToken       common.Address
	CollateralToken common.Address
	Oracle          common.Address
	Irm             common.Address
	LLTV            *big.Int
}

// VaultState holds on-chain vault state.
type VaultState struct {
	TotalAssets *big.Int
	TotalSupply *big.Int
}

// VaultMetadata holds vault metadata from on-chain reads.
type VaultMetadata struct {
	Name     string
	Symbol   string
	Asset    common.Address
	Decimals uint8
	Version  entity.MorphoVaultVersion
}

// TokenMetadata holds token metadata from on-chain reads.
// Symbol may be empty when symbol() reverted or could not be decoded at the
// read block; an empty symbol on a non-error return means "not resolvable at
// this block — the per-block sweep retries it later". Decimals is always
// authoritative (a decimals() revert is a hard error).
type TokenMetadata struct {
	Symbol   string
	Decimals int
}

// blockchainService handles all on-chain reads for Morpho protocol.
type blockchainService struct {
	multicallClient outbound.Multicaller
	morphoBlueABI   *abi.ABI
	metaMorphoABI   *abi.ABI
	adapterABI      *abi.ABI
	vaultV2ABI      *abi.ABI
	erc20ABI        *abi.ABI
	vaultProber     *VaultProber
	adapterProber   *AdapterProber
	metadataCache   map[common.Address]TokenMetadata
	telemetry       *Telemetry
	logger          *slog.Logger
}

func newBlockchainService(
	multicallClient outbound.Multicaller,
	erc20ABI *abi.ABI,
	logger *slog.Logger,
	telemetry *Telemetry,
) (*blockchainService, error) {
	morphoABI, err := abis.GetMorphoBlueReadABI()
	if err != nil {
		return nil, fmt.Errorf("failed to load Morpho Blue read ABI: %w", err)
	}

	metaMorphoABI, err := abis.GetMetaMorphoReadABI()
	if err != nil {
		return nil, fmt.Errorf("failed to load MetaMorpho read ABI: %w", err)
	}

	adapterABI, err := abis.GetVaultV2AdapterReadABI()
	if err != nil {
		return nil, fmt.Errorf("loading VaultV2 adapter read ABI: %w", err)
	}

	vaultV2ABI, err := abis.GetVaultV2ReadABI()
	if err != nil {
		return nil, fmt.Errorf("loading VaultV2 read ABI: %w", err)
	}

	vaultProber, err := NewVaultProber()
	if err != nil {
		return nil, fmt.Errorf("creating vault prober: %w", err)
	}

	adapterProber, err := NewAdapterProber()
	if err != nil {
		return nil, fmt.Errorf("creating adapter prober: %w", err)
	}

	return &blockchainService{
		multicallClient: multicallClient,
		morphoBlueABI:   morphoABI,
		metaMorphoABI:   metaMorphoABI,
		adapterABI:      adapterABI,
		vaultV2ABI:      vaultV2ABI,
		erc20ABI:        erc20ABI,
		vaultProber:     vaultProber,
		adapterProber:   adapterProber,
		metadataCache:   make(map[common.Address]TokenMetadata),
		telemetry:       telemetry,
		logger:          logger.With("component", "morpho-blockchain-service"),
	}, nil
}

// unpackMarketState validates and unpacks a market() multicall result.
func (s *blockchainService) unpackMarketState(result outbound.Result) (*MarketState, error) {
	if !result.Success || len(result.ReturnData) == 0 {
		return nil, fmt.Errorf("market() call failed")
	}
	unpacked, err := s.morphoBlueABI.Unpack("market", result.ReturnData)
	if err != nil {
		return nil, fmt.Errorf("unpacking market(): %w", err)
	}
	if len(unpacked) < 6 {
		return nil, fmt.Errorf("unexpected market() return length: %d", len(unpacked))
	}
	return &MarketState{
		TotalSupplyAssets: bigIntFromAny(unpacked[0]),
		TotalSupplyShares: bigIntFromAny(unpacked[1]),
		TotalBorrowAssets: bigIntFromAny(unpacked[2]),
		TotalBorrowShares: bigIntFromAny(unpacked[3]),
		LastUpdate:        bigIntFromAny(unpacked[4]),
		Fee:               bigIntFromAny(unpacked[5]),
	}, nil
}

// unpackPositionState validates and unpacks a position() multicall result.
func (s *blockchainService) unpackPositionState(result outbound.Result, label string) (*PositionState, error) {
	if !result.Success || len(result.ReturnData) == 0 {
		return nil, fmt.Errorf("position(%s) call failed", label)
	}
	unpacked, err := s.morphoBlueABI.Unpack("position", result.ReturnData)
	if err != nil {
		return nil, fmt.Errorf("unpacking position(%s): %w", label, err)
	}
	if len(unpacked) < 3 {
		return nil, fmt.Errorf("unexpected position(%s) return length: %d", label, len(unpacked))
	}
	return &PositionState{
		SupplyShares: bigIntFromAny(unpacked[0]),
		BorrowShares: bigIntFromAny(unpacked[1]),
		Collateral:   bigIntFromAny(unpacked[2]),
	}, nil
}

// unpackVaultState validates and unpacks totalAssets() + totalSupply() multicall results.
func (s *blockchainService) unpackVaultState(totalAssetsResult, totalSupplyResult outbound.Result, vaultAddress common.Address) (*VaultState, error) {
	if !totalAssetsResult.Success || len(totalAssetsResult.ReturnData) == 0 {
		return nil, fmt.Errorf("totalAssets() call failed for vault %s", vaultAddress.Hex())
	}
	totalAssetsUnpacked, err := s.metaMorphoABI.Unpack("totalAssets", totalAssetsResult.ReturnData)
	if err != nil {
		return nil, fmt.Errorf("unpacking totalAssets(): %w", err)
	}
	if len(totalAssetsUnpacked) == 0 {
		return nil, fmt.Errorf("totalAssets() returned no values for vault %s", vaultAddress.Hex())
	}

	if !totalSupplyResult.Success || len(totalSupplyResult.ReturnData) == 0 {
		return nil, fmt.Errorf("totalSupply() call failed for vault %s", vaultAddress.Hex())
	}
	totalSupplyUnpacked, err := s.metaMorphoABI.Unpack("totalSupply", totalSupplyResult.ReturnData)
	if err != nil {
		return nil, fmt.Errorf("unpacking totalSupply(): %w", err)
	}
	if len(totalSupplyUnpacked) == 0 {
		return nil, fmt.Errorf("totalSupply() returned no values for vault %s", vaultAddress.Hex())
	}

	return &VaultState{
		TotalAssets: bigIntFromAny(totalAssetsUnpacked[0]),
		TotalSupply: bigIntFromAny(totalSupplyUnpacked[0]),
	}, nil
}

// unpackBalance validates and unpacks a balanceOf() multicall result.
func (s *blockchainService) unpackBalance(result outbound.Result, label string, vaultAddress common.Address) (*big.Int, error) {
	if !result.Success || len(result.ReturnData) == 0 {
		return nil, fmt.Errorf("balanceOf(%s) call failed for vault %s", label, vaultAddress.Hex())
	}
	unpacked, err := s.metaMorphoABI.Unpack("balanceOf", result.ReturnData)
	if err != nil {
		return nil, fmt.Errorf("unpacking balanceOf(%s): %w", label, err)
	}
	if len(unpacked) == 0 {
		return nil, fmt.Errorf("balanceOf(%s) returned no values for vault %s", label, vaultAddress.Hex())
	}
	return bigIntFromAny(unpacked[0]), nil
}

// getMarketState fetches the market state from Morpho Blue, pinned to
// blockHash: market() is versioned per-block state (totalSupplyAssets etc.
// change every accrual), so after a reorg an archive node answering
// eth_call-by-number would silently return the new canonical fork's state
// instead of the state for the (blockNumber, version) this event belongs to.
func (s *blockchainService) getMarketState(ctx context.Context, marketID [32]byte, blockHash common.Hash) (retState *MarketState, retErr error) {
	ctx, span := s.telemetry.StartSpan(ctx, "morpho.rpc.getMarketState",
		attribute.String("market.id", fmt.Sprintf("%x", marketID[:8])))
	defer span.End()
	start := time.Now()
	defer func() {
		s.telemetry.RecordRPCCall(ctx, "getMarketState", time.Since(start), retErr)
		if retErr != nil {
			telemetry.SetSpanError(span, retErr, "getMarketState failed")
		}
	}()

	callData, err := s.morphoBlueABI.Pack("market", marketID)
	if err != nil {
		return nil, fmt.Errorf("packing market call: %w", err)
	}

	results, err := s.multicallClient.ExecuteAtHash(ctx, []outbound.Call{{
		Target:       MorphoBlueAddress,
		AllowFailure: false,
		CallData:     callData,
	}}, blockHash)
	if err != nil {
		return nil, fmt.Errorf("multicall market(): %w", err)
	}

	if len(results) == 0 {
		return nil, fmt.Errorf("expected 1 result, got 0")
	}

	return s.unpackMarketState(results[0])
}

// getMarketParams fetches market parameters from Morpho Blue. Number-pinned
// intentionally: a market's params (loanToken, collateralToken, oracle, irm,
// LLTV) are immutable once CreateMarket runs, so this is structurally static
// identity data, not versioned state — the reorg-correctness concern behind
// ExecuteAtHash (VEC-471) doesn't apply here.
func (s *blockchainService) getMarketParams(ctx context.Context, marketID [32]byte, blockNumber int64) (retState *MarketParamsState, retErr error) {
	ctx, span := s.telemetry.StartSpan(ctx, "morpho.rpc.getMarketParams",
		attribute.String("market.id", fmt.Sprintf("%x", marketID[:8])))
	defer span.End()
	start := time.Now()
	defer func() {
		s.telemetry.RecordRPCCall(ctx, "getMarketParams", time.Since(start), retErr)
		if retErr != nil {
			telemetry.SetSpanError(span, retErr, "getMarketParams failed")
		}
	}()

	callData, err := s.morphoBlueABI.Pack("idToMarketParams", marketID)
	if err != nil {
		return nil, fmt.Errorf("packing idToMarketParams call: %w", err)
	}

	results, err := s.multicallClient.Execute(ctx, []outbound.Call{{
		Target:       MorphoBlueAddress,
		AllowFailure: false,
		CallData:     callData,
	}}, big.NewInt(blockNumber))
	if err != nil {
		return nil, fmt.Errorf("multicall idToMarketParams(): %w", err)
	}

	if len(results) == 0 || !results[0].Success || len(results[0].ReturnData) == 0 {
		return nil, fmt.Errorf("idToMarketParams() call failed")
	}

	unpacked, err := s.morphoBlueABI.Unpack("idToMarketParams", results[0].ReturnData)
	if err != nil {
		return nil, fmt.Errorf("unpacking idToMarketParams() result: %w", err)
	}

	if len(unpacked) < 5 {
		return nil, fmt.Errorf("unexpected idToMarketParams() return length: %d", len(unpacked))
	}

	loanToken, ok := unpacked[0].(common.Address)
	if !ok {
		return nil, fmt.Errorf("unexpected type for loanToken: %T", unpacked[0])
	}
	collateralToken, ok := unpacked[1].(common.Address)
	if !ok {
		return nil, fmt.Errorf("unexpected type for collateralToken: %T", unpacked[1])
	}
	oracle, ok := unpacked[2].(common.Address)
	if !ok {
		return nil, fmt.Errorf("unexpected type for oracle: %T", unpacked[2])
	}
	irm, ok := unpacked[3].(common.Address)
	if !ok {
		return nil, fmt.Errorf("unexpected type for irm: %T", unpacked[3])
	}

	return &MarketParamsState{
		LoanToken:       loanToken,
		CollateralToken: collateralToken,
		Oracle:          oracle,
		Irm:             irm,
		LLTV:            bigIntFromAny(unpacked[4]),
	}, nil
}

// getMarketAndPositionState fetches both market and position state in a
// single Multicall3 batch, pinned to blockHash (see getMarketState for why).
func (s *blockchainService) getMarketAndPositionState(ctx context.Context, marketID [32]byte, user common.Address, blockHash common.Hash) (retMS *MarketState, retPS *PositionState, retErr error) {
	ctx, span := s.telemetry.StartSpan(ctx, "morpho.rpc.getMarketAndPositionState",
		attribute.String("market.id", fmt.Sprintf("%x", marketID[:8])))
	defer span.End()
	start := time.Now()
	defer func() {
		s.telemetry.RecordRPCCall(ctx, "getMarketAndPositionState", time.Since(start), retErr)
		if retErr != nil {
			telemetry.SetSpanError(span, retErr, "getMarketAndPositionState failed")
		}
	}()

	marketCallData, err := s.morphoBlueABI.Pack("market", marketID)
	if err != nil {
		return nil, nil, fmt.Errorf("packing market call: %w", err)
	}

	positionCallData, err := s.morphoBlueABI.Pack("position", marketID, user)
	if err != nil {
		return nil, nil, fmt.Errorf("packing position call: %w", err)
	}

	results, err := s.multicallClient.ExecuteAtHash(ctx, []outbound.Call{
		{Target: MorphoBlueAddress, AllowFailure: false, CallData: marketCallData},
		{Target: MorphoBlueAddress, AllowFailure: false, CallData: positionCallData},
	}, blockHash)
	if err != nil {
		return nil, nil, fmt.Errorf("multicall market+position: %w", err)
	}

	if len(results) < 2 {
		return nil, nil, fmt.Errorf("expected 2 results, got %d", len(results))
	}

	ms, err := s.unpackMarketState(results[0])
	if err != nil {
		return nil, nil, err
	}

	ps, err := s.unpackPositionState(results[1], "")
	if err != nil {
		return nil, nil, err
	}

	return ms, ps, nil
}

// getMarketAndTwoPositionStates fetches market state and two user positions in
// a single Multicall3 batch, pinned to blockHash (see getMarketState for why).
// Used by liquidation events where we need the borrower and liquidator positions.
func (s *blockchainService) getMarketAndTwoPositionStates(ctx context.Context, marketID [32]byte, userA, userB common.Address, blockHash common.Hash) (retMS *MarketState, retPSA *PositionState, retPSB *PositionState, retErr error) {
	ctx, span := s.telemetry.StartSpan(ctx, "morpho.rpc.getMarketAndTwoPositionStates",
		attribute.String("market.id", fmt.Sprintf("%x", marketID[:8])))
	defer span.End()
	start := time.Now()
	defer func() {
		s.telemetry.RecordRPCCall(ctx, "getMarketAndTwoPositionStates", time.Since(start), retErr)
		if retErr != nil {
			telemetry.SetSpanError(span, retErr, "getMarketAndTwoPositionStates failed")
		}
	}()

	marketCallData, err := s.morphoBlueABI.Pack("market", marketID)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("packing market call: %w", err)
	}

	posACallData, err := s.morphoBlueABI.Pack("position", marketID, userA)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("packing position(A) call: %w", err)
	}

	posBCallData, err := s.morphoBlueABI.Pack("position", marketID, userB)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("packing position(B) call: %w", err)
	}

	results, err := s.multicallClient.ExecuteAtHash(ctx, []outbound.Call{
		{Target: MorphoBlueAddress, AllowFailure: false, CallData: marketCallData},
		{Target: MorphoBlueAddress, AllowFailure: false, CallData: posACallData},
		{Target: MorphoBlueAddress, AllowFailure: false, CallData: posBCallData},
	}, blockHash)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("multicall market+position(A)+position(B): %w", err)
	}

	if len(results) < 3 {
		return nil, nil, nil, fmt.Errorf("expected 3 results, got %d", len(results))
	}

	ms, err := s.unpackMarketState(results[0])
	if err != nil {
		return nil, nil, nil, err
	}

	psA, err := s.unpackPositionState(results[1], "A")
	if err != nil {
		return nil, nil, nil, err
	}

	psB, err := s.unpackPositionState(results[2], "B")
	if err != nil {
		return nil, nil, nil, err
	}

	return ms, psA, psB, nil
}

// getVaultState fetches vault total assets and total supply in a single
// Multicall3 batch, pinned to blockHash (see getMarketState for why).
func (s *blockchainService) getVaultState(ctx context.Context, vaultAddress common.Address, blockHash common.Hash) (retState *VaultState, retErr error) {
	ctx, span := s.telemetry.StartSpan(ctx, "morpho.rpc.getVaultState",
		attribute.String("vault.address", vaultAddress.Hex()))
	defer span.End()
	start := time.Now()
	defer func() {
		s.telemetry.RecordRPCCall(ctx, "getVaultState", time.Since(start), retErr)
		if retErr != nil {
			telemetry.SetSpanError(span, retErr, "getVaultState failed")
		}
	}()

	totalAssetsData, err := s.metaMorphoABI.Pack("totalAssets")
	if err != nil {
		return nil, fmt.Errorf("packing totalAssets call: %w", err)
	}

	totalSupplyData, err := s.metaMorphoABI.Pack("totalSupply")
	if err != nil {
		return nil, fmt.Errorf("packing totalSupply call: %w", err)
	}

	results, err := s.multicallClient.ExecuteAtHash(ctx, []outbound.Call{
		{Target: vaultAddress, AllowFailure: false, CallData: totalAssetsData},
		{Target: vaultAddress, AllowFailure: false, CallData: totalSupplyData},
	}, blockHash)
	if err != nil {
		return nil, fmt.Errorf("multicall vault state: %w", err)
	}

	if len(results) < 2 {
		return nil, fmt.Errorf("expected 2 results, got %d", len(results))
	}

	return s.unpackVaultState(results[0], results[1], vaultAddress)
}

// getVaultStateAndBalance fetches vault state and a user's balance in a
// single Multicall3 batch, pinned to blockHash (see getMarketState for why).
func (s *blockchainService) getVaultStateAndBalance(ctx context.Context, vaultAddress common.Address, user common.Address, blockHash common.Hash) (retVS *VaultState, retBalance *big.Int, retErr error) {
	ctx, span := s.telemetry.StartSpan(ctx, "morpho.rpc.getVaultStateAndBalance",
		attribute.String("vault.address", vaultAddress.Hex()))
	defer span.End()
	start := time.Now()
	defer func() {
		s.telemetry.RecordRPCCall(ctx, "getVaultStateAndBalance", time.Since(start), retErr)
		if retErr != nil {
			telemetry.SetSpanError(span, retErr, "getVaultStateAndBalance failed")
		}
	}()

	totalAssetsData, err := s.metaMorphoABI.Pack("totalAssets")
	if err != nil {
		return nil, nil, fmt.Errorf("packing totalAssets call: %w", err)
	}
	totalSupplyData, err := s.metaMorphoABI.Pack("totalSupply")
	if err != nil {
		return nil, nil, fmt.Errorf("packing totalSupply call: %w", err)
	}
	balanceData, err := s.metaMorphoABI.Pack("balanceOf", user)
	if err != nil {
		return nil, nil, fmt.Errorf("packing balanceOf call: %w", err)
	}

	results, err := s.multicallClient.ExecuteAtHash(ctx, []outbound.Call{
		{Target: vaultAddress, AllowFailure: false, CallData: totalAssetsData},
		{Target: vaultAddress, AllowFailure: false, CallData: totalSupplyData},
		{Target: vaultAddress, AllowFailure: false, CallData: balanceData},
	}, blockHash)
	if err != nil {
		return nil, nil, fmt.Errorf("multicall vault state+balance: %w", err)
	}

	if len(results) < 3 {
		return nil, nil, fmt.Errorf("expected 3 results, got %d", len(results))
	}

	vs, err := s.unpackVaultState(results[0], results[1], vaultAddress)
	if err != nil {
		return nil, nil, err
	}

	balance, err := s.unpackBalance(results[2], "", vaultAddress)
	if err != nil {
		return nil, nil, err
	}

	return vs, balance, nil
}

// getVaultStateAndTwoBalances fetches vault state and two user balances in a
// single Multicall3 batch, pinned to blockHash (see getMarketState for why).
// Used by vault Transfer events where we need both sender and receiver balances.
func (s *blockchainService) getVaultStateAndTwoBalances(ctx context.Context, vaultAddress common.Address, userA, userB common.Address, blockHash common.Hash) (retVS *VaultState, retBalA *big.Int, retBalB *big.Int, retErr error) {
	ctx, span := s.telemetry.StartSpan(ctx, "morpho.rpc.getVaultStateAndTwoBalances",
		attribute.String("vault.address", vaultAddress.Hex()))
	defer span.End()
	start := time.Now()
	defer func() {
		s.telemetry.RecordRPCCall(ctx, "getVaultStateAndTwoBalances", time.Since(start), retErr)
		if retErr != nil {
			telemetry.SetSpanError(span, retErr, "getVaultStateAndTwoBalances failed")
		}
	}()

	totalAssetsData, err := s.metaMorphoABI.Pack("totalAssets")
	if err != nil {
		return nil, nil, nil, fmt.Errorf("packing totalAssets call: %w", err)
	}
	totalSupplyData, err := s.metaMorphoABI.Pack("totalSupply")
	if err != nil {
		return nil, nil, nil, fmt.Errorf("packing totalSupply call: %w", err)
	}
	balanceAData, err := s.metaMorphoABI.Pack("balanceOf", userA)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("packing balanceOf(A) call: %w", err)
	}
	balanceBData, err := s.metaMorphoABI.Pack("balanceOf", userB)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("packing balanceOf(B) call: %w", err)
	}

	results, err := s.multicallClient.ExecuteAtHash(ctx, []outbound.Call{
		{Target: vaultAddress, AllowFailure: false, CallData: totalAssetsData},
		{Target: vaultAddress, AllowFailure: false, CallData: totalSupplyData},
		{Target: vaultAddress, AllowFailure: false, CallData: balanceAData},
		{Target: vaultAddress, AllowFailure: false, CallData: balanceBData},
	}, blockHash)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("multicall vault state+2 balances: %w", err)
	}

	if len(results) < 4 {
		return nil, nil, nil, fmt.Errorf("expected 4 results, got %d", len(results))
	}

	vs, err := s.unpackVaultState(results[0], results[1], vaultAddress)
	if err != nil {
		return nil, nil, nil, err
	}

	balA, err := s.unpackBalance(results[2], "A", vaultAddress)
	if err != nil {
		return nil, nil, nil, err
	}

	balB, err := s.unpackBalance(results[3], "B", vaultAddress)
	if err != nil {
		return nil, nil, nil, err
	}

	return vs, balA, balB, nil
}

// getAdapterType classifies a VaultV2 liquidity adapter (MarketV1 / VaultV1 /
// Unknown). Number-pinned intentionally: adapter identity is immutable, same
// rationale as getMarketParams (see VEC-471). A both-fail / both-succeed probe
// yields MorphoAdapterTypeUnknown with a nil error (the caller WARNs and still
// records it); only a transport error propagates.
func (s *blockchainService) getAdapterType(ctx context.Context, adapter common.Address, blockNumber int64) (retType entity.MorphoAdapterType, retErr error) {
	ctx, span := s.telemetry.StartSpan(ctx, "morpho.rpc.getAdapterType",
		attribute.String("adapter.address", adapter.Hex()))
	defer span.End()
	start := time.Now()
	defer func() {
		s.telemetry.RecordRPCCall(ctx, "getAdapterType", time.Since(start), retErr)
		if retErr != nil {
			telemetry.SetSpanError(span, retErr, "getAdapterType failed")
		}
	}()

	return s.adapterProber.ProbeAdapterType(ctx, s.multicallClient, adapter, big.NewInt(blockNumber))
}

// errAdapterRealAssetsReverted reports that the adapter's realAssets() call itself
// reverted, as opposed to the multicall failing to reach the node. Only the
// registration seed for an adapter the type probe could not classify treats it as
// "no reading to record"; every other caller treats it as an error. See
// Service.readSeedRealAssets for why the distinction is structural rather than
// best-effort.
var errAdapterRealAssetsReverted = errors.New("realAssets() reverted")

// getAdapterRealAssets reads an adapter's realAssets() — the assets it reports
// holding in its downstream venue — pinned to blockHash. This is versioned
// per-block state (it changes every allocation / accrual), so it uses
// ExecuteAtHash for reorg-correctness (see getMarketState / VEC-471), not
// number-pinning.
//
// The call is AllowFailure purely so a revert is reportable as
// errAdapterRealAssetsReverted rather than reverting the whole batch; it is still an
// error here, and only one caller is allowed to tolerate that specific error.
func (s *blockchainService) getAdapterRealAssets(ctx context.Context, adapter common.Address, blockHash common.Hash) (retAssets *big.Int, retErr error) {
	ctx, span := s.telemetry.StartSpan(ctx, "morpho.rpc.getAdapterRealAssets",
		attribute.String("adapter.address", adapter.Hex()))
	defer span.End()
	start := time.Now()
	defer func() {
		s.telemetry.RecordRPCCall(ctx, "getAdapterRealAssets", time.Since(start), retErr)
		if retErr != nil {
			telemetry.SetSpanError(span, retErr, "getAdapterRealAssets failed")
		}
	}()

	callData, err := s.adapterABI.Pack("realAssets")
	if err != nil {
		return nil, fmt.Errorf("packing realAssets call: %w", err)
	}

	results, err := s.multicallClient.ExecuteAtHash(ctx, []outbound.Call{{
		Target:       adapter,
		AllowFailure: true,
		CallData:     callData,
	}}, blockHash)
	if err != nil {
		return nil, fmt.Errorf("multicall realAssets(): %w", err)
	}
	if len(results) == 0 {
		return nil, fmt.Errorf("realAssets() returned no result for adapter %s", adapter.Hex())
	}
	if !results[0].Success || len(results[0].ReturnData) == 0 {
		return nil, fmt.Errorf("adapter %s: %w", adapter.Hex(), errAdapterRealAssetsReverted)
	}

	unpacked, err := s.adapterABI.Unpack("realAssets", results[0].ReturnData)
	if err != nil {
		return nil, fmt.Errorf("unpacking realAssets() for adapter %s: %w", adapter.Hex(), err)
	}
	if len(unpacked) == 0 {
		return nil, fmt.Errorf("realAssets() returned no values for adapter %s", adapter.Hex())
	}
	return bigIntFromAny(unpacked[0]), nil
}

// enumerateVaultAdapters reads a VaultV2's registered adapter set via
// adaptersLength() then adapters(i), pinned to blockHash. The adapter SET is
// versioned per-block state — AddAdapter/RemoveAdapter mutate it — NOT immutable
// identity, so it is hash-pinned (ExecuteAtHash) for reorg-correctness (VEC-471),
// matching the per-adapter realAssets() seeds the caller reads at the same hash;
// a number-pinned read could straddle a reorg relative to those seeds. Used by
// discovery-time enumeration to seed the registry for a V2 vault found mid-life,
// whose historical AddAdapter events never replay on the live stream.
func (s *blockchainService) enumerateVaultAdapters(ctx context.Context, vaultAddress common.Address, blockHash common.Hash) (retAdapters []common.Address, retErr error) {
	ctx, span := s.telemetry.StartSpan(ctx, "morpho.rpc.enumerateVaultAdapters",
		attribute.String("vault.address", vaultAddress.Hex()))
	defer span.End()
	start := time.Now()
	defer func() {
		s.telemetry.RecordRPCCall(ctx, "enumerateVaultAdapters", time.Since(start), retErr)
		if retErr != nil {
			telemetry.SetSpanError(span, retErr, "enumerateVaultAdapters failed")
		}
	}()

	n, err := s.readAdaptersLength(ctx, vaultAddress, blockHash)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	return s.readAdapterAddresses(ctx, vaultAddress, blockHash, n)
}

// maxVaultAdapters bounds adaptersLength() before the value sizes any allocation.
// It is hostile-input protection, not a chain fact: a VaultV2's adapter set is
// governance-curated and real vaults hold dozens, but any contract that classifies
// as a VaultV2 can return an arbitrary uint256 here, and feeding that straight to
// make() panics in makeslice (or OOMs). The SQS consume path has no recover(), so a
// panic crashloops the worker and stalls all Morpho indexing; an error above the
// bound poison-pills just the offending message instead.
const maxVaultAdapters = 1000

// adaptersPerCall bounds how many adapters(i) sub-calls one multicall aggregate
// carries, so a vault near maxVaultAdapters cannot build a request that exceeds an
// RPC provider's request/response/gas caps (same rationale as uniswapv3's
// ticksPerCall).
const adaptersPerCall = 500

// readAdaptersLength reads adaptersLength() at blockHash and validates it against
// maxVaultAdapters before it is used as a length.
func (s *blockchainService) readAdaptersLength(ctx context.Context, vaultAddress common.Address, blockHash common.Hash) (int, error) {
	lengthData, err := s.vaultV2ABI.Pack("adaptersLength")
	if err != nil {
		return 0, fmt.Errorf("packing adaptersLength call: %w", err)
	}
	lengthResults, err := s.multicallClient.ExecuteAtHash(ctx, []outbound.Call{
		{Target: vaultAddress, AllowFailure: false, CallData: lengthData},
	}, blockHash)
	if err != nil {
		return 0, fmt.Errorf("multicall adaptersLength(): %w", err)
	}
	if len(lengthResults) == 0 || !lengthResults[0].Success || len(lengthResults[0].ReturnData) == 0 {
		return 0, fmt.Errorf("adaptersLength() call failed for vault %s", vaultAddress.Hex())
	}
	lengthUnpacked, err := s.vaultV2ABI.Unpack("adaptersLength", lengthResults[0].ReturnData)
	if err != nil {
		return 0, fmt.Errorf("unpacking adaptersLength() for vault %s: %w", vaultAddress.Hex(), err)
	}
	if len(lengthUnpacked) == 0 {
		return 0, fmt.Errorf("adaptersLength() returned no values for vault %s", vaultAddress.Hex())
	}
	length := bigIntFromAny(lengthUnpacked[0])
	if !length.IsInt64() || length.Sign() < 0 || length.Int64() > maxVaultAdapters {
		return 0, fmt.Errorf("adaptersLength() returned implausible length %s for vault %s (bound %d)",
			length.String(), vaultAddress.Hex(), maxVaultAdapters)
	}
	return int(length.Int64()), nil
}

// readAdapterAddresses reads adapters(0..n-1) at blockHash in bounded multicall
// batches (adaptersPerCall), decoding every result positionally so the returned
// slice keeps the vault's own registry order.
func (s *blockchainService) readAdapterAddresses(ctx context.Context, vaultAddress common.Address, blockHash common.Hash, n int) ([]common.Address, error) {
	indices := make([]int, n)
	for i := range n {
		indices[i] = i
	}

	adapters := make([]common.Address, 0, n)
	for chunk := range slices.Chunk(indices, adaptersPerCall) {
		chunkAdapters, err := s.readAdapterAddressChunk(ctx, vaultAddress, blockHash, chunk)
		if err != nil {
			return nil, err
		}
		adapters = append(adapters, chunkAdapters...)
	}
	return adapters, nil
}

// readAdapterAddressChunk issues one adapters(i) multicall for a bounded batch of
// registry indices and decodes every result.
func (s *blockchainService) readAdapterAddressChunk(ctx context.Context, vaultAddress common.Address, blockHash common.Hash, indices []int) ([]common.Address, error) {
	calls := make([]outbound.Call, len(indices))
	for i, index := range indices {
		callData, err := s.vaultV2ABI.Pack("adapters", big.NewInt(int64(index)))
		if err != nil {
			return nil, fmt.Errorf("packing adapters(%d) call: %w", index, err)
		}
		calls[i] = outbound.Call{Target: vaultAddress, AllowFailure: false, CallData: callData}
	}
	results, err := s.multicallClient.ExecuteAtHash(ctx, calls, blockHash)
	if err != nil {
		return nil, fmt.Errorf("multicall adapters(i): %w", err)
	}
	if len(results) != len(indices) {
		return nil, fmt.Errorf("adapters(i) returned %d results, want %d for vault %s", len(results), len(indices), vaultAddress.Hex())
	}

	adapters := make([]common.Address, len(indices))
	for i, r := range results {
		addr, err := s.unpackAdapterAddress(r, indices[i], vaultAddress)
		if err != nil {
			return nil, err
		}
		adapters[i] = addr
	}
	return adapters, nil
}

// unpackAdapterAddress validates and decodes one adapters(i) result.
func (s *blockchainService) unpackAdapterAddress(result outbound.Result, index int, vaultAddress common.Address) (common.Address, error) {
	if !result.Success || len(result.ReturnData) == 0 {
		return common.Address{}, fmt.Errorf("adapters(%d) call failed for vault %s", index, vaultAddress.Hex())
	}
	unpacked, err := s.vaultV2ABI.Unpack("adapters", result.ReturnData)
	if err != nil {
		return common.Address{}, fmt.Errorf("unpacking adapters(%d) for vault %s: %w", index, vaultAddress.Hex(), err)
	}
	if len(unpacked) == 0 {
		return common.Address{}, fmt.Errorf("adapters(%d) returned no values for vault %s", index, vaultAddress.Hex())
	}
	addr, ok := unpacked[0].(common.Address)
	if !ok {
		return common.Address{}, fmt.Errorf("adapters(%d) returned unexpected type %T for vault %s", index, unpacked[0], vaultAddress.Hex())
	}
	return addr, nil
}

// getVaultCaps reads the two current allocation limits for a cap id off the
// VaultV2, pinned to blockHash. absoluteCap/relativeCap are per-block state (a
// cap event mutates them), so like getAdapterRealAssets this is a hash-pinned
// ExecuteAtHash read for reorg-correctness (VEC-471), not number-pinning. Both
// getters exist on every VaultV2 and cannot fail for a real cap id, so neither
// call is AllowFailure: a revert is a real error that must stop the event.
func (s *blockchainService) getVaultCaps(ctx context.Context, vault common.Address, capID [32]byte, blockHash common.Hash) (retAbsolute, retRelative *big.Int, retErr error) {
	ctx, span := s.telemetry.StartSpan(ctx, "morpho.rpc.getVaultCaps",
		attribute.String("vault.address", vault.Hex()))
	defer span.End()
	start := time.Now()
	defer func() {
		s.telemetry.RecordRPCCall(ctx, "getVaultCaps", time.Since(start), retErr)
		if retErr != nil {
			telemetry.SetSpanError(span, retErr, "getVaultCaps failed")
		}
	}()

	absoluteCallData, err := s.vaultV2ABI.Pack("absoluteCap", capID)
	if err != nil {
		return nil, nil, fmt.Errorf("packing absoluteCap call: %w", err)
	}
	relativeCallData, err := s.vaultV2ABI.Pack("relativeCap", capID)
	if err != nil {
		return nil, nil, fmt.Errorf("packing relativeCap call: %w", err)
	}

	results, err := s.multicallClient.ExecuteAtHash(ctx, []outbound.Call{
		{Target: vault, AllowFailure: false, CallData: absoluteCallData},
		{Target: vault, AllowFailure: false, CallData: relativeCallData},
	}, blockHash)
	if err != nil {
		return nil, nil, fmt.Errorf("multicall absoluteCap()/relativeCap(): %w", err)
	}
	if len(results) != 2 {
		return nil, nil, fmt.Errorf("cap getters returned %d results, want 2", len(results))
	}

	absolute, err := s.unpackVaultCap("absoluteCap", results[0], vault, capID)
	if err != nil {
		return nil, nil, err
	}
	relative, err := s.unpackVaultCap("relativeCap", results[1], vault, capID)
	if err != nil {
		return nil, nil, err
	}
	return absolute, relative, nil
}

// unpackVaultCap validates and decodes one absoluteCap()/relativeCap() result.
func (s *blockchainService) unpackVaultCap(method string, result outbound.Result, vault common.Address, capID [32]byte) (*big.Int, error) {
	if !result.Success || len(result.ReturnData) == 0 {
		return nil, fmt.Errorf("%s() call failed for vault %s cap %x", method, vault.Hex(), capID)
	}
	unpacked, err := s.vaultV2ABI.Unpack(method, result.ReturnData)
	if err != nil {
		return nil, fmt.Errorf("unpacking %s() for vault %s: %w", method, vault.Hex(), err)
	}
	if len(unpacked) == 0 {
		return nil, fmt.Errorf("%s() returned no values for vault %s cap %x", method, vault.Hex(), capID)
	}
	return bigIntFromAny(unpacked[0]), nil
}

// vaultFeeConfig is the full on-chain fee configuration of a VaultV2 at a block:
// both fees (raw uint96 WAD, unscaled) and both recipient addresses.
type vaultFeeConfig struct {
	performanceFee          *big.Int
	managementFee           *big.Int
	performanceFeeRecipient common.Address
	managementFeeRecipient  common.Address
}

// errNoVaultFeeSurface reports that a contract serves NONE of the four VaultV2 fee
// getters. The vault probe only proves curator() and liquidityAdapter() answer, so a
// vault-shaped address that is not a factory-deployed VaultV2 can pass it and still
// have no fee surface at all; the discovery seed treats that as "no fee config to
// record" rather than a failure, because hard-requiring the getters poisoned such an
// address's discovery forever. Callers reacting to a Set* fee EVENT must still treat
// it as an error: the event proves the surface exists.
var errNoVaultFeeSurface = errors.New("contract serves none of the VaultV2 fee getters")

// getVaultFees reads the vault's full fee configuration off the VaultV2, pinned
// to blockHash. The fee config is per-block state (a Set* fee event mutates it),
// so like getVaultCaps this is a hash-pinned ExecuteAtHash read for
// reorg-correctness (VEC-471), not number-pinning.
//
// The four getters are AllowFailure so that "this contract has no fee surface at
// all" is distinguishable from "one getter reverted", which is drift on a contract
// that does have it. All-or-nothing is the only sane split: a real VaultV2 answers
// all four, so a partial answer is never a valid shape and errors (see
// assertFeeSurfaceComplete), while none-of-four returns errNoVaultFeeSurface for the
// caller to decide on.
func (s *blockchainService) getVaultFees(ctx context.Context, vault common.Address, blockHash common.Hash) (retFees *vaultFeeConfig, retErr error) {
	ctx, span := s.telemetry.StartSpan(ctx, "morpho.rpc.getVaultFees",
		attribute.String("vault.address", vault.Hex()))
	defer span.End()
	start := time.Now()
	defer func() {
		s.telemetry.RecordRPCCall(ctx, "getVaultFees", time.Since(start), retErr)
		if retErr != nil {
			telemetry.SetSpanError(span, retErr, "getVaultFees failed")
		}
	}()

	// Order matches the unpack below: performanceFee, managementFee,
	// performanceFeeRecipient, managementFeeRecipient.
	methods := []string{"performanceFee", "managementFee", "performanceFeeRecipient", "managementFeeRecipient"}
	calls := make([]outbound.Call, len(methods))
	for i, m := range methods {
		callData, err := s.vaultV2ABI.Pack(m)
		if err != nil {
			return nil, fmt.Errorf("packing %s() call: %w", m, err)
		}
		calls[i] = outbound.Call{Target: vault, AllowFailure: true, CallData: callData}
	}

	results, err := s.multicallClient.ExecuteAtHash(ctx, calls, blockHash)
	if err != nil {
		return nil, fmt.Errorf("multicall vault fee getters: %w", err)
	}
	if len(results) != len(methods) {
		return nil, fmt.Errorf("vault fee getters returned %d results, want %d", len(results), len(methods))
	}
	if err := assertFeeSurfaceComplete(methods, results, vault); err != nil {
		return nil, err
	}

	performanceFee, err := s.unpackVaultFeeUint("performanceFee", results[0], vault)
	if err != nil {
		return nil, err
	}
	managementFee, err := s.unpackVaultFeeUint("managementFee", results[1], vault)
	if err != nil {
		return nil, err
	}
	performanceFeeRecipient, err := s.unpackVaultFeeAddress("performanceFeeRecipient", results[2], vault)
	if err != nil {
		return nil, err
	}
	managementFeeRecipient, err := s.unpackVaultFeeAddress("managementFeeRecipient", results[3], vault)
	if err != nil {
		return nil, err
	}
	return &vaultFeeConfig{
		performanceFee:          performanceFee,
		managementFee:           managementFee,
		performanceFeeRecipient: performanceFeeRecipient,
		managementFeeRecipient:  managementFeeRecipient,
	}, nil
}

// assertFeeSurfaceComplete classifies a fee-getter batch: all four served is the
// only shape a real VaultV2 produces, none served means the contract has no fee
// surface (errNoVaultFeeSurface), and anything in between is drift the caller must
// stop on — the message names which getters reverted so the vault can be inspected.
func assertFeeSurfaceComplete(methods []string, results []outbound.Result, vault common.Address) error {
	var reverted []string
	for i, m := range methods {
		if !results[i].Success || len(results[i].ReturnData) == 0 {
			reverted = append(reverted, m)
		}
	}
	switch len(reverted) {
	case 0:
		return nil
	case len(methods):
		return fmt.Errorf("vault %s: %w", vault.Hex(), errNoVaultFeeSurface)
	default:
		return fmt.Errorf("vault %s served %d of %d VaultV2 fee getters (%s reverted): a VaultV2 serves all four, so this is contract drift, not a missing fee surface",
			vault.Hex(), len(methods)-len(reverted), len(methods), strings.Join(reverted, ", "))
	}
}

// unpackVaultFeeUint validates and decodes one uint fee getter result.
func (s *blockchainService) unpackVaultFeeUint(method string, result outbound.Result, vault common.Address) (*big.Int, error) {
	if !result.Success || len(result.ReturnData) == 0 {
		return nil, fmt.Errorf("%s() call failed for vault %s", method, vault.Hex())
	}
	unpacked, err := s.vaultV2ABI.Unpack(method, result.ReturnData)
	if err != nil {
		return nil, fmt.Errorf("unpacking %s() for vault %s: %w", method, vault.Hex(), err)
	}
	if len(unpacked) == 0 {
		return nil, fmt.Errorf("%s() returned no values for vault %s", method, vault.Hex())
	}
	return bigIntFromAny(unpacked[0]), nil
}

// unpackVaultFeeAddress validates and decodes one address fee-recipient getter result.
func (s *blockchainService) unpackVaultFeeAddress(method string, result outbound.Result, vault common.Address) (common.Address, error) {
	if !result.Success || len(result.ReturnData) == 0 {
		return common.Address{}, fmt.Errorf("%s() call failed for vault %s", method, vault.Hex())
	}
	unpacked, err := s.vaultV2ABI.Unpack(method, result.ReturnData)
	if err != nil {
		return common.Address{}, fmt.Errorf("unpacking %s() for vault %s: %w", method, vault.Hex(), err)
	}
	if len(unpacked) == 0 {
		return common.Address{}, fmt.Errorf("%s() returned no values for vault %s", method, vault.Hex())
	}
	addr, ok := unpacked[0].(common.Address)
	if !ok {
		return common.Address{}, fmt.Errorf("%s() returned unexpected type %T for vault %s", method, unpacked[0], vault.Hex())
	}
	return addr, nil
}

// getVaultMetadata identifies whether a contract is a Morpho-family vault
// (MetaMorpho V1 / V1.1 or VaultV2), then fetches its metadata.
//
// Split into two multicalls to keep the probe cheap for non-vault contracts:
//  1. Probe: MORPHO + asset + curator + liquidityAdapter — identifies the
//     vault flavour (or rejects with ErrNotVault).
//  2. Metadata: name, symbol, decimals, skimRecipient — only runs for
//     confirmed vaults.
//
// MetaMorpho V1/V1.1 vaults must reference the canonical Morpho Blue
// singleton; we reject any MetaMorpho probe whose MORPHO() points elsewhere.
// VaultV2 has no MORPHO() function and is identified by curator() and
// liquidityAdapter() in vault_probe.go.
//
// Number-pinned intentionally (delegates to vault_probe.go's Execute calls):
// vault identity (MORPHO/asset/curator/liquidityAdapter, name/symbol/decimals)
// is structurally static, not versioned state — see VEC-471.
func (s *blockchainService) getVaultMetadata(ctx context.Context, vaultAddress common.Address, blockNumber int64) (retMD *VaultMetadata, retErr error) {
	ctx, span := s.telemetry.StartSpan(ctx, "morpho.rpc.getVaultMetadata",
		attribute.String("vault.address", vaultAddress.Hex()))
	defer span.End()
	start := time.Now()
	defer func() {
		s.telemetry.RecordRPCCall(ctx, "getVaultMetadata", time.Since(start), retErr)
		if retErr != nil {
			telemetry.SetSpanError(span, retErr, "getVaultMetadata failed")
		}
	}()

	block := big.NewInt(blockNumber)
	probe, err := s.vaultProber.ProbeVault(ctx, s.multicallClient, vaultAddress, block)
	if err != nil {
		return nil, fmt.Errorf("fetching vault probe: %w", err)
	}

	// MetaMorpho variants must point at the canonical Morpho Blue singleton.
	// VaultV2 has no MORPHO() and is exempt from this check. (Zero-address
	// asset is rejected upstream in ParseProbeResults with VaultShaped set
	// from the probe context, so no explicit check is needed here.)
	if probe.Version != entity.MorphoVaultV2 && probe.MorphoAddr != MorphoBlueAddress {
		return nil, &ErrNotVault{
			Err:         fmt.Errorf("MORPHO() returned %s, expected %s — not a MetaMorpho vault", probe.MorphoAddr.Hex(), MorphoBlueAddress.Hex()),
			VaultShaped: true, // MORPHO() returned an address — it's vault-shaped, just not ours.
		}
	}

	md, err := s.fetchVaultDetails(ctx, vaultAddress, probe.Version, block)
	if err != nil {
		return nil, fmt.Errorf("fetching vault details: %w", err)
	}
	md.Asset = probe.AssetAddr

	return md, nil
}

// fetchVaultDetails fetches name, symbol, decimals, and version for a
// confirmed vault. tentativeVersion comes from the probe phase: V1 may be
// upgraded to V1.1 here if skimRecipient succeeds; V2 is preserved.
func (s *blockchainService) fetchVaultDetails(ctx context.Context, vaultAddress common.Address, tentativeVersion entity.MorphoVaultVersion, blockNumber *big.Int) (*VaultMetadata, error) {
	details, err := s.vaultProber.FetchVaultDetails(ctx, s.multicallClient, vaultAddress, tentativeVersion, blockNumber)
	if err != nil {
		return nil, err
	}
	return &VaultMetadata{
		Name:     details.Name,
		Symbol:   details.Symbol,
		Decimals: details.Decimals,
		Version:  details.Version,
	}, nil
}

// zeroAddressTokenMetadata is the canonical metadata returned for the zero
// address. Morpho Blue allows markets where collateralToken = 0x0 ("idle
// markets" used as liquidity buffers); calling decimals() / symbol() on the
// zero address returns empty data, which is otherwise treated as an error.
// Short-circuiting at the metadata layer keeps the rest of the indexer from
// having to special-case 0x0 downstream.
//
// The empty symbol here is final: the zero address is a known sentinel that
// is excluded from the per-block sweep by address, so it is never retried.
var zeroAddressTokenMetadata = TokenMetadata{Symbol: "", Decimals: 0}

// getTokenMetadata fetches token symbol and decimals via ERC20 calls.
// Number-pinned intentionally: symbol/decimals are structurally static
// identity data (immutable per token contract), not versioned state — the
// reorg-correctness concern behind ExecuteAtHash (VEC-471) doesn't apply here.
//
// symbol() is best-effort: a reverted or undecodable symbol() yields
// Symbol="" with no error; the per-block sweep retries it later.
// decimals() is mandatory: a reverted decimals() is a hard error because a
// silent 0-decimals value would corrupt all downstream amount math.
// A non-string symbol() (e.g. MKR-style bytes32) is handled by
// erc20meta.DecodeStringOrBytes32 and still yields a resolved symbol.
//
// The zero address is short-circuited to zeroAddressTokenMetadata without
// issuing any sub-call. See zeroAddressTokenMetadata for the rationale.
func (s *blockchainService) getTokenMetadata(ctx context.Context, tokenAddress common.Address, blockNumber int64) (retMD TokenMetadata, retErr error) {
	if tokenAddress == (common.Address{}) {
		return zeroAddressTokenMetadata, nil
	}

	ctx, span := s.telemetry.StartSpan(ctx, "morpho.rpc.getTokenMetadata",
		attribute.String("token.address", tokenAddress.Hex()))
	defer span.End()
	start := time.Now()
	defer func() {
		s.telemetry.RecordRPCCall(ctx, "getTokenMetadata", time.Since(start), retErr)
		if retErr != nil {
			telemetry.SetSpanError(span, retErr, "getTokenMetadata failed")
		}
	}()

	if cached, ok := s.metadataCache[tokenAddress]; ok {
		return cached, nil
	}

	symbolData, err := s.erc20ABI.Pack("symbol")
	if err != nil {
		return TokenMetadata{}, fmt.Errorf("packing symbol call: %w", err)
	}
	decimalsData, err := s.erc20ABI.Pack("decimals")
	if err != nil {
		return TokenMetadata{}, fmt.Errorf("packing decimals call: %w", err)
	}

	results, err := s.multicallClient.Execute(ctx, []outbound.Call{
		{Target: tokenAddress, AllowFailure: true, CallData: symbolData},
		{Target: tokenAddress, AllowFailure: true, CallData: decimalsData},
	}, big.NewInt(blockNumber))
	if err != nil {
		return TokenMetadata{}, fmt.Errorf("multicall token metadata: %w", err)
	}

	if len(results) != 2 {
		return TokenMetadata{}, fmt.Errorf("getTokenMetadata(%s): expected 2 results, got %d", tokenAddress.Hex(), len(results))
	}
	// decimals() (index 1) must succeed — it drives all amount math. A reverted
	// symbol() (index 0) is tolerated: unpackTokenMetadataResults yields an empty
	// symbol that the per-block sweep fills in later. Narrows VEC-188
	// "Finding 3" to decimals only.
	if !results[1].Success {
		return TokenMetadata{}, fmt.Errorf("getTokenMetadata(%s): decimals() sub-call reverted", tokenAddress.Hex())
	}

	md, err := s.unpackTokenMetadataResults(results[0], results[1], tokenAddress)
	if err != nil {
		return TokenMetadata{}, err
	}

	s.metadataCache[tokenAddress] = md
	return md, nil
}

// unpackTokenMetadataResults unpacks symbol() and decimals() results for a
// single token. Callers must have verified that decimals() succeeded
// (results[decimals index].Success == true) before calling this helper.
//
// symbol() is best-effort: a reverted symbol() sub-call (Success: false) yields
// Symbol="" with no error; the per-block sweep retries it later.
// symbol() supports both modern (`string`) and legacy (`bytes32`, e.g. MKR)
// ABIs via erc20meta.DecodeStringOrBytes32; on total decode failure (when
// symbol() succeeded but the return data is neither a valid ABI string nor
// bytes32) the symbol is left empty — still best-effort, no error returned.
//
// decimals() must decode cleanly — a failure here means the contract is not a
// conformant ERC20 and we surface an error rather than persist 0.
func (s *blockchainService) unpackTokenMetadataResults(symbolResult, decimalsResult outbound.Result, token common.Address) (TokenMetadata, error) {
	md := TokenMetadata{}

	if symbolResult.Success && len(symbolResult.ReturnData) > 0 {
		if sym, err := erc20meta.DecodeStringOrBytes32(s.erc20ABI, "symbol", symbolResult.ReturnData); err == nil {
			md.Symbol = sym
		}
	}

	if len(decimalsResult.ReturnData) == 0 {
		return TokenMetadata{}, fmt.Errorf("decimals() returned no data for token %s", token.Hex())
	}
	decimalsUnpacked, err := s.erc20ABI.Unpack("decimals", decimalsResult.ReturnData)
	if err != nil {
		return TokenMetadata{}, fmt.Errorf("unpacking decimals() for token %s: %w", token.Hex(), err)
	}
	if len(decimalsUnpacked) == 0 {
		return TokenMetadata{}, fmt.Errorf("decimals() returned no values for token %s", token.Hex())
	}
	md.Decimals = intFromAny(decimalsUnpacked[0])

	return md, nil
}

// getTokenPairMetadata fetches metadata for two tokens in a single Multicall3 batch.
// Respects the metadata cache — if both are cached, no RPC call is made; if one is cached,
// only the uncached token's calls are included in the batch.
// Number-pinned intentionally, same rationale as getTokenMetadata: symbol/
// decimals are static identity data, not versioned state.
//
// Either token may be the zero address (Morpho Blue idle markets use
// collateralToken = 0x0); the zero side is short-circuited to
// zeroAddressTokenMetadata, and the non-zero side is fetched via a single
// per-token call rather than a 4-call pair batch.
func (s *blockchainService) getTokenPairMetadata(ctx context.Context, tokenA, tokenB common.Address, blockNumber int64) (retMDA TokenMetadata, retMDB TokenMetadata, retErr error) {
	zeroA := tokenA == (common.Address{})
	zeroB := tokenB == (common.Address{})

	switch {
	case zeroA && zeroB:
		return zeroAddressTokenMetadata, zeroAddressTokenMetadata, nil
	case zeroA:
		mdB, err := s.getTokenMetadata(ctx, tokenB, blockNumber)
		return zeroAddressTokenMetadata, mdB, err
	case zeroB:
		mdA, err := s.getTokenMetadata(ctx, tokenA, blockNumber)
		return mdA, zeroAddressTokenMetadata, err
	}

	cachedA, hasCacheA := s.metadataCache[tokenA]
	cachedB, hasCacheB := s.metadataCache[tokenB]

	if hasCacheA && hasCacheB {
		return cachedA, cachedB, nil
	}

	ctx, span := s.telemetry.StartSpan(ctx, "morpho.rpc.getTokenPairMetadata",
		attribute.String("token_a.address", tokenA.Hex()),
		attribute.String("token_b.address", tokenB.Hex()))
	defer span.End()
	start := time.Now()
	defer func() {
		s.telemetry.RecordRPCCall(ctx, "getTokenPairMetadata", time.Since(start), retErr)
		if retErr != nil {
			telemetry.SetSpanError(span, retErr, "getTokenPairMetadata failed")
		}
	}()

	// If one is cached, fall back to single-token fetch for the uncached one.
	if hasCacheA {
		mdB, err := s.getTokenMetadata(ctx, tokenB, blockNumber)
		return cachedA, mdB, err
	}
	if hasCacheB {
		mdA, err := s.getTokenMetadata(ctx, tokenA, blockNumber)
		return mdA, cachedB, err
	}

	// Neither is cached — batch all 4 sub-calls.
	symbolAData, err := s.erc20ABI.Pack("symbol")
	if err != nil {
		return TokenMetadata{}, TokenMetadata{}, fmt.Errorf("packing symbol(A) call: %w", err)
	}
	decimalsAData, err := s.erc20ABI.Pack("decimals")
	if err != nil {
		return TokenMetadata{}, TokenMetadata{}, fmt.Errorf("packing decimals(A) call: %w", err)
	}
	symbolBData, err := s.erc20ABI.Pack("symbol")
	if err != nil {
		return TokenMetadata{}, TokenMetadata{}, fmt.Errorf("packing symbol(B) call: %w", err)
	}
	decimalsBData, err := s.erc20ABI.Pack("decimals")
	if err != nil {
		return TokenMetadata{}, TokenMetadata{}, fmt.Errorf("packing decimals(B) call: %w", err)
	}

	results, err := s.multicallClient.Execute(ctx, []outbound.Call{
		{Target: tokenA, AllowFailure: true, CallData: symbolAData},
		{Target: tokenA, AllowFailure: true, CallData: decimalsAData},
		{Target: tokenB, AllowFailure: true, CallData: symbolBData},
		{Target: tokenB, AllowFailure: true, CallData: decimalsBData},
	}, big.NewInt(blockNumber))
	if err != nil {
		return TokenMetadata{}, TokenMetadata{}, fmt.Errorf("multicall token pair metadata: %w", err)
	}

	if len(results) != 4 {
		return TokenMetadata{}, TokenMetadata{}, fmt.Errorf("getTokenPairMetadata(%s,%s): expected 4 results, got %d", tokenA.Hex(), tokenB.Hex(), len(results))
	}
	// Only decimals() (indices 1 and 3) must succeed; reverted symbol() calls
	// (indices 0/2) yield empty symbols for later sweep retry. See unpackTokenMetadataResults.
	if !results[1].Success {
		return TokenMetadata{}, TokenMetadata{}, fmt.Errorf("getTokenPairMetadata(%s,%s): decimals() reverted for %s", tokenA.Hex(), tokenB.Hex(), tokenA.Hex())
	}
	if !results[3].Success {
		return TokenMetadata{}, TokenMetadata{}, fmt.Errorf("getTokenPairMetadata(%s,%s): decimals() reverted for %s", tokenA.Hex(), tokenB.Hex(), tokenB.Hex())
	}

	mdA, err := s.unpackTokenMetadataResults(results[0], results[1], tokenA)
	if err != nil {
		return TokenMetadata{}, TokenMetadata{}, err
	}
	mdB, err := s.unpackTokenMetadataResults(results[2], results[3], tokenB)
	if err != nil {
		return TokenMetadata{}, TokenMetadata{}, err
	}

	s.metadataCache[tokenA] = mdA
	s.metadataCache[tokenB] = mdB

	return mdA, mdB, nil
}

// resolveSymbolsAt re-reads symbol() for the given tokens at blockNumber (the
// block currently being processed, never head). It returns only the tokens
// whose symbol() succeeded and decoded; tokens still reverting are omitted so
// the caller leaves them pending. The in-process metadata cache is refreshed for
// resolved tokens that are already cached. Number-pinned intentionally, same
// rationale as getTokenMetadata: symbol() is static identity data, not
// versioned state; the sweep also has no BlockEvent in scope to source a hash
// from (reconcilePendingSymbols runs off chainID+blockNumber alone).
func (s *blockchainService) resolveSymbolsAt(ctx context.Context, tokens []common.Address, blockNumber int64) (map[common.Address]string, error) {
	resolved := make(map[common.Address]string, len(tokens))
	if len(tokens) == 0 {
		return resolved, nil
	}

	symbolData, err := s.erc20ABI.Pack("symbol")
	if err != nil {
		return nil, fmt.Errorf("packing symbol() call: %w", err)
	}
	calls := make([]outbound.Call, len(tokens))
	for i, t := range tokens {
		calls[i] = outbound.Call{Target: t, AllowFailure: true, CallData: symbolData}
	}

	results, err := s.multicallClient.Execute(ctx, calls, big.NewInt(blockNumber))
	if err != nil {
		return nil, fmt.Errorf("multicall resolve symbols at block %d: %w", blockNumber, err)
	}
	if len(results) != len(tokens) {
		return nil, fmt.Errorf("resolve symbols: expected %d results, got %d", len(tokens), len(results))
	}

	for i, r := range results {
		if !r.Success || len(r.ReturnData) == 0 {
			continue
		}
		sym, decErr := erc20meta.DecodeStringOrBytes32(s.erc20ABI, "symbol", r.ReturnData)
		if decErr != nil || sym == "" {
			continue
		}
		resolved[tokens[i]] = sym
		if cached, ok := s.metadataCache[tokens[i]]; ok {
			cached.Symbol = sym
			s.metadataCache[tokens[i]] = cached
		}
	}
	return resolved, nil
}

// bigIntFromAny converts an interface value (typically *big.Int) to *big.Int.
func bigIntFromAny(v any) *big.Int {
	switch val := v.(type) {
	case *big.Int:
		return new(big.Int).Set(val)
	default:
		return new(big.Int)
	}
}

// intFromAny safely converts an interface value to int.
// Handles uint8 (ERC20 decimals) and other numeric types from ABI unpacking.
func intFromAny(v any) int {
	switch val := v.(type) {
	case uint8:
		return int(val)
	case int:
		return val
	case int64:
		return int(val)
	case uint64:
		return int(val)
	default:
		return 0
	}
}

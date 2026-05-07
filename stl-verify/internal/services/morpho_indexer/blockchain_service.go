package morpho_indexer

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"go.opentelemetry.io/otel/attribute"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/erc20meta"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/rpcerr"
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
type TokenMetadata struct {
	Symbol   string
	Decimals int
}

// blockchainService handles all on-chain reads for Morpho protocol.
type blockchainService struct {
	multicallClient outbound.Multicaller
	morphoBlueABI   *abi.ABI
	metaMorphoABI   *abi.ABI
	erc20ABI        *abi.ABI
	vaultProber     *VaultProber
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

	vaultProber, err := NewVaultProber()
	if err != nil {
		return nil, fmt.Errorf("creating vault prober: %w", err)
	}

	return &blockchainService{
		multicallClient: multicallClient,
		morphoBlueABI:   morphoABI,
		metaMorphoABI:   metaMorphoABI,
		erc20ABI:        erc20ABI,
		vaultProber:     vaultProber,
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

// getMarketState fetches the market state from Morpho Blue at a specific block.
func (s *blockchainService) getMarketState(ctx context.Context, marketID [32]byte, blockNumber int64) (retState *MarketState, retErr error) {
	ctx, span := s.telemetry.StartSpan(ctx, "morpho.rpc.getMarketState",
		attribute.String("market.id", fmt.Sprintf("%x", marketID[:8])))
	defer span.End()
	start := time.Now()
	defer func() {
		s.telemetry.RecordRPCCall(ctx, "getMarketState", time.Since(start), retErr)
		if retErr != nil {
			SetSpanError(span, retErr, "getMarketState failed")
		}
	}()

	callData, err := s.morphoBlueABI.Pack("market", marketID)
	if err != nil {
		return nil, fmt.Errorf("packing market call: %w", err)
	}

	results, err := s.multicallClient.Execute(ctx, []outbound.Call{{
		Target:       MorphoBlueAddress,
		AllowFailure: false,
		CallData:     callData,
	}}, big.NewInt(blockNumber))
	if err != nil {
		return nil, fmt.Errorf("multicall market(): %w", err)
	}

	if len(results) == 0 {
		return nil, fmt.Errorf("expected 1 result, got 0")
	}

	return s.unpackMarketState(results[0])
}

// getMarketParams fetches market parameters from Morpho Blue.
func (s *blockchainService) getMarketParams(ctx context.Context, marketID [32]byte, blockNumber int64) (retState *MarketParamsState, retErr error) {
	ctx, span := s.telemetry.StartSpan(ctx, "morpho.rpc.getMarketParams",
		attribute.String("market.id", fmt.Sprintf("%x", marketID[:8])))
	defer span.End()
	start := time.Now()
	defer func() {
		s.telemetry.RecordRPCCall(ctx, "getMarketParams", time.Since(start), retErr)
		if retErr != nil {
			SetSpanError(span, retErr, "getMarketParams failed")
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

// getMarketAndPositionState fetches both market and position state in a single Multicall3 batch.
func (s *blockchainService) getMarketAndPositionState(ctx context.Context, marketID [32]byte, user common.Address, blockNumber int64) (retMS *MarketState, retPS *PositionState, retErr error) {
	ctx, span := s.telemetry.StartSpan(ctx, "morpho.rpc.getMarketAndPositionState",
		attribute.String("market.id", fmt.Sprintf("%x", marketID[:8])))
	defer span.End()
	start := time.Now()
	defer func() {
		s.telemetry.RecordRPCCall(ctx, "getMarketAndPositionState", time.Since(start), retErr)
		if retErr != nil {
			SetSpanError(span, retErr, "getMarketAndPositionState failed")
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

	results, err := s.multicallClient.Execute(ctx, []outbound.Call{
		{Target: MorphoBlueAddress, AllowFailure: false, CallData: marketCallData},
		{Target: MorphoBlueAddress, AllowFailure: false, CallData: positionCallData},
	}, big.NewInt(blockNumber))
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

// getMarketAndTwoPositionStates fetches market state and two user positions in a single Multicall3 batch.
// Used by liquidation events where we need the borrower and liquidator positions.
func (s *blockchainService) getMarketAndTwoPositionStates(ctx context.Context, marketID [32]byte, userA, userB common.Address, blockNumber int64) (retMS *MarketState, retPSA *PositionState, retPSB *PositionState, retErr error) {
	ctx, span := s.telemetry.StartSpan(ctx, "morpho.rpc.getMarketAndTwoPositionStates",
		attribute.String("market.id", fmt.Sprintf("%x", marketID[:8])))
	defer span.End()
	start := time.Now()
	defer func() {
		s.telemetry.RecordRPCCall(ctx, "getMarketAndTwoPositionStates", time.Since(start), retErr)
		if retErr != nil {
			SetSpanError(span, retErr, "getMarketAndTwoPositionStates failed")
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

	results, err := s.multicallClient.Execute(ctx, []outbound.Call{
		{Target: MorphoBlueAddress, AllowFailure: false, CallData: marketCallData},
		{Target: MorphoBlueAddress, AllowFailure: false, CallData: posACallData},
		{Target: MorphoBlueAddress, AllowFailure: false, CallData: posBCallData},
	}, big.NewInt(blockNumber))
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

// getVaultState fetches vault total assets and total supply in a single Multicall3 batch.
func (s *blockchainService) getVaultState(ctx context.Context, vaultAddress common.Address, blockNumber int64) (retState *VaultState, retErr error) {
	ctx, span := s.telemetry.StartSpan(ctx, "morpho.rpc.getVaultState",
		attribute.String("vault.address", vaultAddress.Hex()))
	defer span.End()
	start := time.Now()
	defer func() {
		s.telemetry.RecordRPCCall(ctx, "getVaultState", time.Since(start), retErr)
		if retErr != nil {
			SetSpanError(span, retErr, "getVaultState failed")
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

	results, err := s.multicallClient.Execute(ctx, []outbound.Call{
		{Target: vaultAddress, AllowFailure: false, CallData: totalAssetsData},
		{Target: vaultAddress, AllowFailure: false, CallData: totalSupplyData},
	}, big.NewInt(blockNumber))
	if err != nil {
		return nil, fmt.Errorf("multicall vault state: %w", err)
	}

	if len(results) < 2 {
		return nil, fmt.Errorf("expected 2 results, got %d", len(results))
	}

	return s.unpackVaultState(results[0], results[1], vaultAddress)
}

// getVaultStateAndBalance fetches vault state and a user's balance in a single Multicall3 batch.
func (s *blockchainService) getVaultStateAndBalance(ctx context.Context, vaultAddress common.Address, user common.Address, blockNumber int64) (retVS *VaultState, retBalance *big.Int, retErr error) {
	ctx, span := s.telemetry.StartSpan(ctx, "morpho.rpc.getVaultStateAndBalance",
		attribute.String("vault.address", vaultAddress.Hex()))
	defer span.End()
	start := time.Now()
	defer func() {
		s.telemetry.RecordRPCCall(ctx, "getVaultStateAndBalance", time.Since(start), retErr)
		if retErr != nil {
			SetSpanError(span, retErr, "getVaultStateAndBalance failed")
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

	results, err := s.multicallClient.Execute(ctx, []outbound.Call{
		{Target: vaultAddress, AllowFailure: false, CallData: totalAssetsData},
		{Target: vaultAddress, AllowFailure: false, CallData: totalSupplyData},
		{Target: vaultAddress, AllowFailure: false, CallData: balanceData},
	}, big.NewInt(blockNumber))
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

// getVaultStateAndTwoBalances fetches vault state and two user balances in a single Multicall3 batch.
// Used by vault Transfer events where we need both sender and receiver balances.
func (s *blockchainService) getVaultStateAndTwoBalances(ctx context.Context, vaultAddress common.Address, userA, userB common.Address, blockNumber int64) (retVS *VaultState, retBalA *big.Int, retBalB *big.Int, retErr error) {
	ctx, span := s.telemetry.StartSpan(ctx, "morpho.rpc.getVaultStateAndTwoBalances",
		attribute.String("vault.address", vaultAddress.Hex()))
	defer span.End()
	start := time.Now()
	defer func() {
		s.telemetry.RecordRPCCall(ctx, "getVaultStateAndTwoBalances", time.Since(start), retErr)
		if retErr != nil {
			SetSpanError(span, retErr, "getVaultStateAndTwoBalances failed")
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

	results, err := s.multicallClient.Execute(ctx, []outbound.Call{
		{Target: vaultAddress, AllowFailure: false, CallData: totalAssetsData},
		{Target: vaultAddress, AllowFailure: false, CallData: totalSupplyData},
		{Target: vaultAddress, AllowFailure: false, CallData: balanceAData},
		{Target: vaultAddress, AllowFailure: false, CallData: balanceBData},
	}, big.NewInt(blockNumber))
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

// getVaultMetadata identifies whether a contract is a MetaMorpho vault, then fetches its metadata.
//
// Split into two multicalls to keep the probe cheap for non-vault contracts:
//  1. Probe: MORPHO() + asset() — determines if this is a real MetaMorpho vault.
//  2. Metadata: name, symbol, decimals, skimRecipient — only runs for confirmed vaults.
func (s *blockchainService) getVaultMetadata(ctx context.Context, vaultAddress common.Address, blockNumber int64) (retMD *VaultMetadata, retErr error) {
	ctx, span := s.telemetry.StartSpan(ctx, "morpho.rpc.getVaultMetadata",
		attribute.String("vault.address", vaultAddress.Hex()))
	defer span.End()
	start := time.Now()
	defer func() {
		s.telemetry.RecordRPCCall(ctx, "getVaultMetadata", time.Since(start), retErr)
		if retErr != nil {
			SetSpanError(span, retErr, "getVaultMetadata failed")
		}
	}()

	block := big.NewInt(blockNumber)
	morphoAddr, asset, err := s.probeVault(ctx, vaultAddress, block)
	if err != nil {
		return nil, fmt.Errorf("fetching vault probe: %w", err)
	}
	if morphoAddr != MorphoBlueAddress {
		return nil, &ErrNotVault{Err: fmt.Errorf("MORPHO() returned %s, expected %s — not a MetaMorpho vault", morphoAddr.Hex(), MorphoBlueAddress.Hex())}
	}
	if asset == (common.Address{}) {
		return nil, &ErrNotVault{Err: fmt.Errorf("failed to get vault asset address for %s", vaultAddress.Hex())}
	}

	md, err := s.fetchVaultDetails(ctx, vaultAddress, block)
	if err != nil {
		return nil, fmt.Errorf("fetching vault details: %w", err)
	}
	md.Asset = asset

	return md, nil
}

// probeVault calls MORPHO() and asset() to determine if the contract is a MetaMorpho vault.
// Returns blockchain.ErrNotVault if the calls fail (e.g. non-vault contract), or the MORPHO
// address and asset address on success.
func (s *blockchainService) probeVault(ctx context.Context, vaultAddress common.Address, blockNumber *big.Int) (common.Address, common.Address, error) {
	result, err := s.vaultProber.ProbeVault(ctx, s.multicallClient, vaultAddress, blockNumber)
	if err != nil {
		return common.Address{}, common.Address{}, err
	}
	return result.MorphoAddr, result.AssetAddr, nil
}

// fetchVaultDetails fetches name, symbol, decimals, and version for a confirmed vault.
func (s *blockchainService) fetchVaultDetails(ctx context.Context, vaultAddress common.Address, blockNumber *big.Int) (*VaultMetadata, error) {
	details, err := s.vaultProber.FetchVaultDetails(ctx, s.multicallClient, vaultAddress, blockNumber)
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

// getTokenMetadata fetches token symbol and decimals via ERC20 calls.
//
// Post-VEC-188: any sub-call revert (Success: false) surfaces as an error so
// callers do not silently persist zero-valued metadata (empty Symbol, 0
// Decimals) into the token table. A non-string symbol() return (e.g. MKR-style
// bytes32) is still tolerated with an empty Symbol — that's an unpack concern,
// not a revert.
func (s *blockchainService) getTokenMetadata(ctx context.Context, tokenAddress common.Address, blockNumber int64) (retMD TokenMetadata, retErr error) {
	ctx, span := s.telemetry.StartSpan(ctx, "morpho.rpc.getTokenMetadata",
		attribute.String("token.address", tokenAddress.Hex()))
	defer span.End()
	start := time.Now()
	defer func() {
		s.telemetry.RecordRPCCall(ctx, "getTokenMetadata", time.Since(start), retErr)
		if retErr != nil {
			SetSpanError(span, retErr, "getTokenMetadata failed")
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
	if err := rpcerr.RequireAllSucceeded(results, fmt.Sprintf("getTokenMetadata(%s)", tokenAddress.Hex())); err != nil {
		return TokenMetadata{}, err
	}

	md, err := s.unpackTokenMetadataResults(results[0], results[1], tokenAddress)
	if err != nil {
		return TokenMetadata{}, err
	}

	s.metadataCache[tokenAddress] = md
	return md, nil
}

// unpackTokenMetadataResults unpacks symbol() and decimals() results for a
// single token. Callers must have already verified that both sub-calls
// succeeded (Success: true) — this helper only decodes the return data.
//
// symbol() supports both modern (`string`) and legacy (`bytes32`, e.g. MKR)
// ABIs via erc20meta.DecodeStringOrBytes32; on total decode failure the
// symbol is left empty rather than failing the whole row, since a missing
// display symbol doesn't make the rest of the row unusable. decimals()
// must decode cleanly — a failure here means the contract isn't a
// conformant ERC20 and we surface an error rather than persist 0.
func (s *blockchainService) unpackTokenMetadataResults(symbolResult, decimalsResult outbound.Result, token common.Address) (TokenMetadata, error) {
	md := TokenMetadata{}

	if len(symbolResult.ReturnData) > 0 {
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
func (s *blockchainService) getTokenPairMetadata(ctx context.Context, tokenA, tokenB common.Address, blockNumber int64) (retMDA TokenMetadata, retMDB TokenMetadata, retErr error) {
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
			SetSpanError(span, retErr, "getTokenPairMetadata failed")
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
	if err := rpcerr.RequireAllSucceeded(results, fmt.Sprintf("getTokenPairMetadata(%s,%s)", tokenA.Hex(), tokenB.Hex())); err != nil {
		return TokenMetadata{}, TokenMetadata{}, err
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

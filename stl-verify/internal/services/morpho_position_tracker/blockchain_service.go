package morpho_position_tracker

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"go.opentelemetry.io/otel/attribute"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
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

	return &blockchainService{
		multicallClient: multicallClient,
		morphoBlueABI:   morphoABI,
		metaMorphoABI:   metaMorphoABI,
		erc20ABI:        erc20ABI,
		metadataCache:   make(map[common.Address]TokenMetadata),
		telemetry:       telemetry,
		logger:          logger.With("component", "morpho-blockchain-service"),
	}, nil
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

	if len(results) == 0 || !results[0].Success || len(results[0].ReturnData) == 0 {
		return nil, fmt.Errorf("market() call failed for market %x", marketID[:8])
	}

	unpacked, err := s.morphoBlueABI.Unpack("market", results[0].ReturnData)
	if err != nil {
		return nil, fmt.Errorf("unpacking market() result: %w", err)
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

	// Parse market state
	if !results[0].Success || len(results[0].ReturnData) == 0 {
		return nil, nil, fmt.Errorf("market() call failed")
	}
	marketUnpacked, err := s.morphoBlueABI.Unpack("market", results[0].ReturnData)
	if err != nil {
		return nil, nil, fmt.Errorf("unpacking market(): %w", err)
	}
	if len(marketUnpacked) < 6 {
		return nil, nil, fmt.Errorf("unexpected market() return length: %d", len(marketUnpacked))
	}
	ms := &MarketState{
		TotalSupplyAssets: bigIntFromAny(marketUnpacked[0]),
		TotalSupplyShares: bigIntFromAny(marketUnpacked[1]),
		TotalBorrowAssets: bigIntFromAny(marketUnpacked[2]),
		TotalBorrowShares: bigIntFromAny(marketUnpacked[3]),
		LastUpdate:        bigIntFromAny(marketUnpacked[4]),
		Fee:               bigIntFromAny(marketUnpacked[5]),
	}

	// Parse position state
	if !results[1].Success || len(results[1].ReturnData) == 0 {
		return nil, nil, fmt.Errorf("position() call failed")
	}
	posUnpacked, err := s.morphoBlueABI.Unpack("position", results[1].ReturnData)
	if err != nil {
		return nil, nil, fmt.Errorf("unpacking position(): %w", err)
	}
	if len(posUnpacked) < 3 {
		return nil, nil, fmt.Errorf("unexpected position() return length: %d", len(posUnpacked))
	}
	ps := &PositionState{
		SupplyShares: bigIntFromAny(posUnpacked[0]),
		BorrowShares: bigIntFromAny(posUnpacked[1]),
		Collateral:   bigIntFromAny(posUnpacked[2]),
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

	// Parse market state
	if !results[0].Success || len(results[0].ReturnData) == 0 {
		return nil, nil, nil, fmt.Errorf("market() call failed")
	}
	marketUnpacked, err := s.morphoBlueABI.Unpack("market", results[0].ReturnData)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("unpacking market(): %w", err)
	}
	if len(marketUnpacked) < 6 {
		return nil, nil, nil, fmt.Errorf("unexpected market() return length: %d", len(marketUnpacked))
	}
	ms := &MarketState{
		TotalSupplyAssets: bigIntFromAny(marketUnpacked[0]),
		TotalSupplyShares: bigIntFromAny(marketUnpacked[1]),
		TotalBorrowAssets: bigIntFromAny(marketUnpacked[2]),
		TotalBorrowShares: bigIntFromAny(marketUnpacked[3]),
		LastUpdate:        bigIntFromAny(marketUnpacked[4]),
		Fee:               bigIntFromAny(marketUnpacked[5]),
	}

	// Parse position A
	if !results[1].Success || len(results[1].ReturnData) == 0 {
		return nil, nil, nil, fmt.Errorf("position(A) call failed")
	}
	posAUnpacked, err := s.morphoBlueABI.Unpack("position", results[1].ReturnData)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("unpacking position(A): %w", err)
	}
	if len(posAUnpacked) < 3 {
		return nil, nil, nil, fmt.Errorf("unexpected position(A) return length: %d", len(posAUnpacked))
	}
	psA := &PositionState{
		SupplyShares: bigIntFromAny(posAUnpacked[0]),
		BorrowShares: bigIntFromAny(posAUnpacked[1]),
		Collateral:   bigIntFromAny(posAUnpacked[2]),
	}

	// Parse position B
	if !results[2].Success || len(results[2].ReturnData) == 0 {
		return nil, nil, nil, fmt.Errorf("position(B) call failed")
	}
	posBUnpacked, err := s.morphoBlueABI.Unpack("position", results[2].ReturnData)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("unpacking position(B): %w", err)
	}
	if len(posBUnpacked) < 3 {
		return nil, nil, nil, fmt.Errorf("unexpected position(B) return length: %d", len(posBUnpacked))
	}
	psB := &PositionState{
		SupplyShares: bigIntFromAny(posBUnpacked[0]),
		BorrowShares: bigIntFromAny(posBUnpacked[1]),
		Collateral:   bigIntFromAny(posBUnpacked[2]),
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

	if !results[0].Success || len(results[0].ReturnData) == 0 {
		return nil, fmt.Errorf("totalAssets() call failed for vault %s", vaultAddress.Hex())
	}
	totalAssetsUnpacked, err := s.metaMorphoABI.Unpack("totalAssets", results[0].ReturnData)
	if err != nil {
		return nil, fmt.Errorf("unpacking totalAssets(): %w", err)
	}

	if !results[1].Success || len(results[1].ReturnData) == 0 {
		return nil, fmt.Errorf("totalSupply() call failed for vault %s", vaultAddress.Hex())
	}
	totalSupplyUnpacked, err := s.metaMorphoABI.Unpack("totalSupply", results[1].ReturnData)
	if err != nil {
		return nil, fmt.Errorf("unpacking totalSupply(): %w", err)
	}

	return &VaultState{
		TotalAssets: bigIntFromAny(totalAssetsUnpacked[0]),
		TotalSupply: bigIntFromAny(totalSupplyUnpacked[0]),
	}, nil
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

	if !results[0].Success || len(results[0].ReturnData) == 0 {
		return nil, nil, fmt.Errorf("totalAssets() call failed for vault %s", vaultAddress.Hex())
	}
	totalAssetsUnpacked, err := s.metaMorphoABI.Unpack("totalAssets", results[0].ReturnData)
	if err != nil {
		return nil, nil, fmt.Errorf("unpacking totalAssets(): %w", err)
	}

	if !results[1].Success || len(results[1].ReturnData) == 0 {
		return nil, nil, fmt.Errorf("totalSupply() call failed for vault %s", vaultAddress.Hex())
	}
	totalSupplyUnpacked, err := s.metaMorphoABI.Unpack("totalSupply", results[1].ReturnData)
	if err != nil {
		return nil, nil, fmt.Errorf("unpacking totalSupply(): %w", err)
	}

	if !results[2].Success || len(results[2].ReturnData) == 0 {
		return nil, nil, fmt.Errorf("balanceOf() call failed for vault %s", vaultAddress.Hex())
	}
	balanceUnpacked, err := s.metaMorphoABI.Unpack("balanceOf", results[2].ReturnData)
	if err != nil {
		return nil, nil, fmt.Errorf("unpacking balanceOf(): %w", err)
	}

	vs := &VaultState{
		TotalAssets: bigIntFromAny(totalAssetsUnpacked[0]),
		TotalSupply: bigIntFromAny(totalSupplyUnpacked[0]),
	}
	return vs, bigIntFromAny(balanceUnpacked[0]), nil
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

	if !results[0].Success || len(results[0].ReturnData) == 0 {
		return nil, nil, nil, fmt.Errorf("totalAssets() call failed for vault %s", vaultAddress.Hex())
	}
	totalAssetsUnpacked, err := s.metaMorphoABI.Unpack("totalAssets", results[0].ReturnData)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("unpacking totalAssets(): %w", err)
	}

	if !results[1].Success || len(results[1].ReturnData) == 0 {
		return nil, nil, nil, fmt.Errorf("totalSupply() call failed for vault %s", vaultAddress.Hex())
	}
	totalSupplyUnpacked, err := s.metaMorphoABI.Unpack("totalSupply", results[1].ReturnData)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("unpacking totalSupply(): %w", err)
	}

	if !results[2].Success || len(results[2].ReturnData) == 0 {
		return nil, nil, nil, fmt.Errorf("balanceOf(A) call failed for vault %s", vaultAddress.Hex())
	}
	balanceAUnpacked, err := s.metaMorphoABI.Unpack("balanceOf", results[2].ReturnData)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("unpacking balanceOf(A): %w", err)
	}

	if !results[3].Success || len(results[3].ReturnData) == 0 {
		return nil, nil, nil, fmt.Errorf("balanceOf(B) call failed for vault %s", vaultAddress.Hex())
	}
	balanceBUnpacked, err := s.metaMorphoABI.Unpack("balanceOf", results[3].ReturnData)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("unpacking balanceOf(B): %w", err)
	}

	vs := &VaultState{
		TotalAssets: bigIntFromAny(totalAssetsUnpacked[0]),
		TotalSupply: bigIntFromAny(totalSupplyUnpacked[0]),
	}
	return vs, bigIntFromAny(balanceAUnpacked[0]), bigIntFromAny(balanceBUnpacked[0]), nil
}

// getVaultMetadata fetches vault name, symbol, and asset address in a single Multicall3 batch.
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

	nameData, err := s.metaMorphoABI.Pack("name")
	if err != nil {
		return nil, fmt.Errorf("packing name call: %w", err)
	}
	symbolData, err := s.metaMorphoABI.Pack("symbol")
	if err != nil {
		return nil, fmt.Errorf("packing symbol call: %w", err)
	}
	assetData, err := s.metaMorphoABI.Pack("asset")
	if err != nil {
		return nil, fmt.Errorf("packing asset call: %w", err)
	}
	decimalsData, err := s.metaMorphoABI.Pack("decimals")
	if err != nil {
		return nil, fmt.Errorf("packing decimals call: %w", err)
	}

	results, err := s.multicallClient.Execute(ctx, []outbound.Call{
		{Target: vaultAddress, AllowFailure: false, CallData: nameData},
		{Target: vaultAddress, AllowFailure: false, CallData: symbolData},
		{Target: vaultAddress, AllowFailure: false, CallData: assetData},
		{Target: vaultAddress, AllowFailure: false, CallData: decimalsData},
	}, big.NewInt(blockNumber))
	if err != nil {
		return nil, fmt.Errorf("multicall vault metadata: %w", err)
	}

	if len(results) < 4 {
		return nil, fmt.Errorf("expected 4 results, got %d", len(results))
	}

	md := &VaultMetadata{}

	if results[0].Success && len(results[0].ReturnData) > 0 {
		nameUnpacked, err := s.metaMorphoABI.Unpack("name", results[0].ReturnData)
		if err == nil && len(nameUnpacked) > 0 {
			md.Name, _ = nameUnpacked[0].(string)
		}
	}

	if results[1].Success && len(results[1].ReturnData) > 0 {
		symbolUnpacked, err := s.metaMorphoABI.Unpack("symbol", results[1].ReturnData)
		if err == nil && len(symbolUnpacked) > 0 {
			md.Symbol, _ = symbolUnpacked[0].(string)
		}
	}

	if results[2].Success && len(results[2].ReturnData) > 0 {
		assetUnpacked, err := s.metaMorphoABI.Unpack("asset", results[2].ReturnData)
		if err == nil && len(assetUnpacked) > 0 {
			md.Asset, _ = assetUnpacked[0].(common.Address)
		}
	}

	if results[3].Success && len(results[3].ReturnData) > 0 {
		decimalsUnpacked, err := s.metaMorphoABI.Unpack("decimals", results[3].ReturnData)
		if err == nil && len(decimalsUnpacked) > 0 {
			md.Decimals, _ = decimalsUnpacked[0].(uint8)
		}
	}

	if md.Asset == (common.Address{}) {
		return nil, fmt.Errorf("failed to get vault asset address for %s", vaultAddress.Hex())
	}

	return md, nil
}

// getTokenMetadata fetches token symbol and decimals via ERC20 calls.
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

	md := TokenMetadata{}

	if len(results) > 0 && results[0].Success && len(results[0].ReturnData) > 0 {
		symbolUnpacked, err := s.erc20ABI.Unpack("symbol", results[0].ReturnData)
		if err == nil && len(symbolUnpacked) > 0 {
			md.Symbol, _ = symbolUnpacked[0].(string)
		}
	}

	if len(results) > 1 && results[1].Success && len(results[1].ReturnData) > 0 {
		decimalsUnpacked, err := s.erc20ABI.Unpack("decimals", results[1].ReturnData)
		if err == nil && len(decimalsUnpacked) > 0 {
			md.Decimals = intFromAny(decimalsUnpacked[0])
		}
	}

	if md.Symbol != "" {
		s.metadataCache[tokenAddress] = md
	}

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

	mdA := TokenMetadata{}
	mdB := TokenMetadata{}

	if len(results) > 0 && results[0].Success && len(results[0].ReturnData) > 0 {
		symbolUnpacked, err := s.erc20ABI.Unpack("symbol", results[0].ReturnData)
		if err == nil && len(symbolUnpacked) > 0 {
			mdA.Symbol, _ = symbolUnpacked[0].(string)
		}
	}
	if len(results) > 1 && results[1].Success && len(results[1].ReturnData) > 0 {
		decimalsUnpacked, err := s.erc20ABI.Unpack("decimals", results[1].ReturnData)
		if err == nil && len(decimalsUnpacked) > 0 {
			mdA.Decimals = intFromAny(decimalsUnpacked[0])
		}
	}
	if len(results) > 2 && results[2].Success && len(results[2].ReturnData) > 0 {
		symbolUnpacked, err := s.erc20ABI.Unpack("symbol", results[2].ReturnData)
		if err == nil && len(symbolUnpacked) > 0 {
			mdB.Symbol, _ = symbolUnpacked[0].(string)
		}
	}
	if len(results) > 3 && results[3].Success && len(results[3].ReturnData) > 0 {
		decimalsUnpacked, err := s.erc20ABI.Unpack("decimals", results[3].ReturnData)
		if err == nil && len(decimalsUnpacked) > 0 {
			mdB.Decimals = intFromAny(decimalsUnpacked[0])
		}
	}

	if mdA.Symbol != "" {
		s.metadataCache[tokenA] = mdA
	}
	if mdB.Symbol != "" {
		s.metadataCache[tokenB] = mdB
	}

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

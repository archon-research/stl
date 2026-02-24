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
	morphoData, err := s.metaMorphoABI.Pack("MORPHO")
	if err != nil {
		return nil, fmt.Errorf("packing MORPHO call: %w", err)
	}
	// skimRecipient() only exists on MetaMorpho V2; reverts on V1.1.
	skimRecipientData, err := s.metaMorphoABI.Pack("skimRecipient")
	if err != nil {
		return nil, fmt.Errorf("packing skimRecipient call: %w", err)
	}

	results, err := s.multicallClient.Execute(ctx, []outbound.Call{
		{Target: vaultAddress, AllowFailure: false, CallData: nameData},
		{Target: vaultAddress, AllowFailure: false, CallData: symbolData},
		{Target: vaultAddress, AllowFailure: false, CallData: assetData},
		{Target: vaultAddress, AllowFailure: false, CallData: decimalsData},
		{Target: vaultAddress, AllowFailure: false, CallData: morphoData},
		{Target: vaultAddress, AllowFailure: true, CallData: skimRecipientData},
	}, big.NewInt(blockNumber))
	if err != nil {
		return nil, fmt.Errorf("multicall vault metadata: %w", err)
	}

	if len(results) < 6 {
		return nil, fmt.Errorf("expected 6 results, got %d", len(results))
	}

	// Verify MORPHO() returns the Morpho Blue singleton address.
	// This distinguishes real MetaMorpho vaults from other ERC4626 vaults
	// (e.g. Yearn, Aave) that share the same Transfer/Deposit/Withdraw event topics.
	if !results[4].Success || len(results[4].ReturnData) == 0 {
		return nil, fmt.Errorf("MORPHO() call failed — not a MetaMorpho vault: %s", vaultAddress.Hex())
	}
	morphoUnpacked, err := s.metaMorphoABI.Unpack("MORPHO", results[4].ReturnData)
	if err != nil || len(morphoUnpacked) == 0 {
		return nil, fmt.Errorf("unpacking MORPHO(): %w", err)
	}
	morphoAddr, ok := morphoUnpacked[0].(common.Address)
	if !ok || morphoAddr != MorphoBlueAddress {
		return nil, fmt.Errorf("MORPHO() returned %s, expected %s — not a MetaMorpho vault", morphoAddr.Hex(), MorphoBlueAddress.Hex())
	}

	md := &VaultMetadata{Version: entity.MorphoVaultV1}
	// skimRecipient() only exists on V2 — success means V2.
	if results[5].Success {
		md.Version = entity.MorphoVaultV2
	}

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

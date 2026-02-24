package morpho_position_tracker

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

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
	logger          *slog.Logger
}

func newBlockchainService(
	multicallClient outbound.Multicaller,
	erc20ABI *abi.ABI,
	logger *slog.Logger,
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
		logger:          logger.With("component", "morpho-blockchain-service"),
	}, nil
}

// getMarketState fetches the market state from Morpho Blue at a specific block.
func (s *blockchainService) getMarketState(ctx context.Context, marketID [32]byte, blockNumber int64) (*MarketState, error) {
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

// getPositionState fetches a user's position from Morpho Blue at a specific block.
func (s *blockchainService) getPositionState(ctx context.Context, marketID [32]byte, user common.Address, blockNumber int64) (*PositionState, error) {
	callData, err := s.morphoBlueABI.Pack("position", marketID, user)
	if err != nil {
		return nil, fmt.Errorf("packing position call: %w", err)
	}

	results, err := s.multicallClient.Execute(ctx, []outbound.Call{{
		Target:       MorphoBlueAddress,
		AllowFailure: false,
		CallData:     callData,
	}}, big.NewInt(blockNumber))
	if err != nil {
		return nil, fmt.Errorf("multicall position(): %w", err)
	}

	if len(results) == 0 || !results[0].Success || len(results[0].ReturnData) == 0 {
		return nil, fmt.Errorf("position() call failed")
	}

	unpacked, err := s.morphoBlueABI.Unpack("position", results[0].ReturnData)
	if err != nil {
		return nil, fmt.Errorf("unpacking position() result: %w", err)
	}

	if len(unpacked) < 3 {
		return nil, fmt.Errorf("unexpected position() return length: %d", len(unpacked))
	}

	return &PositionState{
		SupplyShares: bigIntFromAny(unpacked[0]),
		BorrowShares: bigIntFromAny(unpacked[1]),
		Collateral:   bigIntFromAny(unpacked[2]),
	}, nil
}

// getMarketParams fetches market parameters from Morpho Blue.
func (s *blockchainService) getMarketParams(ctx context.Context, marketID [32]byte, blockNumber int64) (*MarketParamsState, error) {
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

	return &MarketParamsState{
		LoanToken:       unpacked[0].(common.Address),
		CollateralToken: unpacked[1].(common.Address),
		Oracle:          unpacked[2].(common.Address),
		Irm:             unpacked[3].(common.Address),
		LLTV:            bigIntFromAny(unpacked[4]),
	}, nil
}

// getMarketAndPositionState fetches both market and position state in a single Multicall3 batch.
func (s *blockchainService) getMarketAndPositionState(ctx context.Context, marketID [32]byte, user common.Address, blockNumber int64) (*MarketState, *PositionState, error) {
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

// getVaultState fetches vault total assets and total supply in a single Multicall3 batch.
func (s *blockchainService) getVaultState(ctx context.Context, vaultAddress common.Address, blockNumber int64) (*VaultState, error) {
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

// getVaultUserBalance fetches a user's share balance in a vault.
func (s *blockchainService) getVaultUserBalance(ctx context.Context, vaultAddress common.Address, user common.Address, blockNumber int64) (*big.Int, error) {
	callData, err := s.metaMorphoABI.Pack("balanceOf", user)
	if err != nil {
		return nil, fmt.Errorf("packing balanceOf call: %w", err)
	}

	results, err := s.multicallClient.Execute(ctx, []outbound.Call{{
		Target:       vaultAddress,
		AllowFailure: false,
		CallData:     callData,
	}}, big.NewInt(blockNumber))
	if err != nil {
		return nil, fmt.Errorf("multicall balanceOf(): %w", err)
	}

	if len(results) == 0 || !results[0].Success || len(results[0].ReturnData) == 0 {
		return nil, fmt.Errorf("balanceOf() call failed")
	}

	unpacked, err := s.metaMorphoABI.Unpack("balanceOf", results[0].ReturnData)
	if err != nil {
		return nil, fmt.Errorf("unpacking balanceOf(): %w", err)
	}

	return bigIntFromAny(unpacked[0]), nil
}

// getVaultMetadata fetches vault name, symbol, and asset address in a single Multicall3 batch.
func (s *blockchainService) getVaultMetadata(ctx context.Context, vaultAddress common.Address, blockNumber int64) (*VaultMetadata, error) {
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
func (s *blockchainService) getTokenMetadata(ctx context.Context, tokenAddress common.Address, blockNumber int64) (TokenMetadata, error) {
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
			md.Decimals = int(decimalsUnpacked[0].(uint8))
		}
	}

	if md.Symbol != "" {
		s.metadataCache[tokenAddress] = md
	}

	return md, nil
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

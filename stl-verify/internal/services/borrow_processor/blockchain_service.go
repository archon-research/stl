package borrow_processor

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"sync"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

type UserReserveData struct {
	UnderlyingAsset                 common.Address
	ScaledATokenBalance             *big.Int
	UsageAsCollateralEnabledOnUser  bool
	StableBorrowRate                *big.Int
	ScaledVariableDebt              *big.Int
	PrincipalStableDebt             *big.Int
	StableBorrowLastUpdateTimestamp *big.Int
}

type Multicall3Request struct {
	Target       common.Address
	AllowFailure bool
	CallData     []byte
}

type Multicall3Result struct {
	Success    bool   `json:"success"`
	ReturnData []byte `json:"returnData"`
}

type TokenMetadata struct {
	Decimals int
	Symbol   string
	Name     string
}

type blockchainService struct {
	ethClient             *ethclient.Client
	logger                *slog.Logger
	getUserReservesABI    *abi.ABI
	getReserveDataABI     *abi.ABI
	erc20ABI              *abi.ABI
	multicallABI          *abi.ABI
	uiPoolDataProvider    common.Address
	poolAddressesProvider common.Address
	multicall3            common.Address
	metadataCache         map[common.Address]TokenMetadata
	metadataCacheMu       sync.RWMutex
}

func newBlockchainService(ethClient *ethclient.Client, multicall3Addr, uiPoolDataProvider, poolAddressesProvider common.Address, logger *slog.Logger) (*blockchainService, error) {
	service := &blockchainService{
		ethClient:             ethClient,
		logger:                logger,
		uiPoolDataProvider:    uiPoolDataProvider,
		poolAddressesProvider: poolAddressesProvider,
		multicall3:            multicall3Addr,
		metadataCache:         make(map[common.Address]TokenMetadata),
	}

	if err := service.loadABIs(); err != nil {
		return nil, err
	}

	return service, nil
}

func (s *blockchainService) loadABIs() error {
	getUserReservesABI := `[{"inputs":[{"name":"provider","type":"address"},{"name":"user","type":"address"}],"name":"getUserReservesData","outputs":[{"components":[{"name":"underlyingAsset","type":"address"},{"name":"scaledATokenBalance","type":"uint256"},{"name":"usageAsCollateralEnabledOnUser","type":"bool"},{"name":"stableBorrowRate","type":"uint256"},{"name":"scaledVariableDebt","type":"uint256"},{"name":"principalStableDebt","type":"uint256"},{"name":"stableBorrowLastUpdateTimestamp","type":"uint256"}],"name":"","type":"tuple[]"},{"name":"","type":"uint8"}],"stateMutability":"view","type":"function"}]`
	parsedGetUserReservesABI, err := abi.JSON(strings.NewReader(getUserReservesABI))
	if err != nil {
		return fmt.Errorf("failed to parse getUserReserves ABI: %w", err)
	}
	s.getUserReservesABI = &parsedGetUserReservesABI

	getReserveDataABI := `[{"inputs":[{"name":"asset","type":"address"}],"name":"getReserveData","outputs":[{"components":[{"name":"configuration","type":"uint256"},{"name":"liquidityIndex","type":"uint128"},{"name":"currentLiquidityRate","type":"uint128"},{"name":"variableBorrowIndex","type":"uint128"},{"name":"currentVariableBorrowRate","type":"uint128"},{"name":"currentStableBorrowRate","type":"uint128"},{"name":"lastUpdateTimestamp","type":"uint40"},{"name":"id","type":"uint16"},{"name":"aTokenAddress","type":"address"},{"name":"stableDebtTokenAddress","type":"address"},{"name":"variableDebtTokenAddress","type":"address"},{"name":"interestRateStrategyAddress","type":"address"},{"name":"accruedToTreasury","type":"uint128"},{"name":"unbacked","type":"uint128"},{"name":"isolationModeTotalDebt","type":"uint128"}],"name":"","type":"tuple"}],"stateMutability":"view","type":"function"}]`
	parsedGetReserveDataABI, err := abi.JSON(strings.NewReader(getReserveDataABI))
	if err != nil {
		return fmt.Errorf("failed to parse getReserveData ABI: %w", err)
	}
	s.getReserveDataABI = &parsedGetReserveDataABI

	erc20ABI := `[{"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"name","outputs":[{"name":"","type":"string"}],"stateMutability":"view","type":"function"}]`
	parsedERC20ABI, err := abi.JSON(strings.NewReader(erc20ABI))
	if err != nil {
		return fmt.Errorf("failed to parse ERC20 ABI: %w", err)
	}
	s.erc20ABI = &parsedERC20ABI

	multicallABI := `[{"inputs":[{"components":[{"internalType":"address","name":"target","type":"address"},{"internalType":"bool","name":"allowFailure","type":"bool"},{"internalType":"bytes","name":"callData","type":"bytes"}],"internalType":"struct Multicall3.Multicall3Request[]","name":"calls","type":"tuple[]"}],"name":"aggregate3","outputs":[{"components":[{"internalType":"bool","name":"success","type":"bool"},{"internalType":"bytes","name":"returnData","type":"bytes"}],"internalType":"struct Multicall3.Result[]","name":"returnData","type":"tuple[]"}],"stateMutability":"payable","type":"function"}]`
	parsedMulticallABI, err := abi.JSON(strings.NewReader(multicallABI))
	if err != nil {
		return fmt.Errorf("failed to parse multicall ABI: %w", err)
	}
	s.multicallABI = &parsedMulticallABI

	s.logger.Info("blockchain service initialized")
	return nil
}

func (s *blockchainService) getUserReservesData(ctx context.Context, user common.Address, blockNumber int64) ([]UserReserveData, error) {
	data, err := s.getUserReservesABI.Pack("getUserReservesData", s.poolAddressesProvider, user)
	if err != nil {
		return nil, fmt.Errorf("failed to pack function call: %w", err)
	}

	msg := ethereum.CallMsg{
		To:   &s.uiPoolDataProvider,
		Data: data,
	}

	result, err := s.ethClient.CallContract(ctx, msg, big.NewInt(blockNumber))
	if err != nil {
		return nil, fmt.Errorf("failed to call contract: %w", err)
	}

	if len(result) == 0 {
		return []UserReserveData{}, nil
	}

	unpacked, err := s.getUserReservesABI.Unpack("getUserReservesData", result)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack result: %w", err)
	}

	if len(unpacked) == 0 {
		return []UserReserveData{}, nil
	}

	reserveSlice := unpacked[0].([]struct {
		UnderlyingAsset                 common.Address `json:"underlyingAsset"`
		ScaledATokenBalance             *big.Int       `json:"scaledATokenBalance"`
		UsageAsCollateralEnabledOnUser  bool           `json:"usageAsCollateralEnabledOnUser"`
		StableBorrowRate                *big.Int       `json:"stableBorrowRate"`
		ScaledVariableDebt              *big.Int       `json:"scaledVariableDebt"`
		PrincipalStableDebt             *big.Int       `json:"principalStableDebt"`
		StableBorrowLastUpdateTimestamp *big.Int       `json:"stableBorrowLastUpdateTimestamp"`
	})

	reserves := make([]UserReserveData, 0, len(reserveSlice))
	for _, r := range reserveSlice {
		if r.UnderlyingAsset == (common.Address{}) {
			continue
		}
		reserves = append(reserves, UserReserveData{
			UnderlyingAsset:                 r.UnderlyingAsset,
			ScaledATokenBalance:             r.ScaledATokenBalance,
			UsageAsCollateralEnabledOnUser:  r.UsageAsCollateralEnabledOnUser,
			StableBorrowRate:                r.StableBorrowRate,
			ScaledVariableDebt:              r.ScaledVariableDebt,
			PrincipalStableDebt:             r.PrincipalStableDebt,
			StableBorrowLastUpdateTimestamp: r.StableBorrowLastUpdateTimestamp,
		})
	}

	return reserves, nil
}

func (s *blockchainService) batchGetTokenMetadata(ctx context.Context, tokens map[common.Address]bool) (map[common.Address]TokenMetadata, error) {
	result := make(map[common.Address]TokenMetadata)

	var tokensToFetch []common.Address
	s.metadataCacheMu.RLock()
	for token := range tokens {
		if metadata, ok := s.metadataCache[token]; ok {
			result[token] = metadata
		} else {
			tokensToFetch = append(tokensToFetch, token)
		}
	}
	s.metadataCacheMu.RUnlock()

	if len(tokensToFetch) == 0 {
		return result, nil
	}

	var calls []Multicall3Request
	for _, token := range tokensToFetch {
		decimalsData, err := s.erc20ABI.Pack("decimals")
		if err != nil {
			continue
		}
		calls = append(calls, Multicall3Request{
			Target:       token,
			AllowFailure: true,
			CallData:     decimalsData,
		})

		symbolData, err := s.erc20ABI.Pack("symbol")
		if err != nil {
			continue
		}
		calls = append(calls, Multicall3Request{
			Target:       token,
			AllowFailure: true,
			CallData:     symbolData,
		})

		nameData, err := s.erc20ABI.Pack("name")
		if err != nil {
			continue
		}
		calls = append(calls, Multicall3Request{
			Target:       token,
			AllowFailure: true,
			CallData:     nameData,
		})
	}

	if len(calls) == 0 {
		return result, nil
	}

	results, err := s.executeMulticall(ctx, calls, nil)
	if err != nil {
		return result, fmt.Errorf("multicall failed: %w", err)
	}

	s.metadataCacheMu.Lock()
	defer s.metadataCacheMu.Unlock()

	for i, token := range tokensToFetch {
		idx := i * 3
		metadata := TokenMetadata{}

		if idx < len(results) && results[idx].Success && len(results[idx].ReturnData) > 0 {
			var decimals uint8
			err := s.erc20ABI.UnpackIntoInterface(&decimals, "decimals", results[idx].ReturnData)
			if err == nil {
				metadata.Decimals = int(decimals)
			}
		}

		if idx+1 < len(results) && results[idx+1].Success && len(results[idx+1].ReturnData) > 0 {
			var symbol string
			err := s.erc20ABI.UnpackIntoInterface(&symbol, "symbol", results[idx+1].ReturnData)
			if err == nil {
				metadata.Symbol = symbol
			}
		}

		if idx+2 < len(results) && results[idx+2].Success && len(results[idx+2].ReturnData) > 0 {
			var name string
			err := s.erc20ABI.UnpackIntoInterface(&name, "name", results[idx+2].ReturnData)
			if err == nil {
				metadata.Name = name
			}
		}

		result[token] = metadata
		s.metadataCache[token] = metadata
	}

	return result, nil
}

func (s *blockchainService) batchGetReserveData(ctx context.Context, protocolAddress common.Address, assets []common.Address, blockNumber int64) (map[common.Address]*big.Int, error) {
	result := make(map[common.Address]*big.Int)

	if len(assets) == 0 {
		return result, nil
	}

	var calls []Multicall3Request
	for _, asset := range assets {
		callData, err := s.getReserveDataABI.Pack("getReserveData", asset)
		if err != nil {
			continue
		}
		calls = append(calls, Multicall3Request{
			Target:       protocolAddress,
			AllowFailure: true,
			CallData:     callData,
		})
	}

	if len(calls) == 0 {
		return result, nil
	}

	blockNum := big.NewInt(blockNumber)
	results, err := s.executeMulticall(ctx, calls, blockNum)
	if err != nil {
		return result, fmt.Errorf("multicall failed: %w", err)
	}

	for i, mcResult := range results {
		if i >= len(assets) {
			break
		}
		asset := assets[i]

		if !mcResult.Success || len(mcResult.ReturnData) < 96 {
			continue
		}

		liquidityIndex := new(big.Int).SetBytes(mcResult.ReturnData[48:64])
		result[asset] = liquidityIndex
	}

	return result, nil
}

func (s *blockchainService) executeMulticall(ctx context.Context, calls []Multicall3Request, blockNumber *big.Int) ([]Multicall3Result, error) {
	if len(calls) == 0 {
		return []Multicall3Result{}, nil
	}

	data, err := s.multicallABI.Pack("aggregate3", calls)
	if err != nil {
		return nil, fmt.Errorf("failed to pack multicall: %w", err)
	}

	msg := ethereum.CallMsg{
		To:   &s.multicall3,
		Data: data,
	}

	result, err := s.ethClient.CallContract(ctx, msg, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to call multicall contract: %w", err)
	}

	unpacked, err := s.multicallABI.Unpack("aggregate3", result)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack multicall response: %w", err)
	}

	resultsRaw := unpacked[0].([]Multicall3Result)

	results := make([]Multicall3Result, len(resultsRaw))
	for i, r := range resultsRaw {
		results[i] = Multicall3Result{
			Success:    r.Success,
			ReturnData: r.ReturnData,
		}
	}

	return results, nil
}

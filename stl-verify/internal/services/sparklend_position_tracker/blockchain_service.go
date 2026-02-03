package sparklend_position_tracker

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
)

type TokenMetadata struct {
	Symbol   string
	Decimals int
	Name     string
}

type UserReserveData struct {
	UnderlyingAsset                common.Address
	ScaledATokenBalance            *big.Int
	UsageAsCollateralEnabledOnUser bool
	ScaledVariableDebt             *big.Int
	// Aave V3 specific fields (will be zero for Sparklend)
	StableBorrowRate                *big.Int
	PrincipalStableDebt             *big.Int
	StableBorrowLastUpdateTimestamp *big.Int
}

type ActualUserReserveData struct {
	Asset                    common.Address
	CurrentATokenBalance     *big.Int
	UsageAsCollateralEnabled bool
	CurrentStableDebt        *big.Int
	CurrentVariableDebt      *big.Int
	PrincipalStableDebt      *big.Int
	ScaledVariableDebt       *big.Int
	StableBorrowRate         *big.Int
	LiquidityRate            *big.Int
	StableRateLastUpdated    uint64
}

// FullReserveData contains combined data from getReserveData and getReserveConfigurationData.
type FullReserveData struct {
	// From getReserveData (Pool contract)
	Unbacked                *big.Int
	AccruedToTreasuryScaled *big.Int
	TotalAToken             *big.Int
	TotalStableDebt         *big.Int
	TotalVariableDebt       *big.Int
	LiquidityRate           *big.Int
	VariableBorrowRate      *big.Int
	StableBorrowRate        *big.Int
	AverageStableBorrowRate *big.Int
	LiquidityIndex          *big.Int
	VariableBorrowIndex     *big.Int
	LastUpdateTimestamp     int64

	// From getReserveConfigurationData (ProtocolDataProvider contract)
	Decimals                 *big.Int
	LTV                      *big.Int
	LiquidationThreshold     *big.Int
	LiquidationBonus         *big.Int
	ReserveFactor            *big.Int
	UsageAsCollateralEnabled bool
	BorrowingEnabled         bool
	StableBorrowRateEnabled  bool
	IsActive                 bool
	IsFrozen                 bool
}


type blockchainService struct {
	ethClient             *ethclient.Client
	multicallClient       outbound.Multicaller
	erc20ABI              *abi.ABI
	getUserReservesABI    *abi.ABI
	getUserReserveDataABI *abi.ABI
	uiPoolDataProvider    common.Address
	poolDataProvider      common.Address
	poolAddressesProvider common.Address
	metadataCache         map[common.Address]TokenMetadata
	logger                *slog.Logger
}

func newBlockchainService(
	ethClient *ethclient.Client,
	multicallClient outbound.Multicaller,
	erc20ABI *abi.ABI,
	uiPoolDataProvider common.Address,
	poolDataProvider common.Address,
	poolAddressesProvider common.Address,
	useAaveABI bool,
	logger *slog.Logger,
) (*blockchainService, error) {
	service := &blockchainService{
		ethClient:             ethClient,
		multicallClient:       multicallClient,
		erc20ABI:              erc20ABI,
		uiPoolDataProvider:    uiPoolDataProvider,
		poolDataProvider:      poolDataProvider,
		poolAddressesProvider: poolAddressesProvider,
		metadataCache:         make(map[common.Address]TokenMetadata),
		logger:                logger.With("component", "blockchain-service"),
	}

	if err := service.loadABIs(useAaveABI); err != nil {
		return nil, err
	}

	return service, nil
}

func (s *blockchainService) loadABIs(useAaveABI bool) error {
	var err error

	if useAaveABI {
		s.getUserReservesABI, err = abis.GetAaveUserReservesDataABI()
	} else {
		s.getUserReservesABI, err = abis.GetSparklendUserReservesDataABI()
	}
	if err != nil {
		return fmt.Errorf("failed to load getUserReservesData ABI: %w", err)
	}

	s.getUserReserveDataABI, err = abis.GetPoolDataProviderUserReserveDataABI()
	if err != nil {
		return fmt.Errorf("failed to load getUserReserveData ABI: %w", err)
	}

	s.getPoolDataProviderReserveConfigurationABI, err = abis.getPoolDataProviderReserveConfigurationABI()
	if err != nil {
		return fmt.Errorf("failed to load getReserveConfigurationData ABI: %w", err)
	}

	s.getPoolDataProviderReserveData, err = abis.getPoolDataProviderReserveData()
	if err != nil {
		return fmt.Errorf("failed to load getReserveData ABI: %w", err)
	}
	return nil
}

func (s *blockchainService) getUserReservesData(
	ctx context.Context,
	user common.Address,
	blockNumber int64,
) ([]UserReserveData, error) {
	data, err := s.getUserReservesABI.Pack(
		"getUserReservesData",
		s.poolAddressesProvider,
		user,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to pack getUserReservesData call: %w", err)
	}

	msg := ethereum.CallMsg{
		To:   &s.uiPoolDataProvider,
		Data: data,
	}

	result, err := s.ethClient.CallContract(ctx, msg, big.NewInt(blockNumber))
	if err != nil {
		return nil, fmt.Errorf("failed to call UiPoolDataProvider: %w", err)
	}
	if len(result) == 0 {
		return []UserReserveData{}, nil
	}

	unpacked, err := s.getUserReservesABI.Unpack("getUserReservesData", result)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack getUserReservesData result: %w", err)
	}
	if len(unpacked) == 0 {
		return []UserReserveData{}, nil
	}

	// Handle both Aave (7-field UserReserveData) and SparkLend (4-field UserReserveData).
	switch v := unpacked[0].(type) {

	// Aave V3 layout
	case []struct {
		UnderlyingAsset                 common.Address `json:"underlyingAsset"`
		ScaledATokenBalance             *big.Int       `json:"scaledATokenBalance"`
		UsageAsCollateralEnabledOnUser  bool           `json:"usageAsCollateralEnabledOnUser"`
		ScaledVariableDebt              *big.Int       `json:"scaledVariableDebt"`
		StableBorrowRate                *big.Int       `json:"stableBorrowRate"`
		PrincipalStableDebt             *big.Int       `json:"principalStableDebt"`
		StableBorrowLastUpdateTimestamp *big.Int       `json:"stableBorrowLastUpdateTimestamp"`
	}:
		reserves := make([]UserReserveData, 0, len(v))
		for _, r := range v {
			if (r.UnderlyingAsset == common.Address{}) {
				continue
			}
			reserves = append(reserves, UserReserveData{
				UnderlyingAsset:                 r.UnderlyingAsset,
				ScaledATokenBalance:             r.ScaledATokenBalance,
				UsageAsCollateralEnabledOnUser:  r.UsageAsCollateralEnabledOnUser,
				ScaledVariableDebt:              r.ScaledVariableDebt,
				StableBorrowRate:                r.StableBorrowRate,
				PrincipalStableDebt:             r.PrincipalStableDebt,
				StableBorrowLastUpdateTimestamp: r.StableBorrowLastUpdateTimestamp,
			})
		}
		return reserves, nil

	// SparkLend layout
	case []struct {
		UnderlyingAsset                common.Address `json:"underlyingAsset"`
		ScaledATokenBalance            *big.Int       `json:"scaledATokenBalance"`
		UsageAsCollateralEnabledOnUser bool           `json:"usageAsCollateralEnabledOnUser"`
		ScaledVariableDebt             *big.Int       `json:"scaledVariableDebt"`
	}:
		reserves := make([]UserReserveData, 0, len(v))
		for _, r := range v {
			if (r.UnderlyingAsset == common.Address{}) {
				continue
			}
			reserves = append(reserves, UserReserveData{
				UnderlyingAsset:                 r.UnderlyingAsset,
				ScaledATokenBalance:             r.ScaledATokenBalance,
				UsageAsCollateralEnabledOnUser:  r.UsageAsCollateralEnabledOnUser,
				ScaledVariableDebt:              r.ScaledVariableDebt,
				StableBorrowRate:                big.NewInt(0),
				PrincipalStableDebt:             big.NewInt(0),
				StableBorrowLastUpdateTimestamp: big.NewInt(0),
			})
		}
		return reserves, nil

	default:
		return nil, fmt.Errorf("unexpected getUserReservesData return type: %T", unpacked[0])
	}
}

func (s *blockchainService) batchGetUserReserveData(ctx context.Context, assets []common.Address, user common.Address, blockNumber int64) (map[common.Address]ActualUserReserveData, error) {
	if len(assets) == 0 {
		return make(map[common.Address]ActualUserReserveData), nil
	}

	calls := make([]outbound.Call, 0, len(assets))
	for _, asset := range assets {
		callData, err := s.getUserReserveDataABI.Pack("getUserReserveData", asset, user)
		if err != nil {
			s.logger.Warn("failed to pack getUserReserveData call", "asset", asset.Hex(), "error", err)
			continue
		}

		calls = append(calls, outbound.Call{
			Target:       s.poolDataProvider,
			AllowFailure: true,
			CallData:     callData,
		})
	}

	if len(calls) == 0 {
		return make(map[common.Address]ActualUserReserveData), nil
	}

	results, err := s.multicallClient.Execute(ctx, calls, big.NewInt(blockNumber))
	if err != nil {
		return nil, fmt.Errorf("multicall failed: %w", err)
	}

	dataMap := make(map[common.Address]ActualUserReserveData)
	for i, result := range results {
		if !result.Success || len(result.ReturnData) == 0 {
			s.logger.Debug("getUserReserveData call failed", "asset", assets[i].Hex())
			continue
		}

		unpacked, err := s.getUserReserveDataABI.Unpack("getUserReserveData", result.ReturnData)
		if err != nil {
			s.logger.Warn("failed to unpack getUserReserveData result", "asset", assets[i].Hex(), "error", err)
			continue
		}

		if len(unpacked) < 9 {
			s.logger.Warn("unexpected getUserReserveData result length", "asset", assets[i].Hex(), "length", len(unpacked))
			continue
		}

		bi, ok := unpacked[7].(*big.Int)
		if !ok || bi == nil {
			s.logger.Warn("unexpected type for stableRateLastUpdated",
				"asset", assets[i].Hex(),
				"type", fmt.Sprintf("%T", unpacked[7]),
			)
			continue
		}

		dataMap[assets[i]] = ActualUserReserveData{
			Asset:                    assets[i],
			CurrentATokenBalance:     unpacked[0].(*big.Int),
			CurrentStableDebt:        unpacked[1].(*big.Int),
			CurrentVariableDebt:      unpacked[2].(*big.Int),
			PrincipalStableDebt:      unpacked[3].(*big.Int),
			ScaledVariableDebt:       unpacked[4].(*big.Int),
			StableBorrowRate:         unpacked[5].(*big.Int),
			LiquidityRate:            unpacked[6].(*big.Int),
			StableRateLastUpdated:    bi.Uint64(), // ← fixed
			UsageAsCollateralEnabled: unpacked[8].(bool),
		}
	}

	return dataMap, nil
}

func (s *blockchainService) batchGetTokenMetadata(ctx context.Context, tokens map[common.Address]bool) (map[common.Address]TokenMetadata, error) {
	tokensToFetch := make([]common.Address, 0)
	result := make(map[common.Address]TokenMetadata)

	for token := range tokens {
		if cached, ok := s.metadataCache[token]; ok {
			result[token] = cached
		} else {
			tokensToFetch = append(tokensToFetch, token)
		}
	}

	if len(tokensToFetch) == 0 {
		return result, nil
	}

	calls := make([]outbound.Call, 0, len(tokensToFetch)*3)
	for _, token := range tokensToFetch {
		decimalsData, _ := s.erc20ABI.Pack("decimals")
		symbolData, _ := s.erc20ABI.Pack("symbol")
		nameData, _ := s.erc20ABI.Pack("name")

		calls = append(calls,
			outbound.Call{Target: token, AllowFailure: true, CallData: decimalsData},
			outbound.Call{Target: token, AllowFailure: true, CallData: symbolData},
			outbound.Call{Target: token, AllowFailure: true, CallData: nameData},
		)
	}

	results, err := s.multicallClient.Execute(ctx, calls, nil)
	if err != nil {
		return nil, fmt.Errorf("multicall failed: %w", err)
	}

	for i := 0; i < len(tokensToFetch); i++ {
		token := tokensToFetch[i]
		decimalsIdx := i * 3
		symbolIdx := i*3 + 1
		nameIdx := i*3 + 2

		var decimals int
		var symbol, name string

		if results[decimalsIdx].Success && len(results[decimalsIdx].ReturnData) > 0 {
			unpacked, err := s.erc20ABI.Unpack("decimals", results[decimalsIdx].ReturnData)
			if err == nil && len(unpacked) > 0 {
				if d, ok := unpacked[0].(uint8); ok {
					decimals = int(d)
				}
			}
		}

		if results[symbolIdx].Success && len(results[symbolIdx].ReturnData) > 0 {
			unpacked, err := s.erc20ABI.Unpack("symbol", results[symbolIdx].ReturnData)
			if err == nil && len(unpacked) > 0 {
				if s, ok := unpacked[0].(string); ok {
					symbol = s
				}
			}
		}

		if results[nameIdx].Success && len(results[nameIdx].ReturnData) > 0 {
			unpacked, err := s.erc20ABI.Unpack("name", results[nameIdx].ReturnData)
			if err == nil && len(unpacked) > 0 {
				if n, ok := unpacked[0].(string); ok {
					name = n
				}
			}
		}

		metadata := TokenMetadata{
			Symbol:   symbol,
			Decimals: decimals,
			Name:     name,
		}

		s.metadataCache[token] = metadata
		result[token] = metadata
	}

	return result, nil
}

// getFullReserveData fetches both reserve data from Pool and configuration data from ProtocolDataProvider.
func (s *blockchainService) getFullReserveData(ctx context.Context, poolAddress common.Address, asset common.Address, blockNumber int64) (*FullReserveData, error) {
	// Build multicall requests for both getReserveData and getReserveConfigurationData
	getReserveDataCallData, err := s.getReserveDataABI.Pack("getReserveData", asset)
	if err != nil {
		return nil, fmt.Errorf("failed to pack getReserveData: %w", err)
	}

	getConfigCallData, err := s.getReserveConfigurationDataABI.Pack("getReserveConfigurationData", asset)
	if err != nil {
		return nil, fmt.Errorf("failed to pack getReserveConfigurationData: %w", err)
	}

	calls := []Multicall3Request{
		{
			Target:       poolAddress,
			AllowFailure: false,
			CallData:     getReserveDataCallData,
		},
		{
			Target:       s.aaveProtocolDataProvider,
			AllowFailure: false,
			CallData:     getConfigCallData,
		},
	}

	results, err := s.executeMulticall(ctx, calls, big.NewInt(blockNumber))
	if err != nil {
		return nil, fmt.Errorf("multicall failed: %w", err)
	}

	if len(results) < 2 {
		return nil, fmt.Errorf("expected 2 results, got %d", len(results))
	}

	// Parse getReserveData result
	if !results[0].Success {
		return nil, fmt.Errorf("getReserveData call failed")
	}
	reserveData, err := s.parseReserveData(results[0].ReturnData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse reserve data: %w", err)
	}

	// Parse getReserveConfigurationData result
	if !results[1].Success {
		return nil, fmt.Errorf("getReserveConfigurationData call failed")
	}
	configData, err := s.parseReserveConfigurationData(results[1].ReturnData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse configuration data: %w", err)
	}

	// Merge into FullReserveData
	return &FullReserveData{
		// From getReserveData
		Unbacked:                reserveData.Unbacked,
		AccruedToTreasuryScaled: reserveData.AccruedToTreasuryScaled,
		TotalAToken:             reserveData.TotalAToken,
		TotalStableDebt:         reserveData.TotalStableDebt,
		TotalVariableDebt:       reserveData.TotalVariableDebt,
		LiquidityRate:           reserveData.LiquidityRate,
		VariableBorrowRate:      reserveData.VariableBorrowRate,
		StableBorrowRate:        reserveData.StableBorrowRate,
		AverageStableBorrowRate: reserveData.AverageStableBorrowRate,
		LiquidityIndex:          reserveData.LiquidityIndex,
		VariableBorrowIndex:     reserveData.VariableBorrowIndex,
		LastUpdateTimestamp:     reserveData.LastUpdateTimestamp,
		// From getReserveConfigurationData
		Decimals:                 configData.Decimals,
		LTV:                      configData.LTV,
		LiquidationThreshold:     configData.LiquidationThreshold,
		LiquidationBonus:         configData.LiquidationBonus,
		ReserveFactor:            configData.ReserveFactor,
		UsageAsCollateralEnabled: configData.UsageAsCollateralEnabled,
		BorrowingEnabled:         configData.BorrowingEnabled,
		StableBorrowRateEnabled:  configData.StableBorrowRateEnabled,
		IsActive:                 configData.IsActive,
		IsFrozen:                 configData.IsFrozen,
	}, nil
}

// reserveDataFromPool holds parsed data from Pool.getReserveData
type reserveDataFromPool struct {
	Unbacked                *big.Int
	AccruedToTreasuryScaled *big.Int
	TotalAToken             *big.Int
	TotalStableDebt         *big.Int
	TotalVariableDebt       *big.Int
	LiquidityRate           *big.Int
	VariableBorrowRate      *big.Int
	StableBorrowRate        *big.Int
	AverageStableBorrowRate *big.Int
	LiquidityIndex          *big.Int
	VariableBorrowIndex     *big.Int
	LastUpdateTimestamp     int64
}

// parseReserveData parses the raw bytes from Pool.getReserveData into structured data.
func (s *blockchainService) parseReserveData(data []byte) (*reserveDataFromPool, error) {
	unpacked, err := s.getReserveDataABI.Unpack("getReserveData", data)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack getReserveData: %w", err)
	}

	if len(unpacked) == 0 {
		return nil, fmt.Errorf("empty result from getReserveData")
	}

	// The result is a struct tuple
	reserveTuple := unpacked[0].(struct {
		Configuration               *big.Int       `json:"configuration"`
		LiquidityIndex              *big.Int       `json:"liquidityIndex"`
		CurrentLiquidityRate        *big.Int       `json:"currentLiquidityRate"`
		VariableBorrowIndex         *big.Int       `json:"variableBorrowIndex"`
		CurrentVariableBorrowRate   *big.Int       `json:"currentVariableBorrowRate"`
		CurrentStableBorrowRate     *big.Int       `json:"currentStableBorrowRate"`
		LastUpdateTimestamp         *big.Int       `json:"lastUpdateTimestamp"`
		Id                          uint16         `json:"id"`
		ATokenAddress               common.Address `json:"aTokenAddress"`
		StableDebtTokenAddress      common.Address `json:"stableDebtTokenAddress"`
		VariableDebtTokenAddress    common.Address `json:"variableDebtTokenAddress"`
		InterestRateStrategyAddress common.Address `json:"interestRateStrategyAddress"`
		AccruedToTreasury           *big.Int       `json:"accruedToTreasury"`
		Unbacked                    *big.Int       `json:"unbacked"`
		IsolationModeTotalDebt      *big.Int       `json:"isolationModeTotalDebt"`
	})

	return &reserveDataFromPool{
		LiquidityIndex:          reserveTuple.LiquidityIndex,
		LiquidityRate:           reserveTuple.CurrentLiquidityRate,
		VariableBorrowIndex:     reserveTuple.VariableBorrowIndex,
		VariableBorrowRate:      reserveTuple.CurrentVariableBorrowRate,
		StableBorrowRate:        reserveTuple.CurrentStableBorrowRate,
		AccruedToTreasuryScaled: reserveTuple.AccruedToTreasury,
		Unbacked:                reserveTuple.Unbacked,
		LastUpdateTimestamp:     reserveTuple.LastUpdateTimestamp.Int64(),
		// Note: TotalAToken, TotalStableDebt, TotalVariableDebt, AverageStableBorrowRate
		// are not directly available from getReserveData - they need to be fetched separately
		// or computed. For now, set to nil/zero.
		TotalAToken:             big.NewInt(0),
		TotalStableDebt:         big.NewInt(0),
		TotalVariableDebt:       big.NewInt(0),
		AverageStableBorrowRate: big.NewInt(0),
	}, nil
}

// reserveConfigData holds parsed data from ProtocolDataProvider.getReserveConfigurationData
type reserveConfigData struct {
	Decimals                 *big.Int
	LTV                      *big.Int
	LiquidationThreshold     *big.Int
	LiquidationBonus         *big.Int
	ReserveFactor            *big.Int
	UsageAsCollateralEnabled bool
	BorrowingEnabled         bool
	StableBorrowRateEnabled  bool
	IsActive                 bool
	IsFrozen                 bool
}

// parseReserveConfigurationData parses the raw bytes from getReserveConfigurationData.
func (s *blockchainService) parseReserveConfigurationData(data []byte) (*reserveConfigData, error) {
	unpacked, err := s.getReserveConfigurationDataABI.Unpack("getReserveConfigurationData", data)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack getReserveConfigurationData: %w", err)
	}

	if len(unpacked) < 10 {
		return nil, fmt.Errorf("expected 10 values from getReserveConfigurationData, got %d", len(unpacked))
	}

	return &reserveConfigData{
		Decimals:                 unpacked[0].(*big.Int),
		LTV:                      unpacked[1].(*big.Int),
		LiquidationThreshold:     unpacked[2].(*big.Int),
		LiquidationBonus:         unpacked[3].(*big.Int),
		ReserveFactor:            unpacked[4].(*big.Int),
		UsageAsCollateralEnabled: unpacked[5].(bool),
		BorrowingEnabled:         unpacked[6].(bool),
		StableBorrowRateEnabled:  unpacked[7].(bool),
		IsActive:                 unpacked[8].(bool),
		IsFrozen:                 unpacked[9].(bool),
	}, nil
}

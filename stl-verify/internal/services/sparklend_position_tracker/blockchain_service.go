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
	ethClient                                  *ethclient.Client
	multicallClient                            outbound.Multicaller
	erc20ABI                                   *abi.ABI
	getUserReservesABI                         *abi.ABI
	getUserReserveDataABI                      *abi.ABI
	getPoolDataProviderReserveConfigurationABI *abi.ABI
	getPoolDataProviderReserveData             *abi.ABI
	uiPoolDataProvider                         common.Address
	poolDataProvider                           common.Address
	poolAddressesProvider                      common.Address
	metadataCache                              map[common.Address]TokenMetadata
	logger                                     *slog.Logger
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

	s.getPoolDataProviderReserveConfigurationABI, err = abis.GetPoolDataProviderReserveConfigurationABI()
	if err != nil {
		return fmt.Errorf("failed to load getReserveConfigurationData ABI: %w", err)
	}

	s.getPoolDataProviderReserveData, err = abis.GetPoolDataProviderReserveData()
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

// getFullReserveData fetches both reserve data and configuration data from ProtocolDataProvider.
func (s *blockchainService) getFullReserveData(ctx context.Context, asset common.Address, blockNumber int64) (*FullReserveData, error) {
	// Build multicall requests for both getReserveData and getReserveConfigurationData
	getReserveDataCallData, err := s.getPoolDataProviderReserveData.Pack("getReserveData", asset)
	if err != nil {
		return nil, fmt.Errorf("failed to pack getReserveData: %w", err)
	}

	getConfigCallData, err := s.getPoolDataProviderReserveConfigurationABI.Pack("getReserveConfigurationData", asset)
	if err != nil {
		return nil, fmt.Errorf("failed to pack getReserveConfigurationData: %w", err)
	}

	calls := []outbound.Call{
		{
			Target:       s.poolDataProvider,
			AllowFailure: false,
			CallData:     getReserveDataCallData,
		},
		{
			Target:       s.poolDataProvider,
			AllowFailure: false,
			CallData:     getConfigCallData,
		},
	}

	results, err := s.multicallClient.Execute(ctx, calls, big.NewInt(blockNumber))
	if err != nil {
		return nil, fmt.Errorf("multicall failed: %w", err)
	}

	if len(results) < 2 {
		return nil, fmt.Errorf("expected 2 results, got %d", len(results))
	}

	// Parse getReserveData result
	if !results[0].Success {
		return nil, fmt.Errorf("getReserveData call failed for asset %s at block %d", asset.Hex(), blockNumber)
	}
	reserveData, err := s.parseReserveData(results[0].ReturnData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse reserve data for asset %s at block %d: %w", asset.Hex(), blockNumber, err)
	}

	// Parse getReserveConfigurationData result
	if !results[1].Success {
		return nil, fmt.Errorf("getReserveConfigurationData call failed for asset %s at block %d", asset.Hex(), blockNumber)
	}
	configData, err := s.parseReserveConfigurationData(results[1].ReturnData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse configuration data for asset %s at block %d: %w", asset.Hex(), blockNumber, err)
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

// reserveDataFromProvider holds parsed data from ProtocolDataProvider.getReserveData
type reserveDataFromProvider struct {
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

// parseReserveData parses the raw bytes from ProtocolDataProvider.getReserveData into structured data.
func (s *blockchainService) parseReserveData(data []byte) (*reserveDataFromProvider, error) {
	unpacked, err := s.getPoolDataProviderReserveData.Unpack("getReserveData", data)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack getReserveData: %w", err)
	}

	if len(unpacked) < 12 {
		return nil, fmt.Errorf("expected 12 values from getReserveData, got %d", len(unpacked))
	}

	result := &reserveDataFromProvider{}

	if v, ok := unpacked[0].(*big.Int); ok {
		result.Unbacked = v
	} else {
		return nil, fmt.Errorf("unpacked[0] expected *big.Int, got %T", unpacked[0])
	}

	if v, ok := unpacked[1].(*big.Int); ok {
		result.AccruedToTreasuryScaled = v
	} else {
		return nil, fmt.Errorf("unpacked[1] expected *big.Int, got %T", unpacked[1])
	}

	if v, ok := unpacked[2].(*big.Int); ok {
		result.TotalAToken = v
	} else {
		return nil, fmt.Errorf("unpacked[2] expected *big.Int, got %T", unpacked[2])
	}

	if v, ok := unpacked[3].(*big.Int); ok {
		result.TotalStableDebt = v
	} else {
		return nil, fmt.Errorf("unpacked[3] expected *big.Int, got %T", unpacked[3])
	}

	if v, ok := unpacked[4].(*big.Int); ok {
		result.TotalVariableDebt = v
	} else {
		return nil, fmt.Errorf("unpacked[4] expected *big.Int, got %T", unpacked[4])
	}

	if v, ok := unpacked[5].(*big.Int); ok {
		result.LiquidityRate = v
	} else {
		return nil, fmt.Errorf("unpacked[5] expected *big.Int, got %T", unpacked[5])
	}

	if v, ok := unpacked[6].(*big.Int); ok {
		result.VariableBorrowRate = v
	} else {
		return nil, fmt.Errorf("unpacked[6] expected *big.Int, got %T", unpacked[6])
	}

	if v, ok := unpacked[7].(*big.Int); ok {
		result.StableBorrowRate = v
	} else {
		return nil, fmt.Errorf("unpacked[7] expected *big.Int, got %T", unpacked[7])
	}

	if v, ok := unpacked[8].(*big.Int); ok {
		result.AverageStableBorrowRate = v
	} else {
		return nil, fmt.Errorf("unpacked[8] expected *big.Int, got %T", unpacked[8])
	}

	if v, ok := unpacked[9].(*big.Int); ok {
		result.LiquidityIndex = v
	} else {
		return nil, fmt.Errorf("unpacked[9] expected *big.Int, got %T", unpacked[9])
	}

	if v, ok := unpacked[10].(*big.Int); ok {
		result.VariableBorrowIndex = v
	} else {
		return nil, fmt.Errorf("unpacked[10] expected *big.Int, got %T", unpacked[10])
	}

	if v, ok := unpacked[11].(*big.Int); ok {
		result.LastUpdateTimestamp = v.Int64()
	} else {
		return nil, fmt.Errorf("unpacked[11] expected *big.Int, got %T", unpacked[11])
	}

	return result, nil
}

// reserveConfigData holds parsed data from PoolDataProvider.getReserveConfigurationData
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
	unpacked, err := s.getPoolDataProviderReserveConfigurationABI.Unpack("getReserveConfigurationData", data)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack getReserveConfigurationData: %w", err)
	}

	if len(unpacked) < 10 {
		return nil, fmt.Errorf("expected 10 values from getReserveConfigurationData, got %d", len(unpacked))
	}

	result := &reserveConfigData{}

	if v, ok := unpacked[0].(*big.Int); ok {
		result.Decimals = v
	} else {
		return nil, fmt.Errorf("unpacked[0] expected *big.Int, got %T", unpacked[0])
	}

	if v, ok := unpacked[1].(*big.Int); ok {
		result.LTV = v
	} else {
		return nil, fmt.Errorf("unpacked[1] expected *big.Int, got %T", unpacked[1])
	}

	if v, ok := unpacked[2].(*big.Int); ok {
		result.LiquidationThreshold = v
	} else {
		return nil, fmt.Errorf("unpacked[2] expected *big.Int, got %T", unpacked[2])
	}

	if v, ok := unpacked[3].(*big.Int); ok {
		result.LiquidationBonus = v
	} else {
		return nil, fmt.Errorf("unpacked[3] expected *big.Int, got %T", unpacked[3])
	}

	if v, ok := unpacked[4].(*big.Int); ok {
		result.ReserveFactor = v
	} else {
		return nil, fmt.Errorf("unpacked[4] expected *big.Int, got %T", unpacked[4])
	}

	if v, ok := unpacked[5].(bool); ok {
		result.UsageAsCollateralEnabled = v
	} else {
		return nil, fmt.Errorf("unpacked[5] expected bool, got %T", unpacked[5])
	}

	if v, ok := unpacked[6].(bool); ok {
		result.BorrowingEnabled = v
	} else {
		return nil, fmt.Errorf("unpacked[6] expected bool, got %T", unpacked[6])
	}

	if v, ok := unpacked[7].(bool); ok {
		result.StableBorrowRateEnabled = v
	} else {
		return nil, fmt.Errorf("unpacked[7] expected bool, got %T", unpacked[7])
	}

	if v, ok := unpacked[8].(bool); ok {
		result.IsActive = v
	} else {
		return nil, fmt.Errorf("unpacked[8] expected bool, got %T", unpacked[8])
	}

	if v, ok := unpacked[9].(bool); ok {
		result.IsFrozen = v
	} else {
		return nil, fmt.Errorf("unpacked[9] expected bool, got %T", unpacked[9])
	}

	return result, nil
}

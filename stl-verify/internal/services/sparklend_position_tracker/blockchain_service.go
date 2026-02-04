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

// Constants for getReserveData return value indices
const (
	reserveDataUnbackedIdx                = 0
	reserveDataAccruedToTreasuryIdx       = 1
	reserveDataTotalATokenIdx             = 2
	reserveDataTotalStableDebtIdx         = 3
	reserveDataTotalVariableDebtIdx       = 4
	reserveDataLiquidityRateIdx           = 5
	reserveDataVariableBorrowRateIdx      = 6
	reserveDataStableBorrowRateIdx        = 7
	reserveDataAverageStableBorrowRateIdx = 8
	reserveDataLiquidityIndexIdx          = 9
	reserveDataVariableBorrowIndexIdx     = 10
	reserveDataLastUpdateTimestampIdx     = 11
)

// Constants for getReserveConfigurationData return value indices
const (
	reserveConfigDecimalsIdx                 = 0
	reserveConfigLTVIdx                      = 1
	reserveConfigLiquidationThresholdIdx     = 2
	reserveConfigLiquidationBonusIdx         = 3
	reserveConfigReserveFactorIdx            = 4
	reserveConfigUsageAsCollateralEnabledIdx = 5
	reserveConfigBorrowingEnabledIdx         = 6
	reserveConfigStableBorrowRateEnabledIdx  = 7
	reserveConfigIsActiveIdx                 = 8
	reserveConfigIsFrozenIdx                 = 9
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

type blockchainService struct {
	ethClient                                  *ethclient.Client
	multicallClient                            outbound.Multicaller
	erc20ABI                                   *abi.ABI
	getUserReservesABI                         *abi.ABI
	getUserReserveDataABI                      *abi.ABI
	getPoolDataProviderReserveConfigurationABI *abi.ABI
	getPoolDataProviderReserveDataABI          *abi.ABI
	uiPoolDataProvider                         common.Address
	poolDataProvider                           common.Address
	poolAddressesProvider                      common.Address
	metadataCache                              map[common.Address]TokenMetadata
	logger                                     *slog.Logger
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

	s.getPoolDataProviderReserveDataABI, err = abis.GetPoolDataProviderReserveData()
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
func (s *blockchainService) getFullReserveData(ctx context.Context, asset common.Address, blockNumber int64) (*reserveDataFromProvider, *reserveConfigData, error) {
	// Build multicall requests for both getReserveData and getReserveConfigurationData
	getReserveDataCallData, err := s.getPoolDataProviderReserveDataABI.Pack("getReserveData", asset)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to pack getReserveData: %w", err)
	}

	getConfigCallData, err := s.getPoolDataProviderReserveConfigurationABI.Pack("getReserveConfigurationData", asset)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to pack getReserveConfigurationData: %w", err)
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
		return nil, nil, fmt.Errorf("multicall failed: %w", err)
	}

	if len(results) < 2 {
		return nil, nil, fmt.Errorf("expected 2 results, got %d", len(results))
	}

	// Parse getReserveData result
	if !results[0].Success {
		return nil, nil, fmt.Errorf("getReserveData call failed for asset %s at block %d", asset.Hex(), blockNumber)
	}
	reserveData, err := s.parseReserveData(results[0].ReturnData)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse reserve data for asset %s at block %d: %w", asset.Hex(), blockNumber, err)
	}

	// Parse getReserveConfigurationData result
	if !results[1].Success {
		return nil, nil, fmt.Errorf("getReserveConfigurationData call failed for asset %s at block %d", asset.Hex(), blockNumber)
	}
	configData, err := s.parseReserveConfigurationData(results[1].ReturnData)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse configuration data for asset %s at block %d: %w", asset.Hex(), blockNumber, err)
	}

	return reserveData, configData, nil
}

// parseReserveData parses the raw bytes from ProtocolDataProvider.getReserveData into structured data.
func (s *blockchainService) parseReserveData(data []byte) (*reserveDataFromProvider, error) {
	unpacked, err := s.getPoolDataProviderReserveDataABI.Unpack("getReserveData", data)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack getReserveData: %w", err)
	}

	if len(unpacked) < 12 {
		return nil, fmt.Errorf("expected 12 values from getReserveData, got %d", len(unpacked))
	}

	result := &reserveDataFromProvider{}

	if v, ok := unpacked[reserveDataUnbackedIdx].(*big.Int); ok {
		result.Unbacked = v
	} else {
		return nil, fmt.Errorf("unpacked[%d] (unbacked) expected *big.Int, got %T", reserveDataUnbackedIdx, unpacked[reserveDataUnbackedIdx])
	}

	if v, ok := unpacked[reserveDataAccruedToTreasuryIdx].(*big.Int); ok {
		result.AccruedToTreasuryScaled = v
	} else {
		return nil, fmt.Errorf("unpacked[%d] (accruedToTreasuryScaled) expected *big.Int, got %T", reserveDataAccruedToTreasuryIdx, unpacked[reserveDataAccruedToTreasuryIdx])
	}

	if v, ok := unpacked[reserveDataTotalATokenIdx].(*big.Int); ok {
		result.TotalAToken = v
	} else {
		return nil, fmt.Errorf("unpacked[%d] (totalAToken) expected *big.Int, got %T", reserveDataTotalATokenIdx, unpacked[reserveDataTotalATokenIdx])
	}

	if v, ok := unpacked[reserveDataTotalStableDebtIdx].(*big.Int); ok {
		result.TotalStableDebt = v
	} else {
		return nil, fmt.Errorf("unpacked[%d] (totalStableDebt) expected *big.Int, got %T", reserveDataTotalStableDebtIdx, unpacked[reserveDataTotalStableDebtIdx])
	}

	if v, ok := unpacked[reserveDataTotalVariableDebtIdx].(*big.Int); ok {
		result.TotalVariableDebt = v
	} else {
		return nil, fmt.Errorf("unpacked[%d] (totalVariableDebt) expected *big.Int, got %T", reserveDataTotalVariableDebtIdx, unpacked[reserveDataTotalVariableDebtIdx])
	}

	if v, ok := unpacked[reserveDataLiquidityRateIdx].(*big.Int); ok {
		result.LiquidityRate = v
	} else {
		return nil, fmt.Errorf("unpacked[%d] (liquidityRate) expected *big.Int, got %T", reserveDataLiquidityRateIdx, unpacked[reserveDataLiquidityRateIdx])
	}

	if v, ok := unpacked[reserveDataVariableBorrowRateIdx].(*big.Int); ok {
		result.VariableBorrowRate = v
	} else {
		return nil, fmt.Errorf("unpacked[%d] (variableBorrowRate) expected *big.Int, got %T", reserveDataVariableBorrowRateIdx, unpacked[reserveDataVariableBorrowRateIdx])
	}

	if v, ok := unpacked[reserveDataStableBorrowRateIdx].(*big.Int); ok {
		result.StableBorrowRate = v
	} else {
		return nil, fmt.Errorf("unpacked[%d] (stableBorrowRate) expected *big.Int, got %T", reserveDataStableBorrowRateIdx, unpacked[reserveDataStableBorrowRateIdx])
	}

	if v, ok := unpacked[reserveDataAverageStableBorrowRateIdx].(*big.Int); ok {
		result.AverageStableBorrowRate = v
	} else {
		return nil, fmt.Errorf("unpacked[%d] (averageStableBorrowRate) expected *big.Int, got %T", reserveDataAverageStableBorrowRateIdx, unpacked[reserveDataAverageStableBorrowRateIdx])
	}

	if v, ok := unpacked[reserveDataLiquidityIndexIdx].(*big.Int); ok {
		result.LiquidityIndex = v
	} else {
		return nil, fmt.Errorf("unpacked[%d] (liquidityIndex) expected *big.Int, got %T", reserveDataLiquidityIndexIdx, unpacked[reserveDataLiquidityIndexIdx])
	}

	if v, ok := unpacked[reserveDataVariableBorrowIndexIdx].(*big.Int); ok {
		result.VariableBorrowIndex = v
	} else {
		return nil, fmt.Errorf("unpacked[%d] (variableBorrowIndex) expected *big.Int, got %T", reserveDataVariableBorrowIndexIdx, unpacked[reserveDataVariableBorrowIndexIdx])
	}

	if v, ok := unpacked[reserveDataLastUpdateTimestampIdx].(*big.Int); ok {
		result.LastUpdateTimestamp = v.Int64()
	} else {
		return nil, fmt.Errorf("unpacked[%d] (lastUpdateTimestamp) expected *big.Int, got %T", reserveDataLastUpdateTimestampIdx, unpacked[reserveDataLastUpdateTimestampIdx])
	}

	return result, nil
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

	if v, ok := unpacked[reserveConfigDecimalsIdx].(*big.Int); ok {
		result.Decimals = v
	} else {
		return nil, fmt.Errorf("unpacked[%d] (decimals) expected *big.Int, got %T", reserveConfigDecimalsIdx, unpacked[reserveConfigDecimalsIdx])
	}

	if v, ok := unpacked[reserveConfigLTVIdx].(*big.Int); ok {
		result.LTV = v
	} else {
		return nil, fmt.Errorf("unpacked[%d] (ltv) expected *big.Int, got %T", reserveConfigLTVIdx, unpacked[reserveConfigLTVIdx])
	}

	if v, ok := unpacked[reserveConfigLiquidationThresholdIdx].(*big.Int); ok {
		result.LiquidationThreshold = v
	} else {
		return nil, fmt.Errorf("unpacked[%d] (liquidationThreshold) expected *big.Int, got %T", reserveConfigLiquidationThresholdIdx, unpacked[reserveConfigLiquidationThresholdIdx])
	}

	if v, ok := unpacked[reserveConfigLiquidationBonusIdx].(*big.Int); ok {
		result.LiquidationBonus = v
	} else {
		return nil, fmt.Errorf("unpacked[%d] (liquidationBonus) expected *big.Int, got %T", reserveConfigLiquidationBonusIdx, unpacked[reserveConfigLiquidationBonusIdx])
	}

	if v, ok := unpacked[reserveConfigReserveFactorIdx].(*big.Int); ok {
		result.ReserveFactor = v
	} else {
		return nil, fmt.Errorf("unpacked[%d] (reserveFactor) expected *big.Int, got %T", reserveConfigReserveFactorIdx, unpacked[reserveConfigReserveFactorIdx])
	}

	if v, ok := unpacked[reserveConfigUsageAsCollateralEnabledIdx].(bool); ok {
		result.UsageAsCollateralEnabled = v
	} else {
		return nil, fmt.Errorf("unpacked[%d] (usageAsCollateralEnabled) expected bool, got %T", reserveConfigUsageAsCollateralEnabledIdx, unpacked[reserveConfigUsageAsCollateralEnabledIdx])
	}

	if v, ok := unpacked[reserveConfigBorrowingEnabledIdx].(bool); ok {
		result.BorrowingEnabled = v
	} else {
		return nil, fmt.Errorf("unpacked[%d] (borrowingEnabled) expected bool, got %T", reserveConfigBorrowingEnabledIdx, unpacked[reserveConfigBorrowingEnabledIdx])
	}

	if v, ok := unpacked[reserveConfigStableBorrowRateEnabledIdx].(bool); ok {
		result.StableBorrowRateEnabled = v
	} else {
		return nil, fmt.Errorf("unpacked[%d] (stableBorrowRateEnabled) expected bool, got %T", reserveConfigStableBorrowRateEnabledIdx, unpacked[reserveConfigStableBorrowRateEnabledIdx])
	}

	if v, ok := unpacked[reserveConfigIsActiveIdx].(bool); ok {
		result.IsActive = v
	} else {
		return nil, fmt.Errorf("unpacked[%d] (isActive) expected bool, got %T", reserveConfigIsActiveIdx, unpacked[reserveConfigIsActiveIdx])
	}

	if v, ok := unpacked[reserveConfigIsFrozenIdx].(bool); ok {
		result.IsFrozen = v
	} else {
		return nil, fmt.Errorf("unpacked[%d] (isFrozen) expected bool, got %T", reserveConfigIsFrozenIdx, unpacked[reserveConfigIsFrozenIdx])
	}

	return result, nil
}

package sparklend_position_tracker

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
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
	// Aave V3 specific fields (will be zero for Sparklend and V2)
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
	protocolVersion                            blockchain.ProtocolVersion
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
	protocolVersion blockchain.ProtocolVersion,
	logger *slog.Logger,
) (*blockchainService, error) {
	service := &blockchainService{
		ethClient:             ethClient,
		multicallClient:       multicallClient,
		erc20ABI:              erc20ABI,
		uiPoolDataProvider:    uiPoolDataProvider,
		poolDataProvider:      poolDataProvider,
		poolAddressesProvider: poolAddressesProvider,
		protocolVersion:       protocolVersion,
		metadataCache:         make(map[common.Address]TokenMetadata),
		logger:                logger.With("component", "blockchain-service"),
	}

	if err := service.loadABIs(protocolVersion); err != nil {
		return nil, err
	}

	return service, nil
}

func (s *blockchainService) loadABIs(protocolVersion blockchain.ProtocolVersion) error {
	var err error

	// Load getUserReservesData ABI based on protocol version
	switch protocolVersion {
	case blockchain.ProtocolVersionAaveV2:
		s.getUserReservesABI, err = abis.GetAaveUserReservesDataABI()
	case blockchain.ProtocolVersionAaveV3:
		s.getUserReservesABI, err = abis.GetAaveUserReservesDataABI()
	case blockchain.ProtocolVersionSparkLend:
		s.getUserReservesABI, err = abis.GetSparklendUserReservesDataABI()
	default:
		return fmt.Errorf("unknown protocol version: %s", protocolVersion)
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

	switch protocolVersion {
	case blockchain.ProtocolVersionAaveV2:
		s.getPoolDataProviderReserveDataABI, err = abis.GetAaveV2PoolDataProviderReserveDataABI()
	case blockchain.ProtocolVersionAaveV3:
		s.getPoolDataProviderReserveDataABI, err = abis.GetPoolDataProviderReserveData()
	case blockchain.ProtocolVersionSparkLend:
		s.getPoolDataProviderReserveDataABI, err = abis.GetSparklendPoolDataProviderReserveDataABI()
	default:
		return fmt.Errorf("unknown protocol version: %s", protocolVersion)
	}
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

	// Aave V2/V3 layout (7 fields)
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

	// SparkLend layout (4 fields)
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
			StableRateLastUpdated:    bi.Uint64(),
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
			AllowFailure: true,
			CallData:     getReserveDataCallData,
		},
		{
			Target:       s.poolDataProvider,
			AllowFailure: true,
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

func (s *blockchainService) parseReserveData(data []byte) (*reserveDataFromProvider, error) {
	unpacked, err := s.getPoolDataProviderReserveDataABI.Unpack("getReserveData", data)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack getReserveData: %w", err)
	}

	outputs := s.getPoolDataProviderReserveDataABI.Methods["getReserveData"].Outputs
	fieldIndex := buildFieldIndexMap(outputs)

	switch s.protocolVersion {
	case blockchain.ProtocolVersionAaveV2:
		return parseReserveDataAaveV2(unpacked, fieldIndex)
	case blockchain.ProtocolVersionAaveV3:
		return parseReserveDataAaveV3(unpacked, fieldIndex)
	case blockchain.ProtocolVersionSparkLend:
		return parseReserveDataSparklend(unpacked, fieldIndex)
	default:
		return nil, fmt.Errorf("unknown protocol version: %s", s.protocolVersion)
	}
}

// parseReserveDataAaveV2 parses Aave V2 reserve data (10 fields).
func parseReserveDataAaveV2(unpacked []interface{}, fieldIndex map[string]int) (*reserveDataFromProvider, error) {
	if len(unpacked) < 10 {
		return nil, fmt.Errorf("expected 10 values from getReserveData (Aave V2), got %d", len(unpacked))
	}

	result := &reserveDataFromProvider{
		Unbacked:                big.NewInt(0), // Not available in V2
		AccruedToTreasuryScaled: big.NewInt(0), // Not available in V2
	}

	var err error

	// Map availableLiquidity to TotalAToken as proxy
	result.TotalAToken, err = getBigIntByName(unpacked, fieldIndex, "availableLiquidity")
	if err != nil {
		return nil, err
	}

	result.TotalStableDebt, err = getBigIntByName(unpacked, fieldIndex, "totalStableDebt")
	if err != nil {
		return nil, err
	}

	result.TotalVariableDebt, err = getBigIntByName(unpacked, fieldIndex, "totalVariableDebt")
	if err != nil {
		return nil, err
	}

	result.LiquidityRate, err = getBigIntByName(unpacked, fieldIndex, "liquidityRate")
	if err != nil {
		return nil, err
	}

	result.VariableBorrowRate, err = getBigIntByName(unpacked, fieldIndex, "variableBorrowRate")
	if err != nil {
		return nil, err
	}

	result.StableBorrowRate, err = getBigIntByName(unpacked, fieldIndex, "stableBorrowRate")
	if err != nil {
		return nil, err
	}

	result.AverageStableBorrowRate, err = getBigIntByName(unpacked, fieldIndex, "averageStableBorrowRate")
	if err != nil {
		return nil, err
	}

	result.LiquidityIndex, err = getBigIntByName(unpacked, fieldIndex, "liquidityIndex")
	if err != nil {
		return nil, err
	}

	result.VariableBorrowIndex, err = getBigIntByName(unpacked, fieldIndex, "variableBorrowIndex")
	if err != nil {
		return nil, err
	}

	timestamp, err := getBigIntByName(unpacked, fieldIndex, "lastUpdateTimestamp")
	if err != nil {
		return nil, err
	}
	result.LastUpdateTimestamp = timestamp.Int64()

	return result, nil
}

// parseReserveDataAaveV3 parses Aave V3 reserve data (12 fields).
func parseReserveDataAaveV3(unpacked []interface{}, fieldIndex map[string]int) (*reserveDataFromProvider, error) {
	if len(unpacked) < 12 {
		return nil, fmt.Errorf("expected 12 values from getReserveData (Aave V3), got %d", len(unpacked))
	}

	result := &reserveDataFromProvider{}
	var err error

	result.Unbacked, err = getBigIntByName(unpacked, fieldIndex, "unbacked")
	if err != nil {
		return nil, err
	}

	result.AccruedToTreasuryScaled, err = getBigIntByName(unpacked, fieldIndex, "accruedToTreasuryScaled")
	if err != nil {
		return nil, err
	}

	result.TotalAToken, err = getBigIntByName(unpacked, fieldIndex, "totalAToken")
	if err != nil {
		return nil, err
	}

	result.TotalStableDebt, err = getBigIntByName(unpacked, fieldIndex, "totalStableDebt")
	if err != nil {
		return nil, err
	}

	result.TotalVariableDebt, err = getBigIntByName(unpacked, fieldIndex, "totalVariableDebt")
	if err != nil {
		return nil, err
	}

	result.LiquidityRate, err = getBigIntByName(unpacked, fieldIndex, "liquidityRate")
	if err != nil {
		return nil, err
	}

	result.VariableBorrowRate, err = getBigIntByName(unpacked, fieldIndex, "variableBorrowRate")
	if err != nil {
		return nil, err
	}

	result.StableBorrowRate, err = getBigIntByName(unpacked, fieldIndex, "stableBorrowRate")
	if err != nil {
		return nil, err
	}

	result.AverageStableBorrowRate, err = getBigIntByName(unpacked, fieldIndex, "averageStableBorrowRate")
	if err != nil {
		return nil, err
	}

	result.LiquidityIndex, err = getBigIntByName(unpacked, fieldIndex, "liquidityIndex")
	if err != nil {
		return nil, err
	}

	result.VariableBorrowIndex, err = getBigIntByName(unpacked, fieldIndex, "variableBorrowIndex")
	if err != nil {
		return nil, err
	}

	timestamp, err := getBigIntByName(unpacked, fieldIndex, "lastUpdateTimestamp")
	if err != nil {
		return nil, err
	}
	result.LastUpdateTimestamp = timestamp.Int64()

	return result, nil
}

// parseReserveDataSparklend parses Sparklend reserve data (11 fields, no averageStableBorrowRate).
func parseReserveDataSparklend(unpacked []interface{}, fieldIndex map[string]int) (*reserveDataFromProvider, error) {
	result := &reserveDataFromProvider{}
	var err error

	result.Unbacked, err = getBigIntByName(unpacked, fieldIndex, "unbacked")
	if err != nil {
		return nil, err
	}

	result.AccruedToTreasuryScaled, err = getBigIntByName(unpacked, fieldIndex, "accruedToTreasuryScaled")
	if err != nil {
		return nil, err
	}

	result.TotalAToken, err = getBigIntByName(unpacked, fieldIndex, "totalAToken")
	if err != nil {
		return nil, err
	}

	result.TotalStableDebt, err = getBigIntByName(unpacked, fieldIndex, "totalStableDebt")
	if err != nil {
		return nil, err
	}

	result.TotalVariableDebt, err = getBigIntByName(unpacked, fieldIndex, "totalVariableDebt")
	if err != nil {
		return nil, err
	}

	result.LiquidityRate, err = getBigIntByName(unpacked, fieldIndex, "liquidityRate")
	if err != nil {
		return nil, err
	}

	result.VariableBorrowRate, err = getBigIntByName(unpacked, fieldIndex, "variableBorrowRate")
	if err != nil {
		return nil, err
	}

	result.StableBorrowRate, err = getBigIntByName(unpacked, fieldIndex, "stableBorrowRate")
	if err != nil {
		return nil, err
	}

	result.AverageStableBorrowRate = big.NewInt(0) // Not available in Sparklend

	result.LiquidityIndex, err = getBigIntByName(unpacked, fieldIndex, "liquidityIndex")
	if err != nil {
		return nil, err
	}

	result.VariableBorrowIndex, err = getBigIntByName(unpacked, fieldIndex, "variableBorrowIndex")
	if err != nil {
		return nil, err
	}

	timestamp, err := getBigIntByName(unpacked, fieldIndex, "lastUpdateTimestamp")
	if err != nil {
		return nil, err
	}
	result.LastUpdateTimestamp = timestamp.Int64()

	return result, nil
}

func (s *blockchainService) parseReserveConfigurationData(data []byte) (*reserveConfigData, error) {
	unpacked, err := s.getPoolDataProviderReserveConfigurationABI.Unpack("getReserveConfigurationData", data)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack getReserveConfigurationData: %w", err)
	}

	if len(unpacked) < 10 {
		return nil, fmt.Errorf("expected 10 values from getReserveConfigurationData, got %d", len(unpacked))
	}

	outputs := s.getPoolDataProviderReserveConfigurationABI.Methods["getReserveConfigurationData"].Outputs
	fieldIndex := buildFieldIndexMap(outputs)

	result := &reserveConfigData{}

	result.Decimals, err = getBigIntByName(unpacked, fieldIndex, "decimals")
	if err != nil {
		return nil, err
	}

	result.LTV, err = getBigIntByName(unpacked, fieldIndex, "ltv")
	if err != nil {
		return nil, err
	}

	result.LiquidationThreshold, err = getBigIntByName(unpacked, fieldIndex, "liquidationThreshold")
	if err != nil {
		return nil, err
	}

	result.LiquidationBonus, err = getBigIntByName(unpacked, fieldIndex, "liquidationBonus")
	if err != nil {
		return nil, err
	}

	result.ReserveFactor, err = getBigIntByName(unpacked, fieldIndex, "reserveFactor")
	if err != nil {
		return nil, err
	}

	result.UsageAsCollateralEnabled, err = getBoolByName(unpacked, fieldIndex, "usageAsCollateralEnabled")
	if err != nil {
		return nil, err
	}

	result.BorrowingEnabled, err = getBoolByName(unpacked, fieldIndex, "borrowingEnabled")
	if err != nil {
		return nil, err
	}

	result.StableBorrowRateEnabled, err = getBoolByName(unpacked, fieldIndex, "stableBorrowRateEnabled")
	if err != nil {
		return nil, err
	}

	result.IsActive, err = getBoolByName(unpacked, fieldIndex, "isActive")
	if err != nil {
		return nil, err
	}

	result.IsFrozen, err = getBoolByName(unpacked, fieldIndex, "isFrozen")
	if err != nil {
		return nil, err
	}

	return result, nil
}

func buildFieldIndexMap(outputs abi.Arguments) map[string]int {
	fieldIndex := make(map[string]int, len(outputs))
	for i, arg := range outputs {
		fieldIndex[arg.Name] = i
	}
	return fieldIndex
}

func getBigIntByName(unpacked []interface{}, fieldIndex map[string]int, fieldName string) (*big.Int, error) {
	idx, ok := fieldIndex[fieldName]
	if !ok {
		return nil, fmt.Errorf("field %s not found in ABI", fieldName)
	}

	if idx >= len(unpacked) {
		return nil, fmt.Errorf("index %d out of range for %s", idx, fieldName)
	}

	v, ok := unpacked[idx].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("field %s expected *big.Int, got %T", fieldName, unpacked[idx])
	}

	return v, nil
}

func getBoolByName(unpacked []interface{}, fieldIndex map[string]int, fieldName string) (bool, error) {
	idx, ok := fieldIndex[fieldName]
	if !ok {
		return false, fmt.Errorf("field %s not found in ABI", fieldName)
	}

	if idx >= len(unpacked) {
		return false, fmt.Errorf("index %d out of range for %s", idx, fieldName)
	}

	v, ok := unpacked[idx].(bool)
	if !ok {
		return false, fmt.Errorf("field %s expected bool, got %T", fieldName, unpacked[idx])
	}

	return v, nil
}

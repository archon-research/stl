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

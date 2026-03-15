package shared

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"sync"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/multicall"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

type PositionSnapshotter struct {
	ethClient    *ethclient.Client
	txManager    outbound.TxManager
	userRepo     outbound.UserRepository
	protocolRepo outbound.ProtocolRepository
	tokenRepo    outbound.TokenRepository
	positionRepo outbound.PositionRepository
	logger       *slog.Logger

	mu                 sync.RWMutex
	blockchainServices map[blockchain.ProtocolKey]*blockchainService
	multicallClient    outbound.Multicaller
	erc20ABI           *abi.ABI
}

type CollateralData struct {
	Asset             common.Address
	Decimals          int
	Symbol            string
	Name              string
	ActualBalance     *big.Int
	CollateralEnabled bool
}

type DebtData struct {
	Asset       common.Address
	Decimals    int
	Symbol      string
	Name        string
	CurrentDebt *big.Int
}

type TokenMetadata struct {
	Symbol   string
	Decimals int
	Name     string
}

type UserReserveData struct {
	UnderlyingAsset                 common.Address
	ScaledATokenBalance             *big.Int
	UsageAsCollateralEnabledOnUser  bool
	ScaledVariableDebt              *big.Int
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
	mu                    sync.RWMutex
	chainID               int64
	ethClient             *ethclient.Client
	multicallClient       outbound.Multicaller
	erc20ABI              *abi.ABI
	getUserReservesABI    *abi.ABI
	getUserReserveDataABI *abi.ABI
	uiPoolDataProvider    common.Address
	poolAddress           common.Address
	poolAddressesProvider common.Address
	protocolVersion       blockchain.ProtocolVersion
	metadataCache         map[common.Address]TokenMetadata
	logger                *slog.Logger
}

func NewPositionSnapshotter(
	ethClient *ethclient.Client,
	txManager outbound.TxManager,
	userRepo outbound.UserRepository,
	protocolRepo outbound.ProtocolRepository,
	tokenRepo outbound.TokenRepository,
	positionRepo outbound.PositionRepository,
	logger *slog.Logger,
) (*PositionSnapshotter, error) {
	if ethClient == nil {
		return nil, fmt.Errorf("ethClient is required")
	}
	if txManager == nil {
		return nil, fmt.Errorf("txManager is required")
	}
	if userRepo == nil {
		return nil, fmt.Errorf("userRepo is required")
	}
	if protocolRepo == nil {
		return nil, fmt.Errorf("protocolRepo is required")
	}
	if tokenRepo == nil {
		return nil, fmt.Errorf("tokenRepo is required")
	}
	if positionRepo == nil {
		return nil, fmt.Errorf("positionRepo is required")
	}
	if logger == nil {
		logger = slog.Default()
	}

	mc, err := multicall.NewClient(ethClient, blockchain.Multicall3)
	if err != nil {
		return nil, fmt.Errorf("failed to create multicall client: %w", err)
	}

	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		return nil, fmt.Errorf("failed to load ERC20 ABI: %w", err)
	}

	return &PositionSnapshotter{
		ethClient:          ethClient,
		txManager:          txManager,
		userRepo:           userRepo,
		protocolRepo:       protocolRepo,
		tokenRepo:          tokenRepo,
		positionRepo:       positionRepo,
		logger:             logger,
		blockchainServices: make(map[blockchain.ProtocolKey]*blockchainService),
		multicallClient:    mc,
		erc20ABI:           erc20ABI,
	}, nil
}

func (s *PositionSnapshotter) SnapshotUser(
	ctx context.Context,
	chainID int64,
	protocolAddress common.Address,
	userAddress common.Address,
	blockNumber int64,
	blockVersion int,
	eventType string,
	txHash []byte,
) error {
	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		return s.SnapshotUserWithTx(ctx, tx, chainID, protocolAddress, userAddress, blockNumber, blockVersion, eventType, txHash)
	})
}

func (s *PositionSnapshotter) SnapshotUserWithTx(
	ctx context.Context,
	tx pgx.Tx,
	chainID int64,
	protocolAddress common.Address,
	userAddress common.Address,
	blockNumber int64,
	blockVersion int,
	eventType string,
	txHash []byte,
) error {
	userID, err := s.userRepo.GetOrCreateUser(ctx, tx, entity.User{
		ChainID:        chainID,
		Address:        userAddress,
		FirstSeenBlock: blockNumber,
	})
	if err != nil {
		return fmt.Errorf("failed to ensure user: %w", err)
	}

	protocolConfig, exists := blockchain.GetProtocolConfig(chainID, protocolAddress)
	if !exists {
		return fmt.Errorf("unknown protocol: chainID=%d address=%s", chainID, protocolAddress.Hex())
	}

	protocolID, err := s.protocolRepo.GetOrCreateProtocol(ctx, tx, chainID, protocolAddress, protocolConfig.Name, normalizeProtocolType(protocolConfig.ProtocolType), blockNumber)
	if err != nil {
		return fmt.Errorf("failed to get protocol: %w", err)
	}

	txHashHex := common.BytesToHash(txHash).Hex()
	collaterals, _, err := s.extractUserPositionData(ctx, userAddress, protocolAddress, chainID, blockNumber, txHashHex)
	if err != nil {
		s.logger.Warn("failed to extract collateral data for user", "user", userAddress.Hex(), "error", err)
		collaterals = []CollateralData{}
	}

	records := make([]outbound.CollateralRecord, 0, len(collaterals))
	for _, col := range collaterals {
		tokenID, err := s.tokenRepo.GetOrCreateToken(ctx, tx, chainID, col.Asset, normalizeTokenSymbol(col.Symbol), col.Decimals, blockNumber)
		if err != nil {
			s.logger.Warn("failed to get collateral token", "token", col.Asset.Hex(), "error", err, "tx", txHashHex)
			continue
		}

		records = append(records, outbound.CollateralRecord{
			UserID:            userID,
			ProtocolID:        protocolID,
			TokenID:           tokenID,
			BlockNumber:       blockNumber,
			BlockVersion:      blockVersion,
			Amount:            FormatAmount(col.ActualBalance, col.Decimals),
			Change:            "0",
			EventType:         eventType,
			TxHash:            txHash,
			CollateralEnabled: col.CollateralEnabled,
		})
	}

	if err := s.positionRepo.SaveBorrowerCollaterals(ctx, tx, records); err != nil {
		return fmt.Errorf("failed to save collaterals: %w", err)
	}

	s.logger.Info("Saved position snapshot", "user", userAddress.Hex(), "tx", txHashHex, "block", blockNumber)
	return nil
}

func (s *PositionSnapshotter) getOrCreateBlockchainService(chainID int64, protocolAddress common.Address) (*blockchainService, error) {
	key := blockchain.ProtocolKey{ChainID: chainID, PoolAddress: protocolAddress}

	s.mu.RLock()
	svc, exists := s.blockchainServices[key]
	s.mu.RUnlock()
	if exists {
		return svc, nil
	}

	protocolConfig, exists := blockchain.GetProtocolConfig(chainID, protocolAddress)
	if !exists {
		return nil, fmt.Errorf("unknown protocol: chainID=%d address=%s", chainID, protocolAddress.Hex())
	}

	newSvc, err := newBlockchainService(
		chainID,
		s.ethClient,
		s.multicallClient,
		s.erc20ABI,
		protocolConfig.UIPoolDataProvider.Address,
		protocolConfig.PoolAddress.Address,
		protocolConfig.PoolAddressesProvider.Address,
		protocolConfig.ProtocolVersion,
		s.logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create blockchain service for %s: %w", protocolConfig.Name, err)
	}

	s.mu.Lock()
	if svc, exists = s.blockchainServices[key]; !exists {
		s.blockchainServices[key] = newSvc
		svc = newSvc
	}
	s.mu.Unlock()

	return svc, nil
}

func (s *PositionSnapshotter) extractUserPositionData(ctx context.Context, user common.Address, protocolAddress common.Address, chainID, blockNumber int64, txHash string) ([]CollateralData, []DebtData, error) {
	blockchainSvc, err := s.getOrCreateBlockchainService(chainID, protocolAddress)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get blockchain service: %w", err)
	}

	reserves, err := blockchainSvc.getUserReservesData(ctx, user, blockNumber)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get user reserves data: %w", err)
	}

	var collateralAssets, debtAssets []common.Address
	tokensToFetch := make(map[common.Address]bool)
	for _, r := range reserves {
		if r.UnderlyingAsset == (common.Address{}) {
			continue
		}
		if r.ScaledATokenBalance.Cmp(big.NewInt(0)) > 0 && r.UsageAsCollateralEnabledOnUser {
			collateralAssets = append(collateralAssets, r.UnderlyingAsset)
			tokensToFetch[r.UnderlyingAsset] = true
		}
		if r.ScaledVariableDebt.Cmp(big.NewInt(0)) > 0 {
			debtAssets = append(debtAssets, r.UnderlyingAsset)
			tokensToFetch[r.UnderlyingAsset] = true
		}
	}

	if len(tokensToFetch) == 0 {
		return []CollateralData{}, []DebtData{}, nil
	}

	metadataMap, err := blockchainSvc.batchGetTokenMetadata(ctx, tokensToFetch, big.NewInt(blockNumber))
	if err != nil {
		s.logger.Warn("failed to batch get token metadata", "error", err, "tx", txHash, "block", blockNumber)
		return []CollateralData{}, []DebtData{}, fmt.Errorf("failed to get token metadata: %w", err)
	}

	allAssets := make([]common.Address, 0, len(tokensToFetch))
	for asset := range tokensToFetch {
		allAssets = append(allAssets, asset)
	}

	actualDataMap, err := blockchainSvc.batchGetUserReserveData(ctx, allAssets, user, blockNumber)
	if err != nil {
		s.logger.Warn("failed to batch get user reserve data", "error", err, "tx", txHash, "block", blockNumber)
		return []CollateralData{}, []DebtData{}, fmt.Errorf("failed to get user reserve data: %w", err)
	}

	var collaterals []CollateralData
	for _, asset := range collateralAssets {
		metadata, ok := metadataMap[asset]
		if !ok || metadata.Decimals == 0 {
			s.logger.Error("Failed to get collateral token metadata",
				"action", "skipped",
				"token", asset.Hex(),
				"tx", txHash,
				"block", blockNumber,
				"user", user.Hex())
			continue
		}

		actualData, ok := actualDataMap[asset]
		if !ok {
			s.logger.Error("Failed to get actual balance",
				"action", "skipped",
				"token", asset.Hex(),
				"tx", txHash,
				"block", blockNumber,
				"user", user.Hex())
			continue
		}

		if actualData.UsageAsCollateralEnabled && actualData.CurrentATokenBalance.Cmp(big.NewInt(0)) > 0 {
			collaterals = append(collaterals, CollateralData{
				Asset:             asset,
				Decimals:          metadata.Decimals,
				Symbol:            metadata.Symbol,
				Name:              metadata.Name,
				ActualBalance:     actualData.CurrentATokenBalance,
				CollateralEnabled: actualData.UsageAsCollateralEnabled,
			})
		}
	}

	var debts []DebtData
	for _, asset := range debtAssets {
		metadata, ok := metadataMap[asset]
		if !ok || metadata.Decimals == 0 {
			s.logger.Error("Failed to get debt token metadata",
				"action", "skipped",
				"token", asset.Hex(),
				"tx", txHash,
				"block", blockNumber,
				"user", user.Hex())
			continue
		}

		actualData, ok := actualDataMap[asset]
		if !ok {
			s.logger.Error("Failed to get actual debt",
				"action", "skipped",
				"token", asset.Hex(),
				"tx", txHash,
				"block", blockNumber,
				"user", user.Hex())
			continue
		}

		if actualData.CurrentVariableDebt.Cmp(big.NewInt(0)) > 0 {
			debts = append(debts, DebtData{
				Asset:       asset,
				Decimals:    metadata.Decimals,
				Symbol:      metadata.Symbol,
				Name:        metadata.Name,
				CurrentDebt: actualData.CurrentVariableDebt,
			})
		}
	}

	return collaterals, debts, nil
}

func newBlockchainService(
	chainID int64,
	ethClient *ethclient.Client,
	multicallClient outbound.Multicaller,
	erc20ABI *abi.ABI,
	uiPoolDataProvider common.Address,
	poolAddress common.Address,
	poolAddressesProvider common.Address,
	protocolVersion blockchain.ProtocolVersion,
	logger *slog.Logger,
) (*blockchainService, error) {
	service := &blockchainService{
		chainID:               chainID,
		ethClient:             ethClient,
		multicallClient:       multicallClient,
		erc20ABI:              erc20ABI,
		uiPoolDataProvider:    uiPoolDataProvider,
		poolAddress:           poolAddress,
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

	switch protocolVersion {
	case blockchain.ProtocolVersionAaveV2, blockchain.ProtocolVersionAaveV3:
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

	return nil
}

func (s *blockchainService) getPoolDataProviderForBlock(blockNumber uint64) (common.Address, error) {
	provider, ok := blockchain.GetPoolDataProviderForBlock(s.chainID, s.poolAddress, blockNumber)
	if !ok {
		return common.Address{}, fmt.Errorf("no PoolDataProvider available for block: chain=%d pool=%s block=%d", s.chainID, s.poolAddress.Hex(), blockNumber)
	}
	return provider, nil
}

func (s *blockchainService) getUserReservesData(ctx context.Context, user common.Address, blockNumber int64) ([]UserReserveData, error) {
	data, err := s.getUserReservesABI.Pack("getUserReservesData", s.poolAddressesProvider, user)
	if err != nil {
		return nil, fmt.Errorf("failed to pack getUserReservesData call: %w", err)
	}

	msg := ethereum.CallMsg{To: &s.uiPoolDataProvider, Data: data}
	result, err := s.ethClient.CallContract(ctx, msg, big.NewInt(blockNumber))
	if err != nil {
		return nil, fmt.Errorf("failed to call UiPoolDataProvider: %w", err)
	}
	if len(result) == 0 {
		return []UserReserveData{}, nil
	}

	unpacked, err := s.getUserReservesABI.Unpack("getUserReservesData", result)
	if err != nil {
		reserves, rawErr := decodeUserReservesRaw(result)
		if rawErr != nil {
			return nil, fmt.Errorf("failed to decode getUserReservesData: %w", errors.Join(err, fmt.Errorf("raw fallback decode failed: %w", rawErr)))
		}
		return reserves, nil
	}
	if len(unpacked) == 0 {
		return []UserReserveData{}, nil
	}

	switch v := unpacked[0].(type) {
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
			if r.UnderlyingAsset == (common.Address{}) {
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
	case []struct {
		UnderlyingAsset                common.Address `json:"underlyingAsset"`
		ScaledATokenBalance            *big.Int       `json:"scaledATokenBalance"`
		UsageAsCollateralEnabledOnUser bool           `json:"usageAsCollateralEnabledOnUser"`
		ScaledVariableDebt             *big.Int       `json:"scaledVariableDebt"`
	}:
		reserves := make([]UserReserveData, 0, len(v))
		for _, r := range v {
			if r.UnderlyingAsset == (common.Address{}) {
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

	poolDataProvider, err := s.getPoolDataProviderForBlock(uint64(blockNumber))
	if err != nil {
		return nil, err
	}

	calls := make([]outbound.Call, 0, len(assets))
	for _, asset := range assets {
		callData, err := s.getUserReserveDataABI.Pack("getUserReserveData", asset, user)
		if err != nil {
			s.logger.Warn("failed to pack getUserReserveData call", "asset", asset.Hex(), "error", err)
			continue
		}

		calls = append(calls, outbound.Call{Target: poolDataProvider, AllowFailure: true, CallData: callData})
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
			continue
		}

		unpacked, err := s.getUserReserveDataABI.Unpack("getUserReserveData", result.ReturnData)
		if err != nil || len(unpacked) < 9 {
			continue
		}

		bi, ok := unpacked[7].(*big.Int)
		if !ok || bi == nil {
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

func (s *blockchainService) batchGetTokenMetadata(ctx context.Context, tokens map[common.Address]bool, blockNumber *big.Int) (map[common.Address]TokenMetadata, error) {
	tokensToFetch := make([]common.Address, 0)
	result := make(map[common.Address]TokenMetadata)

	s.mu.RLock()
	for token := range tokens {
		if cached, ok := s.metadataCache[token]; ok {
			result[token] = cached
		} else {
			tokensToFetch = append(tokensToFetch, token)
		}
	}
	s.mu.RUnlock()

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

	results, err := s.multicallClient.Execute(ctx, calls, blockNumber)
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
			if unpacked, err := s.erc20ABI.Unpack("decimals", results[decimalsIdx].ReturnData); err == nil && len(unpacked) > 0 {
				if d, ok := unpacked[0].(uint8); ok {
					decimals = int(d)
				}
			}
		}

		if results[symbolIdx].Success && len(results[symbolIdx].ReturnData) > 0 {
			if unpacked, err := s.erc20ABI.Unpack("symbol", results[symbolIdx].ReturnData); err == nil && len(unpacked) > 0 {
				if value, ok := unpacked[0].(string); ok {
					symbol = value
				}
			}
		}

		if results[nameIdx].Success && len(results[nameIdx].ReturnData) > 0 {
			if unpacked, err := s.erc20ABI.Unpack("name", results[nameIdx].ReturnData); err == nil && len(unpacked) > 0 {
				if value, ok := unpacked[0].(string); ok {
					name = value
				}
			}
		}

		metadata := TokenMetadata{Symbol: symbol, Decimals: decimals, Name: name}

		s.mu.Lock()
		s.metadataCache[token] = metadata
		s.mu.Unlock()
		result[token] = metadata
	}

	return result, nil
}

func decodeUserReservesRaw(data []byte) ([]UserReserveData, error) {
	const wordSize = 32

	if len(data) < 2*wordSize {
		return nil, fmt.Errorf("response too short: %d bytes", len(data))
	}

	arrayOffset := new(big.Int).SetBytes(data[0:wordSize]).Uint64()
	if arrayOffset > uint64(len(data))-wordSize {
		return nil, fmt.Errorf("array offset %d out of bounds (len=%d)", arrayOffset, len(data))
	}

	arrayStart := arrayOffset
	arrayLen := new(big.Int).SetBytes(data[arrayStart : arrayStart+wordSize]).Uint64()
	if arrayLen == 0 {
		return []UserReserveData{}, nil
	}

	structDataBytes := uint64(len(data)) - arrayStart - wordSize
	bytesPerStruct := structDataBytes / arrayLen

	var fieldsPerStruct uint64
	switch {
	case bytesPerStruct >= 7*wordSize:
		fieldsPerStruct = 7
	case bytesPerStruct >= 4*wordSize:
		fieldsPerStruct = 4
	default:
		return nil, fmt.Errorf("cannot determine struct layout: %d bytes per struct for %d reserves", bytesPerStruct, arrayLen)
	}

	expectedBytes := arrayStart + wordSize + arrayLen*fieldsPerStruct*wordSize
	if uint64(len(data)) < expectedBytes {
		return nil, fmt.Errorf("data too short for %d reserves (%d fields): need %d bytes, have %d", arrayLen, fieldsPerStruct, expectedBytes, len(data))
	}

	reserves := make([]UserReserveData, 0, arrayLen)
	for i := range arrayLen {
		base := arrayStart + wordSize + i*fieldsPerStruct*wordSize

		underlyingAsset := common.BytesToAddress(data[base : base+wordSize])
		if underlyingAsset == (common.Address{}) {
			continue
		}

		scaledATokenBalance := new(big.Int).SetBytes(data[base+wordSize : base+2*wordSize])
		collateralEnabled := new(big.Int).SetBytes(data[base+2*wordSize:base+3*wordSize]).Sign() != 0
		scaledVariableDebt := new(big.Int).SetBytes(data[base+3*wordSize : base+4*wordSize])

		rd := UserReserveData{
			UnderlyingAsset:                underlyingAsset,
			ScaledATokenBalance:            scaledATokenBalance,
			UsageAsCollateralEnabledOnUser: collateralEnabled,
			ScaledVariableDebt:             scaledVariableDebt,
		}

		if fieldsPerStruct == 7 {
			rd.StableBorrowRate = new(big.Int).SetBytes(data[base+4*wordSize : base+5*wordSize])
			rd.PrincipalStableDebt = new(big.Int).SetBytes(data[base+5*wordSize : base+6*wordSize])
			rd.StableBorrowLastUpdateTimestamp = new(big.Int).SetBytes(data[base+6*wordSize : base+7*wordSize])
		} else {
			rd.StableBorrowRate = big.NewInt(0)
			rd.PrincipalStableDebt = big.NewInt(0)
			rd.StableBorrowLastUpdateTimestamp = big.NewInt(0)
		}

		reserves = append(reserves, rd)
	}

	return reserves, nil
}

func normalizeProtocolType(protocolType string) string {
	if strings.TrimSpace(protocolType) == "" {
		return "lending"
	}

	return protocolType
}

func normalizeTokenSymbol(symbol string) string {
	if strings.TrimSpace(symbol) == "" {
		return "UNKNOWN"
	}

	return symbol
}

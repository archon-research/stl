package aavelike

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"sync"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

// PositionReader reads on-chain position data (collateral + debt) for
// Aave and Aave-fork protocols (SparkLend, Aave V2/V3, Lido, etc.) using
// multicall batching. It is safe for concurrent use.
type PositionReader struct {
	ethClient          *ethclient.Client
	multicallClient    outbound.Multicaller
	erc20ABI           *abi.ABI
	blockchainServices map[blockchain.ProtocolKey]*BlockchainService
	mu                 sync.RWMutex
	logger             *slog.Logger
}

// NewPositionReader creates a reader that can query on-chain position
// data for any registered Aave-like protocol.
func NewPositionReader(ethClient *ethclient.Client, multicaller outbound.Multicaller, erc20ABI *abi.ABI, logger *slog.Logger) *PositionReader {
	return &PositionReader{
		ethClient:          ethClient,
		multicallClient:    multicaller,
		erc20ABI:           erc20ABI,
		blockchainServices: make(map[blockchain.ProtocolKey]*BlockchainService),
		logger:             logger,
	}
}

func (r *PositionReader) GetOrCreateBlockchainService(chainID int64, protocolAddress common.Address) (*BlockchainService, error) {
	key := blockchain.ProtocolKey{ChainID: chainID, PoolAddress: protocolAddress}

	// First check: read map under read lock.
	r.mu.RLock()
	svc, exists := r.blockchainServices[key]
	r.mu.RUnlock()
	if exists {
		return svc, nil
	}

	protocolConfig, exists := blockchain.GetProtocolConfig(chainID, protocolAddress)
	if !exists {
		return nil, fmt.Errorf("unknown protocol: chainID=%d address=%s", chainID, protocolAddress.Hex())
	}

	// Construct outside the lock - this hits the network.
	newSvc, err := newBlockchainService(
		chainID,
		r.ethClient,
		r.multicallClient,
		r.erc20ABI,
		protocolConfig.UIPoolDataProvider.Address,
		protocolConfig.PoolAddress.Address,
		protocolConfig.PoolAddressesProvider.Address,
		protocolConfig.ProtocolVersion,
		r.logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create blockchain service for %s: %w", protocolConfig.Name, err)
	}

	// Second check: re-acquire lock, another goroutine may have created it.
	r.mu.Lock()
	if svc, exists = r.blockchainServices[key]; !exists {
		r.blockchainServices[key] = newSvc
		svc = newSvc
		r.logger.Info("created blockchain service",
			"protocol", protocolConfig.Name,
			"chainID", chainID,
			"address", protocolAddress.Hex())
	}
	r.mu.Unlock()

	return svc, nil
}

// GetUserPositionData fetches current collateral and debt positions for a user
// from on-chain data using multicall batching.
func (r *PositionReader) GetUserPositionData(ctx context.Context, user common.Address, protocolAddress common.Address, chainID, blockNumber int64) ([]CollateralData, []DebtData, error) {
	blockchainSvc, err := r.GetOrCreateBlockchainService(chainID, protocolAddress)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get blockchain service: %w", err)
	}

	reserves, err := blockchainSvc.getUserReservesData(ctx, user, blockNumber)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get user reserves data: %w", err)
	}

	var collateralAssets, debtAssets []common.Address
	tokensToFetch := make(map[common.Address]bool)
	for _, rv := range reserves {
		if rv.UnderlyingAsset == (common.Address{}) {
			continue
		}
		if rv.ScaledATokenBalance.Cmp(big.NewInt(0)) > 0 && rv.UsageAsCollateralEnabledOnUser {
			collateralAssets = append(collateralAssets, rv.UnderlyingAsset)
			tokensToFetch[rv.UnderlyingAsset] = true
		}
		if rv.ScaledVariableDebt.Cmp(big.NewInt(0)) > 0 {
			debtAssets = append(debtAssets, rv.UnderlyingAsset)
			tokensToFetch[rv.UnderlyingAsset] = true
		}
	}

	if len(tokensToFetch) == 0 {
		return []CollateralData{}, []DebtData{}, nil
	}

	metadataMap, err := blockchainSvc.BatchGetTokenMetadata(ctx, tokensToFetch, big.NewInt(blockNumber))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get token metadata: %w", err)
	}

	allAssets := make([]common.Address, 0, len(tokensToFetch))
	for asset := range tokensToFetch {
		allAssets = append(allAssets, asset)
	}

	actualDataMap, err := blockchainSvc.batchGetUserReserveData(ctx, allAssets, user, blockNumber)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get user reserve data: %w", err)
	}

	collaterals := buildCollateralData(collateralAssets, metadataMap, actualDataMap, r.logger)
	debts := buildDebtData(debtAssets, metadataMap, actualDataMap, r.logger)

	return collaterals, debts, nil
}

// UserPositionResult holds the position data for a single user from a batch query.
type UserPositionResult struct {
	Collaterals []CollateralData
	Debts       []DebtData
	Err         error
}

// GetBatchUserPositionData fetches position data for multiple users in batch,
// using 2 multicalls instead of 2N. Results are functionally identical to
// calling GetUserPositionData for each user individually.
func (r *PositionReader) GetBatchUserPositionData(
	ctx context.Context,
	users []common.Address,
	protocolAddress common.Address,
	chainID, blockNumber int64,
) (map[common.Address]UserPositionResult, error) {
	if len(users) == 0 {
		return make(map[common.Address]UserPositionResult), nil
	}

	blockchainSvc, err := r.GetOrCreateBlockchainService(chainID, protocolAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to get blockchain service: %w", err)
	}

	// Multicall 1: getUserReservesData for all users in one call.
	reservesMap, reserveErrors, err := blockchainSvc.getUserReservesDataBatch(ctx, users, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("batch getUserReservesData failed: %w", err)
	}

	// For each user, identify collateral/debt assets and collect tokens for metadata.
	type userAssetInfo struct {
		collateralAssets []common.Address
		debtAssets       []common.Address
	}
	perUser := make(map[common.Address]*userAssetInfo, len(users))
	allTokens := make(map[common.Address]bool)
	userAssetsForData := make(map[common.Address][]common.Address) // for batchGetUserReserveDataMultiUser

	for _, user := range users {
		if _, hasErr := reserveErrors[user]; hasErr {
			continue
		}
		reserves, ok := reservesMap[user]
		if !ok {
			return nil, fmt.Errorf("missing reserves result for user %s", user.Hex())
		}

		info := &userAssetInfo{}
		assetsNeeded := make(map[common.Address]bool)
		for _, rv := range reserves {
			if rv.UnderlyingAsset == (common.Address{}) {
				continue
			}
			if rv.ScaledATokenBalance.Cmp(big.NewInt(0)) > 0 && rv.UsageAsCollateralEnabledOnUser {
				info.collateralAssets = append(info.collateralAssets, rv.UnderlyingAsset)
				allTokens[rv.UnderlyingAsset] = true
				assetsNeeded[rv.UnderlyingAsset] = true
			}
			if rv.ScaledVariableDebt.Cmp(big.NewInt(0)) > 0 {
				info.debtAssets = append(info.debtAssets, rv.UnderlyingAsset)
				allTokens[rv.UnderlyingAsset] = true
				assetsNeeded[rv.UnderlyingAsset] = true
			}
		}

		if len(assetsNeeded) > 0 {
			perUser[user] = info
			assets := make([]common.Address, 0, len(assetsNeeded))
			for a := range assetsNeeded {
				assets = append(assets, a)
			}
			userAssetsForData[user] = assets
		}
	}

	// Fetch token metadata for all unique tokens across all users.
	var metadataMap map[common.Address]TokenMetadata
	if len(allTokens) > 0 {
		metadataMap, err = blockchainSvc.BatchGetTokenMetadata(ctx, allTokens, big.NewInt(blockNumber))
		if err != nil {
			return nil, fmt.Errorf("failed to get token metadata: %w", err)
		}
	}

	// Multicall 2: getUserReserveData for all user×asset pairs in one call.
	var allActualData map[common.Address]map[common.Address]ActualUserReserveData
	if len(userAssetsForData) > 0 {
		allActualData, err = blockchainSvc.batchGetUserReserveDataMultiUser(ctx, userAssetsForData, blockNumber)
		if err != nil {
			return nil, fmt.Errorf("batch getUserReserveData failed: %w", err)
		}
	}

	// Build results for each user.
	results := make(map[common.Address]UserPositionResult, len(users))
	for _, user := range users {
		if resErr, hasErr := reserveErrors[user]; hasErr {
			results[user] = UserPositionResult{Err: resErr}
			continue
		}

		info, hasPositions := perUser[user]
		if !hasPositions {
			results[user] = UserPositionResult{
				Collaterals: []CollateralData{},
				Debts:       []DebtData{},
			}
			continue
		}

		userActualData := allActualData[user]
		if userActualData == nil {
			userActualData = make(map[common.Address]ActualUserReserveData)
		}

		collaterals := buildCollateralData(info.collateralAssets, metadataMap, userActualData, r.logger)
		debts := buildDebtData(info.debtAssets, metadataMap, userActualData, r.logger)
		results[user] = UserPositionResult{
			Collaterals: collaterals,
			Debts:       debts,
		}
	}

	return results, nil
}

func buildCollateralData(assets []common.Address, metadataMap map[common.Address]TokenMetadata, actualDataMap map[common.Address]ActualUserReserveData, logger *slog.Logger) []CollateralData {
	var collaterals []CollateralData
	for _, asset := range assets {
		metadata, ok := metadataMap[asset]
		if !ok || metadata.Decimals == 0 {
			logger.Error("Failed to get collateral token metadata",
				"action", "skipped",
				"token", asset.Hex())
			continue
		}

		actualData, ok := actualDataMap[asset]
		if !ok {
			logger.Error("Failed to get actual balance",
				"action", "skipped",
				"token", asset.Hex())
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
	return collaterals
}

func buildDebtData(assets []common.Address, metadataMap map[common.Address]TokenMetadata, actualDataMap map[common.Address]ActualUserReserveData, logger *slog.Logger) []DebtData {
	var debts []DebtData
	for _, asset := range assets {
		metadata, ok := metadataMap[asset]
		if !ok || metadata.Decimals == 0 {
			logger.Error("Failed to get debt token metadata",
				"action", "skipped",
				"token", asset.Hex())
			continue
		}

		actualData, ok := actualDataMap[asset]
		if !ok {
			logger.Error("Failed to get actual debt",
				"action", "skipped",
				"token", asset.Hex())
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
	return debts
}

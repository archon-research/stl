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
//
// NOTE: The UIPoolDataProvider's ScaledVariableDebt field is unreliable on
// upgraded Aave V3 deployments (returns 0 even for users with debt). To work
// around this, we query the PoolDataProvider for ALL reserves and derive debt
// from the actual CurrentVariableDebt values.
func (r *PositionReader) GetUserPositionData(ctx context.Context, user common.Address, protocolAddress common.Address, chainID, blockNumber int64) ([]CollateralData, []DebtData, error) {
	blockchainSvc, err := r.GetOrCreateBlockchainService(chainID, protocolAddress)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get blockchain service: %w", err)
	}

	reserves, err := blockchainSvc.getUserReservesData(ctx, user, blockNumber)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get user reserves data: %w", err)
	}

	// Collect ALL non-zero-address reserves. We query the PoolDataProvider for
	// all of them because the UIPoolDataProvider's ScaledVariableDebt is broken
	// on upgraded Aave V3 deployments.
	var allReserveAssets []common.Address
	tokensToFetch := make(map[common.Address]bool)
	for _, rv := range reserves {
		// Zero-address entries are ABI padding from the contract response, not real reserves.
		if rv.UnderlyingAsset == (common.Address{}) {
			continue
		}
		allReserveAssets = append(allReserveAssets, rv.UnderlyingAsset)
		tokensToFetch[rv.UnderlyingAsset] = true
	}

	if len(tokensToFetch) == 0 {
		return []CollateralData{}, []DebtData{}, nil
	}

	metadataMap, err := blockchainSvc.BatchGetTokenMetadata(ctx, tokensToFetch, big.NewInt(blockNumber))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get token metadata: %w", err)
	}

	actualDataMap, err := blockchainSvc.batchGetUserReserveData(ctx, allReserveAssets, user, blockNumber)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get user reserve data: %w", err)
	}

	// Both buildCollateralData and buildDebtData filter based on actual
	// PoolDataProvider values (CurrentATokenBalance, CurrentVariableDebt),
	// so passing all reserves is safe — only non-zero positions are included.
	collaterals, err := buildCollateralData(allReserveAssets, metadataMap, actualDataMap)
	if err != nil {
		return nil, nil, fmt.Errorf("building collateral data: %w", err)
	}
	debts, err := buildDebtData(allReserveAssets, metadataMap, actualDataMap)
	if err != nil {
		return nil, nil, fmt.Errorf("building debt data: %w", err)
	}

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
//
// NOTE: See GetUserPositionData comment — the UIPoolDataProvider's
// ScaledVariableDebt is unreliable on upgraded Aave V3 deployments, so we
// query the PoolDataProvider for ALL reserves per user and derive both
// collateral and debt from the actual values.
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

	// For each user, collect ALL non-zero-address reserves. We query the
	// PoolDataProvider for all of them because the UIPoolDataProvider's
	// ScaledVariableDebt is broken on upgraded Aave V3 deployments.
	perUserReserves := make(map[common.Address][]common.Address, len(users))
	allTokens := make(map[common.Address]bool)
	userAssetsForData := make(map[common.Address][]common.Address)

	for _, user := range users {
		if _, hasErr := reserveErrors[user]; hasErr {
			continue
		}
		reserves, ok := reservesMap[user]
		if !ok {
			return nil, fmt.Errorf("missing reserves result for user %s", user.Hex())
		}

		var reserveAssets []common.Address
		for _, rv := range reserves {
			// Zero-address entries are ABI padding from the contract response, not real reserves.
			if rv.UnderlyingAsset == (common.Address{}) {
				continue
			}
			reserveAssets = append(reserveAssets, rv.UnderlyingAsset)
			allTokens[rv.UnderlyingAsset] = true
		}

		if len(reserveAssets) > 0 {
			perUserReserves[user] = reserveAssets
			userAssetsForData[user] = reserveAssets
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

	// Multicall 2: getUserReserveData for all user×reserve pairs in one call.
	var allActualData map[common.Address]map[common.Address]ActualUserReserveData
	if len(userAssetsForData) > 0 {
		allActualData, err = blockchainSvc.batchGetUserReserveDataMultiUser(ctx, userAssetsForData, blockNumber)
		if err != nil {
			return nil, fmt.Errorf("batch getUserReserveData failed: %w", err)
		}
	}

	// Build results for each user from actual PoolDataProvider data.
	results := make(map[common.Address]UserPositionResult, len(users))
	for _, user := range users {
		if resErr, hasErr := reserveErrors[user]; hasErr {
			results[user] = UserPositionResult{Err: resErr}
			continue
		}

		reserves, hasReserves := perUserReserves[user]
		if !hasReserves {
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

		collaterals, err := buildCollateralData(reserves, metadataMap, userActualData)
		if err != nil {
			return nil, fmt.Errorf("building collateral data for user %s: %w", user.Hex(), err)
		}
		debts, err := buildDebtData(reserves, metadataMap, userActualData)
		if err != nil {
			return nil, fmt.Errorf("building debt data for user %s: %w", user.Hex(), err)
		}
		results[user] = UserPositionResult{
			Collaterals: collaterals,
			Debts:       debts,
		}
	}

	return results, nil
}

func buildCollateralData(assets []common.Address, metadataMap map[common.Address]TokenMetadata, actualDataMap map[common.Address]ActualUserReserveData) ([]CollateralData, error) {
	var collaterals []CollateralData
	for _, asset := range assets {
		// Missing metadata is a programming bug — always error.
		metadata, ok := metadataMap[asset]
		if !ok || metadata.Decimals == 0 {
			return nil, fmt.Errorf("missing or invalid metadata for collateral token %s", asset.Hex())
		}

		// Missing actual data indicates a transient RPC failure for this asset — skip it.
		actualData, ok := actualDataMap[asset]
		if !ok {
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
	return collaterals, nil
}

func buildDebtData(assets []common.Address, metadataMap map[common.Address]TokenMetadata, actualDataMap map[common.Address]ActualUserReserveData) ([]DebtData, error) {
	var debts []DebtData
	for _, asset := range assets {
		// Missing metadata is a programming bug — always error.
		metadata, ok := metadataMap[asset]
		if !ok || metadata.Decimals == 0 {
			return nil, fmt.Errorf("missing or invalid metadata for debt token %s", asset.Hex())
		}

		// Missing actual data indicates a transient RPC failure for this asset — skip it.
		actualData, ok := actualDataMap[asset]
		if !ok {
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
	return debts, nil
}

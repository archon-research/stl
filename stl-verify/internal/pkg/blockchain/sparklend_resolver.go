package blockchain

import (
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// SparkLendResolver resolves oracle addresses for SparkLend by calling
// getPriceOracle() on its PoolAddressesProvider contract.
type SparkLendResolver struct {
	mc           outbound.Multicaller
	providerAddr common.Address
	providerABI  *abi.ABI
}

// NewSparkLendResolver creates a new SparkLendResolver.
func NewSparkLendResolver(mc outbound.Multicaller, providerAddr common.Address) (*SparkLendResolver, error) {
	providerABI, err := abis.GetPoolAddressesProviderABI()
	if err != nil {
		return nil, fmt.Errorf("loading PoolAddressesProvider ABI: %w", err)
	}
	return &SparkLendResolver{
		mc:           mc,
		providerAddr: providerAddr,
		providerABI:  providerABI,
	}, nil
}

func (r *SparkLendResolver) ResolveOracleAddresses(ctx context.Context, blockNumber int64) ([]common.Address, error) {
	addr, err := resolveViaPoolAddressProvider(ctx, r.mc, r.providerAddr, r.providerABI, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("resolving SparkLend oracle: %w", err)
	}
	return []common.Address{addr}, nil
}

// newSparkLendResolverFromMetadata creates a SparkLendResolver from protocol metadata.
func newSparkLendResolverFromMetadata(protocol *entity.Protocol, mc outbound.Multicaller) (outbound.OracleResolver, error) {
	addrStr, ok := protocol.Metadata["pool_addresses_provider"].(string)
	if !ok || addrStr == "" {
		return nil, fmt.Errorf("protocol %q missing pool_addresses_provider in metadata", protocol.Name)
	}
	providerAddr := common.HexToAddress(addrStr)
	return NewSparkLendResolver(mc, providerAddr)
}

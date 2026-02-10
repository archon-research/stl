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

// AaveResolver resolves oracle addresses for Aave V3 by calling
// getPriceOracle() on its PoolAddressesProvider contract.
type AaveResolver struct {
	mc           outbound.Multicaller
	providerAddr common.Address
	providerABI  *abi.ABI
}

// NewAaveResolver creates a new AaveResolver.
func NewAaveResolver(mc outbound.Multicaller, providerAddr common.Address) (*AaveResolver, error) {
	providerABI, err := abis.GetPoolAddressesProviderABI()
	if err != nil {
		return nil, fmt.Errorf("loading PoolAddressesProvider ABI: %w", err)
	}
	return &AaveResolver{
		mc:           mc,
		providerAddr: providerAddr,
		providerABI:  providerABI,
	}, nil
}

func (r *AaveResolver) ResolveOracleAddresses(ctx context.Context, blockNumber int64) ([]common.Address, error) {
	addr, err := resolveViaPoolAddressProvider(ctx, r.mc, r.providerAddr, r.providerABI, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("resolving Aave oracle: %w", err)
	}
	return []common.Address{addr}, nil
}

// newAaveResolverFromMetadata creates an AaveResolver from protocol metadata.
func newAaveResolverFromMetadata(protocol *entity.Protocol, mc outbound.Multicaller) (outbound.OracleResolver, error) {
	addrStr, ok := protocol.Metadata["pool_addresses_provider"].(string)
	if !ok || addrStr == "" {
		return nil, fmt.Errorf("protocol %q missing pool_addresses_provider in metadata", protocol.Name)
	}
	providerAddr := common.HexToAddress(addrStr)
	return NewAaveResolver(mc, providerAddr)
}

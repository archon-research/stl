package blockchain

import (
	"fmt"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// NewOracleResolver creates an OracleResolver from protocol metadata.
// Returns an error if oracle_resolver_type is set but not recognized.
func NewOracleResolver(protocol *entity.Protocol, mc outbound.Multicaller) (outbound.OracleResolver, error) {
	resolverType, _ := protocol.Metadata["oracle_resolver_type"].(string)
	if resolverType == "" {
		return nil, fmt.Errorf("protocol %q has no oracle_resolver_type in metadata", protocol.Name)
	}
	switch resolverType {
	case "sparklend":
		return newSparkLendResolverFromMetadata(protocol, mc)
	case "aave":
		return newAaveResolverFromMetadata(protocol, mc)
	default:
		return nil, fmt.Errorf("unsupported oracle_resolver_type %q for protocol %q", resolverType, protocol.Name)
	}
}

package outbound

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
)

// OracleResolver resolves onchain oracle addresses for a protocol at a given block.
// A protocol may use multiple oracles (e.g., separate feeds for different asset groups).
type OracleResolver interface {
	ResolveOracleAddresses(ctx context.Context, blockNumber int64) ([]common.Address, error)
}

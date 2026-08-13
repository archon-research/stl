package uniswapv3indexer

import "github.com/archon-research/stl/stl-verify/internal/ports/outbound"

// RegisteredPoolsFromRows maps UniswapV3PoolRow slices to RegisteredPool slices.
func RegisteredPoolsFromRows(rows []outbound.UniswapV3PoolRow) []RegisteredPool {
	pools := make([]RegisteredPool, 0, len(rows))
	for _, row := range rows {
		pools = append(pools, RegisteredPool{
			ID:             row.ID,
			Address:        row.Address,
			Token0:         row.Token0,
			Token1:         row.Token1,
			Token0Decimals: row.Token0Decimals,
			Token1Decimals: row.Token1Decimals,
			Fee:            row.Fee,
			TickSpacing:    row.TickSpacing,
			DeployBlock:    row.DeployBlock,
		})
	}
	return pools
}

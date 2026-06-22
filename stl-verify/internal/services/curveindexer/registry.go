package curveindexer

import "github.com/archon-research/stl/stl-verify/internal/ports/outbound"

// IndexPoolsByAddress maps CurvePoolRow slices to RegisteredPool slices.
func IndexPoolsByAddress(rows []outbound.CurvePoolRow) []RegisteredPool {
	pools := make([]RegisteredPool, 0, len(rows))
	for _, row := range rows {
		pools = append(pools, RegisteredPool{
			ID:           row.ID,
			Address:      row.Address,
			Kind:         PoolKind(row.Kind),
			NCoins:       row.NCoins,
			CoinTokenIDs: row.CoinTokenIDs,
			CoinDecimals: row.CoinDecimals,
			DeployBlock:  row.DeployBlock,
		})
	}
	return pools
}

// NewHandlerRegistry builds the standard PoolKind -> PoolClassHandler map for
// the Curve worker, where both stableswap kinds share the same handler.
func NewHandlerRegistry(stable, crypto PoolClassHandler) map[PoolKind]PoolClassHandler {
	return map[PoolKind]PoolClassHandler{
		KindStableswapPreNG: stable,
		KindStableswapNG:    stable,
		KindCryptoswap:      crypto,
	}
}

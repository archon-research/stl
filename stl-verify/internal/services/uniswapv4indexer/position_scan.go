package uniswapv4indexer

import (
	"fmt"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// modifyLiquidityEventName is the one PoolManager event a position can be
// discovered from: v4-core exposes no way to enumerate a pool's positions, so a
// key exists only where a ModifyLiquidity log named it.
const modifyLiquidityEventName = "ModifyLiquidity"

// ModifyLiquidityTopic0 returns the topic0 an eth_getLogs filter asks for to
// find every position a pool has ever held.
func ModifyLiquidityTopic0() (common.Hash, error) {
	a, err := PoolManagerABI()
	if err != nil {
		return common.Hash{}, err
	}
	ev, ok := a.Events[modifyLiquidityEventName]
	if !ok {
		return common.Hash{}, fmt.Errorf("%s is not in the PoolManager ABI", modifyLiquidityEventName)
	}
	return ev.ID, nil
}

// PositionKeysFromLogs decodes a historical batch of ModifyLiquidity logs into
// the deduplicated, Compare-sorted position keys per registered pool, keyed by
// the pool's uniswap_v4_pool id. It applies the same emitter, hex-word and
// PoolId-routing guards the per-block decode path does.
//
// A log for a pool outside the registry is dropped, exactly as
// decodePoolKeyedLog drops one. Everything else fails the batch: the caller
// asked the node for this contract and this topic0, so a foreign emitter or a
// different event means the filter or the response is wrong, and silently
// skipping it would leave a hole no rerun could find.
func PositionKeysFromLogs(
	logs []shared.Log,
	poolsByID map[common.Hash]RegisteredPool,
	poolManager common.Address,
) (map[int64][]entity.UniswapV4PositionKey, error) {
	a, err := PoolManagerABI()
	if err != nil {
		return nil, fmt.Errorf("loading PoolManager ABI: %w", err)
	}
	ev, ok := a.Events[modifyLiquidityEventName]
	if !ok {
		return nil, fmt.Errorf("%s is not in the PoolManager ABI", modifyLiquidityEventName)
	}

	byPool := make(map[int64][]entity.UniswapV4PositionKey)
	for _, log := range logs {
		poolID, key, tracked, err := decodeScannedModifyLiquidity(ev, log, poolsByID, poolManager)
		if err != nil {
			return nil, err
		}
		if tracked {
			byPool[poolID] = append(byPool[poolID], key)
		}
	}

	for poolID, keys := range byPool {
		byPool[poolID] = MergePositionKeys(keys, nil)
	}
	return byPool, nil
}

// decodeScannedModifyLiquidity turns one scanned log into the pool it belongs
// to and the position key it touched. tracked is false only for a log whose
// PoolId is outside the registry.
func decodeScannedModifyLiquidity(
	ev abi.Event,
	log shared.Log,
	poolsByID map[common.Hash]RegisteredPool,
	poolManager common.Address,
) (poolID int64, key entity.UniswapV4PositionKey, tracked bool, err error) {
	if err := assertScannedLogSite(ev, log, poolManager); err != nil {
		return 0, entity.UniswapV4PositionKey{}, false, err
	}

	pool, tracked := poolsByID[mustIndexedPoolID(log)]
	if !tracked {
		return 0, entity.UniswapV4PositionKey{}, false, nil
	}

	data, err := shared.DecodeLog(ev, log)
	if err != nil {
		return 0, entity.UniswapV4PositionKey{}, false,
			fmt.Errorf("decoding %s log (tx %s, index %s): %w", ev.Name, log.TransactionHash, log.LogIndex, err)
	}
	key, err = modifyLiquidityKey(data)
	if err != nil {
		return 0, entity.UniswapV4PositionKey{}, false,
			fmt.Errorf("reading position key from %s log (tx %s, index %s): %w", ev.Name, log.TransactionHash, log.LogIndex, err)
	}
	// The key is packed straight into an int24 getPositionInfo argument, which
	// the ABI encoder does not range-check.
	if err := key.Validate(); err != nil {
		return 0, entity.UniswapV4PositionKey{}, false,
			fmt.Errorf("position key %+v from %s log (tx %s, index %s): %w", key, ev.Name, log.TransactionHash, log.LogIndex, err)
	}
	return pool.ID, key, true, nil
}

// assertScannedLogSite rejects anything the node returned that the filter did
// not ask for, plus the malformed-hex-word cases assertHexWords covers.
func assertScannedLogSite(ev abi.Event, log shared.Log, poolManager common.Address) error {
	if !common.IsHexAddress(log.Address) {
		return fmt.Errorf("scanned log (index %s) has invalid address %q", log.LogIndex, log.Address)
	}
	if addr := common.HexToAddress(log.Address); !shared.LogBelongsTo(addr, poolManager) {
		return fmt.Errorf("scanned log (tx %s, index %s) was emitted by %s, not the PoolManager %s", log.TransactionHash, log.LogIndex, addr, poolManager)
	}
	if err := assertHexWords(log); err != nil {
		return err
	}
	if _, err := shared.ParseHexUint(log.LogIndex); err != nil {
		return fmt.Errorf("parsing log index %q: %w", log.LogIndex, err)
	}
	if len(log.Topics) == 0 || common.HexToHash(log.Topics[0]) != ev.ID {
		return fmt.Errorf("scanned log (tx %s, index %s) topic0 is not %s", log.TransactionHash, log.LogIndex, ev.Name)
	}
	if len(log.Topics) < 2 {
		return fmt.Errorf("%s log (tx %s, index %s) carries no indexed pool id", ev.Name, log.TransactionHash, log.LogIndex)
	}
	return nil
}

// mustIndexedPoolID reads topics[1]; assertScannedLogSite has already proved it
// is present and a full 32-byte hex word.
func mustIndexedPoolID(log shared.Log) common.Hash {
	return common.HexToHash(log.Topics[1])
}

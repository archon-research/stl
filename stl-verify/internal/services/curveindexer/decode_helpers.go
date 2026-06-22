package curveindexer

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// decodeLog extracts both indexed (from topics) and non-indexed (from data) fields
// into a flat map, following the morpho_indexer parseTopics/parseData pattern.
func decodeLog(ev *abi.Event, log shared.Log) (map[string]any, error) {
	out := make(map[string]any)

	var indexed abi.Arguments
	for _, arg := range ev.Inputs {
		if arg.Indexed {
			indexed = append(indexed, arg)
		}
	}
	if len(indexed) > 0 {
		hashes := make([]common.Hash, 0, len(log.Topics)-1)
		for i := 1; i < len(log.Topics); i++ {
			hashes = append(hashes, common.HexToHash(log.Topics[i]))
		}
		if err := abi.ParseTopicsIntoMap(out, indexed, hashes); err != nil {
			return nil, fmt.Errorf("parsing indexed params: %w", err)
		}
	}

	var nonIndexed abi.Arguments
	for _, arg := range ev.Inputs {
		if !arg.Indexed {
			nonIndexed = append(nonIndexed, arg)
		}
	}
	if len(nonIndexed) > 0 && len(log.Data) > 2 {
		raw := common.FromHex(log.Data)
		if err := nonIndexed.UnpackIntoMap(out, raw); err != nil {
			return nil, fmt.Errorf("parsing non-indexed params: %w", err)
		}
	}

	return out, nil
}

func getAddrField(data map[string]any, key string) (common.Address, error) {
	v, ok := data[key]
	if !ok {
		return common.Address{}, fmt.Errorf("missing field: %s", key)
	}
	addr, ok := v.(common.Address)
	if !ok {
		return common.Address{}, fmt.Errorf("field %s: unexpected type %T", key, v)
	}
	return addr, nil
}

func getBigIntField(data map[string]any, key string) (*big.Int, error) {
	v, ok := data[key]
	if !ok {
		return nil, fmt.Errorf("missing field: %s", key)
	}
	b, ok := v.(*big.Int)
	if !ok {
		return nil, fmt.Errorf("field %s: unexpected type %T", key, v)
	}
	return b, nil
}

func getBigIntSliceField(data map[string]any, key string) ([]*big.Int, error) {
	v, ok := data[key]
	if !ok {
		return nil, fmt.Errorf("missing field: %s", key)
	}
	slice, ok := v.([]*big.Int)
	if !ok {
		return nil, fmt.Errorf("field %s: unexpected type %T", key, v)
	}
	return slice, nil
}

// parseHexUint parses a 0x-prefixed hex string into a uint, as used in
// shared.Log.LogIndex.
func parseHexUint(s string) (uint, error) {
	s = strings.TrimPrefix(s, "0x")
	if s == "" {
		return 0, nil
	}
	n, err := strconv.ParseUint(s, 16, 64)
	if err != nil {
		return 0, err
	}
	return uint(n), nil
}

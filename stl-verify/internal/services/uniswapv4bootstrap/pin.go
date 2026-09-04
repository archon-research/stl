package uniswapv4bootstrap

import (
	"context"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/pkg/hexutil"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

type pinnedBlock struct {
	number int64
	hash   common.Hash
	ts     time.Time
}

func pinBlock(ctx context.Context, client outbound.LogScanClient, finalityDepth, override int64) (pinnedBlock, error) {
	head, err := client.GetCurrentBlockNumber(ctx)
	if err != nil {
		return pinnedBlock{}, fmt.Errorf("reading the chain head: %w", err)
	}
	number, err := finalitySafeHeight(head, finalityDepth, override)
	if err != nil {
		return pinnedBlock{}, err
	}

	header, err := client.GetBlockHeaderByNumber(ctx, number)
	if err != nil {
		return pinnedBlock{}, fmt.Errorf("reading the header of pinned block %d: %w", number, err)
	}
	return parsePinnedHeader(number, header)
}

// Everything downstream rests on the pin being unreorgable: rows are stamped
// block_version 0, and a redelivery of a shallow pin's height would make the
// live indexer re-read every historical position of that pool at once.
func finalitySafeHeight(head, finalityDepth, override int64) (int64, error) {
	deepest := head - finalityDepth
	if deepest <= 0 {
		return 0, fmt.Errorf("chain head %d is not %d blocks past genesis: no finality-safe block to pin", head, finalityDepth)
	}
	if override == 0 {
		return deepest, nil
	}
	if override > deepest {
		return 0, fmt.Errorf("pinned block %d is inside the reorg window: with chain head %d and a finality depth of %d blocks, the deepest finality-safe block is %d",
			override, head, finalityDepth, deepest)
	}
	return override, nil
}

func parsePinnedHeader(number int64, header *outbound.BlockHeader) (pinnedBlock, error) {
	if header == nil {
		return pinnedBlock{}, fmt.Errorf("block %d: the node returned no header", number)
	}
	if !shared.IsHexWord(header.Hash) {
		return pinnedBlock{}, fmt.Errorf("block %d: hash %q is not a 32-byte hex word", number, header.Hash)
	}
	got, err := hexutil.ParseInt64(header.Number)
	if err != nil {
		return pinnedBlock{}, fmt.Errorf("block %d: parsing header number %q: %w", number, header.Number, err)
	}
	if got != number {
		return pinnedBlock{}, fmt.Errorf("block %d: the node answered with a header whose number is %d", number, got)
	}
	seconds, err := hexutil.ParseInt64(header.Timestamp)
	if err != nil {
		return pinnedBlock{}, fmt.Errorf("block %d: parsing header timestamp %q: %w", number, header.Timestamp, err)
	}
	// A zero timestamp lands rows in 1970, outside the chunk every sibling query
	// bounds its scan to.
	if seconds <= 0 {
		return pinnedBlock{}, fmt.Errorf("block %d: header timestamp is %d", number, seconds)
	}

	return pinnedBlock{
		number: number,
		hash:   common.HexToHash(header.Hash),
		ts:     time.Unix(seconds, 0).UTC(),
	}, nil
}

func assertPinStable(ctx context.Context, client outbound.LogScanClient, pin pinnedBlock) error {
	header, err := client.GetBlockHeaderByNumber(ctx, pin.number)
	if err != nil {
		return fmt.Errorf("re-reading pinned block %d to confirm it is stable: %w", pin.number, err)
	}
	current, err := parsePinnedHeader(pin.number, header)
	if err != nil {
		return err
	}
	if current.hash != pin.hash {
		return fmt.Errorf("pinned block %d was %s at the start of the scan and is %s now: the chain reorged past the finality depth, re-run against a fresh pin",
			pin.number, pin.hash, current.hash)
	}
	return nil
}

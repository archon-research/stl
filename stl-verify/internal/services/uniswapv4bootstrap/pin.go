package uniswapv4bootstrap

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/pkg/hexutil"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// pinnedBlock is the single block every read is pinned to and every row is
// stamped with. One block for the whole run is what makes the snapshot
// internally consistent: a position read at height N and another at N+1000
// would not describe the same moment.
type pinnedBlock struct {
	number int64
	hash   common.Hash
	ts     time.Time
}

// pinBlock resolves the run's pinned block: override when non-zero, otherwise
// finalityDepth below the head. It reads the header so the run has the hash to
// pin its state reads to and the timestamp to stamp its rows with.
func pinBlock(ctx context.Context, client outbound.LogScanClient, finalityDepth, override int64) (pinnedBlock, error) {
	number := override
	if number == 0 {
		head, err := client.GetCurrentBlockNumber(ctx)
		if err != nil {
			return pinnedBlock{}, fmt.Errorf("reading the chain head: %w", err)
		}
		number = head - finalityDepth
		if number <= 0 {
			return pinnedBlock{}, fmt.Errorf("chain head %d is not %d blocks past genesis: no finality-safe block to pin", head, finalityDepth)
		}
	}

	header, err := client.GetBlockHeaderByNumber(ctx, number)
	if err != nil {
		return pinnedBlock{}, fmt.Errorf("reading the header of pinned block %d: %w", number, err)
	}
	return parsePinnedHeader(number, header)
}

// parsePinnedHeader converts a header into the pin, rejecting every field the
// run would otherwise carry silently wrong: a truncated hash pins reads to a
// block that does not exist, a header for another height means the node
// answered a different question, and a zero timestamp lands rows in 1970 —
// outside the hypertable chunk every sibling query bounds its scan to.
func parsePinnedHeader(number int64, header *outbound.BlockHeader) (pinnedBlock, error) {
	if header == nil {
		return pinnedBlock{}, fmt.Errorf("block %d: the node returned no header", number)
	}
	if !isHexWord(header.Hash) {
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
	if seconds <= 0 {
		return pinnedBlock{}, fmt.Errorf("block %d: header timestamp is %d", number, seconds)
	}

	return pinnedBlock{
		number: number,
		hash:   common.HexToHash(header.Hash),
		ts:     time.Unix(seconds, 0).UTC(),
	}, nil
}

// assertPinStable re-reads the pinned height and fails if it now names another
// block. The pin sits past finality, so this cannot legitimately fire; when it
// does, the run's whole premise — one stable block behind every read — is gone,
// and the only safe answer is to stop and re-run against a fresh pin rather
// than persist a snapshot stitched across two forks.
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

// isHexWord mirrors the indexer's log guard: common.HexToHash left-pads a short
// value and truncates at the first non-hex character, so a malformed hash
// otherwise becomes a plausible-looking wrong one.
func isHexWord(value string) bool {
	return len(value) == 66 && strings.HasPrefix(value, "0x") && common.IsHexHash(value)
}

package outbound

import "context"

// ArchiveReader reports what the raw block archive already holds at a height: the
// version slots that are taken, and the block a taken slot holds. A correction
// goes into a free slot, and only where the archive does not already hold the
// canonical block.
type ArchiveReader interface {
	// HighestVersion returns the highest version any archived object carries for
	// blockNumber. found is false when the archive holds no object at all at that
	// height, and version says nothing then. An object of any data type occupies
	// its version, so a height archived only halfway still answers found.
	HighestVersion(ctx context.Context, blockNumber int64) (version int, found bool, err error)

	// BlockHashAt returns the block hash the archive holds at (blockNumber,
	// version). found is false when nothing there names one — no block object, no
	// receipts, or a payload that identifies no block — which is a height to
	// repair, not a failure. A read that fails is an error.
	BlockHashAt(ctx context.Context, blockNumber int64, version int) (hash string, found bool, err error)
}

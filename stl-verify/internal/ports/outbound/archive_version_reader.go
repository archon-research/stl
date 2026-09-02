package outbound

import "context"

// ArchiveVersionReader reports which version slots the raw block archive already
// holds at a height, so a correction can be written into a free one instead of
// over data that is already there.
type ArchiveVersionReader interface {
	// HighestVersion returns the highest version any archived object carries for
	// blockNumber. found is false when the archive holds no object at all at that
	// height, and version says nothing then. An object of any data type occupies
	// its version, so a height archived only halfway still answers found.
	HighestVersion(ctx context.Context, blockNumber int64) (version int, found bool, err error)
}

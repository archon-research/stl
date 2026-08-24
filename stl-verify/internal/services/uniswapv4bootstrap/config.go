package uniswapv4bootstrap

import "fmt"

// Defaults for the knobs a normal run never sets.
const (
	// DefaultFinalityDepth is how far below the head the run pins. 64 blocks is
	// two epochs on Ethereum mainnet — comfortably past finalisation — which is
	// what lets this backfiller ignore reorgs entirely instead of versioning
	// rows: the pinned block cannot be reorged out under it.
	DefaultFinalityDepth = int64(64)
	// DefaultInitialWindow starts the log scan wide and lets the bisect find the
	// provider's real ceiling, rather than crawling a known-safe 10k range over
	// four million blocks.
	DefaultInitialWindow = int64(500_000)
	// DefaultMinWindow is one block: below that there is nothing left to bisect,
	// and a refusal there is a hard failure rather than a narrower retry.
	DefaultMinWindow = int64(1)
	DefaultMaxWindow = int64(1_000_000)
	// DefaultPositionBatch matches the getPositionInfo multicall cap, so one
	// persisted batch is one RPC round trip.
	DefaultPositionBatch = 500
)

// Config is the one-shot run's tuning. Zero values take the defaults above;
// FromBlock and PinBlock stay zero for a normal run and are set only to
// reproduce or narrow a run.
type Config struct {
	ChainID int64
	// FromBlock overrides the scan start. Zero means the lowest deploy_block
	// among the pools being scanned, which is the earliest height any of their
	// positions can exist at.
	FromBlock int64
	// PinBlock overrides the pinned height. Zero means head - FinalityDepth.
	PinBlock      int64
	FinalityDepth int64
	InitialWindow int64
	MinWindow     int64
	MaxWindow     int64
	// PositionBatch bounds how many positions one read-and-persist unit carries,
	// keeping both the multicall response and the write transaction bounded.
	PositionBatch int
}

func (c Config) withDefaults() Config {
	if c.FinalityDepth == 0 {
		c.FinalityDepth = DefaultFinalityDepth
	}
	if c.InitialWindow == 0 {
		c.InitialWindow = DefaultInitialWindow
	}
	if c.MinWindow == 0 {
		c.MinWindow = DefaultMinWindow
	}
	if c.MaxWindow == 0 {
		c.MaxWindow = DefaultMaxWindow
	}
	if c.PositionBatch == 0 {
		c.PositionBatch = DefaultPositionBatch
	}
	return c
}

// Validate reports whether the config is usable once the unset knobs take
// their defaults. Exported so an entry point can reject bad settings while
// parsing, before it opens a database connection or dials an RPC endpoint.
func (c Config) Validate() error {
	return c.withDefaults().validate()
}

func (c Config) validate() error {
	switch {
	case c.ChainID <= 0:
		return fmt.Errorf("chainID must be positive, got %d", c.ChainID)
	case c.FinalityDepth < 0:
		return fmt.Errorf("finality depth must not be negative, got %d", c.FinalityDepth)
	case c.FromBlock < 0:
		return fmt.Errorf("fromBlock must not be negative, got %d", c.FromBlock)
	case c.PinBlock < 0:
		return fmt.Errorf("pinBlock must not be negative, got %d", c.PinBlock)
	case c.PinBlock > 0 && c.FromBlock > c.PinBlock:
		return fmt.Errorf("fromBlock %d is above pinBlock %d", c.FromBlock, c.PinBlock)
	case c.MinWindow <= 0:
		return fmt.Errorf("minWindow must be positive, got %d", c.MinWindow)
	case c.MaxWindow < c.MinWindow:
		return fmt.Errorf("maxWindow %d is below minWindow %d", c.MaxWindow, c.MinWindow)
	case c.InitialWindow < c.MinWindow || c.InitialWindow > c.MaxWindow:
		return fmt.Errorf("initialWindow %d is outside [minWindow %d, maxWindow %d]", c.InitialWindow, c.MinWindow, c.MaxWindow)
	case c.PositionBatch <= 0:
		return fmt.Errorf("positionBatch must be positive, got %d", c.PositionBatch)
	}
	return nil
}

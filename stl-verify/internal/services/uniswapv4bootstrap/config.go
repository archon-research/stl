package uniswapv4bootstrap

import "fmt"

const (
	// 64 blocks is two epochs on Ethereum mainnet, past finalisation: the pinned
	// block cannot be reorged out under the run. It is a mainnet number (two
	// minutes on Base, sixteen seconds on Arbitrum), so it is the default on
	// mainnet only; every other chain states its own depth.
	DefaultFinalityDepth = int64(64)
	// Wide on purpose: the bisect finds the provider's real ceiling in fewer
	// requests than crawling four million blocks at a known-safe 10k.
	DefaultInitialWindow = int64(500_000)
	DefaultMinWindow     = int64(1)
	DefaultMaxWindow     = int64(1_000_000)
	// The getPositionInfo multicall cap, so one persisted batch is one round trip.
	DefaultPositionBatch = 500
)

type Config struct {
	ChainID       int64
	FromBlock     int64
	PinBlock      int64
	FinalityDepth int64
	InitialWindow int64
	MinWindow     int64
	MaxWindow     int64
	PositionBatch int
}

const mainnetChainID = int64(1)

func (c Config) withDefaults() Config {
	if c.FinalityDepth == 0 && c.ChainID == mainnetChainID {
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

// Validate applies the defaults first, so an entry point can reject bad
// settings while parsing rather than after it has dialled anything.
func (c Config) Validate() error {
	return c.withDefaults().validate()
}

func (c Config) validate() error {
	switch {
	case c.ChainID <= 0:
		return fmt.Errorf("chainID must be positive, got %d", c.ChainID)
	case c.FinalityDepth < 0:
		return fmt.Errorf("finality depth must not be negative, got %d", c.FinalityDepth)
	// An explicit pin alone is not enough: finalitySafeHeight's reorg-window
	// refusal is `pin > head - depth`, which a zero depth disables.
	case c.FinalityDepth == 0:
		return fmt.Errorf("chain %d has no default finality depth: pass one explicitly (%d is two mainnet epochs and means nothing elsewhere)", c.ChainID, DefaultFinalityDepth)
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

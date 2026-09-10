package main

import (
	"errors"
	"flag"
	"fmt"

	"github.com/archon-research/stl/stl-verify/internal/pkg/chainutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/services/uniswapv4bootstrap"
)

type config struct {
	dbURL     string
	rpcURL    string
	bootstrap uniswapv4bootstrap.Config
}

func parseConfig(args []string) (config, error) {
	fs := flag.NewFlagSet("uniswap-v4-position-bootstrap", flag.ContinueOnError)
	dbURL := fs.String("db", "", "PostgreSQL connection URL (default: DATABASE_URL)")
	rpcURL := fs.String("rpc-url", "", "Ethereum HTTP RPC endpoint (default: ALCHEMY_HTTP_URL + ALCHEMY_API_KEY)")
	chainID := fs.Int64("chain-id", 0, "Chain ID (default: CHAIN_ID, else 1)")
	fromBlock := fs.Int64("from", 0, "First block to scan (default: FROM_BLOCK, else the lowest pool deploy_block)")
	pinBlock := fs.Int64("pin", 0, "Block to snapshot at (default: PIN_BLOCK, else head minus the finality depth)")
	finalityDepth := fs.Int64("finality-depth", 0, fmt.Sprintf("Blocks below the head to pin at (default: FINALITY_DEPTH, else %d)", uniswapv4bootstrap.DefaultFinalityDepth))
	initialWindow := fs.Int64("initial-window", 0, fmt.Sprintf("Blocks per eth_getLogs window before adaptation (default: INITIAL_WINDOW, else %d)", uniswapv4bootstrap.DefaultInitialWindow))
	minWindow := fs.Int64("min-window", 0, fmt.Sprintf("Smallest window the bisect may narrow to (default: MIN_WINDOW, else %d)", uniswapv4bootstrap.DefaultMinWindow))
	maxWindow := fs.Int64("max-window", 0, fmt.Sprintf("Largest window the growth may widen to (default: MAX_WINDOW, else %d)", uniswapv4bootstrap.DefaultMaxWindow))
	positionBatch := fs.Int("position-batch", 0, fmt.Sprintf("Positions per write transaction (default: POSITION_BATCH, else %d)", uniswapv4bootstrap.DefaultPositionBatch))
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}

	cfg := config{
		dbURL:  *dbURL,
		rpcURL: *rpcURL,
		bootstrap: uniswapv4bootstrap.Config{
			ChainID:       *chainID,
			FromBlock:     *fromBlock,
			PinBlock:      *pinBlock,
			FinalityDepth: *finalityDepth,
			InitialWindow: *initialWindow,
			MinWindow:     *minWindow,
			MaxWindow:     *maxWindow,
			PositionBatch: *positionBatch,
		},
	}
	if err := cfg.applyEnvFallbacks(); err != nil {
		return config{}, err
	}
	if err := cfg.bootstrap.Validate(); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func (c *config) applyEnvFallbacks() error {
	if c.dbURL == "" {
		c.dbURL = env.Get("DATABASE_URL", "")
	}
	if c.dbURL == "" {
		return fmt.Errorf("database URL not provided (use -db or DATABASE_URL)")
	}

	positionBatch := int64(c.bootstrap.PositionBatch)
	if err := errors.Join(
		fillInt64FromEnv(&c.bootstrap.ChainID, "CHAIN_ID", 1),
		fillInt64FromEnv(&c.bootstrap.FromBlock, "FROM_BLOCK", 0),
		fillInt64FromEnv(&c.bootstrap.PinBlock, "PIN_BLOCK", 0),
		fillInt64FromEnv(&c.bootstrap.FinalityDepth, "FINALITY_DEPTH", 0),
		fillInt64FromEnv(&c.bootstrap.InitialWindow, "INITIAL_WINDOW", 0),
		fillInt64FromEnv(&c.bootstrap.MinWindow, "MIN_WINDOW", 0),
		fillInt64FromEnv(&c.bootstrap.MaxWindow, "MAX_WINDOW", 0),
		fillInt64FromEnv(&positionBatch, "POSITION_BATCH", 0),
	); err != nil {
		return err
	}
	c.bootstrap.PositionBatch = int(positionBatch)

	if c.rpcURL == "" {
		url, err := chainutil.AlchemyRPCURL(c.bootstrap.ChainID)
		if err != nil {
			return fmt.Errorf("RPC endpoint not provided (use -rpc-url, or set ALCHEMY_API_KEY): %w", err)
		}
		c.rpcURL = url
	}
	return nil
}

// A flag always wins over the environment.
func fillInt64FromEnv(into *int64, key string, fallback int64) error {
	if *into != 0 {
		return nil
	}
	value, err := env.GetInt64(key, fallback)
	if err != nil {
		return err
	}
	*into = value
	return nil
}

package main

import (
	"errors"
	"flag"
	"fmt"
	"strconv"
	"strings"

	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/services/uniswapv4bootstrap"
)

// defaultAlchemyHTTPURL is the mainnet endpoint the API key is appended to when
// ALCHEMY_HTTP_URL is unset, matching the dex indexer's own default.
const defaultAlchemyHTTPURL = "https://eth-mainnet.g.alchemy.com/v2"

type config struct {
	dbURL     string
	rpcURL    string
	bootstrap uniswapv4bootstrap.Config
}

// parseConfig resolves the run's settings from flags first, then the
// environment. Every knob has a flag so a one-off rerun can narrow the range
// without touching the deployed environment.
func parseConfig(args []string) (config, error) {
	fs := flag.NewFlagSet("uniswap-v4-position-bootstrap", flag.ContinueOnError)
	dbURL := fs.String("db", "", "PostgreSQL connection URL (default: DATABASE_URL)")
	rpcURL := fs.String("rpc-url", "", "Ethereum HTTP RPC endpoint (default: ALCHEMY_HTTP_URL + ALCHEMY_API_KEY)")
	chainID := fs.Int64("chain-id", 0, "Chain ID (default: CHAIN_ID, else 1)")
	fromBlock := fs.Int64("from", 0, "First block to scan (default: FROM_BLOCK, else the lowest pool deploy_block)")
	pinBlock := fs.Int64("pin", 0, "Block to snapshot at (default: PIN_BLOCK, else head minus the finality depth)")
	finalityDepth := fs.Int64("finality-depth", 0, fmt.Sprintf("Blocks below the head to pin at (default %d)", uniswapv4bootstrap.DefaultFinalityDepth))
	initialWindow := fs.Int64("initial-window", 0, fmt.Sprintf("Blocks per eth_getLogs window before adaptation (default %d)", uniswapv4bootstrap.DefaultInitialWindow))
	minWindow := fs.Int64("min-window", 0, fmt.Sprintf("Smallest window the bisect may narrow to (default %d)", uniswapv4bootstrap.DefaultMinWindow))
	maxWindow := fs.Int64("max-window", 0, fmt.Sprintf("Largest window the growth may widen to (default %d)", uniswapv4bootstrap.DefaultMaxWindow))
	positionBatch := fs.Int("position-batch", 0, fmt.Sprintf("Positions per multicall and per write transaction (default %d)", uniswapv4bootstrap.DefaultPositionBatch))
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

	if c.rpcURL == "" {
		url, err := alchemyURLFromEnv()
		if err != nil {
			return err
		}
		c.rpcURL = url
	}

	return errors.Join(
		fillInt64FromEnv(&c.bootstrap.ChainID, "CHAIN_ID", 1),
		fillInt64FromEnv(&c.bootstrap.FromBlock, "FROM_BLOCK", 0),
		fillInt64FromEnv(&c.bootstrap.PinBlock, "PIN_BLOCK", 0),
	)
}

// fillInt64FromEnv leaves an already-set flag value alone, so a flag always
// wins over the environment.
func fillInt64FromEnv(into *int64, key string, fallback int64) error {
	if *into != 0 {
		return nil
	}
	value, err := envInt64(key, fallback)
	if err != nil {
		return err
	}
	*into = value
	return nil
}

// alchemyURLFromEnv composes the endpoint the way every Alchemy-backed worker
// does. The trailing slash is trimmed so a configured base ending in "/" does
// not produce a "//" before the key.
func alchemyURLFromEnv() (string, error) {
	apiKey := env.Get("ALCHEMY_API_KEY", "")
	if apiKey == "" {
		return "", fmt.Errorf("RPC endpoint not provided (use -rpc-url, or set ALCHEMY_API_KEY)")
	}
	base := env.Get("ALCHEMY_HTTP_URL", "")
	if base == "" {
		base = defaultAlchemyHTTPURL
	}
	return strings.TrimRight(base, "/") + "/" + apiKey, nil
}

func envInt64(key string, fallback int64) (int64, error) {
	raw := env.Get(key, "")
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing %s %q: %w", key, raw, err)
	}
	return value, nil
}

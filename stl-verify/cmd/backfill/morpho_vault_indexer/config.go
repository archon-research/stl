package main

import (
	"flag"
	"fmt"

	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
)

type config struct {
	from       int64
	to         int64
	bucket     string
	dbURL      string
	rpcURL     string
	chainID    int64
	goroutines int
	probeBatch int
}

func parseConfig(args []string) (config, error) {
	fs := flag.NewFlagSet("morpho-vault-backfill", flag.ContinueOnError)
	from := fs.Int64("from", 0, "Start block number (inclusive)")
	to := fs.Int64("to", 0, "End block number (inclusive)")
	bucket := fs.String("bucket", "", "S3 bucket name")
	dbURL := fs.String("db", "", "PostgreSQL connection URL")
	rpcURL := fs.String("rpc-url", "", "Ethereum HTTP RPC endpoint")
	chainID := fs.Int64("chain-id", 1, "Chain ID")
	goroutines := fs.Int("goroutines", 16, "Number of concurrent partition workers")
	probeBatch := fs.Int("probe-batch", 50, "Number of candidates to probe per multicall batch")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}

	cfg := config{
		from:       *from,
		to:         *to,
		bucket:     *bucket,
		dbURL:      *dbURL,
		rpcURL:     *rpcURL,
		chainID:    *chainID,
		goroutines: *goroutines,
		probeBatch: *probeBatch,
	}

	if cfg.bucket == "" {
		cfg.bucket = env.Get("S3_BUCKET", "")
	}
	if cfg.bucket == "" {
		return config{}, fmt.Errorf("bucket not provided (use -bucket flag or S3_BUCKET env var)")
	}

	if cfg.dbURL == "" {
		cfg.dbURL = env.Get("DATABASE_URL", "")
	}
	if cfg.dbURL == "" {
		return config{}, fmt.Errorf("database URL not provided (use -db flag or DATABASE_URL env var)")
	}

	if cfg.rpcURL == "" {
		cfg.rpcURL = env.Get("RPC_URL", "")
	}
	if cfg.rpcURL == "" {
		return config{}, fmt.Errorf("RPC URL not provided (use -rpc-url flag or RPC_URL env var)")
	}

	if cfg.from <= 0 {
		return config{}, fmt.Errorf("-from must be a positive block number")
	}
	if cfg.to <= 0 {
		return config{}, fmt.Errorf("-to must be a positive block number")
	}
	if cfg.from > cfg.to {
		return config{}, fmt.Errorf("-from (%d) must be <= -to (%d)", cfg.from, cfg.to)
	}
	if cfg.goroutines <= 0 {
		return config{}, fmt.Errorf("-goroutines must be positive")
	}
	if cfg.probeBatch <= 0 {
		return config{}, fmt.Errorf("-probe-batch must be positive")
	}

	return cfg, nil
}

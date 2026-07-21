package main

import (
	"flag"
	"fmt"

	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/services/morpho_indexer"
)

type config struct {
	from               int64
	to                 int64
	bucket             string
	dbURL              string
	rpcURL             string
	chainID            int64
	goroutines         int
	probeBatch         int
	replayProgressFile string
}

func parseConfig(args []string) (config, error) {
	fs := flag.NewFlagSet("morpho-vault-backfill", flag.ContinueOnError)
	from := fs.Int64("from", 0, "Start block number (inclusive). Optional when -from-v2-deploy is set; an explicit -from always wins.")
	to := fs.Int64("to", 0, "End block number (inclusive)")
	fromV2Deploy := fs.Bool("from-v2-deploy", false, "Default -from to the VaultV2 factory deploy block for the chain. Ignored when -from is given explicitly.")
	bucket := fs.String("bucket", "", "S3 bucket name")
	dbURL := fs.String("db", "", "PostgreSQL connection URL")
	rpcURL := fs.String("rpc-url", "", "Ethereum HTTP RPC endpoint")
	chainID := fs.Int64("chain-id", 1, "Chain ID")
	goroutines := fs.Int("goroutines", 16, "Number of concurrent partition workers")
	probeBatch := fs.Int("probe-batch", 50, "Number of candidates to probe per multicall batch")
	replayProgressFile := fs.String("replay-progress-file", "", "Append-only JSONL checkpoint file bounding V2 structured-event replay cost across reruns (empty = no checkpointing)")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}

	cfg := config{
		from:               *from,
		to:                 *to,
		bucket:             *bucket,
		dbURL:              *dbURL,
		rpcURL:             *rpcURL,
		chainID:            *chainID,
		goroutines:         *goroutines,
		probeBatch:         *probeBatch,
		replayProgressFile: *replayProgressFile,
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

	if cfg.chainID <= 0 {
		return config{}, fmt.Errorf("-chain-id must be positive")
	}

	// An explicit -from always wins; -from-v2-deploy only fills an unset -from.
	if *fromV2Deploy && cfg.from <= 0 {
		deployBlock, err := morpho_indexer.VaultV2FactoryDeployBlock(cfg.chainID)
		if err != nil {
			return config{}, fmt.Errorf("-from-v2-deploy: %w", err)
		}
		cfg.from = deployBlock
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
